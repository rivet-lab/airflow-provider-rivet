# DAG builders

`build_batch_dag` and `build_cdc_dag` build whole DAGs from the operators.

## DAG builders

### Batch: `build_batch_dag`

```python
from airflow_provider_rivet.dags import build_batch_dag

dag = build_batch_dag(
    "rivet_postgres",
    config="/opt/pipelines/postgres.yaml",
    state_dir="/var/lib/rivet/postgres",
    plan_file="/opt/pipelines/postgres.plan.json",  # or exports=["users", "orders"]
    compact_exports=["orders"],
    operator_kwargs={"env_from_connections": {"RIVET_PG_URL": "rivet_postgres"}},
)
```

```
plan ──> apply: [a] [b] ─> wave_2_done ─> [big] ─> after_big ─> [huge]
  │              │   │                     │                     │
  └─> (every     │   │                     │                     │
      apply)    [a] [b]            load: [big]                 [huge]
         load:                             │
      compact:                           [big]          every task ─> watcher
```

- Three TaskGroups, `apply`, `load`, `compact`. They are the picture; **dependencies run per
  table**: `apply.X >> load.X >> compact.X`, all `all_success`. A failed `apply.a` leaves
  `apply.b`, `load.b` and every later wave runnable; it blocks `load.a` and `compact.a`.
- Waves come from the plan file, as in the recipe, and each wave is ordered the way `rivet apply`'s
  cost gate orders it: every heavy export alone, one after another, then the cheap exports of the
  wave together; wave N+1 starts after wave N. Ordering goes through tasks that
  do nothing, `apply.wave_<n>_done` and `apply.after_<export>`, with `trigger_rule="all_done"`:
  the next export waits for the earlier one to *finish*, not to succeed.
- Every rivet task is `trigger_rule="all_success"` whatever `default_args` says; a `trigger_rule`
  in `operator_kwargs` is refused, and so is `max_active_tis_per_dag` (it is 1 on every rivet task, see
  [limits](limits.md#overlap)). `default_args` you pass are merged over the builders'
  (`retries=2`, `retry_delay` 5 minutes), so adding a callback keeps the retries.
- `exports`, `compact_exports` and `tables` are lists; a bare string is refused. A name that is
  not an ASCII task id (`daily orders`, `orders:v2`, `заказы`) gets a task id with each character
  outside `[A-Za-z0-9_.-]` replaced by `_` and an 8-character hash of the name (`apply.daily_orders_<hash>`); rivet still
  receives the real name.
- `plan` is a direct upstream of every `apply` task: a failed plan refresh blocks the whole
  run (`upstream_failed`), because a run that could not plan has nothing it should extract.
- **A run with a failed task is a failed run.** Airflow colours a run by its leaf tasks, and an
  ordering task that succeeds after a failure would otherwise leave the run green. Every DAG the
  builders make ends in one task, `watcher` (`trigger_rule="one_failed"`, no retries), downstream
  of every other task: it is skipped when nothing failed and fails when anything did (the
  watcher pattern of Airflow's documentation).

| What fails | Its own table | Other tables | Run |
|---|---|---|---|
| `plan` | every `apply`, `load`, `compact`: `upstream_failed`; nothing reaches rivet | the same | failed |
| `apply.X` | `load.X`, `compact.X`: `upstream_failed` | run; the next of a heavy chain and the next wave still start | failed |
| `load.X` | `compact.X`: `upstream_failed` | run | failed |
| `compact.X` | | run | failed |
| nothing | | | success (`watcher` skipped) |
- `plan_file` is read at parse time (no database access); without one, pass `exports=[...]`.
- With `plan_file`, a `plan` task rewrites that file from `rivet plan` at the start of every run;
  `refresh_plan=False` drops the task and keeps the file as it is.
- `extract="run"` uses `rivet run -e X` instead of plan + apply.
- **Compact tasks are created only for the exports named in `compact_exports`.** Whether a table
  compacts depends on the load mode, `load.layout` and the warehouse, which only rivet resolves
  (`full` never compacts; see ADR-0039 D3 for the skip reasons). A named export that rivet skips
  shows as an Airflow skip with its `skip_reason` in XCom; an export left out of `compact_exports`
  has no compact task to show it.
- `load=False` drops the load group (a config with no `load:` block).

### CDC: `build_cdc_dag`

```python
from airflow_provider_rivet.dags import build_cdc_dag

dag = build_cdc_dag(
    "rivet_orders_cdc",
    config="/opt/pipelines/cdc.yaml",
    export="app_cdc",
    tables=["orders", "users"],
    state_dir="/var/lib/rivet/cdc",
)
```

- `run` is ONE task for the stream. `load.T` and `compact.T` are separate tasks per table.
- `tables` are written as the config names them: `dbo.orders` is loaded and compacted as the
  warehouse table `dbo_orders` (the schema's dot folded to `_`, as rivet names it).
- `run >> load.T >> compact.T`, all `all_success`: **nothing is loaded or compacted after a
  drain that did not succeed** (a refusal such as a log gap means the files of this cycle are
  not the stream), and a failed `load.T` blocks `compact.T` but not another table. The parts of
  an earlier cycle whose load failed are loaded by the next run whose drain succeeds: the load
  consumes what rivet's ledger says is unloaded. A load that finds nothing to do is a success
  (never an Airflow skip: a skip would propagate to `compact.T`).
- The same `watcher` task ends the DAG: a failed `run`, `load.T` or `compact.T` fails the run.
- On `stop_reason: max_events` the run task succeeds, puts it in XCom and logs a `note` line
  `stopped at max_events (...)` (record `rivet.cdc.max_events`). It does not loop.
- `load=False` drops the load and compact tasks (the stream's files are the product);
  `compact=False` keeps `load.T` and drops `compact.T`. Use `compact=False` on Snowflake and
  ClickHouse, where there is nothing to compact (see ["Storage and warehouse connections"](connections-and-secrets.md#storage-and-warehouse-connections)).
- `operator_kwargs` go to every task, as in the batch builder.

### One DAG per database: `build_source_dags`

Name the databases; each gets a DAG, created paused, that writes its own config the first time it runs. The operator's
work is to unpause it.

```python
from datetime import datetime

from airflow_provider_rivet.dags import build_source_dags

globals().update(
    build_source_dags(
        ["shop", "billing"],  # Airflow Connection ids, else data sources of odbc.ini
        mode="cdc",  # or "batch"
        config_dir="/opt/airflow/dags/rivet",
        state_root="/opt/airflow/rivet-state",
        state_connection="rivet_state",  # a PostgreSQL server; each source gets the database rivet_state_<name>
        init_args=[
            "--tls",
            "verify-full",
            "--gcs-bucket",
            "my-bucket",
            "--bigquery-project",
            "my-project",
            "--bigquery-dataset",
            "{name}",
        ],
        operator_kwargs={"cloud_credentials": ["gcp"]},
        schedule="0 */2 * * *",
        start_date=datetime(2026, 1, 1),
        catchup=False,
    )
)
```

For each name `<name>`:

| | |
|---|---|
| DAG | `rivet_<name>_cdc` / `rivet_<name>_batch`, paused at creation (`is_paused_upon_creation`) |
| source | the Airflow Connection `source_connection` (default `{name}`; `rivet_{name}` for the layout of [the cron recipe](cron-to-airflow-cdc.md)); without one, the data source `[<name>]` of `odbc_ini` (default `/etc/odbc.ini`; `None` turns it off). The task log says when odbc.ini was used, and the password of the data source is masked in it. The file is read as unixODBC reads it (the name in any case, the first section and the first value win, `;` and `#` comments); a file that is missing or unreadable on the worker is refused with that cause, not as a missing data source. odbc.ini: MySQL, PostgreSQL, SQL Server (`Server = host,port`, a SQL login) and Oracle (`ServerName = host:port/service`) drivers; MongoDB only through a Connection |
| config | `<config_dir>/<name>.yaml` |
| state | the database `rivet_state_<name>` on the server of `state_connection`, and the folder `<state_root>/<name>` |

**Before the config exists**, the DAG has one task, `init`. When it runs:

1. it creates `rivet_state_<name>` if missing (the state Connection's user needs `CREATEDB`) and `<state_root>/<name>`
   (`state_root` must exist: a missing one is a volume that is not mounted, and is refused);
2. runs `rivet init --source-env RIVET_SOURCE_URL [--mode cdc] -o <state_root>/<name>/.init/<name>.yaml <init_args>`,
   `{name}` in `init_args` replaced (`--source`, `--source-env`, `--source-file`, `-o`/`--output` and, in a CDC DAG,
   `--mode` are the operator's: refused in any spelling, `--output=x` and `-oX` too; `--mode cdc` in a batch DAG is
   refused, a value that reads `cdc` is not). `init` names the stream's `server_id`, slot, checkpoint and bucket prefix after the
   file name, so two sources never share them;
3. runs `rivet check` on it;
4. refuses a config that writes to a bucket prefix or a warehouse dataset another config in `config_dir` writes to
   (give each source its own, e.g. `--bigquery-dataset {name}`);
5. writes `<config_dir>/<name>.yaml`, never over an existing file, and logs each export and its tables.

A failed step writes no config and is not retried: read the log, fix the cause, and clear the task in the latest run
only (an `init` cleared in several runs at once fails in each run that starts after the first one wrote the config,
since the DAG has no `init` task then). The bucket and the
warehouse dataset are not created: they must exist.

**Once the config exists**, the next parse builds the same DAG from it: for `cdc`, each `mode: cdc` export of the
config as a stream (`run`, then a load per table when the config has a `load:` block and a compaction of each table
whose layout rivet resolves to a base and a buffer); for `batch`, every export through `build_batch_dag`, with a load
when it has `load:` and a compaction of each export whose layout rivet resolves the same way. The layout is read as
rivet resolves it (ADR-0034 D7), from the config alone: BigQuery (the one warehouse that compacts), a load that
accumulates (`incremental`, `cdc`; never `full`, and an export without `mode:` is `full`), and `layout: base_buffer`
written at `load:`, over it at the export's `load:`, over it at the captured table's `load.tables.<t>`; with no layout
written, a stream with `cdc.backfill` is a base and a buffer, anything else a change log with no compaction.
`rivet init --mode cdc` writes one stream over all tables on MySQL and PostgreSQL's `public`, which keeps the
`build_cdc_dag` shape (`run`, `load.T`, `compact.T`), and one stream per table on SQL Server, Oracle, MongoDB and other
PostgreSQL schemas: each stream is then a group named after its export (`dbo_orders.run`, `dbo_orders.load.dbo_orders`: a load takes the warehouse name, the schema's dot folded to `_`),
the streams running side by side. A batch DAG takes init's suggested mode per table (`full` for a small one), or the one `init_args` names
(`--mode incremental`). One name is one config and one state database: a source in both a CDC and a batch DAG needs
two names, or a `config_dir` and `state_root` per mode, and a batch DAG over a config that has a `mode: cdc` export is
refused (below). Oracle CDC signs in as a common user (`c##…`) that LogMiner
lets mine, with `--schema` naming the tables' owner. `init_args` are one list for every name of a call: sources that need other flags
(`--tls require` for SQL Server, `--schema`) go in a call of their own. So
unpausing runs `init` at once, and the stream starts at the next scheduled run (or trigger one).

- The config folder must be one the scheduler (Airflow 3: the dag-processor) and every worker read, as the DAGs
  folder. If `init` keeps running, the file it wrote is not where the scheduler reads.
- Edit the config like any other: the DAG follows it at the next parse. Delete it to have `init` write it again.
- A config the builder cannot build a DAG from (not YAML, a wrong shape, no `mode: cdc` export for a CDC DAG, a
  `mode: cdc` export under a batch DAG, no export at all) never ends the DAG file: that source's DAG keeps its id and
  has one task, `config`, which fails without retry naming the cause (`RIVET_AIRFLOW_CONFIG_UNREADABLE` with the line
  and column of a YAML error, `..._NO_CDC_EXPORT`, `..._HAS_CDC_EXPORT`, `..._NO_EXPORT`, `..._UNBUILDABLE`) and the
  remedy; the other sources of the call are built as they are. Fix the config (or delete it for `init`) and the next
  parse builds the real DAG. A wrong argument of the call itself (a bad name, `operator_kwargs`, a DAG setting) is still
  an import error, as it is the same for every source.
- An export whose id in the `apply` group is the id of an ordering point of a plan (`after_<heavy export>`,
  `wave_<n>_done`) keeps its id; the ordering point gets `_barrier` appended.
- A config moved from cron (`docs/cron-to-airflow-cdc.md`) works as it is: the source goes to the variable its
  `url_env` names.
- `operator_kwargs` go to every task, `init` included; `env_from_connections`, `env_from_variables`, `config` and
  `state_dir` are the builder's.
- `env_from_variables` (`{ENV_NAME: "<variable key>"}`, `{name}` replaced in the key) fills an environment variable from an
  Airflow Variable per source, read when each task runs and never at parse. For a config with
  `url_secret: ${RIVET_SOURCE_URL_SECRET}`: `env_from_variables={"RIVET_SOURCE_URL_SECRET": "rivet_{name}_url_secret"}`,
  one Variable `rivet_<name>_url_secret` per source. A missing Variable fails that source's tasks with
  `RIVET_AIRFLOW_VARIABLE_MISSING`, before rivet starts, and no other source. The walk-through, the IAM grant and the
  comparison with Airflow's Secret Manager backend are in
  [Connections and secrets](connections-and-secrets.md#one-variable-per-source-the-secret-name-at-task-run-time).
- The DAG file reads only files: no database is reached while the scheduler parses it.
