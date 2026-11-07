# DAGs from one file

Every source database of a deployment, one DAG each, from one YAML file and a one-line DAG file. Adding a source is
one line in the YAML; no Python is edited. Needs `airflow-provider-rivet` **0.4.0** or later (see
[Versions](#versions)).

## The two files

`dags/rivet_sources.yaml`:

```yaml
# yaml-language-server: $schema=https://raw.githubusercontent.com/rivet-lab/airflow-provider-rivet/main/airflow_provider_rivet/schemas/rivet-sources.schema.json
version: 1
config_dir: rivet                      # dags/rivet/<name>.yaml, beside this file
state_root: /opt/airflow/rivet-state   # a volume every Airflow service mounts at this path
state_connection: rivet_state          # a PostgreSQL server; each source's state is the database rivet_state_<name>
source_connection: "rivet_{name}"      # the Airflow Connection of each source
template: shop                         # the sources with no config yet take shop's bucket, project and TLS mode

defaults:
  bigquery_dataset: "{name}"
  schedule: "0 */2 * * *"
  start_date: "2026-10-01"
  cloud_credentials: [gcp]

sources:
  shop:
    gcs_bucket: my-bucket
    bigquery_project: my-project
    tls: verify-full
  billing:
    exclude: ["*_tmp", "audit_log"]
  analytics:
    include: [pageviews, visitors]
```

`dags/rivet_sources.py`:

```python
from pathlib import Path

from airflow_provider_rivet.dags import build_dags_from_file

globals().update(build_dags_from_file(Path(__file__).with_name("rivet_sources.yaml")))
```

The path is absolute: a relative one is refused, naming the form above. Both files are in
[`examples/`](../examples/rivet_sources.yaml).

Each source is built the way [`build_source_dags`](dag-builders.md#one-dag-per-database-build_source_dags) builds it:
the DAG `rivet_<name>_cdc` (or `_batch`), paused at creation. Its first run's one task, `init`, writes
`<config_dir>/<name>.yaml` with `rivet init`; the next parse builds the stream from that config. The DAG ids, task ids,
config paths and state folders are the ones `build_source_dags` gives, so moving from a hand-written loop over
`build_source_dags` to this file changes no id.

## The file

| Key | | Default |
|---|---|---|
| `version` | `1` | required |
| `mode` | `cdc` or `batch` | `cdc` |
| `config_dir` | the folder of the configs `<name>.yaml`; relative to the file | `rivet` |
| `state_root` | the parent of each source's state folder `<state_root>/<name>` | required |
| `state_connection` | the Airflow Connection (postgres) of the state server | required |
| `source_connection` | each source's Airflow Connection, `{name}` replaced | `{name}` |
| `odbc_ini` | the odbc.ini whose data source `[<name>]` is used when there is no Connection; `null` turns it off | `/etc/odbc.ini` |
| `template` | a source of this file whose config gives its GCS bucket, BigQuery project and TLS mode to the sources with no config yet | none |
| `defaults` | settings of every source | none |
| `sources` | each source by name (`[a-z0-9_]`), with its own settings, or nothing | required |

Settings (in `defaults` and under a source; a source's own win, and `defaults` win over the template's):

| Setting | What it does |
|---|---|
| `include`, `exclude` | `rivet init --include` / `--exclude`: table globs (`*` any run, `?` one character, the whole name, case-sensitive; exclude wins). Quote a glob that starts with `*`: `"*_tmp"`. Compared with the config once it exists ([below](#changing-include-and-exclude-after-the-config-exists)) |
| `tls`, `tls_ca` | `rivet init --tls` (`disable`, `require`, `verify-ca`, `verify-full`) and `--tls-ca` |
| `schema` | `rivet init --schema` |
| `gcs_bucket`, `bigquery_project`, `bigquery_dataset` | `rivet init --gcs-bucket`, `--bigquery-project`, `--bigquery-dataset`. Give each source its own dataset (`"{name}"`, `"rivet_{name}"`) |
| `init_args` | any other `rivet init` arguments, as written; a flag a setting above sets is refused here |
| `state_database` | the source's state database on the state server; `connection` uses the database the state Connection names itself | `rivet_state_{name}` |
| `schedule`, `start_date` (`YYYY-MM-DD`, in the past), `catchup`, `tags` | the DAG's own |
| `cloud_credentials` | `[gcp]`, `[aws]`, `[azure]`: the worker's credentials rivet receives |
| `env_from_variables` | `{ENV_NAME: "<variable key>"}`, `{name}` replaced in the key ([Connections and secrets](connections-and-secrets.md#one-variable-per-source-the-secret-name-at-task-run-time)) |
| `preflight` | `false` leaves out the `preflight` task that runs first in the DAG ([below](#before-the-first-run)); default `true` |
| `stream_stderr` | `false` stops copying rivet's stderr lines into the task log as they come; default `true` |

`{name}` is the one placeholder, in any text value. Callbacks and other Python values are given to every source as
arguments: `build_dags_from_file(path, operator_kwargs={"rivet_bin": ...}, on_failure_callback=...)`;
what the file says wins over them.

**`template`.** A new source usually writes to the same bucket and project, and reaches its server the same way, as a
source that already runs. `template: shop` reads `dags/rivet/shop.yaml` when the DAG file is parsed and
takes its GCS bucket (`exports[].destination.bucket`), BigQuery project (`load.project`) and TLS mode
(`source.tls.mode`). While the template has no config yet, a source with no config of its own is refused
(`RIVET_AIRFLOW_SOURCES_FILE_REFUSED`, "run the DAG of `shop` first"); name the values in `defaults` instead to
start every source at once.

## Errors

The file is read strictly when the DAG file is parsed. A file it cannot read is an import error (`airflow dags
list-import-errors`) that names every place at once, as `file:line:column`:

```
/opt/airflow/dags/rivet_sources.yaml:14:15: `*_tmp` starts with `*`, which YAML reads as an alias, not text: quote the glob, "*_tmp"
/opt/airflow/dags/rivet_sources.yaml:9:3: unknown key `dataset`; did you mean `bigquery_dataset`?
/opt/airflow/dags/rivet_sources.yaml:11:8: `defaults.tls`: `off` is not one of disable, require, verify-ca, verify-full
/opt/airflow/dags/rivet_sources.yaml:8:21: `{dsn}` is no placeholder of this file; the one placeholder is `{name}`, the source's name
```

It also refuses what text pasted from a messenger or a terminal leaves behind: typographic quotes (`“ ”`), no-break and
zero-width spaces, tabs in the indentation, a key written twice, and lines that look like shell commands (`EOF`,
`docker compose ...`, `cat <<EOF`): a heredoc whose end marker was lost writes the commands after it into the file.

A source the builder cannot build (its template has no config yet, an argument `build_source_dags` refuses) is a DAG of
one task, `config`, that fails naming why (`RIVET_AIRFLOW_SOURCES_FILE_REFUSED`); the other sources are built as they
are. A config that cannot be built is refused as in `build_source_dags` (`RIVET_AIRFLOW_CONFIG_*`).

**Copying the file onto the server.** Copy it as a file (`scp`, the editor of a mounted folder) rather than through a
chat. With a heredoc, quote the marker so the shell expands nothing, and check that the marker line is alone:

```bash
cat > dags/rivet_sources.yaml <<'EOF'
...
EOF
```

**Your editor.** The first line of the example makes VS Code (the YAML extension) and JetBrains IDEs check the file as
you type. The schema is also in the package: `airflow_provider_rivet/schemas/rivet-sources.schema.json`.

## Changing include and exclude after the config exists

`include` and `exclude` are applied by `rivet init`, which runs once, when the config does not exist. A table lives
in several places of the config it wrote (the stream's `tables:` or an export per table, and `load.tables.<table>`),
and the DAG's load and compact tasks come from the config. So once the config exists, the builder compares it with
the file at every parse. When the config captures a table the file leaves out, or does not capture a table an include
names literally (a glob that may match a table the source has cannot be judged without the source), the DAG gets two
more tasks before the stream:

```
spec_drift ──> spec_checked ──> run ──> load.T ──> compact.T
```

When `spec_drift` runs:

- **The stream has not started**: no checkpoint file in the state folder and no run in rivet's run ledger
  (`rivet state runs --json`). It moves the config to `<config_dir>/.replaced/<name>.<UTC time>.yaml`, writes it
  again with `rivet init` and the file's include and exclude (`rivet check` first, as `init` does), and skips the
  rest of the run. The next parse builds the DAG of the new config, and its next run starts the stream. If the new
  init fails, the old config is put back.
- **The stream has started** (or the ledger cannot be read): nothing is written. The task fails without a retry,
  naming each extra table and where the config writes it, and the run is failed by the watcher; `spec_checked` still
  runs, so the stream and its loads go on. Edit the config at the places named (`rivet check` it; the DAG follows at
  the next parse), or give the file the tables the config captures. Whether a stream accepts a changed table list
  is rivet's decision.

The two tasks disappear at the next parse once the config and the file agree. A filter of the DAG's tasks alone
would be wrong: `rivet run` captures every table of the config, so the files of a table without a load task would pile
up unloaded.

## Moving from `build_cdc_dag`

A DAG built per source with `build_cdc_dag` ([`examples/rivet_cdc_per_source.py`](../examples/rivet_cdc_per_source.py), deprecated,
the layout of [the cron recipe](cron-to-airflow-cdc.md)) keeps its DAG id, its state folder and its task ids when the
same source is built from the file: `rivet_<src>_cdc`, `<state_root>/<src>`, `run`, `load.<table>`,
`compact.<table>` (a test holds this). What changes is where the state database's name comes from:

| | `build_cdc_dag` | the file (`build_source_dags`) |
|---|---|---|
| state database | the one the Connection names (`--conn-schema`) | `rivet_state_<name>` on the Connection's server, unless `state_database` says otherwise |
| state Connection | one per source (`rivet_state_<src>`) | one for every source (`state_connection`) |

If a source's Connection names another database than `rivet_state_<name>`, the moved stream would open an empty state
database. A `run` task refuses that before rivet starts: a state database it names in place of its Connection's own
that holds no table while the stream's checkpoint exists is `RIVET_AIRFLOW_STATE_DATABASE_MISMATCH`, which names both
databases (needs `psycopg2` on the worker; without it the check is skipped and the log says so).

The steps, per source, with the DAG paused:

1. Install `airflow-provider-rivet>=0.4.0` in the image (`pip show airflow-provider-rivet` in the worker).
2. Write the file: `config_dir` the folder of the existing configs, `state_root` the parent of the state folders,
   `source_connection: "rivet_{name}"`. For the state:
   - each source has its own Connection `rivet_state_<src>` whose database is `rivet_state_<src>` (the recipe's layout):
     `state_connection` is any one of them (the server is the same), and each source's database is its own name;
   - a source whose Connection names another database: `state_database: connection` under that source, with
     `state_connection` its own Connection, or copy the state into `rivet_state_<name>` first
     (`pg_dump <old> | psql rivet_state_<name>`).
3. Keep the configs as they are. A config moved from cron that says `url_env: RIVET_MYSQL_URL` gets the source under
   that variable.
4. Replace the old DAG file with the one-line one, then compare the tasks with the old ones:
   `airflow tasks list rivet_<src>_cdc` (`run`, `load.*`, `compact.*`, `watcher`). A `spec_drift` task means the file's
   include or exclude differ from the config: leave them out to keep the config as it is.
5. Unpause. The first run continues from the checkpoint in `<state_root>/<src>`: no new baseline.

The way back is the old DAG file: nothing in the state folder or database is changed by the move.

## Before the first run

Each DAG starts with a `preflight` task (`RivetPreflightOperator`): it checks what the next tasks need and writes one
line per check, PASS, WARN, SKIP or FAIL, with what to change. A FAIL fails it with no retry, and nothing else runs:

```
PASS rivet binary: rivet 0.33.0 (5ee08c0a) at /home/airflow/.local/bin/rivet
PASS config folder: /opt/airflow/dags/rivet is writable
PASS state folder: /opt/airflow/rivet-state is writable; /opt/airflow/rivet-state/shop is created there
FAIL state database tls: Connection `rivet_state` points at pg-state, a remote host, and names no sslmode; rivet refuses it (RIVET_STATE_URL_TLS_REQUIRED) -> fix: set the Connection's extra sslmode: `require` (or `verify-ca`, `verify-full`) with TLS on the state server, or `disable` for plaintext on a private network
PASS state database: `rivet_state_shop` on pg-state:5432 (Connection `rivet_state`) does not exist yet; `init` creates it (the role may CREATEDB)
FAIL mysql binlog_row_metadata: is MINIMAL -> fix: SET PERSIST binlog_row_metadata = 'FULL';
PASS mysql grants: REPLICATION SLAVE, REPLICATION CLIENT, SELECT
FAIL gcs gs://my-bucket/rivet/shop/: the bucket does not exist -> fix: gcloud storage buckets create gs://my-bucket --location=<the dataset's>
SKIP bigquery my-project.rivet_shop: google-cloud-bigquery is not installed -> fix: run by hand: bq show my-project:rivet_shop
```

It reaches each thing as the tasks will: the binary and its version, the config and state folders, the state
database (and whether rivet will dial it: a remote state server needs an `sslmode`), the source's CDC settings (MySQL: `log_bin`, `binlog_format`, `binlog_row_image`, `binlog_row_metadata`,
`log_replica_updates` on a replica, the grants, a free `server_id`; PostgreSQL: `wal_level`, `REPLICATION`, a free
slot), and the bucket and dataset until the stream has a checkpoint; once the config exists, `rivet check` of it. A
check whose client library is not in the image (psycopg2, MySQLdb or pymysql, google-cloud-storage,
google-cloud-bigquery) is a SKIP naming the command to run by hand. `preflight: false` leaves the task out.

- **The config folder must be writable by the worker.** `init` writes `<config_dir>/<name>.yaml`; it checks the folder
  before it creates anything, and a folder it cannot write fails the task naming the worker's uid and gid and the
  folder's owner and mode. On the host: `sudo chown -R <AIRFLOW_UID>:0 dags/rivet && sudo chmod -R g+w dags/rivet`
  (the official image runs as `AIRFLOW_UID`, 50000 by default, in group 0); a world-writable folder is not needed.
  The folders and the config a task creates are group-writable (`g+w`), never world-writable, so the scheduler, the
  dag-processor and a person in the group can all edit them.
- **The bucket and the dataset are not created for you.** Neither `rivet init` nor a task creates the GCS bucket or the
  BigQuery dataset the config names; `preflight` says when one is missing and the command that creates it. Create the
  dataset in the bucket's location (or a region inside its multi-region): a load reads a bucket in the dataset's
  location only. The state database is the one thing `init` creates, when the state Connection's role may.
- **MySQL CDC prerequisites are checked by `preflight`, and by rivet when the stream runs, not by `init`.** Without
  `preflight` (or without a MySQL driver in the image), check them by hand on the server the source Connection names
  (on a replica, the replica):

  ```sql
  SELECT @@log_bin, @@binlog_format, @@binlog_row_image, @@binlog_row_metadata, @@log_replica_updates;
  -- 1, ROW, FULL, FULL, and on a replica 1
  ```

  and the grants `REPLICATION SLAVE`, `REPLICATION CLIENT`, `SELECT`. A server that misses one fails the first `run`
  with `RIVET_SOURCE_CDC_PREREQUISITE` and rivet's remedy.

## What the task log says while it runs

Every task first says where it reads and writes:

```
state    database `rivet_state_shop` on pg-state:5432 (Connection `rivet_state`)
writes   bigquery my-project.rivet_shop, gcs gs://my-bucket/rivet/shop/
```

A first CDC run copies every table before it streams, which can take hours. While a rivet step runs, its stderr lines
come into the task log as rivet writes them (`stream_stderr`), and every minute (`progress_every`, seconds) a heartbeat
says how long it has run, how big its stderr is and when rivet last wrote; the first one says how to follow it:

```
rivet run running for 1m 0s; stderr 14.2 KiB, last written 3 s ago; follow it: tail -f /opt/airflow/rivet-state/shop/logs/...run.stderr.log
rivet run running for 2m 0s; stderr 31.0 KiB, last written 1 s ago
```

rivet writes a stderr line when a table's snapshot ends and nothing while it reads one, so during the snapshot of a big
table "last written" grows: the heartbeat says the step still runs, not how far it is (3,000,000 rows of MySQL took
3.5 minutes and came as one line at the end). The path is on the worker: from the host, run
`docker compose exec airflow-worker tail -f <path>`, or read it where the host mounts the state folder.

## Running it on Docker Compose

- **Two paths for one folder.** A config, a state folder and a log path are named as the containers see them
  (`/opt/airflow/dags/rivet/shop.yaml`); on the host the same file is where the compose file mounts it from
  (`./dags/rivet/shop.yaml`). Edit on the host and name the container path in the sources file and in Connections.
- **Two compose files and an env file.** With Airflow's `docker-compose.yaml` and your own override, name both, and the
  env file, on every command, or one service runs with other settings than the rest:
  `docker compose -f docker-compose.yaml -f docker-compose.override.yaml --env-file .env up -d --build`.
- **`up -d --build` after a change to the image** (a new `airflow-provider-rivet` or `rivet-cli` in the Dockerfile):
  without `--build` the services keep the old image. Check every service runs the same versions:
  `docker compose exec airflow-worker pip show airflow-provider-rivet rivet-cli` and the same in the scheduler and
  (Airflow 3) the dag-processor.
- **A grey task "waiting" (queued, no log) is no worker taking it**: the worker is not running, or listens to another
  queue. `docker compose ps` and `docker compose logs airflow-worker`.
- **The config is a working copy.** A task runs `<state_dir>/<hash>.yaml`, a copy of `<config_dir>/<name>.yaml` made
  when it starts. Edit `<config_dir>/<name>.yaml`; an edit of the hashed copy is lost on the next task.
- **Restarting a source** that never anchored (no checkpoint): delete its config, its state folder
  `<state_root>/<name>` and its state database, and trigger the DAG; `init` writes the config again. A stream that has
  run keeps its checkpoint: see [After a first run that failed](#after-a-first-run-that-failed) before deleting
  anything.

## Troubleshooting

| What you see | Why | What to do |
|---|---|---|
| The task log in the UI: `Could not read served logs: 403 Client Error: FORBIDDEN` | the webserver (Airflow 3: the api-server) signs its request for a log with its secret key, and the worker checks it with its own; they differ | give every service the same `AIRFLOW__WEBSERVER__SECRET_KEY` (Airflow 3: also `AIRFLOW__API_AUTH__JWT_SECRET`), e.g. once in `.env`, then `up -d` |
| `preflight` FAIL `config folder ... is not writable by this worker (uid 50000, gid 0 ...)` | the host folder belongs to your user | `sudo chown -R 50000:0 dags/rivet && sudo chmod -R g+w dags/rivet` |
| `preflight` FAIL `gcs ...: the bucket does not exist` or `bigquery ...: the dataset does not exist` | neither is created for you | the command on the line, in one location for both |
| The DAG of a source shows only `preflight` and `init` | its config does not exist yet | run it once; the next parse builds the stream's tasks from the config `init` wrote |
| `***` in the task log where a word should be (`***_state_shop`) | Airflow masks every Connection password wherever it appears, and this password is also a word of a name | give the Connection a password that is no word of its names |
| `init` below `watcher` in the grid, after `init` has written the config | the grid keeps the tasks of every version of the DAG; `init` is a task of version 1, built before the config existed | nothing: the current version has `preflight`, `run`, `load.*`, `compact.*` and `watcher` |
| A run that seems stuck | a first CDC run copies every table first | read the heartbeat lines, or `tail -f` the stderr path the first one names |
| `preflight` WARN `baseline: ... read in one SELECT each though rivet would page them` | the same one-`SELECT` baseline, found before `init` from `rivet init --discover` | after `init`, page the baseline: in `<config_dir>/<source>.yaml` replace `initial: snapshot` with `backfill: auto` and add a batch export of the table as its recipe (`mode: chunked`, `chunk_by_key: <key>`, `parallel: 4`), shaped as `init` writes one for two tables |
| `run` fails after about 5 minutes: `[RIVET_SOURCE_STATEMENT_TIMEOUT] mysql: statement timeout after 300s` | a source of one table gets a baseline of one `SELECT` of the whole table (two or more tables are paged), and the `balanced` profile stops a statement at 300 s; 3,000,000 rows of MySQL took 3.5 minutes | page the baseline as in the row above (rivet-lab/rivet#878: `init` writes the one `SELECT` for one table), or raise `tuning: { statement_timeout_s: 3600 }` under the export, which keeps one `SELECT` on one connection; then run again: a first run that failed reads the table again from the start |

## After a first run that failed

A first run that failed (a prerequisite, a grant, the bucket) may leave:

- **the state database `rivet_state_<name>`**, created by `init` (empty, or with the primary keys `rivet init`
  recorded). It is safe: the next `init` uses it as it is.
- **an empty `cdc/` folder in `<state_root>/<name>`**. Safe: rivet writes the checkpoint there once the stream has
  anchored.
- **a config**, when `init` succeeded and `run` failed: fix the cause and run again; delete the config to have `init`
  write it again.

rivet refuses to continue only when its state says the stream has run and the checkpoint is missing or empty
(`RIVET_SOURCE_CDC_CHECKPOINT_INVALID`): put the checkpoint back, and do not re-baseline as the message offers unless a
new baseline is what you want. A stream that never anchored has nothing to lose: delete the config, the state
folder and the state database to start over.

## Versions

| Function | Since |
|---|---|
| operators, `build_batch_dag`, `build_cdc_dag`, `slack_failure_callback` | 0.1.0 |
| `build_source_dags`, `RivetInitOperator`, `odbc_ini` | 0.3.0 |
| `build_config_dag`, `env_from_variables`, `url_secret` / `password_secret` configs | 0.4.0 |
| `build_dags_from_file`, the sources file, `build_source_dags(state_database=, include=, exclude=)`, `spec_drift` | 0.4.0 |

`ImportError: cannot import name 'build_source_dags'` is a package older than 0.3.0; `build_dags_from_file`, older than
0.4.0. Check with `pip show airflow-provider-rivet` in the worker and the scheduler (Airflow 3: the dag-processor).
