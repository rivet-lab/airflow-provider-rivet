"""DAG builders: TaskGroups are the picture, dependencies run per table, and a watcher fails the run on any failure."""

from __future__ import annotations

import hashlib
import inspect
import json
import re
from collections.abc import Mapping, Sequence
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any

from ._compat import DAG, TaskGroup
from .exports import compacts, entries_of, may_be_cdc, mode_of, name_of, tables_of
from .invocation import PLAN_ERRORS, plan_layout, warehouse_table
from .operators import (
    RivetApplyOperator,
    RivetBaseOperator,
    RivetCdcRunOperator,
    RivetCompactOperator,
    RivetConfigRefused,
    RivetInitOperator,
    RivetLoadOperator,
    RivetPlanOperator,
    RivetRunOperator,
    RivetRunWatcher,
    RivetWaveBarrier,
    _RivetTask,
    config_present,
)
from .preflight import ConfigRefusal, PreflightRefusal, read_config

ORDER_ONLY = "all_done"
WATCHER_ID = "watcher"
GATE = "all_success"
DEFAULT_ARGS = {"retries": 2, "retry_delay": timedelta(minutes=5)}
SOURCE_NAME = re.compile(r"[a-z0-9_]+")  # a file stem, a database name and a dag_id part as it is
SOURCE_ENV = "RIVET_SOURCE_URL"
# The parameters of a task that runs rivet's ADR-0039 commands which `init` has no use for (locks, extra_args, crash
# retries, ...): `operator_kwargs` goes to every task of a source's DAG, and `init` takes only its own.
RUN_ONLY = (
    frozenset(inspect.signature(RivetBaseOperator.__init__).parameters)
    - frozenset(inspect.signature(RivetInitOperator.__init__).parameters)
    - frozenset(inspect.signature(_RivetTask.__init__).parameters)
)
NOT_IN_TASK_ID = re.compile(r"[^A-Za-z0-9_.-]")  # ASCII only: Airflow 3 cannot run a non-ASCII task id


def task_name(name: str) -> str:
    """The task id part for an export or table name: itself when it is ASCII `[A-Za-z0-9_.-]`, else sanitised plus a hash (D29).

    The hash is of the real name, so `daily orders` and `daily_orders` never share an id; the real name stays in `export=`.
    """
    safe = NOT_IN_TASK_ID.sub("_", name)
    if safe == name and name:
        return name
    return f"{safe}_{hashlib.sha256(name.encode()).hexdigest()[:8]}"


def _names(field: str, value: Any) -> list[str]:
    """A list of export or table names; a bare string would be split into one task per character (D28)."""
    if isinstance(value, str | bytes):
        raise ValueError(f"`{field}` is a list of names, not a string: write {field}=[{value!r}]")
    return list(value)


#: what a builder sets on every rivet task for the run's sake; `operator_kwargs` may not say otherwise
OWNED_BY_BUILDERS = {
    "trigger_rule": (
        "every rivet task runs only after its upstream succeeded "
        "(load after its extract, compact after its load); the watcher fails the run on any failure"
    ),
    "max_active_tis_per_dag": (
        "it is 1 on every rivet task, so a task never runs beside another try of itself (docs/limits.md, Overlap)"
    ),
}


def _refuse_owned_kwargs(operator_kwargs: Mapping[str, Any] | None) -> None:
    """Every builder's tasks run only after their upstream succeeded and one try at a time: `operator_kwargs` may not
    say otherwise (D24)."""
    for key, why in OWNED_BY_BUILDERS.items():
        if operator_kwargs and key in operator_kwargs:
            raise ValueError(f"operator_kwargs may not set `{key}`: {why}")


def _common(config: str, state_dir: str | None, operator_kwargs: Mapping[str, Any] | None) -> dict[str, Any]:
    """The arguments every rivet task gets; its trigger rule is the builder's gate, whatever default_args says (D24)."""
    _refuse_owned_kwargs(operator_kwargs)
    return {
        "config": config,
        "state_dir": state_dir,
        "max_active_tis_per_dag": 1,
        **(operator_kwargs or {}),
        "trigger_rule": GATE,
    }


def read_plan_layout(plan_file: str) -> list[dict[str, Any]]:
    """Waves from a `rivet plan --format json` file: `[{"wave", "exports", "heavy"}]`, lowest wave first."""
    try:
        return plan_layout(json.loads(Path(plan_file).read_text()))
    except (OSError, *PLAN_ERRORS) as exc:
        raise RuntimeError(
            f"{plan_file} is missing or is not `rivet plan --format json` output ({type(exc).__name__}); "
            f"write it with: rivet plan --config <config> --format json > {plan_file}"
        ) from exc


def _watch(dag: DAG) -> None:
    """Add the task that fails the run when any other task failed; it carries no failure callback of its own."""
    tasks = list(dag.tasks)
    watcher = RivetRunWatcher(task_id=WATCHER_ID, trigger_rule="one_failed", on_failure_callback=None)  # in the open DAG
    watcher.retries = 0  # pragma: no mutate -- Airflow 3 stores None as 0; Airflow 2.10 keeps None, and its CI job asserts 0
    tasks >> watcher


def _dag(dag_id: str, tags: Sequence[str], dag_kwargs: Mapping[str, Any]) -> DAG:
    """A DAG with the overlap limits ADR-0039's lease will later make redundant."""
    settings: dict[str, Any] = {
        "schedule": None,
        "start_date": datetime(2026, 1, 1),
        "catchup": False,
        "max_active_runs": 1,
        "tags": list(tags),
    }
    settings.update(dag_kwargs)
    settings["default_args"] = {**DEFAULT_ARGS, **(dag_kwargs.get("default_args") or {})}  # D25: merged, not replaced
    return DAG(dag_id=dag_id, **settings)


def _barrier_id(wanted: str, taken: set[str]) -> str:
    """An ordering point's id in the `apply` group, which an export of that clean name already has: `after_sales` for an
    export named so would be a duplicate task id and the DAG file would not import. The first free `<id>_barrier` instead."""
    if wanted in taken:
        return _barrier_id(f"{wanted}_barrier", taken)
    taken.add(wanted)
    return wanted


def build_batch_dag(
    dag_id: str,
    *,
    config: str,
    state_dir: str | None = None,
    plan_file: str | None = None,
    exports: Sequence[str] | None = None,
    refresh_plan: bool = True,
    extract: str = "apply",
    load: bool = True,
    compact_exports: Sequence[str] = (),
    operator_kwargs: Mapping[str, Any] | None = None,
    **dag_kwargs: Any,
) -> DAG:
    """plan -> apply / load / compact groups, chained per export: apply.X >> load.X >> compact.X."""
    exports = None if exports is None else _names("exports", exports)
    compact_exports = _names("compact_exports", compact_exports)
    if (plan_file is None) == (exports is None):
        raise ValueError("give exactly one of `plan_file` (waves from rivet plan) or `exports` (one wave)")
    if exports is not None and not exports:
        raise ValueError("`exports` is empty: name at least one export, or give `plan_file`")
    if extract not in ("apply", "run"):
        raise ValueError("`extract` is 'apply' or 'run'")
    # one wave from `exports`: no barrier follows the last wave, so it needs no number
    waves = read_plan_layout(plan_file) if plan_file else [{"exports": list(exports or ()), "heavy": []}]
    names = [n for w in waves for n in w["exports"]]
    unknown = sorted(set(compact_exports) - set(names))
    if unknown:
        raise ValueError(f"compact_exports names exports the plan does not have: {unknown}")
    if compact_exports and not load:
        raise ValueError("compact_exports needs load=True: a compaction merges what the load buffered")
    common = _common(config, state_dir, operator_kwargs)
    extract_cls = RivetApplyOperator if extract == "apply" else RivetRunOperator
    taken = {task_name(n) for n in names}  # the ids of the group the barriers are in: no barrier takes an export's

    with _dag(dag_id, ("rivet", "batch"), dag_kwargs) as dag:
        # each gate is a list of at most one task, so no stand-in for "no gate yet" can become an upstream
        plan = [RivetPlanOperator(task_id="plan", plan_file=plan_file, **common)] if plan_file and refresh_plan else []
        apply_tasks: dict[str, Any] = {}
        with TaskGroup(group_id="apply"):
            wave_done: list[Any] = []
            for index, wave in enumerate(waves):
                # D26: rivet's `cost_gate_batches` order: each heavy export alone, one after another, then the cheap batch
                heavy = [n for n in wave["exports"] if n in wave["heavy"]]
                cheap = [n for n in wave["exports"] if n not in wave["heavy"]]
                order_done: list[Any] = []
                for position, name in enumerate(heavy + cheap):
                    task = extract_cls(task_id=task_name(name), export=name, **common)
                    apply_tasks[name] = task
                    [*plan, *wave_done, *order_done] >> task
                    if name in heavy and position < len(heavy + cheap) - 1:
                        order_done = [
                            RivetWaveBarrier(task_id=_barrier_id(f"after_{task_name(name)}", taken), trigger_rule=ORDER_ONLY)
                        ]
                        task >> order_done
                if index < len(waves) - 1:
                    wave_done = [
                        RivetWaveBarrier(task_id=_barrier_id(f"wave_{wave['wave']}_done", taken), trigger_rule=ORDER_ONLY)
                    ]
                    [apply_tasks[name] for name in wave["exports"]] >> wave_done[0]
        load_tasks: dict[str, Any] = {}
        if load:
            with TaskGroup(group_id="load"):
                for name in names:
                    load_tasks[name] = RivetLoadOperator(task_id=task_name(name), export=name, **common)
                    apply_tasks[name] >> load_tasks[name]
        if compact_exports:
            with TaskGroup(group_id="compact"):
                for name in names:
                    if name in compact_exports:
                        load_tasks[name] >> RivetCompactOperator(task_id=task_name(name), export=name, **common)
        _watch(dag)
    return dag


def build_cdc_dag(
    dag_id: str,
    *,
    config: str,
    export: str,
    tables: Sequence[str],
    state_dir: str | None = None,
    load: bool = True,
    compact: bool = True,
    operator_kwargs: Mapping[str, Any] | None = None,
    **dag_kwargs: Any,
) -> DAG:
    """One `run` task for the stream, then load.T >> compact.T per captured table."""
    tables = _names("tables", tables)
    return _cdc_dag(
        dag_id, [(export, tables, tables if compact else [])], load, _common(config, state_dir, operator_kwargs), dag_kwargs
    )


def _cdc_dag(
    dag_id: str,
    streams: Sequence[tuple[str, Sequence[str], Sequence[str]]],
    load: bool,
    common: Mapping[str, Any],
    dag_kwargs: Mapping[str, Any],
) -> DAG:
    """The CDC DAG of one or more streams: one stream's tasks at the top (`run`, `load.T`, `compact.T`), several streams'
    each in a group named after its export (rivet init writes one stream per table on SQL Server, Oracle, MongoDB and
    PostgreSQL outside `public`); the streams run side by side, each under its own export's lock."""
    if load and any(not tables for _, tables, _ in streams):
        raise ValueError("`tables` lists the captured tables the load and compact tasks are built for")
    with _dag(dag_id, ("rivet", "cdc"), dag_kwargs) as dag:
        for export, tables, compacted in streams:
            if len(streams) == 1:
                _cdc_stream(export, tables, load, compacted, common)
            else:
                with TaskGroup(group_id=task_name(export)):
                    _cdc_stream(export, tables, load, compacted, common)
        _watch(dag)
    return dag


def _cdc_stream(export: str, tables: Sequence[str], load: bool, compacted: Sequence[str], common: Mapping[str, Any]) -> None:
    """One stream's tasks in the DAG or group being built: run, then load.T >> compact.T per captured table."""
    run = RivetCdcRunOperator(task_id="run", export=export, **common)
    if load:
        loads = {}
        with TaskGroup(group_id="load"):
            for table in tables:
                loads[table] = RivetLoadOperator(task_id=task_name(table), export=export, table=table, **common)
                run >> loads[table]
        if compacted:
            with TaskGroup(group_id="compact"):
                for table in tables:
                    if table in compacted:
                        loads[table] >> RivetCompactOperator(task_id=task_name(table), export=export, table=table, **common)


def build_source_dags(
    sources: Sequence[str],
    *,
    mode: str,
    config_dir: str,
    state_root: str,
    state_connection: str,
    source_connection: str = "{name}",
    env_from_variables: Mapping[str, str] | None = None,
    init_args: Sequence[str] = (),
    odbc_ini: str | None = "/etc/odbc.ini",
    operator_kwargs: Mapping[str, Any] | None = None,
    **dag_kwargs: Any,
) -> dict[str, DAG]:
    """One DAG `rivet_<name>_<mode>` per named database, created paused; put them in the DAG file's globals.

    A name's source is the Airflow Connection `source_connection` (`{name}` replaced), else (with `odbc_ini`) the data
    source `[name]` of that file. Until
    `<config_dir>/<name>.yaml` exists the DAG has one task, `init`, which writes it with `rivet init` (`init_args` as
    written, `{name}` replaced) and checks it; the next parse builds the DAG from the config: one CDC stream with a load
    and compaction per table, or every batch export with its load. The state of a source is the database
    `rivet_state_<name>` on the PostgreSQL server of `state_connection` (created by `init` if missing) and the folder
    `<state_root>/<name>`. Reads only files at parse time: no database is reached until a task runs.

    `env_from_variables` maps an environment variable to the Airflow Variable that holds its value (`{name}` replaced
    in the key), e.g. `{"RIVET_SOURCE_URL_SECRET": "rivet_{name}_url_secret"}` for a config with
    `url_secret: ${RIVET_SOURCE_URL_SECRET}`. Every task of the source reads its Variables when it runs, never at parse;
    a missing one fails that source's tasks with `RIVET_AIRFLOW_VARIABLE_MISSING` and no other source.
    """
    names = _names("sources", sources)
    bad = [n for n in names if not SOURCE_NAME.fullmatch(n)]
    twice = sorted({n for n in names if names.count(n) > 1})
    if bad or twice:
        raise ValueError(f"source names are distinct and of [a-z0-9_]: {bad or twice}")
    if mode not in ("cdc", "batch"):
        raise ValueError("`mode` is 'cdc' or 'batch'")
    taken = sorted({"env_from_connections", "env_from_variables", "config", "state_dir"} & set(operator_kwargs or {}))
    if taken:
        raise ValueError(f"operator_kwargs may not set {taken}: each source's DAG sets them")
    _refuse_owned_kwargs(operator_kwargs)  # before any source has a config, as the builders it calls later
    dags: dict[str, DAG] = {}
    for name in names:
        config = Path(config_dir) / f"{name}.yaml"
        source = {
            "conn_id": source_connection.replace("{name}", name),
            **({"odbc_ini": odbc_ini, "dsn": name} if odbc_ini else {}),
        }
        state = {"conn_id": state_connection, "database": f"rivet_state_{name}"}
        variables = {env: key.replace("{name}", name) for env, key in (env_from_variables or {}).items()}
        kwargs = {
            **(operator_kwargs or {}),
            "env_from_connections": {SOURCE_ENV: source, "RIVET_STATE_URL": state},
            "env_from_variables": variables,
        }
        settings = {"is_paused_upon_creation": True, **dag_kwargs}
        dag_id = f"rivet_{name}_{mode}"
        common = {"config": str(config), "state_dir": str(Path(state_root) / name), "operator_kwargs": kwargs, **settings}
        if not config_present(config):
            dags[dag_id] = _init_dag(dag_id, mode, [a.replace("{name}", name) for a in init_args], common)
            continue
        try:
            dags[dag_id] = _config_dag(dag_id, mode, config, common, kwargs, source, state)
        except Exception as exc:  # one source's config must not take the DAG file's other sources with it
            dags[dag_id] = _refused_dag(dag_id, mode, config, exc, common)
    return dags


def _config_dag(
    dag_id: str,
    mode: str,
    config: Path,
    common: Mapping[str, Any],
    kwargs: dict[str, Any],
    source: Mapping[str, Any],
    state: Mapping[str, Any],
) -> DAG:
    """The DAG built from a source's config."""
    doc = read_config(str(config))
    source_block = doc.get("source") or {}
    url_env = source_block.get("url_env")
    # rivet reads the secret itself: no source Connection is read for a `url_secret`, nor for a `password_secret` whose
    # host, user and database are written in the config; a `password_secret` with `url_env` still reads the URL's Connection
    if source_block.get("url_secret") is not None or (source_block.get("password_secret") is not None and not url_env):
        kwargs["env_from_connections"] = {"RIVET_STATE_URL": state}
    elif url_env and url_env != SOURCE_ENV:  # a config moved from cron names its own variable
        kwargs["env_from_connections"] = {url_env: source, "RIVET_STATE_URL": state}
    return _dag_of_config(dag_id, mode == "cdc", config, doc, common)


def build_config_dag(
    dag_id: str,
    *,
    config: str,
    mode: str | None = None,
    state_dir: str | None = None,
    operator_kwargs: Mapping[str, Any] | None = None,
    **dag_kwargs: Any,
) -> DAG:
    """The DAG of a rivet config, built as build_source_dags builds it: every `mode: cdc` stream (a group per stream
    when there are several) with a load and, where the target merges, a compaction per table, or every batch export
    with its load. `mode` is inferred from the config when not given: 'cdc' if it has a `mode: cdc` export. Reads only
    the config at parse time; a config it cannot build is a DAG whose one task fails naming why, as in build_source_dags."""
    if mode not in (None, "cdc", "batch"):
        raise ValueError("`mode` is 'cdc', 'batch' or None (read from the config)")
    _refuse_owned_kwargs(operator_kwargs)
    common = {"config": config, "state_dir": state_dir, "operator_kwargs": dict(operator_kwargs or {}), **dag_kwargs}
    try:
        doc = read_config(config)
        as_cdc = any(mode_of(e) == "cdc" for e in entries_of(doc)) if mode is None else mode == "cdc"
        return _dag_of_config(dag_id, as_cdc, Path(config), doc, common)
    except Exception as exc:
        return _refused_dag(dag_id, mode or "config", Path(config), exc, common)


def _dag_of_config(dag_id: str, as_cdc: bool, config: Path, doc: Mapping[str, Any], common: Mapping[str, Any]) -> DAG:
    """The CDC (`as_cdc`) or batch DAG of a config read as `doc`; a config with no export for the mode is refused, coded."""
    exports = entries_of(doc)
    cdc = [e for e in exports if mode_of(e) == "cdc"]
    if as_cdc:
        # init: one `tables:` stream on MySQL and `public` PostgreSQL, one stream per table elsewhere (`table:`)
        if not cdc:
            raise ConfigRefusal(
                "RIVET_AIRFLOW_CONFIG_NO_CDC_EXPORT",
                f"{config} has no export with `mode: cdc`, so there is no stream for a CDC DAG; build this source with "
                f"mode='batch', or delete the file to have `init` write it again with the CDC mode",
            )
        streams = [
            (
                name_of(e),
                [warehouse_table(t) for t in tables_of(e)],
                [warehouse_table(t) for t in tables_of(e) if compacts(doc, e, t)],  # the layout rivet resolves
            )
            for e in cdc
        ]
        task_common = _common(common["config"], common["state_dir"], common["operator_kwargs"])
        dag_settings = {k: v for k, v in common.items() if k not in ("config", "state_dir", "operator_kwargs")}
        return _cdc_dag(dag_id, streams, bool(doc.get("load")), task_common, dag_settings)
    maybe = [
        name_of(e) if mode_of(e) == "cdc" else f"{name_of(e)}: `{mode_of(e)}` may be cdc"
        for e in exports
        if may_be_cdc(mode_of(e))
    ]
    if maybe:
        raise ConfigRefusal(
            "RIVET_AIRFLOW_CONFIG_HAS_CDC_EXPORT",
            f"{config} has `mode: cdc` exports ({', '.join(maybe)}): a batch DAG over it would run on the config and the "
            "state database of the CDC DAG, and load the same tables beside it. A source in both a "
            "CDC and a batch DAG needs two names, or a `config_dir` and a `state_root` per mode: build the batch DAG "
            "from its own",
        )
    if not exports:
        raise ConfigRefusal(
            "RIVET_AIRFLOW_CONFIG_NO_EXPORT",
            f"{config} has no export; delete the file to have `init` write it again, or add the exports",
        )
    return build_batch_dag(
        dag_id,
        exports=[name_of(e) for e in exports],
        load=bool(doc.get("load")),
        compact_exports=[name_of(e) for e in exports if compacts(doc, e)],  # a base_buffer export merges what it loads
        **common,
    )


def _refused_dag(dag_id: str, mode: str, config: Path, exc: Exception, common: Mapping[str, Any]) -> DAG:
    """The DAG of a source whose config could not be built: its one task fails naming the refusal. A refusal is shown as
    coded; any other exception (a config shape nobody foresaw) as what it is, so no config can end the DAG file."""
    refusal = (
        exc
        if isinstance(exc, PreflightRefusal)
        else PreflightRefusal(
            "RIVET_AIRFLOW_CONFIG_UNBUILDABLE", f"cannot build a DAG from {config}: {type(exc).__name__}: {exc}"
        )
    )
    settings = {k: v for k, v in common.items() if k not in ("config", "state_dir", "operator_kwargs")}
    with _dag(dag_id, ("rivet", mode, "refused"), settings) as dag:
        # Airflow 3 stores retries=None as 0 and Airflow 2.10 keeps None: its CI job asserts 0
        RivetConfigRefused(task_id="config", refusal=str(refusal), code=refusal.code, retries=0)  # pragma: no mutate
    return dag


def _init_dag(dag_id: str, mode: str, init_args: list[str], common: Mapping[str, Any]) -> DAG:
    """The DAG of a source with no config yet: its one task writes the config."""
    settings = {k: v for k, v in common.items() if k not in ("config", "state_dir", "operator_kwargs")}
    task_kwargs = {k: v for k, v in common["operator_kwargs"].items() if k not in RUN_ONLY}
    with _dag(dag_id, ("rivet", mode, "init"), settings) as dag:
        RivetInitOperator(
            task_id="init",
            mode=mode,
            source_env=SOURCE_ENV,
            init_args=init_args,
            config=common["config"],
            state_dir=common["state_dir"],
            **{**task_kwargs, "retries": 0},  # a failed init is re-run by the operator, after reading why it failed
        )
    return dag
