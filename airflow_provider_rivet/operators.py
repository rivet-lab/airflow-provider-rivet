"""Airflow operators: thin adapters over `invocation.invoke`; none of them builds argv."""

from __future__ import annotations

import logging
import os
import shutil
import subprocess
import tempfile
from collections.abc import Callable, Mapping, Sequence
from datetime import UTC, datetime
from itertools import pairwise
from pathlib import Path
from typing import Any, NoReturn, TypeVar

from . import checks
from ._compat import (
    AirflowException,
    AirflowFailException,
    AirflowNotFoundException,
    AirflowSkipException,
    BaseHook,
    BaseOperator,
    Variable,
    executor_spreading_hosts,
    mask_secret,
)
from ._yaml import params_of
from .capabilities import MIN_RIVET_VERSION, ProbeFailed, probe
from .checks import FAIL, PASS, SKIP, WARN, Check, not_writable, write_probe
from .codes import Code
from .connections import (
    EnvSpec,
    build_environment,
    connection_url,
    database_ref,
    missing_variable,
    resolve_database,
    with_database,
)
from .events import Event
from .exports import checkpoints, entries_of, mode_of, name_of, server_id_of, staging_of, tables_of
from .invocation import (
    CLOUD_ENV,
    Busy,
    Outcome,
    Request,
    Stopped,
    crashed_ledger,
    crashed_retry_allowed,
    invoke,
    spawn,
    warehouse_table,
    worker_environment,
)
from .preflight import STATE_URL_ENV, PreflightRefusal, read_config, resolve_binary
from .reading import _json_of
from .result import BUSY, REFUSED, STOPPED, ErrorObject, error_from_exit, error_text, unstarted
from .sources import drift
from .tasklog import TaskLog

T = TypeVar("T")
MODES = ("cdc", "batch")

XCOM_KEY = "return_value"


def airflow_connection(conn_id: str) -> Any:
    """The Airflow Connection of this id, or None when no backend defines it."""
    try:
        return BaseHook.get_connection(conn_id)
    except AirflowNotFoundException:
        return None


def airflow_variable(key: str) -> str | None:
    """The Airflow Variable of this key, or None when no backend defines it (read at task run time only)."""
    return Variable.get(key, None)


def get_connection(conn_id: str) -> Any:
    """The Airflow Connection the task names; a refusal when no backend defines it, since a retry cannot create it."""
    return resolve_database({"conn_id": conn_id}, airflow_connection)[0]


class _RivetTask(BaseOperator):
    """What every rivet task of this package has: the environment rivet runs with (Connections, odbc.ini data sources,
    key files), the task log, and how a task that ends before or between rivet's steps reports it (a refusal, a stop
    Airflow asked for), with `on_kill` reaching the running rivet. `RivetBaseOperator` adds the ADR-0039 invocation,
    `RivetInitOperator` the writing of a config."""

    command = ""
    export: str | None = None
    table: str | None = None
    template_fields: Sequence[str] = ("config", "state_dir")
    ui_color = "#e8f0fe"
    _tasklog: TaskLog  # this try's task log; `execute` starts a new one before anything is recorded

    def __init__(
        self,
        *,
        config: str,
        state_dir: str | None = None,
        rivet_bin: str = "rivet",
        env: Mapping[str, str] | None = None,
        env_from_connections: Mapping[str, Any] | None = None,
        env_from_variables: Mapping[str, str] | None = None,
        env_passthrough: Sequence[str] = (),
        cloud_credentials: Sequence[str] = (),
        connections: Sequence[str] = (),
        **kwargs: Any,
    ) -> None:
        super().__init__(**kwargs)
        if set(cloud_credentials) - set(CLOUD_ENV):
            raise ValueError(f"cloud_credentials names {sorted(CLOUD_ENV)}, got {sorted(cloud_credentials)}")
        both = sorted(set(env_from_variables or {}) & set(env or {}))
        if both:
            raise ValueError(f"env and env_from_variables both set {both}: a variable has one value")
        self.config = config
        self.state_dir = state_dir
        self.rivet_bin = rivet_bin
        self.env = env
        self.env_from_connections = env_from_connections
        self.env_from_variables = dict(env_from_variables or {})
        self.env_passthrough = tuple(env_passthrough)
        self.cloud_credentials = tuple(cloud_credentials)
        self.connections = tuple(connections)
        self._proc: Any = None
        self._servers: dict[str, Any] = {}
        self._stop_requested = False  # pragma: no mutate - only ever read as a truth value, so None reads as False

    def _variables(self) -> dict[str, str]:
        """`env_from_variables` read now, at task run time: each variable's value is the Airflow Variable of its key. A
        Variable no backend defines is a refusal, and a value that is empty counts as none."""
        out: dict[str, str] = {}
        for name, key in self.env_from_variables.items():
            value = airflow_variable(key)
            if not value:
                raise missing_variable(key, name)
            out[name] = value
        return out

    def _environment(self, secrets: Path) -> dict[str, str]:
        """The subprocess environment (`connections.build_environment`), with every Connection and Variable looked up
        here; key files go to `secrets`, which the task removes when rivet has exited."""
        refs = {name: database_ref(ref) for name, ref in (self.env_from_connections or {}).items()}
        variables = self._variables()
        doc = self._config_doc(variables) if self.connections else {}
        spec = EnvSpec(
            passthrough=self.env_passthrough,
            clouds=self.cloud_credentials,
            env={**variables, **(self.env or {})},
            urls={name: (self._database(ref), ref.get("scheme")) for name, ref in refs.items()},
            config=doc,
        )
        env = build_environment(os.environ, spec, [get_connection(c) for c in self.connections], secrets)  # pragma: no mutate
        self._say_where(refs, doc or self._written_places_doc(variables))
        return env

    def _database(self, ref: Mapping[str, Any]) -> Any:
        """The database an `env_from_connections` entry names: its Airflow Connection, else (with `odbc_ini`) the data
        source of that name in the file; its `database` instead of the one stored. The server is kept in `_servers`."""
        server, dsn = resolve_database({**ref, "database": None}, airflow_connection)
        if dsn is not None:
            if server.password:
                mask_secret(server.password)
            self.log.info("database `%s`: no such Connection, ODBC data source [%s] in %s", ref["conn_id"], dsn, ref["odbc_ini"])
        self._servers[ref["conn_id"]] = server
        return with_database(server, ref.get("database"))

    def _server(self, ref: Mapping[str, Any]) -> Any:
        """The server an `env_from_connections` entry names, as its Connection stores it, looked up once per task."""
        if ref["conn_id"] not in self._servers:
            self._database(ref)
        return self._servers[ref["conn_id"]]

    def _written_places_doc(self, variables: Mapping[str, str]) -> Mapping[str, Any]:
        """The config for the places it writes, or none when it cannot be read yet."""
        try:
            return self._config_doc(variables)
        except PreflightRefusal:
            return {}

    def _say_where(self, refs: Mapping[str, Mapping[str, Any]], doc: Mapping[str, Any]) -> None:
        """Log the state database (name, server, Connection) and each place the config writes."""
        state = refs.get(STATE_URL_ENV)
        if state is not None:
            database = state.get("database") or self._server(state).schema
            self.log.info("state    database `%s` on %s (Connection `%s`)", database, self._host_of(state), state["conn_id"])
        written = sorted(places(doc))
        if written:
            self.log.info("writes   %s", ", ".join(written))

    def _host_of(self, ref: Mapping[str, Any]) -> str:
        """`host:port` of the server an entry names; 5432 when the Connection stores no port."""
        server = self._server(ref)
        return f"{server.host}:{server.port or 5432}"

    def _config_doc(self, variables: Mapping[str, str]) -> dict[str, Any]:
        """The config, for the names a storage or warehouse Connection is filed under; a task with no config to read
        (`init`, before it wrote one) files each under its own name."""
        return {}

    def _emit(self, record: dict[str, Any]) -> None:
        """Keep one structured record: written as JSON at DEBUG, read into the task's readable lines (`tasklog`)."""
        self._tasklog.record(record)

    def _write(self, level: int, text: str) -> None:
        """One line of the task log at a `logging` level."""
        method = {logging.DEBUG: "debug", logging.INFO: "info", logging.WARNING: "warning"}.get(level, "error")
        getattr(self.log, method)("%s", text)  # pragma: no mutate - a logger formats no message given no args

    def _finish(self, ti: Any, payload: dict[str, Any]) -> None:
        """Write the task's readable block: the outcome's headline, a line per step, the notes."""
        self._tasklog.finish(payload, ti.try_number, ti.max_tries, self._stop_requested)

    def _hold(self, proc: Any) -> None:
        """Remember the running process so `on_kill` can stop it; one started after a stop is stopped at once (D40)."""
        self._proc = proc
        if self._stop_requested:
            proc.request_stop()

    def on_kill(self) -> None:
        """Ask rivet to stop when Airflow kills the task: a signal only, since this runs inside the SIGTERM handler;
        the step that started the process reaps it and kills it after the grace (D02). The stop is recorded first, so a
        stop that meets no running process (between two steps, before the first) still keeps the next one from
        starting: Airflow 3's handler returns instead of raising (D40)."""
        self._stop_requested = True
        if self._proc is not None:
            self._proc.request_stop()

    def _fail(self, ti: Any, payload: dict[str, Any], text: str, retry: bool) -> NoReturn:
        """Publish the payload and write the task's readable block, then raise the exception Airflow retries or the one
        it does not."""
        ti.xcom_push(key=XCOM_KEY, value=payload)
        self._finish(ti, payload)
        raise (AirflowException if retry else AirflowFailException)(text)

    def _refuse(self, ti: Any, refusal: PreflightRefusal) -> NoReturn:
        """Log and publish a refusal made before rivet started, then fail without retry."""
        self._emit({"event": Event.REFUSED, "code": refusal.code, "text": str(refusal)})
        payload = unstarted(self.command, REFUSED, refusal.code)
        self._fail(ti, payload, str(refusal), retry=False)  # pragma: no mutate - `retry` is read as a truth value

    def _stopped(self, ti: Any, stop: Stopped) -> NoReturn:
        """Log and publish a stop Airflow asked for before a rivet step started, then fail for a retry (D40)."""
        self._emit({"event": Event.STOPPED, "step": stop.step, "text": str(stop)})
        self._fail(ti, unstarted(self.command, STOPPED, stop.step), str(stop), retry=True)

    def _guarded(self, ti: Any, work: Callable[[], T]) -> T:
        """Run one stretch of the task's work, reporting a refusal or a stop it raises, with no process held after it."""
        try:
            return work()
        except PreflightRefusal as refusal:
            self._refuse(ti, refusal)
        except Stopped as stop:
            self._stopped(ti, stop)
        finally:
            self._proc = None

    def execute(self, context: Any) -> dict[str, Any]:
        """Run the command; return the XCom payload or raise by the contract's retry table."""
        ti = context["ti"]
        run_id = str(context.get("run_id") or getattr(ti, "run_id", "run"))
        map_index = getattr(ti, "map_index", None)
        task_part = ti.task_id if map_index in (-1, None) else f"{ti.task_id}[{map_index}]"
        self._tasklog = TaskLog(self.command, self.export, self.table and warehouse_table(self.table), self._write)
        secrets = Path(tempfile.mkdtemp(prefix="rivet-airflow-secrets-"))
        try:
            return self._run_with_secrets(ti, run_id, task_part, secrets)
        finally:
            shutil.rmtree(secrets, ignore_errors=True)

    def _run_with_secrets(self, ti: Any, run_id: str, task_part: str, secrets: Path) -> dict[str, Any]:
        """The task's work, with a private folder for the key files of this try."""
        raise NotImplementedError


class RivetBaseOperator(_RivetTask):
    """Runs one rivet command on the worker and turns its ADR-0039 result into an Airflow outcome."""

    command = ""
    needs_export = False
    template_fields: Sequence[str] = (
        "config",
        "export",
        "table",
        "state_dir",
        "plan_file",
        "extra_args",
        "stderr_dir",
        "cwd",
    )

    def __init__(
        self,
        *,
        config: str,
        export: str | None = None,
        table: str | None = None,
        state_dir: str | None = None,
        plan_file: str | None = None,
        rivet_bin: str = "rivet",
        env: Mapping[str, str] | None = None,
        env_from_connections: Mapping[str, Any] | None = None,
        env_from_variables: Mapping[str, str] | None = None,
        env_passthrough: Sequence[str] = (),
        cloud_credentials: Sequence[str] = (),
        connections: Sequence[str] = (),
        deployment: str = "auto",
        extra_args: Sequence[str] | None = None,
        lock_wait: int = 600,
        rivet_notifications: bool = False,
        crashed_retries: int = 1,
        stderr_dir: str | None = None,
        stream_stderr: bool = False,
        progress_every: float | None = 60,
        log_keep: int | None = 10,
        cwd: str | None = None,
        single_host: bool = False,
        **kwargs: Any,
    ) -> None:
        super().__init__(
            config=config,
            state_dir=state_dir,
            rivet_bin=rivet_bin,
            env=env,
            env_from_connections=env_from_connections,
            env_from_variables=env_from_variables,
            env_passthrough=env_passthrough,
            cloud_credentials=cloud_credentials,
            connections=connections,
            **kwargs,
        )
        if self.needs_export and not export:
            raise ValueError(f"{type(self).__name__} needs `export`")
        self.export = export
        self.table = table
        self.plan_file = plan_file
        self.deployment = deployment
        self.single_host = single_host
        self.extra_args = extra_args
        self.lock_wait = lock_wait
        self.rivet_notifications = rivet_notifications
        self.crashed_retries = crashed_retries
        self.stderr_dir = stderr_dir
        self.stream_stderr = stream_stderr
        self.progress_every = progress_every
        self.log_keep = log_keep
        self.cwd = cwd

    def _config_doc(self, variables: Mapping[str, str] | None = None) -> dict[str, Any]:
        """The config as rivet builds it, for the names a Connection is filed under; read by the preflight's reader, so a
        config it refuses is refused here too, and a `${...}` in `load.password_env` / `load.connection` resolves from
        `-p` and the task's environment (the worker's allow-listed variables, `env` and `env_from_variables`) as rivet
        resolves it (D06)."""
        env = {**worker_environment(self.env_passthrough, self.cloud_credentials), **(self.env or {}), **(variables or {})}
        return read_config(self.config, {str(k): str(v) for k, v in env.items()}, params_of(self.extra_args or ()))

    def _upstream_markers(self, ti: Any) -> list[str]:
        """State-directory markers the direct upstream rivet tasks of this run reported."""
        upstream = sorted(self.upstream_task_ids)
        if not upstream:
            return []
        pulled = ti.xcom_pull(task_ids=upstream, key=XCOM_KEY)  # pragma: no mutate - XCOM_KEY is Airflow's default key
        values = [] if pulled is None else [pulled] if isinstance(pulled, dict) else list(pulled)
        return [p["state_marker"] for p in values if isinstance(p, dict) and p.get("state_marker")]

    def _run_with_secrets(self, ti: Any, run_id: str, task_part: str, secrets: Path) -> dict[str, Any]:
        """`execute` with a private folder for the key files of this try."""
        env = self._guarded(ti, lambda: self._environment(secrets))
        request = Request(
            command=self.command,
            config=self.config,
            env=env,
            export=self.export,
            table=self.table,
            rivet_bin=self.rivet_bin,
            state_dir=self.state_dir,
            deployment=self.deployment,
            spreads_hosts=None if self.single_host else executor_spreading_hosts(),
            extra_args=list(self.extra_args or ()),
            lock_wait=self.lock_wait,
            rivet_notifications=self.rivet_notifications,
            plan_file=self.plan_file,
            cwd=self.cwd,
            stderr_dir=self.stderr_dir,
            stream_stderr=self.stream_stderr,
            progress_every=self.progress_every,
            log_keep=self.log_keep,
            label=(ti.dag_id, run_id, task_part, ti.try_number),
            series=ti.max_tries,
            upstream_markers=self._upstream_markers(ti),
        )
        outcome = self._guarded(ti, lambda: self._invoke(ti, request))
        return self._settle(ti, run_id, task_part, outcome)

    def _invoke(self, ti: Any, request: Request) -> Outcome:
        """Run the request; a lock another run holds fails the task for a retry."""
        try:
            return invoke(request, self._emit, self._hold, lambda: self._stop_requested)
        except Busy as busy:
            self._emit({"event": Event.BUSY, "code": busy.code, "text": str(busy)})
            self._fail(ti, unstarted(self.command, BUSY, busy.code), str(busy), retry=True)

    def _settle(self, ti: Any, run_id: str, task_part: str, outcome: Outcome) -> dict[str, Any]:
        """Map the outcome's decision onto success, skip, retry or fail."""
        payload = outcome.to_xcom()
        decision = outcome.decision
        if decision == "success":
            if self.command == "compact" and outcome.units and all(u.status == "skipped" for u in outcome.units):
                ti.xcom_push(key=XCOM_KEY, value=payload)
                self._finish(ti, payload)
                raise AirflowSkipException(
                    f"compact skipped: {outcome.skip_reason}" if outcome.skip_reason else "compact skipped"
                )
            stops = [u.export for u in outcome.units if u.stop_reason == "max_events"]
            if stops:
                self._emit({"event": Event.CDC_MAX_EVENTS, "exports": stops, "text": "stopped on max_events; backlog remains"})
            self._finish(ti, payload)
            return payload
        assert outcome.error is not None
        failed = [u.export for u in outcome.units if u.status == "failed"]
        text = outcome.error.describe(failed[0] if len(failed) == 1 else self.export)
        self._emit(  # rivet's reason, safe to print (`error_text`): the task log's failure lines read it from here
            {
                "event": Event.ERROR,
                "text": error_text(outcome.error.message),
                "units": [
                    {"export": u.export, "table": u.table, "text": error_text(u.error.message)}
                    for u in outcome.units
                    if u.error is not None  # a unit has an error exactly when it failed (`Reading`)
                ],
            }
        )
        if decision == "retry_crashed":
            ledger = None
            # The ledger is a file on this worker's disk: it counts every crash only while every try runs here, which
            # SQLite state guarantees and PostgreSQL state does not (a retry on another host saw an empty ledger and
            # granted the budget again). Elsewhere the budget is the first `crashed_retries` tries, as on a pod.
            if outcome.marker is not None and self.state_dir and outcome.state_kind == "sqlite":
                ledger = crashed_ledger(self.state_dir, ti.dag_id, run_id, task_part)
            allowed, problem = crashed_retry_allowed(ledger, ti.try_number, ti.max_tries, self.crashed_retries)
            allowed = allowed and ti.try_number <= ti.max_tries  # a retry needs a try Airflow still has
            payload["crashed_retry"] = allowed
            if ledger is None or problem:
                payload["degraded"] = sorted({*payload["degraded"], "crashed_ledger"})
            self._emit(
                {
                    "event": Event.CRASHED,
                    "retry": allowed,
                    "budget": self.crashed_retries,
                    "try_number": ti.try_number,
                    "airflow_tries_left": ti.try_number <= ti.max_tries,
                    "ledger_problem": problem,
                }
            )
            self._fail(ti, payload, text, retry=allowed)
        self._fail(ti, payload, text, retry=decision == "retry")
        return payload


class RivetRunOperator(RivetBaseOperator):
    """`rivet run`, for one export or the whole config."""

    command = "run"


class RivetApplyOperator(RivetBaseOperator):
    """`rivet plan -e <export>` into a sealed artifact, then `rivet apply` of it; a retry replays the artifact."""

    command = "apply"
    needs_export = True


class RivetCdcRunOperator(RivetBaseOperator):
    """One bounded CDC drain of a stream; succeeds on `max_events` and reports it."""

    command = "cdc_run"
    needs_export = True
    ui_color = "#e6f4ea"

    def _environment(self, secrets: Path) -> dict[str, str]:
        """The subprocess environment, once the state database the task names is known to be the stream's own."""
        env = super()._environment(secrets)
        self._refuse_empty_state_of_a_started_stream()
        return env

    def _refuse_empty_state_of_a_started_stream(self) -> None:
        """Refuse a state database the task names in place of its Connection's own (`build_source_dags`'s
        `rivet_state_<name>`) when it holds no table while the stream's checkpoint exists: the stream ran on another
        state, most likely the Connection's own database (a DAG moved from `build_cdc_dag`), and rivet would start its
        state over in the empty one."""
        ref = (self.env_from_connections or {}).get(STATE_URL_ENV)
        ref = database_ref(ref) if ref else {}
        database = ref.get("database")
        if not database or not self.state_dir:
            return
        conn = self._server(ref)
        own = conn.schema
        if not own or own == database:
            return
        checkpoint = checkpoints(self._config_doc(), Path(self.state_dir)).get(str(self.export))
        if checkpoint is None or not checkpoint.is_file() or not checkpoint.stat().st_size:
            return
        tables = self._table_count(with_database(conn, database))
        if tables is None or tables:
            return
        raise PreflightRefusal(
            Code.STATE_DATABASE_MISMATCH,
            f"the state database `{database}` this task names (on the server of Connection `{ref['conn_id']}`) has no "
            f"table, while the stream's checkpoint {checkpoint} exists: the stream ran on another state, and the "
            f"Connection names the database `{own}`. rivet would start the state over in the empty one. If the state "
            f"is in `{own}` (a DAG moved from build_cdc_dag), use it: `state_database: connection` for this source in "
            f"the sources file, or build_source_dags(state_database=None); to keep `{database}`, copy the state into it "
            f"first (pg_dump {own} | psql {database}); see docs/dags-from-file.md",
        )

    def _table_count(self, conn: Any) -> int | None:
        """The number of tables of a PostgreSQL database outside the system schemas, or None when it cannot be read
        (no psycopg2, no such database, no access): rivet then meets the database as it is."""
        try:
            import psycopg2
        except ImportError:
            self.log.warning("psycopg2 is not installed: the state database `%s` was not checked for tables", conn.schema)
            return None
        try:
            db = psycopg2.connect(connection_url(conn))
        except psycopg2.Error:
            return None
        try:
            with db.cursor() as cur:
                cur.execute(
                    "SELECT count(*) FROM information_schema.tables "
                    "WHERE table_schema NOT IN ('pg_catalog', 'information_schema')"
                )
                row = cur.fetchone()
                return int(row[0]) if row else None
        finally:
            db.close()


class RivetLoadOperator(RivetBaseOperator):
    """`rivet load` for one export, and one table when asked."""

    command = "load"
    ui_color = "#fef7e0"


class RivetCompactOperator(RivetBaseOperator):
    """`rivet compact` for one export; a compaction rivet reports as skipped is an Airflow skip."""

    command = "compact"
    ui_color = "#fce8e6"


class RivetPlanOperator(RivetBaseOperator):
    """`rivet plan --format json` for the whole config, written atomically to `plan_file`."""

    command = "plan"

    def __init__(self, *, plan_file: str, **kwargs: Any) -> None:
        super().__init__(plan_file=plan_file, **kwargs)


def config_present(path: Path) -> bool:
    """Whether the config file is there, by opening it: a stat can answer from a stale cache (Docker Desktop's file
    sharing kept a deleted file `exists()`), and the generator and `init` must agree with the reader that follows."""
    try:
        path.open("rb").close()
    except FileNotFoundError:
        return False
    return True


URL_SCHEMES = {"gcs": "gs"}


def places(doc: Mapping[str, Any]) -> set[str]:
    """Where a config writes: each export's bucket prefix (`s3://b/p/`) and its warehouse dataset or database."""
    out = set()
    for export in entries_of(doc):
        dest = export.get("destination") or {}
        if dest.get("bucket"):
            scheme = URL_SCHEMES.get(str(dest.get("type")), dest.get("type"))
            out.add(f"{scheme}://{dest['bucket']}/{dest.get('prefix') or ''}")
    load = doc.get("load") or {}
    names = [load.get(k) for k in ("project", "dataset", "database", "schema") if load.get(k)]
    if names:
        out.add(f"{load.get('target')} {'.'.join(map(str, names))}")
    return out


def _flag_of(arg: str) -> str:
    """The flag a command-line token sets as clap reads it: `--output=x` is `--output`, `-oX` and `-o=X` are `-o`."""
    if arg.startswith("--"):
        return arg.partition("=")[0]
    if arg.startswith("-"):
        return arg[:2]
    return arg


def _asks_for_cdc(args: Sequence[str]) -> bool:
    """Whether `args` set `--mode` to `cdc` (`--mode cdc` or `--mode=cdc`); a value that reads `cdc` is only a value."""
    return "--mode=cdc" in args or any(a == "--mode" and b == "cdc" for a, b in pairwise(args))


def check_mode(mode: str) -> None:
    """Refuse a source mode other than 'cdc' or 'batch'."""
    if mode not in MODES:
        raise ValueError("`mode` is 'cdc' or 'batch'")


def check_writable(folder: Path) -> None:
    """Raise `_InitFailed` naming the remedy when this process cannot create a file in `folder` (made if missing)."""
    try:
        make_shared_folder(folder)
        write_probe(folder)
    except OSError as exc:
        found, fix = not_writable(folder, exc)
        raise _InitFailed(
            f"{found}, so `rivet init` could not write the config there; {fix}; nothing was written", _refusal()
        ) from None


def make_shared_folder(folder: Path) -> None:
    """Create `folder` and its missing parents group-writable (g+w, never world-writable); existing ones are untouched."""
    missing = [p for p in (folder, *folder.parents) if not p.exists()]
    for path in reversed(missing):
        path.mkdir(exist_ok=True)
        os.chmod(path, path.stat().st_mode | 0o020)


def _refusal() -> ErrorObject:
    """The error object of a refusal the init makes itself: rivet's `refusal` class, never retried."""
    return ErrorObject(code=None, kind=None, class_="refusal", exit_code=None, retryable=False, action=None)


class _InitFailed(AirflowFailException):
    """A failure of the init itself: the exception's text, and the error object its XCom payload carries."""

    def __init__(self, text: str, error: ErrorObject) -> None:
        super().__init__(text)
        self.error = error


class _SourceTask(_RivetTask):
    """A task about one source before or beside its rivet tasks: its mode, the `env_from_connections` entry of the data
    source, the arguments of its `rivet init`, and rivet commands run with their output in the task log."""

    def __init__(self, *, mode: str, source_env: str | None = None, init_args: Sequence[str] = (), **kwargs: Any) -> None:
        super().__init__(**kwargs)
        check_mode(mode)
        if source_env is not None and not (self.env_from_connections or {}).get(source_env):
            raise ValueError(f"env_from_connections names no `{source_env}`, the source `rivet init` reads")
        owned = {"--source", "--source-env", "--source-file", "-o", "--output"} | ({"--mode"} if mode == "cdc" else set())
        taken = [a for a in init_args if _flag_of(a) in owned]
        if taken:
            raise ValueError(f"init_args may not set {taken}: the operator sets the source, the output and the CDC mode")
        if _asks_for_cdc(init_args):
            raise ValueError("init_args may not ask for `--mode cdc` in a batch DAG: use mode='cdc'")
        self.mode = mode
        self.source_env = source_env
        self.init_args = list(init_args)

    def _rivet(
        self, binary: str, argv: list[str], env: Mapping[str, str], cwd: Path, outcome: str = ", and no config was written"
    ) -> None:
        """Run one rivet command, stoppable by `on_kill`, its output in the task log; a nonzero exit fails the task
        without a retry, its text ending in `outcome`. A stop requested before it starts raises `Stopped`."""
        if self._stop_requested:
            raise Stopped(argv[0])
        output = cwd / f"{argv[0]}.log"
        with output.open("w") as out:
            code = spawn([binary, *argv], out, subprocess.STDOUT, env, str(cwd), self._hold)
        for line in output.read_text(errors="replace").splitlines():
            self.log.info(
                "%s", line
            )  # pragma: no mutate - info(line) is the same line: a logger formats no message given no args
        if code < 0:
            raise _InitFailed(
                f"`rivet {argv[0]}` was killed by signal {-code}; its output is above{outcome}",
                error_from_exit(None, -code),
            )
        if code != 0:
            raise _InitFailed(f"`rivet {argv[0]}` exited {code}; its output is above{outcome}", error_from_exit(code, None))


class RivetInitOperator(_SourceTask):
    """Writes the config a source's DAG is built from: `rivet init` against the source, `rivet check` of what it wrote,
    then the file at `config`, never over one that exists. The data source is the `source_env` entry of
    `env_from_connections`; a `database` named for RIVET_STATE_URL is created on its PostgreSQL server if missing."""

    command = "init"
    template_fields: Sequence[str] = ("config", "state_dir", "init_args")
    ui_color = "#f3e8fd"

    def __init__(self, *, mode: str, source_env: str, init_args: Sequence[str] = (), **kwargs: Any) -> None:
        super().__init__(mode=mode, source_env=source_env, init_args=init_args, **kwargs)
        self.init_source_env = source_env

    def _run_with_secrets(self, ti: Any, run_id: str, task_part: str, secrets: Path) -> dict[str, Any]:
        """Write the config, or report the one already there; `execute` gives the private folder for key files."""
        target = Path(self.config)
        if config_present(target):
            self.log.info(
                "%s exists: the next DAG parse builds this DAG's rivet tasks from it. If this task keeps running instead, "
                "the scheduler does not see this file: put the config folder where both read it",
                target,
            )
            return {"config": str(target), "written": False}
        self._writable_or_failed(ti, target.parent)
        env, binary = self._guarded(ti, lambda: self._prepared(secrets))
        return self._guarded(ti, lambda: self._written(ti, target, env, binary))

    def _writable_or_failed(self, ti: Any, folder: Path) -> None:
        """Fail without a retry, before the state database or rivet, when this worker cannot write the config's folder."""
        try:
            check_writable(folder)
        except _InitFailed as failure:
            self._failed(ti, failure)

    def _prepared(self, secrets: Path) -> tuple[dict[str, str], str]:
        """The environment and the rivet binary `init` runs with, the state database created first."""
        env = self._environment(secrets)
        binary = resolve_binary(self.rivet_bin, env)
        self._create_state_database()
        return env, binary

    def _written(self, ti: Any, target: Path, env: Mapping[str, str], binary: str) -> dict[str, Any]:
        """Write the config; a failure of the init itself fails the task without a retry."""
        try:
            return self._write_config(target, env, binary)
        except _InitFailed as failure:
            self._failed(ti, failure)

    def _failed(self, ti: Any, failure: _InitFailed) -> NoReturn:
        """Publish and log a failure of the init itself, which Airflow does not retry."""
        payload = {"command": self.command, "decision": "fail", "error": failure.error.to_xcom(), "units": []}
        self._fail(ti, payload, str(failure), retry=False)  # pragma: no mutate - `retry` is read as a truth value

    def _create_state_database(self) -> None:
        """CREATE DATABASE for a RIVET_STATE_URL entry that names a `database`, on the server its Connection points at."""
        ref = (self.env_from_connections or {}).get(STATE_URL_ENV)
        ref = database_ref(ref) if ref else {}
        if not ref.get("database"):
            return
        server = self._server(ref)
        name = ref["database"]
        try:
            import psycopg2
        except ImportError:
            raise PreflightRefusal(
                Code.STATE_DATABASE, f"creating state database `{name}` needs psycopg2 on the worker"
            ) from None
        conn = psycopg2.connect(connection_url(with_database(server, server.schema or "postgres")))
        try:
            conn.autocommit = True
            with conn.cursor() as cur:
                cur.execute("SELECT 1 FROM pg_database WHERE datname = %s", (name,))
                if cur.fetchone():
                    self.log.info("state database `%s` exists on %s (Connection `%s`)", name, self._host_of(ref), ref["conn_id"])
                    return
                quoted = '"' + name.replace('"', '""') + '"'
                cur.execute(f"CREATE DATABASE {quoted}")
                self.log.info("state database `%s` created on %s (Connection `%s`)", name, self._host_of(ref), ref["conn_id"])
        finally:
            conn.close()

    def _refuse_shared_places(self, doc: Mapping[str, Any], target: Path) -> None:
        """Refuse a config that writes where another config of the folder writes: one bucket prefix (their files mix) or
        one warehouse dataset or database (two sources' tables of one name become one table)."""
        mine = places(doc)
        for other in sorted(target.parent.glob("*.yaml")):
            if other == target:
                continue
            try:
                shared = sorted(mine & places(read_config(str(other))))
            except PreflightRefusal:
                continue  # a config this reader cannot read is no evidence either way
            if shared:
                raise _InitFailed(
                    f"the config `rivet init` wrote for {target.name} writes to {', '.join(shared)}, as {other} does; give each "
                    f"source its own place in init_args (`--gcs-bucket`/`--s3-bucket`, `--bigquery-dataset {target.stem}`, "
                    f"`--clickhouse-database {target.stem}`); no config was written",
                    _refusal(),
                )

    def _write_config(self, target: Path, env: Mapping[str, str], binary: str) -> dict[str, Any]:
        """`rivet init` into the state folder (where the tasks run the config), `rivet check` there, then `target`."""
        state_dir = Path(self.state_dir or target.parent)
        if not state_dir.parent.is_dir():
            raise _InitFailed(f"{state_dir.parent} does not exist on this worker: mount the state folder first", _refusal())
        make_shared_folder(state_dir)
        work = state_dir / ".init"
        work.mkdir(exist_ok=True)
        draft = work / target.name
        draft.unlink(missing_ok=True)
        mode = ["--mode", "cdc"] if self.mode == "cdc" else []
        self._rivet(binary, ["init", "--source-env", self.init_source_env, *mode, "-o", str(draft), *self.init_args], env, work)
        self._rivet(binary, ["check", "--config", str(draft)], env, work)
        doc = read_config(str(draft))
        make_shared_folder(target.parent)
        self._refuse_shared_places(doc, target)
        staged = target.with_name(f".{target.name}.{os.getpid()}.tmp")
        shutil.copyfile(draft, staged)
        os.chmod(staged, 0o664)
        try:
            os.link(staged, target)
        except FileExistsError:
            self.log.info("%s was written meanwhile; it is kept", target)
            return {"config": str(target), "written": False}
        finally:
            staged.unlink()
        exports = [{"name": name_of(e), "mode": mode_of(e), "tables": tables_of(e)} for e in entries_of(doc)]
        for e in exports:
            self.log.info("export %s (%s)%s", e["name"], e["mode"], f": {', '.join(e['tables'])}" if e["tables"] else "")
        self.log.info("%s written; the next DAG parse replaces this task with the rivet tasks it describes", target)
        return {"config": str(target), "written": True, "exports": exports}


class RivetSpecDriftOperator(RivetInitOperator):
    """Compares a source's config with the include and exclude its sources file names. When they differ and the stream
    has not started (no checkpoint file, no run in rivet's run ledger), it writes the config again with `rivet init`,
    keeps the old one in `<config folder>/.replaced/`, and skips the rest of the run, so the next parse builds the DAG of
    the new config. When it has started, it fails without a retry naming each place to edit, and the stream goes on."""

    template_fields: Sequence[str] = ("config", "state_dir", "init_args", "include", "exclude")
    ui_color = "#fef7e0"

    def __init__(self, *, include: Sequence[str] = (), exclude: Sequence[str] = (), **kwargs: Any) -> None:
        super().__init__(**kwargs)
        self.include = list(include)
        self.exclude = list(exclude)

    def _run_with_secrets(self, ti: Any, run_id: str, task_part: str, secrets: Path) -> dict[str, Any]:
        """Compare, then rewrite the config of a stream that has not started or name what to edit in one that has."""
        target = Path(self.config)
        doc = self._guarded(ti, lambda: read_config(str(target)))
        found = drift(doc, self.mode, self.include, self.exclude)
        if not found:
            self.log.info("%s matches the include and exclude of the sources file", target)
            return {"config": str(target), "drift": False}
        self.log.warning("%s", found.text(target))
        self._writable_or_failed(ti, target.parent)
        env, binary = self._guarded(ti, lambda: self._prepared(secrets))
        started = self._started(doc, env, binary)
        if started:
            self._failed(
                ti,
                _InitFailed(
                    f"{found.text(target)}. The include and exclude of the sources file are applied by `rivet init`, and "
                    f"the stream has started ({'; '.join(started)}), so the config is not written again: edit it at "
                    "the places named (the DAG follows at the next parse; `rivet check` it first), or give the "
                    "sources file the tables the config captures. The stream's other tasks run as before",
                    _refusal(),
                ),
            )
        aside = target.parent / ".replaced" / f"{target.stem}.{datetime.now(UTC):%Y%m%dT%H%M%SZ}.yaml"
        aside.parent.mkdir(exist_ok=True)
        os.replace(target, aside)
        try:
            out = self._guarded(ti, lambda: self._written(ti, target, env, binary))
        except BaseException:
            if not config_present(target):
                os.replace(aside, target)
            raise
        tables = sorted({t for e in out.get("exports") or () for t in e["tables"]})
        raise AirflowSkipException(
            f"{target} written again by `rivet init` with the include and exclude of the sources file "
            f"({', '.join(tables) or 'no table'}); the old config is {aside}. The rest of this run is skipped: the next "
            "parse builds the DAG of the new config, and its next run starts the stream"
        )

    def _started(self, doc: Mapping[str, Any], env: Mapping[str, str], binary: str) -> list[str]:
        """What says the stream has started: a checkpoint file of an export in the state folder, a run in rivet's run
        ledger, or a ledger that could not be read."""
        base = Path(self.state_dir or Path(self.config).parent)
        out = [f"the checkpoint {path} exists" for path in checkpoints(doc, base).values() if path.exists()]
        runs = self._runs(env, binary)
        if runs is None:
            out.append("rivet's run ledger could not be read (`rivet state runs`; its output is above)")
        elif runs:
            out.append(f"rivet's run ledger has a run ({runs[0].get('run_id') if isinstance(runs[0], dict) else runs[0]})")
        return out

    def _runs(self, env: Mapping[str, str], binary: str) -> list[Any] | None:
        """The latest run of rivet's run ledger (`rivet state runs --json`), or None when rivet could not say."""
        work = Path(self.state_dir or Path(self.config).parent) / ".init"
        work.mkdir(parents=True, exist_ok=True)
        stdout, stderr = work / "state-runs.json", work / "state-runs.log"
        argv = [binary, "state", "runs", "--config", str(self.config), "--json", "--last", "1"]
        with stdout.open("w") as out, stderr.open("w") as err:
            code = spawn(argv, out, err, env, str(work), self._hold)
        for line in stderr.read_text(errors="replace").splitlines():
            self.log.info(
                "%s", line
            )  # pragma: no mutate - info(line) is the same line: a logger formats no message given no args
        runs = _json_of(stdout.read_bytes()) if code == 0 else None
        return runs if isinstance(runs, list) else None


class RivetPreflightOperator(_SourceTask):
    """Checks, first in a source's DAG, what its tasks will need, one line each (`checks`): the rivet binary, the config
    and state folders, the state database, the source's CDC settings, the bucket and dataset before the stream starts,
    and `rivet check` of a config. A FAIL fails the task without a retry; a check with no client library is a SKIP."""

    command = "preflight"
    template_fields: Sequence[str] = ("config", "state_dir", "init_args")
    ui_color = "#e6f4ea"

    def _config_doc(self, variables: Mapping[str, str] | None = None) -> dict[str, Any]:
        """The source's config once `init` wrote it, else none."""
        if not config_present(Path(self.config)):
            return {}
        return read_config(self.config, {**(self.env or {}), **(variables or {})})

    def _run_with_secrets(self, ti: Any, run_id: str, task_part: str, secrets: Path) -> dict[str, Any]:
        """Run every check, log its line, and fail without a retry when one failed."""
        env = self._guarded(ti, lambda: self._environment(secrets))
        found = self._guarded(ti, lambda: self._checks(env))
        levels = {PASS: logging.INFO, SKIP: logging.INFO, WARN: logging.WARNING, FAIL: logging.ERROR}
        for check in found:
            self._write(levels[check.status], check.line())
        counts = checks.summary(found)
        self.log.info("preflight: %s", ", ".join(f"{n} {s}" for s, n in counts.items()))
        bad = checks.failed(found)
        payload = {"command": self.command, "decision": "fail" if bad else "pass", "checks": [c.line() for c in found]}
        if bad:
            text = f"preflight: {len(bad)} check(s) failed: {'; '.join(c.name for c in bad)}; each line above says what to change"
            self._fail(ti, {**payload, "error": _refusal().to_xcom(), "units": []}, text, retry=False)
        return payload

    def _checks(self, env: Mapping[str, str]) -> list[Check]:
        """Every check of the source, in the order its tasks need them."""
        try:
            doc = self._config_doc(env)
        except PreflightRefusal as refusal:
            return [Check("config", FAIL, str(refusal), f"fix {self.config}, or move it aside so `init` writes it again")]
        state_dir = Path(self.state_dir or Path(self.config).parent)
        binary, out = self._binary(env)
        out.append(checks.folder("config folder", Path(self.config).parent))
        if state_dir.exists():
            out.append(checks.folder("state folder", state_dir))
        else:
            out.append(checks.folder("state folder", state_dir.parent, must_exist=True))
        refs = {name: database_ref(ref) for name, ref in (self.env_from_connections or {}).items()}
        if STATE_URL_ENV in refs:
            state = refs[STATE_URL_ENV]
            out.append(checks.state_tls(self._server(state), state["conn_id"]))
            out += checks.state(self._server(state), state.get("database"), state["conn_id"])
        if self.mode == "cdc":
            out += self._source(doc, refs)
        if any(path.exists() for path in checkpoints(doc, state_dir).values()):
            out.append(Check("bucket and dataset", SKIP, "the stream has a checkpoint, so its first run reached them"))
        else:
            out += self._staging(doc, env)
        if doc and binary:
            out.append(self._rivet_check(binary, env, state_dir))
        return out

    def _binary(self, env: Mapping[str, str]) -> tuple[str | None, list[Check]]:
        """The rivet binary the tasks will run, and whether it is new enough for this provider."""
        floor = ".".join(str(n) for n in MIN_RIVET_VERSION)
        fix = f"pip install 'rivet-cli>={floor}' in the image every Airflow service runs"
        try:
            binary = resolve_binary(self.rivet_bin, env)
            caps = probe(binary)
        except (PreflightRefusal, ProbeFailed) as exc:
            return None, [Check("rivet binary", FAIL, str(exc), fix)]
        if not caps.new_enough():
            return binary, [
                Check("rivet binary", FAIL, f"rivet {caps.version_text} at {binary}; this provider needs {floor} or later", fix)
            ]
        return binary, [Check("rivet binary", PASS, f"rivet {caps.version_text} at {binary}")]

    def _source(self, doc: Mapping[str, Any], refs: Mapping[str, Mapping[str, Any]]) -> list[Check]:
        """The source's CDC settings, read with the Connection its tasks read it with."""
        name = self.source_env or str(_mapping_of(doc.get("source")).get("url_env") or "")
        ref = refs.get(name)
        if ref is None:
            return [
                Check("source", SKIP, f"env_from_connections names no `{name or 'source'}` entry, so the source was not reached")
            ]
        conn = self._database(ref)
        kind = str(getattr(conn, "conn_type", "") or "")
        if kind == "mysql":
            return checks.mysql_source(conn, server_id_of(doc, checks.RIVET_DEFAULT_SERVER_ID) if doc else None)
        if kind in ("postgres", "postgresql"):
            return checks.postgres_source(conn)
        return [
            Check("source", SKIP, f"Connection `{ref['conn_id']}` is of type `{kind}`; only MySQL and PostgreSQL are checked")
        ]

    def _staging(self, doc: Mapping[str, Any], env: Mapping[str, str]) -> list[Check]:
        """The bucket and the dataset the stream writes, from the config or, before it exists, the `init_args`."""
        staged = staging_of(doc) if doc else {}
        if not doc:
            flags = {"--gcs-bucket": "bucket", "--bigquery-project": "project", "--bigquery-dataset": "dataset"}
            staged = {key: value for flag, key in flags.items() if (value := _value_of(self.init_args, flag))}
        if not staged.get("bucket"):
            return [Check("bucket", SKIP, "the config names no GCS bucket")]
        key_file = env.get("GOOGLE_APPLICATION_CREDENTIALS")
        out, location = checks.gcs(staged["bucket"], staged.get("prefix", ""), key_file=key_file)
        if staged.get("project") and staged.get("dataset"):
            out += checks.bigquery(staged["project"], staged["dataset"], location, key_file=key_file)
        return out

    def _rivet_check(self, binary: str, env: Mapping[str, str], state_dir: Path) -> Check:
        """`rivet check` of the config, its output in the task log."""
        work = state_dir / ".init"
        try:
            make_shared_folder(work)
            self._rivet(binary, ["check", "--config", str(self.config)], env, work, outcome="")
        except OSError as exc:
            return Check("rivet check", SKIP, f"{work} could not be made ({exc.strerror or exc}), so it did not run")
        except _InitFailed as failure:
            return Check("rivet check", FAIL, str(failure), "its output is above")
        return Check("rivet check", PASS, f"{self.config} checks clean")


def _mapping_of(value: Any) -> Mapping[str, Any]:
    """`value` when it is a mapping, else an empty one."""
    return value if isinstance(value, Mapping) else {}


def _value_of(args: Sequence[str], flag: str) -> str | None:
    """The value a command line gives `flag` (`--flag value` or `--flag=value`); the last one wins."""
    value = None
    for i, arg in enumerate(args):
        if arg == flag and i + 1 < len(args):
            value = args[i + 1]
        elif arg.startswith(f"{flag}="):
            value = arg.partition("=")[2]
    return value


class RivetWaveBarrier(BaseOperator):
    """Does nothing: an ordering point between two waves, run when the wave before it has finished."""

    ui_color = "#f1f3f4"

    def execute(self, context: Any) -> None:
        """Nothing to run."""
        return None


class RivetConfigRefused(BaseOperator):
    """The one task of a source's DAG whose config could not be built into rivet tasks: it fails, naming the refusal, so
    the DAG is there to be seen and its run is red, and the other sources of the DAG file are built as they are."""

    ui_color = "#fce8e6"
    template_fields: Sequence[str] = ()

    def __init__(self, *, refusal: str, code: str, **kwargs: Any) -> None:
        super().__init__(**kwargs)
        self.refusal = refusal
        self.code = code

    def execute(self, context: Any) -> None:
        """Publish the refusal as every refused task does, then fail without retry."""
        context["ti"].xcom_push(key=XCOM_KEY, value=unstarted("config", REFUSED, self.code))
        raise AirflowFailException(self.refusal)


class RivetRunWatcher(BaseOperator):
    """Fails when any task of the run failed, so the DAG run is failed whichever tasks are its leaves."""

    ui_color = "#f1f3f4"

    def execute(self, context: Any) -> None:
        """Runs only under `trigger_rule="one_failed"`; always fails."""
        raise AirflowFailException("a task of this DAG run failed; see the failed and upstream_failed tasks")
