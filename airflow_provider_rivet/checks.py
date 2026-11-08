"""The checks of a source's `preflight` task: one PASS / WARN / FAIL / SKIP line each, with what to change."""

from __future__ import annotations

import ipaddress
import os
import tempfile
import uuid
from collections.abc import Callable, Iterable, Mapping, Sequence
from dataclasses import dataclass
from pathlib import Path
from time import sleep
from typing import Any
from urllib.parse import parse_qsl

from .sources import flag_of

PASS, WARN, FAIL, SKIP = "PASS", "WARN", "FAIL", "SKIP"
MYSQL_CDC_SETTINGS = (
    ("log_bin", ("1", "ON"), "enable the binary log: `log_bin` (and a `server_id`) under [mysqld], then restart"),
    ("binlog_format", ("ROW",), "SET PERSIST binlog_format = 'ROW';"),
    ("binlog_row_image", ("FULL",), "SET PERSIST binlog_row_image = 'FULL';"),
    ("binlog_row_metadata", ("FULL",), "SET PERSIST binlog_row_metadata = 'FULL';"),
)
MYSQL_GRANTS = ("REPLICATION SLAVE", "REPLICATION CLIENT", "SELECT")
MYSQL_GRANT_NAMES = {"BINLOG MONITOR": "REPLICATION CLIENT", "REPLICATION REPLICA": "REPLICATION SLAVE"}
RIVET_DEFAULT_SERVER_ID = 4271
STATE_SSLMODES = ("disable", "require", "verify-ca", "verify-full")
GCP_SCOPE = "https://www.googleapis.com/auth/cloud-platform"
DISCOVERY_FLAGS = ("--include", "--exclude", "--schema", "--table", "--tls", "--tls-ca")


@dataclass(frozen=True)
class Check:
    """One check's outcome: what was checked, PASS / WARN / FAIL / SKIP, what was found, and what to change."""

    name: str
    status: str
    detail: str
    fix: str = ""

    def line(self) -> str:
        """`PASS state database: ...`, with `-> fix: ...` when there is something to change."""
        fix = f" -> fix: {self.fix}" if self.fix and self.status != PASS else ""
        return f"{self.status:<4} {self.name}: {self.detail}{fix}"


def write_probe(folder: Path) -> None:
    """Create and remove a file in `folder`; OSError when this process cannot."""
    handle, probe = tempfile.mkstemp(dir=folder, prefix=".rivet-airflow-probe-")
    os.close(handle)
    os.unlink(probe)


def not_writable(folder: Path, exc: OSError) -> tuple[str, str]:
    """What a failed `write_probe` found (who this process is, who owns the nearest existing folder) and the remedy."""
    owner = next((p for p in (folder, *folder.parents) if p.exists()), folder)
    st = owner.stat()
    uid, gid = os.getuid(), os.getgid()
    found = (
        f"{folder} is not writable by this worker (uid {uid}, gid {gid}: {exc.strerror or exc}); {owner} is owned by uid "
        f"{st.st_uid}, gid {st.st_gid}, mode {st.st_mode & 0o7777:o}"
    )
    fix = (
        f"on the host, `sudo chown -R {uid}:0 <folder> && sudo chmod -R g+w <folder>` (the official Airflow image runs as "
        "AIRFLOW_UID, 50000 by default, in group 0), not a world-writable folder"
    )
    return found, fix


def folder(name: str, path: Path, must_exist: bool = False) -> Check:
    """Whether this worker can create a file in `path` or its nearest existing parent; a `must_exist` one is never made."""
    if must_exist and not path.is_dir():
        return Check(
            name, FAIL, f"{path} does not exist on this worker", "mount the volume at this path in every Airflow service"
        )
    probe_dir = next((p for p in (path, *path.parents) if p.exists()), path)
    try:
        write_probe(probe_dir)
    except OSError as exc:
        return Check(name, FAIL, *not_writable(probe_dir, exc))
    return Check(name, PASS, f"{path} is writable" if probe_dir == path else f"{probe_dir} is writable; {path} is created there")


CONNECT_TRIES = 3


def _reach(connect: Callable[[Any], Any], target: Any) -> Any:
    """`connect(target)`, tried `CONNECT_TRIES` times 1s, 2s, ... apart; the last failure is raised."""
    for attempt in range(1, CONNECT_TRIES):
        try:
            return connect(target)
        except Exception:
            sleep(attempt)
    return connect(target)


def _optional(*modules: str) -> Any:
    """The first of `modules` this worker can import, or None."""
    for module in modules:
        try:
            return __import__(module, fromlist=["_"])
        except ImportError:
            continue
    return None


def state(server: Any, database: str | None, conn_id: str, connect: Callable[[Any], Any] | None = None) -> list[Check]:
    """The state database the tasks will use, by name and server, and whether it exists or can be created."""
    target = database or server.schema or server.login
    where = f"`{target}` on {server.host}:{server.port or 5432} (Connection `{conn_id}`)"
    name = "state database"
    if connect is None:
        psycopg2 = _optional("psycopg2")
        if psycopg2 is None:
            return [
                Check(name, SKIP, f"{where}; psycopg2 is not installed, so it was not reached", "pip install psycopg2-binary")
            ]
        from .connections import connection_url, with_database

        def _connect(c: Any) -> Any:
            return psycopg2.connect(connection_url(with_database(c, c.schema or "postgres")))

        connect = _connect

    try:
        db = _reach(connect, server)
    except Exception as exc:
        return [
            Check(
                name,
                FAIL,
                f"{where}: cannot connect after {CONNECT_TRIES} tries ({type(exc).__name__}: {exc})",
                f"check Connection `{conn_id}`",
            )
        ]
    try:
        with db.cursor() as cur:
            cur.execute("SELECT 1 FROM pg_database WHERE datname = %s", (target,))
            exists = cur.fetchone() is not None
            cur.execute("SELECT rolcreatedb OR rolsuper FROM pg_roles WHERE rolname = current_user")
            row = cur.fetchone()
            can_create = bool(row and row[0])
    finally:
        db.close()
    if exists:
        return [Check(name, PASS, f"{where} exists; every task of this source reads and writes its state there")]
    if database and can_create:
        return [Check(name, PASS, f"{where} does not exist yet; `init` creates it (the role may CREATEDB)")]
    if can_create:
        return [
            Check(
                name,
                FAIL,
                f"{where} does not exist; `init` creates only a state database it names",
                f'CREATE DATABASE "{target}";',
            )
        ]
    return [
        Check(
            name,
            FAIL,
            f"{where} does not exist and the Connection's role may not create it",
            f'CREATE DATABASE "{target}"; or ALTER ROLE {server.login} CREATEDB;',
        )
    ]


def state_tls(server: Any, conn_id: str) -> Check:
    """Whether rivet will dial the state server: a remote host needs an `sslmode` rivet negotiates, or `disable`."""
    extra = getattr(server, "extra_dejson", None) or {}
    params = dict(parse_qsl(extra.get("rivet_params")))
    mode = str(params.get("sslmode") or extra.get("sslmode") or "")
    host = str(server.host or "")
    if mode in STATE_SSLMODES:
        return Check("state database tls", PASS, f"Connection `{conn_id}` says sslmode={mode}")
    if host and all(_loopback(h) for h in host.split(",")):
        return Check("state database tls", PASS, f"{host} is this machine, so no sslmode is needed")
    asked = f"says sslmode={mode}, which rivet cannot negotiate" if mode else "names no sslmode"
    return Check(
        "state database tls",
        FAIL,
        f"Connection `{conn_id}` points at {host}, a remote host, and {asked}; rivet refuses it (RIVET_STATE_URL_TLS_REQUIRED)",
        "set the Connection's extra sslmode: `require` (or `verify-ca`, `verify-full`) with TLS on the state server, "
        "or `disable` for plaintext on a private network",
    )


def _loopback(host: str) -> bool:
    """Whether a host stays on this machine as rivet reads it: localhost, a loopback address, or a socket path."""
    host = host.strip().removeprefix("[").removesuffix("]")
    if host.lower() == "localhost" or host.startswith("/"):
        return True
    try:
        address = ipaddress.ip_address(host)
    except ValueError:
        return False
    return address.is_loopback and getattr(address, "ipv4_mapped", None) is None


def mysql_source(
    source: Any, server_id: int | None, connect: Callable[[Any], Any] | None = None, tls: str | None = None
) -> list[Check]:
    """The MySQL settings, grants and replica id a CDC stream needs, read from the source as rivet will read them."""
    name = "mysql cdc"
    sql = "SELECT @@log_bin, @@binlog_format, @@binlog_row_image, @@binlog_row_metadata; SHOW GRANTS"
    if connect is None:
        driver = _optional("MySQLdb", "pymysql")
        if driver is None:
            return [
                Check(
                    name,
                    SKIP,
                    "no MySQL driver on this worker (MySQLdb or pymysql), so it was not reached",
                    f"run by hand: {sql}",
                )
            ]

        def _connect(c: Any) -> Any:
            password = "passwd" if driver.__name__ == "MySQLdb" else "password"
            args = {
                "host": c.host,
                "port": int(c.port or 3306),
                "user": c.login,
                password: c.password or "",
                "connect_timeout": 10,
            }
            if tls not in (None, "disable"):
                args.update(
                    {"ssl_mode": "REQUIRED"} if driver.__name__ == "MySQLdb" else {"ssl": {"fake_flag_to_enable_tls": True}}
                )
            return driver.connect(**args)

        connect = _connect

    try:
        db = _reach(connect, source)
    except Exception as exc:
        return [
            Check(
                name,
                FAIL,
                f"cannot connect to {source.host}:{source.port or 3306} after {CONNECT_TRIES} tries ({type(exc).__name__})",
                f"run by hand: {sql}",
            )
        ]
    out: list[Check] = []
    try:
        cur = db.cursor()
        values = {}
        for variable, _, _ in MYSQL_CDC_SETTINGS:
            values[variable] = _first(cur, f"SELECT @@global.{variable}")
        for variable, wanted, fix in MYSQL_CDC_SETTINGS:
            got = values[variable]
            if got is None:
                out.append(Check(f"mysql {variable}", WARN, "could not be read", f"run by hand: SELECT @@global.{variable};"))
                continue
            ok = str(got).upper() in wanted
            out.append(Check(f"mysql {variable}", PASS if ok else FAIL, f"is {got}", "" if ok else fix))
        replicating = _rows(cur, "SHOW REPLICA STATUS") or _rows(cur, "SHOW SLAVE STATUS")
        if replicating:
            relog = _first(cur, "SELECT @@global.log_replica_updates") or _first(cur, "SELECT @@global.log_slave_updates")
            ok = str(relog).upper() in ("1", "ON")
            out.append(
                Check(
                    "mysql log_replica_updates",
                    PASS if ok else FAIL,
                    f"this server is a replica and log_replica_updates is {relog}",
                    "" if ok else "log_replica_updates = ON under [mysqld] (read-only at run time), then restart",
                )
            )
        held = _mysql_grants(str(r[0]) for r in _rows(cur, "SHOW GRANTS"))
        missing = [g for g in MYSQL_GRANTS if g not in held]
        out.append(
            Check(
                "mysql grants",
                FAIL if missing else PASS,
                f"missing {', '.join(missing)}" if missing else "REPLICATION SLAVE, REPLICATION CLIENT, SELECT",
                f"GRANT {', '.join(missing)} ON *.* TO '{source.login}'@'%';" if missing else "",
            )
        )
        own = _first(cur, "SELECT @@global.server_id")
        if server_id is None:
            out.append(Check("mysql server_id", SKIP, "`rivet init` names the stream's server_id; the next run checks it"))
            return out
        wanted_id = server_id
        peers = [r for r in _rows(cur, "SHOW REPLICAS") or _rows(cur, "SHOW SLAVE HOSTS") if r and str(r[0]) == str(wanted_id)]
        clash = str(own) == str(wanted_id) or peers
        out.append(
            Check(
                "mysql server_id",
                WARN if clash else PASS,
                f"the stream's server_id {wanted_id} "
                + ("is taken (the server's own or a connected replica's)" if clash else f"is free (the server's is {own})"),
                "set `cdc.server_id` in the config to an id no replica of this server uses" if clash else "",
            )
        )
    finally:
        db.close()
    return out


def _mysql_grants(lines: Iterable[str]) -> set[str]:
    """The privileges `SHOW GRANTS` gives the stream: replication ones only `ON *.*`, SELECT on any database."""
    held: set[str] = set()
    for line in lines:
        privileges, _, target = line.upper().removeprefix("GRANT ").partition(" ON ")
        names = {MYSQL_GRANT_NAMES.get(n.strip(), n.strip()) for n in privileges.split(",")}
        names = set(MYSQL_GRANTS) if "ALL PRIVILEGES" in names else names
        held |= names if target.startswith("*.*") else names & {"SELECT"}
    return held


def _first(cur: Any, sql: str) -> Any:
    """The first value of a query, or None when the server refuses it (an unknown variable on an older MySQL)."""
    try:
        cur.execute(sql)
        row = cur.fetchone()
    except Exception:
        return None
    return row[0] if row else None


def _rows(cur: Any, sql: str) -> list[Any]:
    """Every row of a query, or none when the server refuses it."""
    try:
        cur.execute(sql)
        return list(cur.fetchall() or [])
    except Exception:
        return []


def postgres_source(source: Any, slot: str | None = None, connect: Callable[[Any], Any] | None = None) -> list[Check]:
    """What a PostgreSQL CDC stream needs: `wal_level = logical`, a role with REPLICATION, a free replication slot."""
    name = "postgres cdc"
    if connect is None:
        psycopg2 = _optional("psycopg2")
        if psycopg2 is None:
            return [Check(name, SKIP, "psycopg2 is not installed, so the source was not reached", "run by hand: SHOW wal_level;")]
        from .connections import connection_url

        def _connect(c: Any) -> Any:
            return psycopg2.connect(connection_url(c, "postgresql"))

        connect = _connect

    try:
        db = _reach(connect, source)
    except Exception as exc:
        return [
            Check(
                name,
                FAIL,
                f"cannot connect to {source.host} after {CONNECT_TRIES} tries ({type(exc).__name__})",
                "check the source Connection",
            )
        ]
    try:
        with db.cursor() as cur:
            wal = _first(cur, "SHOW wal_level")
            replication = _first(cur, "SELECT rolreplication OR rolsuper FROM pg_roles WHERE rolname = current_user")
            free = _first(
                cur,
                "SELECT current_setting('max_replication_slots')::int - (SELECT count(*) FROM pg_replication_slots)",
            )
            slots = _first(cur, "SELECT array_agg(slot_name) FROM pg_replication_slots") or []
    finally:
        db.close()
    return [
        Check(
            "postgres wal_level",
            PASS if wal == "logical" else FAIL,
            f"is {wal}",
            "ALTER SYSTEM SET wal_level = logical; then restart",
        ),
        Check(
            "postgres replication role",
            PASS if replication else FAIL,
            "the role may replicate" if replication else "the role has no REPLICATION",
            f"ALTER ROLE {source.login} REPLICATION;",
        ),
        Check("postgres replication slots", PASS, f"the stream's slot `{slot}` exists")
        if slot in slots
        else Check(
            "postgres replication slots",
            PASS if (free or 0) > 0 else FAIL,
            f"{free} free",
            "raise max_replication_slots, or drop an unused slot",
        ),
    ]


def _same_place(dataset: str, bucket: str) -> bool:
    """Whether a load can read the bucket from the dataset: one location, or a region inside the other's multi-region."""
    a, b = dataset.lower(), bucket.lower()
    regions = {"us": "us-", "eu": "europe-"}
    return a == b or (a in regions and b.startswith(regions[a])) or (b in regions and a.startswith(regions[b]))


def _google_client(module: str, key_file: str | None, project: str | None = None) -> Any:
    """A client of `module` on rivet's key file or the default credentials; None when `module` is not installed."""
    lib = _optional(module)
    if lib is None:
        return None
    if not key_file:
        return lib.Client(project=project) if project else lib.Client()
    credentials, own_project = _optional("google.auth").load_credentials_from_file(key_file, scopes=[GCP_SCOPE])
    return lib.Client(credentials=credentials, project=project or own_project)


def _credentials_failed(name: str, key_file: str | None, exc: Exception) -> Check:
    """The check of a client that could not be made from the worker's credentials."""
    source = f"the key file {key_file}" if key_file else "the default credentials"
    return Check(
        name,
        FAIL,
        f"cannot use {source} ({type(exc).__name__}: {exc})",
        "GOOGLE_APPLICATION_CREDENTIALS names a file every worker can read: a service account key or the file of "
        "`gcloud auth application-default login`",
    )


def gcs(bucket: str, prefix: str, client: Any = None, key_file: str | None = None) -> tuple[list[Check], str | None]:
    """Write, list and delete a small object under the stream's prefix, as rivet will; the bucket's location."""
    name = f"gcs gs://{bucket}/{prefix}"
    if client is None:
        try:
            client = _google_client("google.cloud.storage", key_file)
        except Exception as exc:
            return [_credentials_failed(name, key_file, exc)], None
        if client is None:
            return [
                Check(name, SKIP, "google-cloud-storage is not installed", f"run by hand: gcloud storage ls gs://{bucket}/")
            ], None
    try:
        handle = client.lookup_bucket(bucket)
    except Exception as exc:
        return [
            Check(
                name,
                FAIL,
                f"cannot read the bucket ({type(exc).__name__}: {exc})",
                "grant the worker's identity roles/storage.objectAdmin on it",
            )
        ], None
    if handle is None:
        return [
            Check(
                name, FAIL, "the bucket does not exist", f"gcloud storage buckets create gs://{bucket} --location=<the dataset's>"
            )
        ], None
    blob = handle.blob(f"{prefix}.rivet-airflow-preflight-{uuid.uuid4().hex}")
    try:
        blob.upload_from_string(b"")
        list(client.list_blobs(bucket, prefix=prefix, max_results=1))
        blob.delete()
    except Exception as exc:
        return [
            Check(
                name,
                FAIL,
                f"cannot write, list and delete there ({type(exc).__name__}: {exc})",
                "grant the worker's identity roles/storage.objectAdmin on the bucket",
            )
        ], handle.location
    return [Check(name, PASS, f"exists in {handle.location}; an object was written, listed and deleted")], handle.location


def bigquery(
    project: str, dataset: str, bucket_location: str | None, client: Any = None, key_file: str | None = None
) -> list[Check]:
    """The dataset exists, sits where the bucket is, and the identity can create and drop a table in it."""
    name = f"bigquery {project}.{dataset}"
    if client is None:
        try:
            client = _google_client("google.cloud.bigquery", key_file, project)
        except Exception as exc:
            return [_credentials_failed(name, key_file, exc)]
        if client is None:
            return [Check(name, SKIP, "google-cloud-bigquery is not installed", f"run by hand: bq show {project}:{dataset}")]
    try:
        ds = client.get_dataset(f"{project}.{dataset}")
    except Exception as exc:
        missing = type(exc).__name__ == "NotFound"
        return [
            Check(
                name,
                FAIL,
                "the dataset does not exist" if missing else f"cannot read it ({type(exc).__name__}: {exc})",
                f"bq mk --dataset --location=<the bucket's> {project}:{dataset}"
                if missing
                else "grant roles/bigquery.dataEditor and roles/bigquery.jobUser",
            )
        ]
    out = []
    if bucket_location and not _same_place(str(ds.location), str(bucket_location)):
        out.append(
            Check(
                f"{name} location",
                WARN,
                f"the dataset is in {ds.location}, the bucket in {bucket_location}",
                "a load reads a bucket in the dataset's location (or a region inside its multi-region); put them together",
            )
        )
    table = f"{project}.{dataset}._rivet_airflow_preflight_{uuid.uuid4().hex[:12]}"
    try:
        client.query(f"CREATE TABLE `{table}` (id INT64); DROP TABLE `{table}`;").result()
    except Exception as exc:
        out.append(
            Check(
                name,
                FAIL,
                f"cannot create and drop a table there ({type(exc).__name__}: {exc})",
                "grant roles/bigquery.dataEditor on the dataset and roles/bigquery.jobUser on the project",
            )
        )
        return out
    out.insert(0, Check(name, PASS, f"exists in {ds.location}; a table was created and dropped"))
    return out


def discovery_args(init_args: Sequence[str]) -> list[str]:
    """The arguments of `init_args` that choose the tables, for `rivet init --discover`, which refuses the others."""
    out: list[str] = []
    keep = False
    for arg in init_args:
        if arg.startswith("-"):
            keep = flag_of(arg) in DISCOVERY_FLAGS
        if keep:
            out.append(arg)
    return out


def baseline(discovery: Mapping[str, Any], config: str) -> Check:
    """Whether the first run reads a table rivet would page in one SELECT, as `rivet init --mode cdc` lays it out."""
    name = "baseline"
    tables = [t for t in discovery.get("tables") or [] if isinstance(t, Mapping)]
    if not tables:
        return Check(name, WARN, "`rivet init --discover` finds no table", "check the source's `schema`, `include` and `exclude`")
    kind = discovery.get("source_type")
    if len(tables) > 1 and (kind == "mysql" or (kind == "postgres" and all(t.get("schema") == "public" for t in tables))):
        return Check(name, PASS, f"{len(tables)} tables in one stream; each baseline is read through its batch recipe")
    paged = [t for t in tables if t.get("suggested_mode") not in (None, "full")]
    if not paged:
        return Check(name, PASS, f"{_tables(tables)} read in one SELECT each, as rivet suggests")
    return Check(
        name,
        WARN,
        f"{_tables(paged)} read in one SELECT each on one connection though rivet would page them, and "
        "`tuning.statement_timeout_s` stops a SELECT (300 s under the `balanced` profile)",
        f"after `init`, in {config} replace `initial: snapshot` with `backfill: auto` and add a batch export of each of these "
        "tables as its recipe (`mode: chunked`, `chunk_by_key: <key>`, `parallel: 4`); raising `tuning.statement_timeout_s` "
        "instead keeps one SELECT (rivet-lab/rivet#878)",
    )


def _tables(tables: Sequence[Mapping[str, Any]]) -> str:
    """The tables by name with rivet's estimate of their rows."""
    named = []
    for t in tables:
        rows = t.get("row_estimate")
        label = ".".join(str(p) for p in (t.get("schema"), t.get("table")) if p)
        named.append(f"`{label}` (~{rows:,} rows)" if isinstance(rows, int) else f"`{label}`")
    return ", ".join(named)


def failed(checks: Sequence[Check]) -> list[Check]:
    """The checks that failed."""
    return [c for c in checks if c.status == FAIL]


def summary(checks: Sequence[Check]) -> Mapping[str, int]:
    """How many checks ended each way."""
    return {s: sum(1 for c in checks if c.status == s) for s in (PASS, WARN, FAIL, SKIP)}
