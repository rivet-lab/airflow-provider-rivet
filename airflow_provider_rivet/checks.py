"""The checks of a source's `preflight` task: one line each, PASS / WARN / FAIL / SKIP, with what to change.

Each check reaches one thing the DAG's tasks will need (the rivet binary, the state server, the folders, the source's
CDC settings, the bucket, the dataset) the way those tasks will, and says what is wrong in the words of the setting to
change. A check whose client library is not on the worker (psycopg2, MySQLdb or pymysql, google-cloud-storage,
google-cloud-bigquery) is a SKIP naming the command to run by hand; it never fails the task.
"""

from __future__ import annotations

import os
import tempfile
import uuid
from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass
from pathlib import Path
from typing import Any

PASS, WARN, FAIL, SKIP = "PASS", "WARN", "FAIL", "SKIP"
MYSQL_CDC_SETTINGS = (
    ("log_bin", ("1", "ON"), "enable the binary log: `log_bin` (and a `server_id`) under [mysqld], then restart"),
    ("binlog_format", ("ROW",), "SET PERSIST binlog_format = 'ROW';"),
    ("binlog_row_image", ("FULL",), "SET PERSIST binlog_row_image = 'FULL';"),
    ("binlog_row_metadata", ("FULL",), "SET PERSIST binlog_row_metadata = 'FULL';"),
)
MYSQL_GRANTS = ("REPLICATION SLAVE", "REPLICATION CLIENT", "SELECT")
DEFAULT_SERVER_ID = 4271


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


def folder(name: str, path: Path, must_exist: bool = False) -> Check:
    """Whether this worker can create a file in `path`; a folder `must_exist` (a mounted volume) is never created."""
    if must_exist and not path.is_dir():
        return Check(name, FAIL, f"{path} does not exist on this worker", "mount the volume at this path in every Airflow service")
    probe_dir = next((p for p in (path, *path.parents) if p.exists()), path)
    try:
        handle, probe = tempfile.mkstemp(dir=probe_dir, prefix=".rivet-airflow-probe-")
        os.close(handle)
        os.unlink(probe)
    except OSError as exc:
        st = probe_dir.stat()
        return Check(
            name,
            FAIL,
            f"{probe_dir} is not writable by uid {os.getuid()}, gid {os.getgid()} ({exc.strerror or exc}); owner uid "
            f"{st.st_uid}, gid {st.st_gid}, mode {st.st_mode & 0o7777:o}",
            f"sudo chown -R {os.getuid()}:0 <folder> && sudo chmod -R g+w <folder> on the host",
        )
    return Check(name, PASS, f"{path} is writable")


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
    where = f"`{database or server.schema}` on {server.host}:{server.port or 5432} (Connection `{conn_id}`)"
    name = "state database"
    if connect is None:
        psycopg2 = _optional("psycopg2")
        if psycopg2 is None:
            return [Check(name, SKIP, f"{where}; psycopg2 is not installed, so it was not reached", "pip install psycopg2-binary")]
        from .connections import connection_url, with_database

        def connect(c: Any) -> Any:
            return psycopg2.connect(connection_url(with_database(c, c.schema or "postgres")))

    try:
        db = connect(server)
    except Exception as exc:
        return [Check(name, FAIL, f"{where}: cannot connect ({type(exc).__name__}: {exc})", f"check Connection `{conn_id}`")]
    try:
        with db.cursor() as cur:
            target = database or server.schema
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
    return [
        Check(
            name,
            FAIL,
            f"{where} does not exist and the Connection's role may not create it",
            f"CREATE DATABASE \"{target}\"; or ALTER ROLE {server.login} CREATEDB;",
        )
    ]


def mysql_source(source: Any, server_id: int | None, connect: Callable[[Any], Any] | None = None) -> list[Check]:
    """The MySQL settings, grants and replica id a CDC stream needs, read from the source as rivet will read them."""
    name = "mysql cdc"
    sql = (
        "SELECT @@log_bin, @@binlog_format, @@binlog_row_image, @@binlog_row_metadata, @@log_replica_updates; "
        "SHOW GRANTS"
    )
    if connect is None:
        driver = _optional("MySQLdb", "pymysql")
        if driver is None:
            return [Check(name, SKIP, "no MySQL driver on this worker (MySQLdb or pymysql), so it was not reached", f"run by hand: {sql}")]

        def connect(c: Any) -> Any:
            password = "passwd" if driver.__name__ == "MySQLdb" else "password"
            args = {"host": c.host, "port": int(c.port or 3306), "user": c.login, password: c.password or "", "connect_timeout": 10}
            return driver.connect(**args)

    try:
        db = connect(source)
    except Exception as exc:
        return [Check(name, FAIL, f"cannot connect to {source.host}:{source.port or 3306} ({type(exc).__name__})", f"run by hand: {sql}")]
    out: list[Check] = []
    try:
        cur = db.cursor()
        values = {}
        for variable, _, _ in MYSQL_CDC_SETTINGS:
            values[variable] = _first(cur, f"SELECT @@global.{variable}")
        for variable, wanted, fix in MYSQL_CDC_SETTINGS:
            got = values[variable]
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
        grants = " ".join(str(r[0]).upper() for r in _rows(cur, "SHOW GRANTS"))
        missing = [g for g in MYSQL_GRANTS if g not in grants and "ALL PRIVILEGES" not in grants]
        out.append(
            Check(
                "mysql grants",
                FAIL if missing else PASS,
                f"missing {', '.join(missing)}" if missing else "REPLICATION SLAVE, REPLICATION CLIENT, SELECT",
                f"GRANT {', '.join(missing)} ON *.* TO '{source.login}'@'%';" if missing else "",
            )
        )
        own = _first(cur, "SELECT @@global.server_id")
        wanted_id = server_id or DEFAULT_SERVER_ID
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


def postgres_source(source: Any, connect: Callable[[Any], Any] | None = None) -> list[Check]:
    """What a PostgreSQL CDC stream needs: `wal_level = logical`, a role with REPLICATION, a free replication slot."""
    name = "postgres cdc"
    if connect is None:
        psycopg2 = _optional("psycopg2")
        if psycopg2 is None:
            return [Check(name, SKIP, "psycopg2 is not installed, so the source was not reached", "run by hand: SHOW wal_level;")]
        from .connections import connection_url

        def connect(c: Any) -> Any:
            return psycopg2.connect(connection_url(c, "postgresql"))

    try:
        db = connect(source)
    except Exception as exc:
        return [Check(name, FAIL, f"cannot connect to {source.host} ({type(exc).__name__})", "check the source Connection")]
    try:
        with db.cursor() as cur:
            wal = _first(cur, "SHOW wal_level")
            replication = _first(cur, "SELECT rolreplication OR rolsuper FROM pg_roles WHERE rolname = current_user")
            free = _first(
                cur,
                "SELECT current_setting('max_replication_slots')::int - (SELECT count(*) FROM pg_replication_slots)",
            )
    finally:
        db.close()
    return [
        Check("postgres wal_level", PASS if wal == "logical" else FAIL, f"is {wal}", "ALTER SYSTEM SET wal_level = logical; then restart"),
        Check(
            "postgres replication role",
            PASS if replication else FAIL,
            "the role may replicate" if replication else "the role has no REPLICATION",
            f"ALTER ROLE {source.login} REPLICATION;",
        ),
        Check(
            "postgres replication slots",
            PASS if (free or 0) > 0 else FAIL,
            f"{free} free",
            "raise max_replication_slots, or drop an unused slot",
        ),
    ]


def gcs(bucket: str, prefix: str, client: Any = None) -> tuple[list[Check], str | None]:
    """Write, list and delete a small object under the stream's prefix, as rivet will; the bucket's location."""
    name = f"gcs gs://{bucket}/{prefix}"
    if client is None:
        storage = _optional("google.cloud.storage")
        if storage is None:
            return [Check(name, SKIP, "google-cloud-storage is not installed", f"run by hand: gcloud storage ls gs://{bucket}/")], None
        client = storage.Client()
    try:
        handle = client.lookup_bucket(bucket)
    except Exception as exc:
        return [Check(name, FAIL, f"cannot read the bucket ({type(exc).__name__}: {exc})", "grant the worker's identity roles/storage.objectAdmin on it")], None
    if handle is None:
        return [Check(name, FAIL, "the bucket does not exist", f"gcloud storage buckets create gs://{bucket} --location=<the dataset's>")], None
    blob = handle.blob(f"{prefix}.rivet-airflow-preflight-{uuid.uuid4().hex}")
    try:
        blob.upload_from_string(b"")
        list(client.list_blobs(bucket, prefix=prefix, max_results=1))
        blob.delete()
    except Exception as exc:
        return [
            Check(name, FAIL, f"cannot write, list and delete there ({type(exc).__name__}: {exc})", "grant the worker's identity roles/storage.objectAdmin on the bucket")
        ], handle.location
    return [Check(name, PASS, f"exists in {handle.location}; an object was written, listed and deleted")], handle.location


def bigquery(project: str, dataset: str, bucket_location: str | None, client: Any = None) -> list[Check]:
    """The dataset exists, sits where the bucket is, and the identity can create and drop a table in it (as a load and a
    MERGE do)."""
    name = f"bigquery {project}.{dataset}"
    if client is None:
        bq = _optional("google.cloud.bigquery")
        if bq is None:
            return [Check(name, SKIP, "google-cloud-bigquery is not installed", f"run by hand: bq show {project}:{dataset}")]
        client = bq.Client(project=project)
    try:
        ds = client.get_dataset(f"{project}.{dataset}")
    except Exception as exc:
        missing = type(exc).__name__ == "NotFound"
        return [
            Check(
                name,
                FAIL,
                "the dataset does not exist" if missing else f"cannot read it ({type(exc).__name__}: {exc})",
                f"bq mk --dataset --location=<the bucket's> {project}:{dataset}" if missing else "grant roles/bigquery.dataEditor and roles/bigquery.jobUser",
            )
        ]
    out = []
    if bucket_location and str(ds.location).upper() != str(bucket_location).upper():
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
            Check(name, FAIL, f"cannot create and drop a table there ({type(exc).__name__}: {exc})", "grant roles/bigquery.dataEditor on the dataset and roles/bigquery.jobUser on the project")
        )
        return out
    out.insert(0, Check(name, PASS, f"exists in {ds.location}; a table was created and dropped"))
    return out


def failed(checks: Sequence[Check]) -> list[Check]:
    """The checks that failed."""
    return [c for c in checks if c.status == FAIL]


def summary(checks: Sequence[Check]) -> Mapping[str, int]:
    """How many checks ended each way."""
    return {s: sum(1 for c in checks if c.status == s) for s in (PASS, WARN, FAIL, SKIP)}
