"""The first run of a source: a long step is not silent, the preflight checks, the folders other services share."""

from __future__ import annotations

import logging
import os
import stat
import sys
import time
import types
from pathlib import Path
from typing import Any

import pytest
from airflow.exceptions import AirflowFailException
from conftest import FakeTI, Rig
from test_source_dags import Init, connection, lookup

from airflow_provider_rivet import checks
from airflow_provider_rivet.checks import FAIL, PASS, SKIP, WARN, Check
from airflow_provider_rivet.events import Event
from airflow_provider_rivet.operators import RivetPreflightOperator, RivetRunOperator, make_shared_folder, places
from airflow_provider_rivet.stderr import Follower
from airflow_provider_rivet.tasklog import TaskLog


def follow(path: Path, every: float | None = None, lines: bool = True) -> tuple[Follower, list[str], list[tuple]]:
    seen: list[str] = []
    beats: list[tuple] = []
    follower = Follower(path, seen.append if lines else None, lambda *b: beats.append(b), every, poll=0.05)
    return follower, seen, beats


def wait_for(condition: Any, timeout: float = 5.0) -> None:
    deadline = time.monotonic() + timeout
    while not condition():
        assert time.monotonic() < deadline, "timed out"
        time.sleep(0.02)


def test_the_follower_passes_each_whole_line_as_it_comes_and_the_unfinished_one_at_the_end(tmp_path: Path) -> None:
    path = tmp_path / "err.log"
    path.write_bytes(b"")
    follower, seen, _ = follow(path)
    with follower, open(path, "ab", buffering=0) as out:
        out.write(b"first\r\nsec")
        wait_for(lambda: seen == ["first"])
        out.write(b"ond\nlast, no newline")
        wait_for(lambda: seen == ["first", "second"])
    assert seen == ["first", "second", "last, no newline"]


def test_the_follower_beats_with_the_time_run_the_size_and_the_time_since_the_file_grew(tmp_path: Path) -> None:
    path = tmp_path / "err.log"
    path.write_bytes(b"12345\n")
    follower, _, beats = follow(path, every=0.1)
    with follower:
        wait_for(lambda: len(beats) >= 2)
    elapsed, size, idle = beats[-1]
    assert size == 6 and elapsed >= idle > 0 and elapsed >= 0.1


def test_without_lines_to_pass_the_follower_never_reads_the_file_only_its_size(tmp_path: Path) -> None:
    path = tmp_path / "err.log"
    path.write_bytes(b"secret-ish text\n")
    path.chmod(0)
    follower, seen, beats = follow(path, every=0.1, lines=False)
    with follower:
        wait_for(lambda: beats)
    assert seen == [] and beats[-1][1] == 16


def test_a_follower_with_nothing_to_do_starts_no_thread_and_a_missing_file_is_no_error(tmp_path: Path) -> None:
    follower, seen, beats = follow(tmp_path / "never.log", every=None, lines=False)
    with follower:
        pass
    assert not follower.thread.is_alive() and follower.thread.ident is None and (seen, beats) == ([], [])
    with follow(tmp_path / "never.log", every=0.05)[0]:
        time.sleep(0.15)


def test_a_heartbeat_says_how_long_the_step_ran_and_only_the_first_says_how_to_follow_it() -> None:
    lines: list[tuple[int, str]] = []
    log = TaskLog("run", "orders", None, lambda level, text: lines.append((level, text)))
    log.record({"event": Event.START, "step": "run"})
    beat = {"event": Event.PROGRESS, "step": "run", "stderr_bytes": 2048, "stderr_path": "/s/run.stderr.log"}
    log.record({**beat, "elapsed": 61.0, "idle": 4.0})
    log.record({**beat, "elapsed": 3725.0, "idle": 125.0})
    shown = [text for level, text in lines if level == logging.INFO]
    assert shown == [
        "rivet run running for 1m 1s; stderr 2.0 KiB, last written 4 s ago; follow it: tail -f /s/run.stderr.log",
        "rivet run running for 1h 2m; stderr 2.0 KiB, last written 2m 5s ago",
    ]


def test_a_long_rivet_step_writes_heartbeats_and_its_stderr_lines_into_the_task_log(rig: Rig, tmp_path: Path) -> None:
    rig.scenario(run={"stderr": "connected to the source", "sleep": 2.5, "started": str(tmp_path / "started")})
    payload, _ = rig.run(rig.operator(RivetRunOperator, export="users", progress_every=0.5, stream_stderr=True))
    assert payload["decision"] == "success", payload
    beats = rig.events(Event.PROGRESS)
    assert beats and beats[0]["step"] == "run" and beats[0]["stderr_path"] == payload["stderr_path"]
    assert any("connected to the source" in str(r) for r in rig.records)


def test_progress_every_none_writes_no_heartbeat(rig: Rig, tmp_path: Path) -> None:
    rig.scenario(run={"sleep": 1.5, "started": str(tmp_path / "started")})
    payload, _ = rig.run(rig.operator(RivetRunOperator, export="users", progress_every=None))
    assert payload["decision"] == "success" and rig.events(Event.PROGRESS) == []


def test_a_shared_folder_and_its_missing_parents_are_group_writable_never_world_writable(tmp_path: Path) -> None:
    existing = tmp_path / "volume"
    existing.mkdir(mode=0o755)
    existing.chmod(0o755)
    make_shared_folder(existing / "rivet" / "shop")
    for path in (existing / "rivet", existing / "rivet" / "shop"):
        mode = path.stat().st_mode
        assert mode & stat.S_IWGRP and not mode & stat.S_IWOTH, f"{path}: {mode:o}"
    assert stat.S_IMODE(existing.stat().st_mode) == 0o755


def test_the_config_init_writes_is_group_writable_and_its_folder_too(init: Init) -> None:
    out = init.run(init_args=["--tls", "disable"])
    assert out["written"] is True
    assert stat.S_IMODE(init.config.stat().st_mode) == 0o664
    assert init.config.parent.stat().st_mode & stat.S_IWGRP and not init.config.parent.stat().st_mode & stat.S_IWOTH


@pytest.fixture
def init(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Init:
    return Init(tmp_path, monkeypatch)


def test_a_check_line_names_the_fix_unless_it_passed() -> None:
    assert Check("x", FAIL, "is off", "turn it on").line() == "FAIL x: is off -> fix: turn it on"
    assert Check("x", PASS, "is on", "turn it on").line() == "PASS x: is on"
    found = [Check("a", PASS, ""), Check("b", FAIL, ""), Check("c", SKIP, ""), Check("d", FAIL, "")]
    assert checks.summary(found) == {PASS: 1, WARN: 0, FAIL: 2, SKIP: 1}
    assert [c.name for c in checks.failed(found)] == ["b", "d"]


def test_a_folder_is_writable_or_created_in_its_nearest_existing_parent(tmp_path: Path) -> None:
    assert checks.folder("f", tmp_path) == Check("f", PASS, f"{tmp_path} is writable")
    later = tmp_path / "a" / "b"
    assert checks.folder("f", later) == Check("f", PASS, f"{tmp_path} is writable; {later} is created there")
    assert not later.exists()
    gone = checks.folder("f", later, must_exist=True)
    assert gone.status == FAIL and gone.fix == "mount the volume at this path in every Airflow service"


@pytest.mark.skipif(os.geteuid() == 0, reason="root writes into a read-only folder")
def test_a_folder_this_worker_cannot_write_says_who_it_is_and_who_owns_it(tmp_path: Path) -> None:
    locked = tmp_path / "locked"
    locked.mkdir()
    locked.chmod(0o555)
    try:
        found = checks.folder("config folder", locked / "rivet")
    finally:
        locked.chmod(0o755)
    assert found.status == FAIL
    assert f"(uid {os.getuid()}, gid {os.getgid()}: Permission denied); {locked} is owned by uid" in found.detail
    assert "mode 555" in found.detail and "chmod -R g+w" in found.fix and "777" not in found.fix


class Cursor:
    """A DB-API cursor answering each statement from `answers` (a row, a list of rows, or an exception)."""

    def __init__(self, answers: dict[str, Any]) -> None:
        self.answers, self.last = answers, None

    def __enter__(self) -> Cursor:
        return self

    def __exit__(self, *exc: Any) -> None:
        return None

    def execute(self, sql: str, args: tuple = ()) -> None:
        answer = self.answers.get(sql)
        if isinstance(answer, Exception):
            raise answer
        self.last = answer

    def fetchone(self) -> Any:
        return self.last[0] if isinstance(self.last, list) else self.last

    def fetchall(self) -> Any:
        return self.last if isinstance(self.last, list) else ([self.last] if self.last else [])


def db_of(answers: dict[str, Any]) -> Any:
    db = types.SimpleNamespace(closed=False)
    db.cursor = lambda: Cursor(answers)
    db.close = lambda: setattr(db, "closed", True)
    return db


SERVER = types.SimpleNamespace(host="pg", port=None, schema="admin", login="rivet")
EXISTS = "SELECT 1 FROM pg_database WHERE datname = %s"
CREATEDB = "SELECT rolcreatedb OR rolsuper FROM pg_roles WHERE rolname = current_user"


@pytest.mark.parametrize(
    ("answers", "status", "said"),
    [
        ({EXISTS: (1,), CREATEDB: (False,)}, PASS, "exists"),
        ({EXISTS: None, CREATEDB: (True,)}, PASS, "does not exist yet; `init` creates it"),
        ({EXISTS: None, CREATEDB: (False,)}, FAIL, "may not create it"),
    ],
)
def test_the_state_database_check_says_whether_it_exists_or_can_be_created(answers: dict, status: str, said: str) -> None:
    (found,) = checks.state(SERVER, "rivet_state_shop", "rivet_state", connect=lambda _: db_of(answers))
    assert found.status == status and said in found.detail
    assert found.detail.startswith("`rivet_state_shop` on pg:5432 (Connection `rivet_state`)")
    if status == FAIL:
        assert found.fix == 'CREATE DATABASE "rivet_state_shop"; or ALTER ROLE rivet CREATEDB;'


def test_a_state_server_it_cannot_reach_fails_naming_the_connection() -> None:
    def refuse(_: Any) -> Any:
        raise OSError("timeout")

    (found,) = checks.state(SERVER, None, "rivet_state", connect=refuse)
    assert (found.status, found.fix) == (FAIL, "check Connection `rivet_state`") and "OSError: timeout" in found.detail


@pytest.mark.parametrize(
    ("host", "extra", "status"),
    [
        ("pg-state", {"sslmode": "disable"}, PASS),
        ("pg-state", {"sslmode": "verify-full"}, PASS),
        ("localhost", {}, PASS),
        ("127.0.0.1", {}, PASS),
        ("pg-state", {}, FAIL),
        ("pg-state", {"sslmode": "prefer"}, FAIL),
        ("localhost", {"sslmode": "prefer"}, PASS),
        ("::1", {}, PASS),
        ("[::1]", {"sslmode": "prefer"}, PASS),
        ("127.8.0.1", {}, PASS),
        ("127.0.0.1,pg-state", {}, FAIL),
        ("127.evil.com", {}, FAIL),
    ],
)
def test_a_remote_state_server_needs_an_sslmode_rivet_dials(host: str, extra: dict, status: str) -> None:
    found = checks.state_tls(types.SimpleNamespace(host=host, extra_dejson=extra), "rivet_state")
    assert (found.name, found.status) == ("state database tls", status)


def test_a_remote_state_server_without_sslmode_names_the_refusal_and_both_ways_out() -> None:
    found = checks.state_tls(types.SimpleNamespace(host="pg-state", extra_dejson={}), "rivet_state")
    assert found.detail == (
        "Connection `rivet_state` points at pg-state, a remote host, and names no sslmode; "
        "rivet refuses it (RIVET_STATE_URL_TLS_REQUIRED)"
    )
    assert "`require`" in found.fix and "`disable`" in found.fix


def test_without_psycopg2_the_state_database_is_a_skip(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setitem(sys.modules, "psycopg2", None)
    (found,) = checks.state(SERVER, "d", "c")
    assert found.status == SKIP and found.fix == "pip install psycopg2-binary"


MYSQL = types.SimpleNamespace(host="my", port=3307, login="reader")
MYSQL_OK = {
    "SELECT @@global.log_bin": (1,),
    "SELECT @@global.binlog_format": ("ROW",),
    "SELECT @@global.binlog_row_image": ("FULL",),
    "SELECT @@global.binlog_row_metadata": ("full",),
    "SHOW REPLICA STATUS": [],
    "SHOW SLAVE STATUS": [],
    "SHOW GRANTS": [("GRANT SELECT, REPLICATION SLAVE, REPLICATION CLIENT ON *.* TO `reader`@`%`",)],
    "SELECT @@global.server_id": (1,),
    "SHOW REPLICAS": [],
    "SHOW SLAVE HOSTS": [],
}


def mysql(answers: dict[str, Any], server_id: int | None = 4271) -> dict[str, Check]:
    db = db_of({**MYSQL_OK, **answers})
    found = checks.mysql_source(MYSQL, server_id, connect=lambda _: db)
    assert db.closed is True
    return {c.name: c for c in found}


def test_a_mysql_source_ready_for_cdc_passes_every_check() -> None:
    found = mysql({})
    assert {n: c.status for n, c in found.items()} == {
        "mysql log_bin": PASS,
        "mysql binlog_format": PASS,
        "mysql binlog_row_image": PASS,
        "mysql binlog_row_metadata": PASS,
        "mysql grants": PASS,
        "mysql server_id": PASS,
    }
    assert found["mysql server_id"].detail == "the stream's server_id 4271 is free (the server's is 1)"


def test_a_mysql_setting_a_cdc_stream_cannot_read_fails_with_the_statement_that_fixes_it() -> None:
    found = mysql({"SELECT @@global.binlog_format": ("STATEMENT",), "SELECT @@global.binlog_row_metadata": RuntimeError()})
    assert found["mysql binlog_format"] == Check(
        "mysql binlog_format", FAIL, "is STATEMENT", "SET PERSIST binlog_format = 'ROW';"
    )
    assert found["mysql binlog_row_metadata"].status == WARN


def test_a_missing_mysql_grant_is_named_with_its_grant_statement() -> None:
    found = mysql({"SHOW GRANTS": [("GRANT SELECT ON `shop`.* TO `reader`@`%`",)]})["mysql grants"]
    assert found.status == FAIL and found.fix == "GRANT REPLICATION SLAVE, REPLICATION CLIENT ON *.* TO 'reader'@'%';"
    assert mysql({"SHOW GRANTS": [("GRANT ALL PRIVILEGES ON *.* TO `reader`@`%`",)]})["mysql grants"].status == PASS


def test_a_replica_id_another_replica_uses_is_a_warning_and_no_id_yet_is_a_skip() -> None:
    assert mysql({"SHOW REPLICAS": [(4271, "", 3306, 1)]})["mysql server_id"].status == WARN
    assert mysql({"SELECT @@global.server_id": (4271,)})["mysql server_id"].status == WARN
    assert mysql({}, server_id=None)["mysql server_id"].status == SKIP


def test_a_mysql_replica_without_log_replica_updates_fails() -> None:
    found = mysql({"SHOW REPLICA STATUS": [("primary",)], "SELECT @@global.log_replica_updates": (0,)})
    assert found["mysql log_replica_updates"].status == FAIL


def test_a_postgres_source_reports_wal_level_role_and_free_slots() -> None:
    answers = {
        "SHOW wal_level": ("replica",),
        "SELECT rolreplication OR rolsuper FROM pg_roles WHERE rolname = current_user": (True,),
        "SELECT current_setting('max_replication_slots')::int - (SELECT count(*) FROM pg_replication_slots)": (0,),
    }
    found = {c.name: c for c in checks.postgres_source(SERVER, connect=lambda _: db_of(answers))}
    assert {n: c.status for n, c in found.items()} == {
        "postgres wal_level": FAIL,
        "postgres replication role": PASS,
        "postgres replication slots": FAIL,
    }
    assert found["postgres wal_level"].fix == "ALTER SYSTEM SET wal_level = logical; then restart"


class Blob:
    def __init__(self, bucket: Bucket, name: str) -> None:
        self.bucket, self.name = bucket, name

    def upload_from_string(self, data: bytes) -> None:
        if self.bucket.read_only:
            raise PermissionError("403 storage.objects.create denied")
        self.bucket.objects.add(self.name)

    def delete(self) -> None:
        self.bucket.objects.discard(self.name)


class Bucket:
    def __init__(self, location: str, read_only: bool = False) -> None:
        self.location, self.read_only, self.objects = location, read_only, set()

    def blob(self, name: str) -> Blob:
        return Blob(self, name)


class Storage:
    def __init__(self, bucket: Bucket | None) -> None:
        self.bucket = bucket

    def lookup_bucket(self, name: str) -> Bucket | None:
        return self.bucket

    def list_blobs(self, name: str, prefix: str, max_results: int) -> list:
        return []


def test_a_bucket_is_written_listed_and_cleaned_and_its_location_returned() -> None:
    bucket = Bucket("US")
    found, location = checks.gcs("b", "rivet/shop/", client=Storage(bucket))
    assert [c.status for c in found] == [PASS] and location == "US" and bucket.objects == set()
    assert found[0].name == "gcs gs://b/rivet/shop/"


def test_a_missing_bucket_is_not_created_and_says_how_to_create_it() -> None:
    (found,), location = checks.gcs("b", "", client=Storage(None))
    assert found.status == FAIL and location is None
    assert found.fix == "gcloud storage buckets create gs://b --location=<the dataset's>"


def test_a_bucket_the_identity_cannot_write_fails_naming_the_role() -> None:
    (found,), location = checks.gcs("b", "p/", client=Storage(Bucket("EU", read_only=True)))
    assert found.status == FAIL and "403" in found.detail and "roles/storage.objectAdmin" in found.fix and location == "EU"


class NotFound(Exception):
    pass


class BigQuery:
    def __init__(self, location: str | None, ddl_error: Exception | None = None) -> None:
        self.location, self.ddl_error, self.sql = location, ddl_error, []

    def get_dataset(self, ref: str) -> Any:
        if self.location is None:
            raise NotFound(ref)
        return types.SimpleNamespace(location=self.location)

    def query(self, sql: str) -> Any:
        self.sql.append(sql)
        if self.ddl_error:
            raise self.ddl_error
        return types.SimpleNamespace(result=lambda: None)


def test_a_dataset_in_the_buckets_location_passes_after_a_table_is_created_and_dropped() -> None:
    client = BigQuery("us")
    assert [c.status for c in checks.bigquery("p", "d", "US", client=client)] == [PASS]
    assert "CREATE TABLE `p.d._rivet_airflow_preflight_" in client.sql[0] and "DROP TABLE" in client.sql[0]


def test_a_dataset_away_from_the_bucket_is_a_warning_and_a_missing_one_says_how_to_make_it() -> None:
    found = checks.bigquery("p", "d", "EU", client=BigQuery("US"))
    assert [c.status for c in found] == [PASS, WARN] and "the dataset is in US, the bucket in EU" in found[1].detail
    (missing,) = checks.bigquery("p", "d", None, client=BigQuery(None))
    assert missing.status == FAIL and missing.fix == "bq mk --dataset --location=<the bucket's> p:d"
    (denied,) = checks.bigquery("p", "d", None, client=BigQuery("US", ddl_error=PermissionError("403")))
    assert denied.status == FAIL and "roles/bigquery.dataEditor" in denied.fix


def test_without_the_google_libraries_the_bucket_and_dataset_are_skips(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(checks, "_optional", lambda *_: None)
    (bucket,), _ = checks.gcs("b", "")
    (dataset,) = checks.bigquery("p", "d", None)
    assert (bucket.status, dataset.status) == (SKIP, SKIP) and dataset.fix == "run by hand: bq show p:d"


class GoogleLibs:
    def __init__(self, key_error: Exception | None = None) -> None:
        self.key_error, self.made, self.read = key_error, [], []
        self.auth = types.SimpleNamespace(load_credentials_from_file=self.load)
        self.storage = types.SimpleNamespace(Client=lambda **kw: self.client(Storage(Bucket("EU")), kw))
        self.bigquery = types.SimpleNamespace(Client=lambda **kw: self.client(BigQuery("EU"), kw))

    def load(self, path: str, scopes: list[str]) -> tuple[str, str | None]:
        self.read.append((path, scopes))
        if self.key_error:
            raise self.key_error
        return "user-credentials", None

    def client(self, client: Any, kwargs: dict[str, Any]) -> Any:
        self.made.append(kwargs)
        return client

    def module(self, name: str) -> Any:
        return {"google.auth": self.auth, "google.cloud.storage": self.storage, "google.cloud.bigquery": self.bigquery}[name]


def test_a_users_key_file_reaches_both_clients_as_credentials(monkeypatch: pytest.MonkeyPatch) -> None:
    libs = GoogleLibs()
    monkeypatch.setattr(checks, "_optional", libs.module)
    found, location = checks.gcs("b", "p/", key_file="/opt/airflow/gcp.json")
    assert [c.status for c in found] == [PASS] and location == "EU"
    assert [c.status for c in checks.bigquery("proj", "d", location, key_file="/opt/airflow/gcp.json")] == [PASS]
    assert libs.read == [("/opt/airflow/gcp.json", [checks.GCP_SCOPE])] * 2
    assert libs.made == [
        {"credentials": "user-credentials", "project": None},
        {"credentials": "user-credentials", "project": "proj"},
    ]


def test_a_key_file_google_auth_cannot_read_is_a_failed_check_not_a_crash(monkeypatch: pytest.MonkeyPatch) -> None:
    libs = GoogleLibs(key_error=ValueError("missing fields token_uri, client_email"))
    monkeypatch.setattr(checks, "_optional", libs.module)
    (bucket,), location = checks.gcs("b", "", key_file="/k.json")
    (dataset,) = checks.bigquery("p", "d", None, key_file="/k.json")
    assert (bucket.status, dataset.status, location) == (FAIL, FAIL, None)
    assert bucket.detail == "cannot use the key file /k.json (ValueError: missing fields token_uri, client_email)"
    assert "gcloud auth application-default login" in dataset.fix and libs.made == []


class Caps:
    def __init__(self, version: str, new: bool) -> None:
        self.version_text, self.new = version, new

    def new_enough(self) -> bool:
        return self.new


def preflight(init: Init, monkeypatch: pytest.MonkeyPatch, caps: Caps | None = None, **kwargs: Any) -> Any:
    monkeypatch.setattr("airflow_provider_rivet.operators.probe", lambda _: caps or Caps("0.33.0 (abc)", True))
    monkeypatch.setattr(checks, "_optional", lambda *_: None)
    kwargs.setdefault("env_from_connections", {"RIVET_SOURCE_URL": {"conn_id": "shop", "odbc_ini": str(init.tmp / "odbc.ini")}})
    op = RivetPreflightOperator(
        task_id="preflight",
        mode="cdc",
        source_env="RIVET_SOURCE_URL",
        config=str(init.config),
        state_dir=str(init.tmp / "state" / "shop"),
        rivet_bin=str(init.tmp / "bin" / "rivet"),
        **kwargs,
    )
    try:
        return op.execute({"ti": FakeTI(task_id="preflight"), "run_id": "r"})
    except Exception as exc:
        return exc


def test_preflight_before_init_checks_the_binary_the_folders_and_skips_what_it_cannot_reach(
    init: Init, monkeypatch: pytest.MonkeyPatch
) -> None:
    out = preflight(init, monkeypatch, init_args=["--gcs-bucket", "b", "--tls", "disable"])
    assert out["decision"] == "pass", out
    by_name = {line.split(":")[0].split(None, 1)[1]: line.split()[0] for line in out["checks"]}
    assert by_name == {
        "rivet binary": PASS,
        "config folder": PASS,
        "state folder": PASS,
        "mysql cdc": SKIP,
        "baseline": PASS,
        "gcs gs": SKIP,
    }
    assert [c["argv"] for c in init.calls()] == [["init", "--source-env", "RIVET_SOURCE_URL", "--discover", "--tls", "disable"]]
    assert not init.config.exists() and sorted(p.name for p in (init.tmp / "state" / "shop" / ".init").iterdir()) == []


def test_preflight_of_a_written_config_runs_rivet_check_on_it(init: Init, monkeypatch: pytest.MonkeyPatch) -> None:
    init.run(init_args=["--tls", "disable"])
    out = preflight(init, monkeypatch)
    assert out["decision"] == "pass" and f"PASS rivet check: {init.config} checks clean" in out["checks"]
    assert init.calls()[-1]["argv"] == ["check", "--config", str(init.config)]


def test_a_failed_preflight_check_fails_the_task_without_a_retry_naming_each_check(
    init: Init, monkeypatch: pytest.MonkeyPatch
) -> None:
    out = preflight(init, monkeypatch, caps=Caps("0.20.0", False))
    assert isinstance(out, AirflowFailException), out
    assert "preflight: 1 check(s) failed: rivet binary; each line above says what to change" in str(out)


def test_preflight_checks_the_state_database_the_tasks_will_use(init: Init, monkeypatch: pytest.MonkeyPatch) -> None:
    lookup(monkeypatch, {"rivet_state": connection("rivet_state", "postgres", schema="admin", extra={"sslmode": "require"})})
    refs = {
        "RIVET_SOURCE_URL": {"conn_id": "shop", "odbc_ini": str(init.tmp / "odbc.ini")},
        "RIVET_STATE_URL": {"conn_id": "rivet_state", "database": "rivet_state_shop"},
    }
    out = preflight(init, monkeypatch, env_from_connections=refs)
    assert "PASS state database tls: Connection `rivet_state` says sslmode=require" in out["checks"]
    assert (
        "SKIP state database: `rivet_state_shop` on rivet_state.host:5432 (Connection `rivet_state`); psycopg2 is not "
        "installed, so it was not reached -> fix: pip install psycopg2-binary"
    ) in out["checks"]


def test_discovery_keeps_only_the_arguments_that_choose_the_tables() -> None:
    args = ["--gcs-bucket", "b", "--include", "a", "b*", "--bigquery-dataset=d", "--tls=disable", "--schema", "s"]
    assert checks.discovery_args(args) == ["--include", "a", "b*", "--tls=disable", "--schema", "s"]


def table(name: str, mode: str, rows: int | None = 3_000_000, schema: str = "") -> dict[str, Any]:
    return {"schema": schema, "table": name, "row_estimate": rows, "suggested_mode": mode}


@pytest.mark.parametrize(
    ("kind", "tables", "status", "said"),
    [
        ("mysql", [table("a", "chunked"), table("b", "full")], PASS, "2 tables in one stream; each baseline is read through"),
        ("postgres", [table("a", "chunked", schema="public"), table("b", "full", schema="public")], PASS, "2 tables in one"),
        ("mysql", [table("events", "full", rows=900)], PASS, "`events` (~900 rows) read in one SELECT each, as rivet"),
        ("mysql", [table("events", "chunked")], WARN, "`events` (~3,000,000 rows) read in one SELECT each on one connection"),
        (
            "postgres",
            [table("a", "full", schema="public"), table("b", "chunked", rows=None, schema="sales")],
            WARN,
            "`sales.b` read",
        ),
        ("mysql", [], WARN, "`rivet init --discover` finds no table"),
    ],
)
def test_a_baseline_rivet_would_page_but_reads_in_one_select_is_a_warning(
    kind: str, tables: list, status: str, said: str
) -> None:
    check = checks.baseline({"source_type": kind, "tables": tables}, "/cfg/shop.yaml")
    assert check.status == status and check.detail.startswith(said), check


BASELINE_FIX = (
    "after `init`, in {config} replace `initial: snapshot` with `backfill: auto` and add a batch export of each of these "
    "tables as its recipe (`mode: chunked`, `chunk_by_key: <key>`, `parallel: 4`); raising `tuning.statement_timeout_s` "
    "instead keeps one SELECT (rivet-lab/rivet#878)"
)


def test_the_baseline_warning_says_why_and_how_to_page_the_baseline() -> None:
    check = checks.baseline({"source_type": "mysql", "tables": [table("events", "chunked")]}, "/cfg/shop.yaml")
    assert check.detail.endswith("`tuning.statement_timeout_s` stops a SELECT (300 s under the `balanced` profile)")
    assert check.fix == BASELINE_FIX.format(config="/cfg/shop.yaml")


def test_preflight_before_init_warns_of_a_one_table_baseline_rivet_would_page(
    init: Init, monkeypatch: pytest.MonkeyPatch
) -> None:
    init.spec(discover={"source_type": "mysql", "tables": [table("events", "chunked")]})
    out = preflight(init, monkeypatch)
    assert out["decision"] == "pass", out
    assert [line for line in out["checks"] if line.startswith("WARN baseline")] == [
        "WARN baseline: `events` (~3,000,000 rows) read in one SELECT each on one connection though rivet would page them, "
        "and `tuning.statement_timeout_s` stops a SELECT (300 s under the `balanced` profile) -> fix: "
        + BASELINE_FIX.format(config=init.config)
    ]


def test_a_discovery_that_fails_is_a_warning_with_the_command_to_run_by_hand(init: Init, monkeypatch: pytest.MonkeyPatch) -> None:
    init.spec(discover_exit=3)
    out = preflight(init, monkeypatch)
    assert out["decision"] == "pass", out
    assert (
        "WARN baseline: `rivet init --discover` exited 3; its stderr is above -> fix: run by hand: rivet init --source-env "
        "RIVET_SOURCE_URL --discover"
    ) in out["checks"]


def test_preflight_of_a_written_config_does_not_discover_again(init: Init, monkeypatch: pytest.MonkeyPatch) -> None:
    init.run(init_args=["--tls", "disable"])
    preflight(init, monkeypatch)
    assert [c["argv"][:4] for c in init.calls() if "--discover" in c["argv"]] == []


def staging_calls(monkeypatch: pytest.MonkeyPatch, status: str = PASS) -> list[tuple]:
    calls: list[tuple] = []

    def gcs(bucket: str, prefix: str, key_file: str | None = None) -> tuple[list[Check], str]:
        calls.append(("gcs", bucket, prefix))
        return [Check(f"gcs gs://{bucket}/{prefix}", status, "probed")], "US"

    def bigquery(project: str, dataset: str, location: str, key_file: str | None = None) -> list[Check]:
        calls.append(("bigquery", project, dataset))
        return [Check(f"bigquery {project}.{dataset}", PASS, "probed")]

    monkeypatch.setattr(checks, "gcs", gcs)
    monkeypatch.setattr(checks, "bigquery", bigquery)
    return calls


def test_preflight_writes_to_the_bucket_and_dataset_only_before_init(init: Init, monkeypatch: pytest.MonkeyPatch) -> None:
    calls = staging_calls(monkeypatch)
    assert "SKIP bucket: init_args name no GCS bucket" in preflight(init, monkeypatch)["checks"]
    args = ["--gcs-bucket", "b", "--bigquery-project", "p", "--bigquery-dataset", "d"]
    preflight(init, monkeypatch, init_args=args)
    assert calls == [("gcs", "b", ""), ("bigquery", "p", "d")]
    init.run(init_args=args)
    out = preflight(init, monkeypatch, init_args=args)
    assert calls == [("gcs", "b", ""), ("bigquery", "p", "d")]
    assert "SKIP bucket and dataset: the config exists; the preflight before `init` wrote to them" in out["checks"]


def test_a_config_that_differs_from_include_fails_before_the_stream_starts_and_warns_after(
    init: Init, monkeypatch: pytest.MonkeyPatch
) -> None:
    staging_calls(monkeypatch)
    init.run()
    assert (
        "PASS include and exclude: the config captures what the sources file names"
        in preflight(init, monkeypatch, include=["orders", "customers"])["checks"]
    )
    refused = preflight(init, monkeypatch, include=["orders"])
    assert isinstance(refused, AirflowFailException) and "include and exclude" in str(refused)
    checkpoint = init.tmp / "state" / "shop" / "cdc" / "shop_cdc.ckpt"
    checkpoint.parent.mkdir(parents=True)
    checkpoint.write_text("pos")
    warned = preflight(init, monkeypatch, include=["orders"])
    line = next(c for c in warned["checks"] if c.startswith("WARN include and exclude"))
    assert "captures customers" in line and "edit the config at the places named" in line


def test_the_places_a_config_writes_are_urls_an_operator_can_paste() -> None:
    doc = {
        "exports": [
            {"name": "a", "destination": {"type": "gcs", "bucket": "b", "prefix": "shop/cdc/"}},
            {"name": "c", "destination": {"type": "s3", "bucket": "s", "prefix": "x/"}},
        ],
        "load": {"target": "bigquery", "project": "p", "dataset": "d"},
    }
    assert places(doc) == {"gs://b/shop/cdc/", "s3://s/x/", "bigquery p.d"}
