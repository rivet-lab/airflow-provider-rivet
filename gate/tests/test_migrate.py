"""The migration leg: SQLite state under rivet 0.32.1 carried to PostgreSQL with `rivet state copy`.

Each test names the mutant it is RED on.
"""

from __future__ import annotations

from pathlib import Path
from types import SimpleNamespace

import pytest

from gate import __main__ as gate_main
from gate import migrate
from gate.migrate import checkpoint_problems, fingerprints, run_problems, state_problems, stream_equals
from gate.sources import MYSQL
from gate.stand import Stand

TASKS = {"run": {"state": "success"}, "load.gate_orders": {"state": "success"}, "compact.gate_orders": {"state": "skipped"}}


def payload(rows: int = 3, code: str | None = None) -> dict:
    return {"error": {"code": code} if code else None, "exit_status": 5 if code else 0, "units": [] if code else [{"rows": rows}]}


def test_a_run_after_the_switch_moves_exactly_the_changes_made_since() -> None:
    assert run_problems("success", TASKS, payload(3), 3) == []
    # Mutant: the count not compared, or the direction not told (a gap and a duplicate read the same).
    assert run_problems("success", TASKS, payload(2), 3) == ["the run moved 2 row(s), the source changed 3 (a gap)"]
    assert run_problems("success", TASKS, payload(4), 3) == ["the run moved 4 row(s), the source changed 3 (a duplicate)"]
    assert run_problems("success", TASKS, payload(6000), None) == [], "the baseline is not counted"
    # Mutant: a refusal read as success.
    assert run_problems("failed", {"run": {"state": "failed"}}, payload(code="RIVET_X"), 3) == [
        "run failed",
        "tasks {'run': 'failed'}",
        "rivet stopped [RIVET_X] exit 5",
    ]


def test_the_postgresql_state_holds_what_sqlite_held() -> None:
    sqlite = {"counts": {"export_state": 2, "run_status": 5, "schema_version": 1}, "cursors": [["cdc", None], ["t", "x"]]}
    pg = {
        "counts": {"export_state": 2, "run_status": 5, "schema_version": 40, "extra": 0},
        "cursors": [["cdc", None], ["t", "x"]],
    }
    assert state_problems(sqlite, pg) == []
    # Mutant: the row counts not compared.
    assert state_problems(sqlite, {**pg, "counts": {**pg["counts"], "run_status": 4}}) == [
        "run_status: 5 row(s) in SQLite, 4 in PostgreSQL"
    ]
    assert state_problems(sqlite, {**pg, "counts": {"export_state": 2, "schema_version": 1}}) == [
        "run_status: 5 row(s) in SQLite, no table in PostgreSQL"
    ]
    # Mutant: the cursors not compared.
    assert state_problems(sqlite, {**pg, "cursors": [["cdc", None], ["t", "y"]]})[0].startswith("stored cursors")
    # Mutant: the vacuity guard dropped (an empty SQLite state "equals" any target).
    assert state_problems({"counts": {"export_state": 0}, "cursors": []}, pg)[0].endswith("proves nothing")


def test_the_checkpoint_file_is_where_the_position_is_and_nothing_moves_it(tmp_path: Path) -> None:
    (tmp_path / "cdc").mkdir()
    (tmp_path / "cdc" / "cdc.ckpt").write_text('{"file":"binlog.000002","pos":1234}')
    before = fingerprints(tmp_path)
    assert list(before) == ["cdc/cdc.ckpt"]
    assert checkpoint_problems(before, fingerprints(tmp_path), "the copy") == []
    # Mutant: the files compared by name only (a moved position passes).
    (tmp_path / "cdc" / "cdc.ckpt").write_text('{"file":"binlog.000002","pos":9999}')
    assert checkpoint_problems(before, fingerprints(tmp_path), "the copy")[0].startswith("the copy: checkpoint files")
    # Mutant: an empty state_dir passes (no checkpoint where the copy says it is).
    assert checkpoint_problems({}, {}, "the copy")[0].endswith("proves nothing")


def test_the_stream_equals_the_source_with_its_deletes_applied() -> None:
    source = {"mig_orders": [{"id": 1, "status": "moved"}]}
    cols = {"mig_orders": ["id", "status"]}
    ok = {
        "mig_orders": [{"id": 1, "status": "moved", "__is_deleted": False}, {"id": 4997, "status": "new", "__is_deleted": True}]
    }
    assert stream_equals(ok, source, cols) == []
    # Mutant: a delete not applied passes (the key the source lacks, not flagged).
    unflagged = {"mig_orders": [ok["mig_orders"][0], {**ok["mig_orders"][1], "__is_deleted": False}]}
    assert any("absent at the source" in p for p in stream_equals(unflagged, source, cols))
    # Mutant: a duplicate of a live key passes (the current-state view holds a key twice).
    twice = {"mig_orders": [ok["mig_orders"][0], ok["mig_orders"][0]]}
    assert any("unexpected" in p for p in stream_equals(twice, source, cols))


def test_the_steps_count_the_changes_they_make() -> None:
    for step, (sql, ops) in migrate.STEPS.items():
        made = {"insert": sql.count("INSERT") and sql.count("), (") + 1, "delete": sql.count("DELETE")}
        assert made["delete"] == sum(per.get("delete", 0) for per in ops.values()), step
        assert (made["insert"] or 0) == sum(per.get("insert", 0) for per in ops.values()), step
    assert migrate.events(migrate.STEPS["before-switch"][1]) == 3


def migration_specs(key_file: Path | None = None) -> dict[str, dict]:
    src = SimpleNamespace(cloud=SimpleNamespace(tag="t", dataset=lambda stack: "d"), stack="3.3.2")
    leg = migrate.Migration(src)
    leg.stream = {"name": "cdc", "tables": ["mig_customers", "mig_orders"]}
    leg.b_exports = ["mig_customers", "mig_orders"]
    return {s["dag_id"]: s for s in leg.specs(SimpleNamespace(gcp_key_file=key_file))}


def test_the_three_dags_share_one_config_and_state_dir_and_differ_in_binary_and_state() -> None:
    made = migration_specs()
    sqlite, pg, back = (made[migrate.DAGS[k]]["build"] for k in ("sqlite", "pg", "back"))
    # Mutant: the PostgreSQL DAG on SQLite, the SQLite DAG on this binary, or the way back on the old binary.
    assert sqlite["operator_kwargs"]["rivet_bin"] == migrate.OLD_BIN
    assert "rivet_bin" not in pg["operator_kwargs"] and "rivet_bin" not in back["operator_kwargs"]
    assert pg["operator_kwargs"]["env_from_connections"] == {migrate.URL_ENV: migrate.CONN, "RIVET_STATE_URL": migrate.STATE_CONN}
    for on_sqlite in (sqlite, back):
        assert (
            on_sqlite["operator_kwargs"]["env"] == {"RIVET_STATE_URL": ""} and on_sqlite["operator_kwargs"]["single_host"] is True
        )
        assert on_sqlite["operator_kwargs"]["env_from_connections"] == {migrate.URL_ENV: migrate.CONN}
    assert "env" not in pg["operator_kwargs"]
    assert {b["state_dir"] for b in (sqlite, pg, back)} == {f"/opt/rivet/state/{migrate.STATE_SUB}"}
    assert {b["config"] for b in (sqlite, pg, back)} == {f"{migrate.CONFIG_DIR}/rivet.yaml"}


def test_the_old_binary_and_the_connections_reach_the_stack(tmp_path: Path) -> None:
    old = tmp_path / "rivet-old"
    old.write_text("#!/bin/sh\n")
    stand = Stand("3.3.2", Path("/bin/true"), tmp_path / "w", sources=(MYSQL,), migrate_from=old, state_place="compose-postgres")
    conns = stand.connections()
    # Mutant: the migration database's Connection on the leg's own database.
    assert conns["rivet_gate_mysql_mig"]["schema"] == "rivet_mig" and conns["rivet_gate_mysql"]["schema"] == "rivet"
    assert conns["rivet_gate_state_mysql_mig"]["host"] == "postgres"
    assert list(Stand("3.3.2", Path("/bin/true"), tmp_path / "x", sources=(MYSQL,)).migration_connections()) == [
        "rivet_gate_state_mysql_unq"
    ], "without --migrate-from, only the state of the config beside the same-named database"
    stand.lay_out()
    copied = stand.rivet_dir / "bin" / "rivet-0.32.1"
    assert copied.read_text() == "#!/bin/sh\n" and copied.stat().st_mode & 0o111


def test_the_migration_needs_the_mysql_leg(capsys) -> None:
    with pytest.raises(SystemExit):
        gate_main.main(["--rivet-bin", "x", "--migrate-from", "y", "--warehouses", "bigquery", "--sources", "mssql"])
    assert "pass --sources with mysql" in capsys.readouterr().err


def test_the_migration_tables_share_no_name_with_the_legs() -> None:
    # Mutant: the leg's own names (rivet refuses an unqualified CDC table another database holds, live: the leg's stream
    # stopped RIVET_SOURCE_CDC_TABLE_UNRESOLVED once `rivet_mig` held gate_customers too).
    assert not set(migrate.TABLES) & set(MYSQL.tables)
    assert sorted(migrate.TABLES) == ["mig_customers", "mig_orders"]
    assert "gate_" not in MYSQL.seed.replace("gate_", "mig_")
    assert all("gate_" not in sql and set(ops) <= set(migrate.TABLES) for sql, ops in migrate.STEPS.values())


def test_the_state_is_copied_from_the_whole_config_not_a_loads_cut() -> None:
    # Live (3.3.2, 0.32.1): the state_dir held `rivet.f8546eeb.yaml` and `rivet.f8546eeb--cdc.yaml` (the load's copy, cut
    # to the stream). Mutant: every `rivet.*.yaml` taken (two copies: the leg stops before the copy).
    assert migrate.full_copies(["rivet.f8546eeb--cdc.yaml", "rivet.f8546eeb.yaml"]) == ["rivet.f8546eeb.yaml"]
    assert migrate.full_copies(["rivet.f8546eeb--gate_orders.yaml"]) == []


def test_the_way_back_loads_only_the_runs_its_ledger_had_not_loaded() -> None:
    assert migrate.load_rows_problems({"mig_customers": 1, "mig_orders": 1}, {"mig_customers": 1, "mig_orders": 1}) == []
    # Mutant: the per-table rows not compared (a load that took the PostgreSQL-era runs again passes).
    assert migrate.load_rows_problems({"mig_customers": 3, "mig_orders": 1}, {"mig_customers": 1, "mig_orders": 1}) == [
        "load.mig_customers moved 3 row(s), the runs it had not loaded hold 1"
    ]
    assert migrate.load_rows_problems({"mig_orders": 1}, {"mig_customers": 1, "mig_orders": 1}) == [
        "load.mig_customers moved None row(s), the runs it had not loaded hold 1"
    ]


def test_a_base_holds_each_key_once() -> None:
    assert migrate.duplicate_keys({"mig_orders": (5003, 5003)}) == []
    # Mutant: rows and keys not compared (a base that holds a key twice passes).
    assert migrate.duplicate_keys({"mig_orders": (5004, 5003)}) == ["mig_orders: 5004 row(s), 5003 key(s)"]
    assert migrate.duplicate_keys({}) == ["no base read"]


def test_the_batch_pipeline_migrates_with_its_own_config_state_and_dags() -> None:
    made = migration_specs()
    sqlite, pg, back = (made[migrate.B_DAGS[k]]["build"] for k in ("sqlite", "pg", "back"))
    # Mutant: the batch PostgreSQL DAG on the stream's state, or the batch way back on the old binary.
    assert pg["operator_kwargs"]["env_from_connections"]["RIVET_STATE_URL"] == migrate.B_STATE_CONN != migrate.STATE_CONN
    assert sqlite["operator_kwargs"]["rivet_bin"] == migrate.OLD_BIN and "rivet_bin" not in back["operator_kwargs"]
    assert all(b["compact_exports"] == ["mig_customers", "mig_orders"] for b in (sqlite, pg, back))
    assert migrate.B_STATE_DB != migrate.STATE_DB and migrate.B_STATE_SUB != migrate.STATE_SUB
    assert all("DELETE" not in sql for sql, _ in migrate.B_STEPS.values()), "an incremental extract cannot see a delete"


def test_the_migration_dags_reach_gcp_the_way_the_stack_does() -> None:
    # Mutant: the GCP decision made here, not by the stack's key file (a client stack would be given a Connection it lacks).
    assert all(s["build"]["operator_kwargs"]["connections"] == ["gate_gcp"] for s in migration_specs().values())
    keyed = migration_specs(Path("/k.json"))
    assert all(s["build"]["operator_kwargs"]["cloud_credentials"] == ["gcp"] for s in keyed.values())
    assert not any("connections" in s["build"]["operator_kwargs"] for s in keyed.values())
