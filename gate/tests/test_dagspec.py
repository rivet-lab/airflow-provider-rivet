"""The DAG specs: what a leg writes for the stack's DAG file, and what the host grades from the same spec.

The real builders are exercised in tests/test_gate_dag_file.py (CI's Airflow matrix); here, with Airflow present, the
specs every leg really writes are built by the real DAG file and held to the tasks the host grades.
Each test names the mutant it is RED on.
"""

from __future__ import annotations

import glob
import runpy
from pathlib import Path
from types import SimpleNamespace
from typing import Any

import pytest
from test_destinations import prepared_s3
from test_stand_hygiene import written_specs

from gate import dagspec, multihost, scenario, warehouse
from gate.stand import Stand

DAG_FILE = Path(dagspec.__file__).parent / "stand" / "dags" / "rivet_gate.py"
COMMON = {"config": "/c.yaml", "state_dir": "/s", "operator_kwargs": {}, "tags": []}
RIVET_TASK = ("apply.", "load.", "compact.", "run", "load")


def test_the_tasks_the_host_grades_are_the_ones_the_builder_makes() -> None:
    # Mutant: a group left out, or a compaction of an export that does not compact.
    assert dagspec.task_ids(dagspec.batch("d", exports=["a", "b"], load=False, **COMMON)) == ["apply.a", "apply.b"]
    assert dagspec.task_ids(dagspec.batch("d", exports=["a", "b"], compact_exports=["b"], **COMMON)) == [
        "apply.a", "load.a", "apply.b", "load.b", "compact.b",
    ]  # fmt: skip
    assert dagspec.task_ids(dagspec.cdc("d", export="s", tables=["a"], load=False, **COMMON)) == ["run"]
    assert dagspec.task_ids(dagspec.cdc("d", export="s", tables=["a"], compact=False, **COMMON)) == ["run", "load.a"]
    assert dagspec.task_ids(dagspec.cdc("d", export="s", tables=["a"], **COMMON)) == ["run", "load.a", "compact.a"]
    assert dagspec.task_ids(dagspec.load_only("d", export="a", **COMMON)) == ["load"]
    # A compaction needs the load it merges: `compact` with `load=False` makes none (the builder refuses it too).
    assert dagspec.task_ids(dagspec.cdc("d", export="s", tables=["a"], load=False, compact=True, **COMMON)) == ["run"]


def test_a_spec_with_an_unknown_kind_or_a_repeated_id_is_never_published(tmp_path: Path) -> None:
    (tmp_path / "configs").mkdir()
    bad = {**dagspec.batch("d", exports=["a"], **COMMON), "kind": "stream"}
    with pytest.raises(ValueError, match="kind"):
        dagspec.write(tmp_path, "x", [bad])
    # Mutant: a half-written file left behind (the DAG processor parses it).
    assert list((tmp_path / "configs").iterdir()) == []
    dagspec.write(tmp_path, "x", [dagspec.batch("d", exports=["a"], **COMMON)])
    assert [p.name for p in (tmp_path / "configs").iterdir()] == ["dag_specs_x.json"]


def test_the_gcp_decision_is_the_stands_key_file() -> None:
    # Mutant: the decision taken from a second place (a client stack handed a Connection it does not have).
    assert dagspec.gcp(SimpleNamespace(gcp_key_file="/k")) == {"cloud_credentials": ["gcp"]}
    assert dagspec.gcp(SimpleNamespace()) == {"connections": ["gate_gcp"]}
    assert dagspec.env_key(Stand("3.3.2", Path("/bin/true"), Path("/tmp/x"))) is False


# --- every leg's specs, built by the real DAG file ----------------------------------------------------------------------


def build(specs: list[dict[str, Any]], tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> dict[str, Any]:
    (tmp_path / "configs").mkdir(parents=True, exist_ok=True)
    dagspec.write(tmp_path, "under_test", specs)
    real = glob.glob
    monkeypatch.setattr(glob, "glob", lambda pattern: real(str(tmp_path / "configs" / Path(pattern).name)))
    return runpy.run_path(str(DAG_FILE))


def agree(specs: list[dict[str, Any]], tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    pytest.importorskip("airflow")
    made = build(specs, tmp_path, monkeypatch)
    assert specs
    for spec in specs:
        rivet = {t.task_id for t in made[spec["dag_id"]].tasks if t.task_id.startswith(RIVET_TASK)}
        assert rivet == set(dagspec.task_ids(spec)), spec["dag_id"]


def test_the_scenario_and_warehouse_dags_build_to_the_tasks_they_are_graded_on(tmp_path: Path, monkeypatch) -> None:
    agree(scenario.dag_specs(), tmp_path, monkeypatch)
    agree(warehouse.dag_specs(scenario.TABLES, warehouse.WAREHOUSES), tmp_path, monkeypatch)
    agree(warehouse.dag_specs(scenario.TABLES, warehouse.WAREHOUSES, env_key=True), tmp_path, monkeypatch)


def test_the_s3_legs_dags_build_to_the_tasks_they_are_graded_on(tmp_path: Path, monkeypatch) -> None:
    leg, stand = prepared_s3(tmp_path / "s3", monkeypatch)
    specs = list(written_specs(stand, "s3").values())
    agree(specs, tmp_path / "built", monkeypatch)
    assert {s["dag_id"] for s in specs} >= {*leg.dags, *leg.probe_dags}, "every DAG the leg runs has a spec"


def test_the_multi_host_pipelines_build_to_their_tasks(tmp_path: Path, monkeypatch) -> None:
    both = ["gate_customers", "gate_orders"]
    stream = {"name": "cdc", "tables": both}
    specs = [
        multihost.MultiHost.spec(p, {"exports": both, "streams": [stream] if p.mode == "cdc" else []})
        for p in multihost.PIPELINES
    ]
    agree(specs, tmp_path, monkeypatch)
    chain = next(s for s in specs if s["dag_id"] == "gate_mh_chain")
    # The chain pipeline loads one export only, on the queue the gate moves.
    assert dagspec.task_ids(chain) == ["apply.gate_customers", "load.gate_customers"]
    assert chain["task_queue"]["load.gate_customers"] == "gate_b"


def test_the_migration_dags_build_to_their_tasks(tmp_path: Path, monkeypatch) -> None:
    from test_migrate import migration_specs

    agree(list(migration_specs().values()), tmp_path, monkeypatch)
