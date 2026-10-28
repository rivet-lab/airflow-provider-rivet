"""The gate's one DAG file (gate/stand/dags/rivet_gate.py) builds the specs the gate's legs write (gate/dagspec.py).

Run against the real DAG builders, so what the host grades (`dagspec.task_ids`) is held to the tasks Airflow really gets.
Each test names the mutant it is RED on.
"""

from __future__ import annotations

import glob
import importlib.util
import runpy
from datetime import timedelta
from pathlib import Path
from typing import Any

import pytest

ROOT = Path(__file__).resolve().parents[1]
DAG_FILE = ROOT / "gate" / "stand" / "dags" / "rivet_gate.py"
_spec = importlib.util.spec_from_file_location("gate_dagspec", ROOT / "gate" / "dagspec.py")
dagspec = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(dagspec)

KWARGS = {"env_from_connections": {"SRC_URL": "src", "RIVET_STATE_URL": "state"}, "connections": ["s3"]}
COMMON = {
    "config": "/opt/rivet/configs/c.yaml",
    "state_dir": "/opt/rivet/state/c",
    "operator_kwargs": KWARGS,
    "tags": ["rivet", "x"],
}
RIVET_TASK = ("apply.", "load.", "compact.", "run", "load")


def specs() -> list[dict[str, Any]]:
    return [
        dagspec.batch("g_batch", exports=["a", "b"], **COMMON),
        dagspec.batch("g_batch_load", exports=["a", "b"], compact_exports=["b"], **COMMON),
        dagspec.batch("g_batch_extract", exports=["a"], load=False, retries=0, **COMMON),
        dagspec.batch(
            "g_held", exports=["a", "b"], load=False, retries=1, retry_delay=90,
            task_env={"apply.b": {"RIVET_TEST_PAUSE_AT": "x:1"}}, task_queue={"apply.a": "q_a", "apply.b": "q_b"}, **COMMON,
        ),  # fmt: skip
        dagspec.cdc("g_cdc", export="s", tables=["a", "b"], **COMMON),
        dagspec.cdc("g_cdc_extract", export="s", tables=["a", "b"], load=False, **COMMON),
        dagspec.cdc("g_cdc_nocompact", export="s", tables=["a"], compact=False, task_queue={"run": "q_cdc"}, **COMMON),
        dagspec.load_only("g_load", export="a", retries=0, config=COMMON["config"], state_dir=COMMON["state_dir"],
                          operator_kwargs=KWARGS, tags=["rivet", "x"]),
    ]  # fmt: skip


@pytest.fixture
def built(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> dict[str, Any]:
    (tmp_path / "configs").mkdir()
    dagspec.write(tmp_path, "one", specs()[:4])
    dagspec.write(tmp_path, "two", specs()[4:])
    real = glob.glob
    monkeypatch.setattr(glob, "glob", lambda pattern: real(str(tmp_path / "configs" / Path(pattern).name)))
    return runpy.run_path(str(DAG_FILE))


def rivet_tasks(dag: Any) -> set[str]:
    return {t.task_id for t in dag.tasks if t.task_id.startswith(RIVET_TASK)}


def test_every_spec_becomes_its_dag_with_exactly_the_tasks_the_host_grades(built: dict[str, Any]) -> None:
    for spec in specs():
        # Mutant: a task list the host derives on its own (task_ids drifting from the builders), or a spec skipped.
        assert spec["dag_id"] in built, spec["dag_id"]
        assert rivet_tasks(built[spec["dag_id"]]) == set(dagspec.task_ids(spec)), spec["dag_id"]


def test_the_dag_carries_the_tags_retries_and_operator_kwargs_the_spec_names(built: dict[str, Any]) -> None:
    held = built["g_held"]
    assert set(held.tags) == {"rivet", "x"}
    # Mutant: `retry_delay` left as the number of seconds (Airflow needs a timedelta), or the retries dropped.
    assert held.default_args["retries"] == 1 and held.default_args["retry_delay"] == timedelta(seconds=90)
    assert built["g_load"].default_args == {"retries": 0}
    assert built["g_batch_extract"].default_args["retries"] == 0
    # The spec's operator kwargs reach every rivet task (the Connections it reads).
    for task in built["g_cdc"].tasks:
        if task.task_id.startswith(RIVET_TASK):
            assert task.env_from_connections == KWARGS["env_from_connections"], task.task_id
    load = built["g_load"].get_task("load")
    assert (type(load).__name__, load.export, load.config, load.state_dir) == (
        "RivetLoadOperator",
        "a",
        COMMON["config"],
        COMMON["state_dir"],
    )
    assert load.env_from_connections == KWARGS["env_from_connections"] and list(load.connections) == KWARGS["connections"]
    # Mutant: the load-only DAG allowed two runs at once or catching up (the builders' DAGs are neither).
    for spec in specs():
        assert built[spec["dag_id"]].max_active_runs == 1 and built[spec["dag_id"]].catchup is False, spec["dag_id"]


def test_a_tasks_pause_env_and_queue_are_set_where_the_spec_says(built: dict[str, Any]) -> None:
    held = built["g_held"]
    # Mutant: the env on every apply (the paused one is not alone), or the queues not applied.
    assert held.get_task("apply.b").env == {"RIVET_TEST_PAUSE_AT": "x:1"}
    assert not held.get_task("apply.a").env
    assert {t.task_id: t.queue for t in held.tasks if t.task_id.startswith("apply.")} == {"apply.a": "q_a", "apply.b": "q_b"}
    assert built["g_cdc_nocompact"].get_task("run").queue == "q_cdc"
    assert built["g_cdc"].get_task("run").queue == "default"


def test_a_spec_file_that_repeats_an_id_is_refused_before_the_stack_parses_it(tmp_path: Path) -> None:
    (tmp_path / "configs").mkdir()
    with pytest.raises(ValueError, match="repeat"):
        dagspec.write(tmp_path, "x", [specs()[0], specs()[0]])
    assert not list((tmp_path / "configs").iterdir()), "nothing half-written"
