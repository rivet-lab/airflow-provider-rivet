"""operator_kwargs: what a builder sets on every rivet task for the run's sake is refused there, never overridden (L1)."""

from __future__ import annotations

from pathlib import Path
from typing import Any

import pytest

from airflow_provider_rivet.dags import build_batch_dag, build_cdc_dag, build_source_dags

SLOT_TEXT = (
    "operator_kwargs may not set `max_active_tis_per_dag`: it is 1 on every rivet task, so a task never runs beside "
    "another try of itself (docs/limits.md, Overlap)"
)
GATE_TEXT = (
    "operator_kwargs may not set `trigger_rule`: every rivet task runs only after its upstream succeeded "
    "(load after its extract, compact after its load); the watcher fails the run on any failure"
)


def build(kind: str, tmp_path: Path, operator_kwargs: dict[str, Any]) -> Any:
    if kind == "batch":
        return build_batch_dag("d", config="c", exports=["a"], operator_kwargs=operator_kwargs)
    if kind == "cdc":
        return build_cdc_dag("d", config="c", export="e", tables=["t"], operator_kwargs=operator_kwargs)
    return build_source_dags(
        ["shop"], mode="cdc", config_dir=str(tmp_path), state_root="/s", state_connection="st", operator_kwargs=operator_kwargs
    )


@pytest.mark.parametrize("kind", ["batch", "cdc", "sources"])
@pytest.mark.parametrize("value", [1, 4, None])
def test_a_slot_limit_in_operator_kwargs_is_refused_not_overridden(tmp_path: Path, kind: str, value: Any) -> None:
    with pytest.raises(ValueError) as exc:
        build(kind, tmp_path, {"max_active_tis_per_dag": value})
    assert str(exc.value) == SLOT_TEXT


@pytest.mark.parametrize("kind", ["batch", "cdc", "sources"])
def test_the_trigger_rule_refusal_and_the_other_kwargs_are_what_they_were(tmp_path: Path, kind: str) -> None:
    with pytest.raises(ValueError) as exc:
        build(kind, tmp_path, {"trigger_rule": "all_done"})
    assert str(exc.value) == GATE_TEXT
    if kind != "sources":
        dag = build(kind, tmp_path, {"retries": 3, "pool": "p"})
        assert {t.max_active_tis_per_dag for t in dag.tasks if t.task_id != "watcher"} == {1}
        assert {t.pool for t in dag.tasks if t.task_id != "watcher"} == {"p"}
