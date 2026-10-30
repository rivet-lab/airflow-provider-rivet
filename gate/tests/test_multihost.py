"""The multi-host leg's SQLite cell: every extract of the run refused before rivet started, nothing else accepted."""

from __future__ import annotations

from pathlib import Path
from types import SimpleNamespace

import pytest

from gate.multihost import SQLITE_MULTI_HOST, TABLES, MultiHost, cdc_prefix

FIXTURES = Path(__file__).parent / "fixtures"


def judge(state: str, tasks: dict[str, str], codes: dict[str, str | None]) -> list[str]:
    stand = SimpleNamespace(
        tasks=lambda dag, run_id: {t: {"state": s} for t, s in tasks.items()},
        xcom=lambda dag, run_id, task: {"preflight_refusal": codes.get(task)},
    )
    leg = MultiHost.__new__(MultiHost)
    leg.stand = stand
    return leg.sqlite_refused(state, "run")


def test_a_run_whose_extracts_were_all_refused_passes() -> None:
    tasks = {"apply.a": "failed", "apply.b": "failed", "watcher": "failed"}
    assert judge("failed", tasks, {"apply.a": SQLITE_MULTI_HOST, "apply.b": SQLITE_MULTI_HOST}) == []


@pytest.mark.parametrize(
    ("state", "tasks", "codes"),
    [
        ("success", {"apply.a": "success"}, {}),  # rivet ran on SQLite state across hosts
        ("failed", {"apply.a": "failed", "apply.b": "success"}, {"apply.a": SQLITE_MULTI_HOST}),  # one ran
        ("failed", {"apply.a": "failed"}, {"apply.a": "RIVET_AIRFLOW_STATE_DIR_MISSING"}),  # refused for another reason
        ("failed", {"apply.a": "failed"}, {"apply.a": None}),  # rivet started and failed
    ],
)
def test_anything_but_the_refusal_of_every_extract_fails(state: str, tasks: dict, codes: dict) -> None:
    assert judge(state, tasks, codes)


def test_the_cdc_tables_are_looked_up_under_the_prefix_init_wrote() -> None:
    # Mutant: the prefix hard-coded as `cdc/` (0.32.2 writes the stream under `rivet/cdc/`): the baseline never found.
    leg = MultiHost.__new__(MultiHost)
    leg.cdc_prefix = cdc_prefix((FIXTURES / "init_mysql_cdc.yaml").read_text())
    asked: list[str] = []
    leg.files = lambda p, under: asked.append(under) or set()
    leg._new_snapshots(None, 1)
    assert asked == [f"rivet/cdc/{t}/snapshot" for t in TABLES]
