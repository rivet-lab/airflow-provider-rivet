"""The SQLite pause window, graded by trigger_cycle on a fake clock: the work after the marker, not the time to start it."""

from __future__ import annotations

from types import SimpleNamespace

import pytest

from gate import scenario


class FakeClock:
    """`time` for the scenario module: the monotonic clock moves only when a fake step advances it."""

    def __init__(self) -> None:
        self.now = 1000.0

    def monotonic(self) -> float:
        """The fake clock's reading."""
        return self.now

    def sleep(self, seconds: float) -> None:
        """Advance the fake clock."""
        self.now += seconds


def run_cycle(monkeypatch: pytest.MonkeyPatch, tmp_path, work_s: float) -> scenario.Verdicts:
    """trigger_cycle with the other DAGs taking `work_s` seconds, the CDC run holding its full pause."""
    clock = FakeClock()
    cdc_s = scenario.PAUSE_MS / 1000

    def wait_runs(stand, dags, run_id, timeout=900):
        clock.now += cdc_s if "gate_cdc_sqlite" in dags else work_s
        return {dag: "success" for dag in dags}

    monkeypatch.setattr(scenario, "time", clock)
    monkeypatch.setattr(scenario, "wait_file", lambda path, timeout=180: True)
    monkeypatch.setattr(scenario, "wait_runs", wait_runs)
    stand = SimpleNamespace(rivet_dir=tmp_path, trigger=lambda dag, run_id: None)
    v = scenario.Verdicts(stack="2.10.5")
    scenario.trigger_cycle(stand, v, "delta", "run", ["gate_cdc_sqlite", "gate_batch_sqlite"])
    return v


def window_row(v: scenario.Verdicts) -> dict[str, str]:
    """The graded pause-window cell."""
    (row,) = [r for r in v.rows if r["cell"] == "delta/sqlite-pause-window-opened"]
    return row


def test_work_that_outlasts_the_pause_fails_the_window(monkeypatch: pytest.MonkeyPatch, tmp_path) -> None:
    """Mutant: the window is measured from the marker to the start of the wait, so a 120 s batch run reads as 0 s."""
    row = window_row(run_cycle(monkeypatch, tmp_path, work_s=120))
    assert row["status"] == "FAIL"
    assert "the pause is 45000 ms" in row["detail"]


def test_work_inside_the_pause_passes_the_window(monkeypatch: pytest.MonkeyPatch, tmp_path) -> None:
    assert window_row(run_cycle(monkeypatch, tmp_path, work_s=10))["status"] == "PASS"
