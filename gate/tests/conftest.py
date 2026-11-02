"""Gate oracle tests: with RIVET_GATE_NO_SKIPS set, a skipped test fails the run."""

from __future__ import annotations

import os

import pytest

_SKIPPED: list[str] = []


def _note(report: pytest.TestReport | pytest.CollectReport) -> None:
    """Remember a skip that is not an xfail."""
    if report.skipped and not hasattr(report, "wasxfail"):
        _SKIPPED.append(report.nodeid)


@pytest.fixture(autouse=True)
def no_inherited_git_env(monkeypatch: pytest.MonkeyPatch) -> None:
    """Drop every GIT_* variable: a git hook exports GIT_DIR into the process that runs pytest."""
    for name in [n for n in os.environ if n.startswith("GIT_")]:
        monkeypatch.delenv(name)


def pytest_collectreport(report: pytest.CollectReport) -> None:
    """Record module-level skips."""
    _note(report)


def pytest_runtest_logreport(report: pytest.TestReport) -> None:
    """Record test skips."""
    _note(report)


def pytest_sessionfinish(session: pytest.Session, exitstatus: int) -> None:
    """Fail a green run that skipped a test while the environment forbids skips."""
    if os.environ.get("RIVET_GATE_NO_SKIPS") and _SKIPPED and exitstatus == 0:
        print(f"\nFAIL: {len(_SKIPPED)} skipped test(s) with RIVET_GATE_NO_SKIPS set, e.g. {_SKIPPED[:3]}")
        session.exitstatus = 1
