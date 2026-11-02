"""dev/skip_census.py: each CI job's skips are allowed by its dated ledger, and the ledger only shrinks."""

from __future__ import annotations

import importlib.util
from pathlib import Path

import pytest

SPEC = importlib.util.spec_from_file_location("skip_census", Path(__file__).resolve().parents[1] / "dev" / "skip_census.py")
census = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(census)

SLACK = "tests.test_slack_alerts::test_the_fake_slack_hooks_have_the_shape_of_the_slack_providers_hooks"


def _junit(path: Path, *skipped: tuple[str, str, str]) -> Path:
    cases = (
        "".join(f'<testcase classname="{c}" name="{n}"><skipped message="{m}"/></testcase>' for c, n, m in skipped)
        + '<testcase classname="tests.test_ok" name="test_runs"/>'
    )
    path.write_text(f'<?xml version="1.0"?><testsuites><testsuite name="p">{cases}</testsuite></testsuites>')
    return path


def test_each_jobs_ceiling_is_the_length_of_its_ledger() -> None:
    assert census.SKIP_CEILING == {job: len(entries) for job, entries in census.SKIP_ALLOWED.items()}


def test_a_skip_the_ledger_does_not_allow_fails_its_job(tmp_path: Path) -> None:
    junit = _junit(tmp_path / "j.xml", ("tests.test_x", "test_y", "no airflow here"))
    problems = census.grade(census.skipped(junit), "gate-oracles")
    assert problems == ["unallowed skip tests.test_x::test_y: no airflow here", "1 skips over the gate-oracles ceiling of 0"]


def test_the_locked_ledger_passes_when_every_entry_is_skipped_and_names_a_stale_one(tmp_path: Path) -> None:
    everything = [tuple(test.split("::", 1)) + ("a",) for test in census.SKIP_ALLOWED["locked"]]
    assert census.grade(census.skipped(_junit(tmp_path / "j.xml", *everything)), "locked") == []
    without_slack = [case for case in everything if f"{case[0]}::{case[1]}" != SLACK]
    assert census.grade(census.skipped(_junit(tmp_path / "s.xml", *without_slack)), "locked") == [
        f"stale ledger entry {SLACK}: no run of locked skips it any more, remove it"
    ]


def test_a_skip_over_the_ceiling_fails_even_when_every_other_skip_is_allowed(tmp_path: Path) -> None:
    allowed = [tuple(test.split("::", 1)) + ("a",) for test in census.SKIP_ALLOWED["locked"]]
    junit = _junit(tmp_path / "j.xml", *allowed, ("tests.z", "test_q", "b"))
    problems = census.grade(census.skipped(junit), "locked")
    assert problems == [
        "unallowed skip tests.z::test_q: b",
        f"{len(allowed) + 1} skips over the locked ceiling of {census.SKIP_CEILING['locked']}",
    ]


def test_main_exits_one_on_a_problem_and_zero_on_a_clean_job(tmp_path: Path, capsys: pytest.CaptureFixture) -> None:
    assert census.main([str(_junit(tmp_path / "a.xml")), "--job", "gate-oracles"]) == 0
    assert census.main([str(_junit(tmp_path / "b.xml", ("tests.q", "test_r", "x"))), "--job", "gate-oracles"]) == 1
    assert "unallowed skip tests.q::test_r: x" in capsys.readouterr().out
