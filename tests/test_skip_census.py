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
    problems = census.grade(census.skipped(junit), "airflow")
    assert problems == ["unallowed skip tests.test_x::test_y: no airflow here", "1 skips over the airflow ceiling of 0"]


def test_the_allowed_slack_skip_of_the_locked_job_passes_and_its_absence_is_a_stale_entry(tmp_path: Path) -> None:
    junit = _junit(
        tmp_path / "j.xml",
        ("tests.test_slack_alerts", "test_the_fake_slack_hooks_have_the_shape_of_the_slack_providers_hooks", "no module"),
    )
    assert census.grade(census.skipped(junit), "locked") == []
    empty = _junit(tmp_path / "e.xml")
    assert census.grade(census.skipped(empty), "locked") == [
        f"stale ledger entry {SLACK}: no run of locked skips it any more, remove it"
    ]


def test_a_second_skip_over_the_ceiling_fails_even_when_it_is_allowed_by_name(tmp_path: Path) -> None:
    junit = _junit(
        tmp_path / "j.xml",
        ("tests.test_slack_alerts", "test_the_fake_slack_hooks_have_the_shape_of_the_slack_providers_hooks", "a"),
        ("tests.z", "test_q", "b"),
    )
    problems = census.grade(census.skipped(junit), "locked")
    assert problems == ["unallowed skip tests.z::test_q: b", "2 skips over the locked ceiling of 1"]


def test_main_exits_one_on_a_problem_and_zero_on_a_clean_job(tmp_path: Path, capsys: pytest.CaptureFixture) -> None:
    assert census.main([str(_junit(tmp_path / "a.xml")), "--job", "airflow"]) == 0
    assert census.main([str(_junit(tmp_path / "b.xml", ("tests.q", "test_r", "x"))), "--job", "airflow"]) == 1
    assert "unallowed skip tests.q::test_r: x" in capsys.readouterr().out
