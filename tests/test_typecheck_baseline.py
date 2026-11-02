"""dev/typecheck_baseline.py: diagnostics are counted per file and rule, and the baseline may not grow or stay stale."""

from __future__ import annotations

import importlib.util
import json
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]
SPEC = importlib.util.spec_from_file_location("typecheck_baseline", ROOT / "dev" / "typecheck_baseline.py")
baseline = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(baseline)


def _report(*rules: tuple[str, str]) -> dict:
    return {"generalDiagnostics": [{"file": str(ROOT / path), "rule": rule} for path, rule in rules]}


def test_diagnostics_are_counted_by_file_and_rule() -> None:
    report = _report(
        ("airflow_provider_rivet/a.py", "reportX"),
        ("airflow_provider_rivet/a.py", "reportX"),
        ("airflow_provider_rivet/b.py", "reportY"),
    )
    assert baseline.counts(report) == {"airflow_provider_rivet/a.py::reportX": 2, "airflow_provider_rivet/b.py::reportY": 1}


def _check(tmp_path: Path, monkeypatch: pytest.MonkeyPatch, base: dict, *rules: tuple[str, str]) -> int:
    path = tmp_path / "baseline.json"
    path.write_text(json.dumps(base))
    report = tmp_path / "report.json"
    report.write_text(json.dumps(_report(*rules)))
    monkeypatch.setattr(baseline, "BASELINE", path)
    return baseline.main(["--json", str(report)])


def test_a_new_diagnostic_fails_and_a_stale_baseline_fails_and_an_equal_one_passes(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    one = {"airflow_provider_rivet/a.py::reportX": 1}
    assert _check(tmp_path, monkeypatch, one, ("airflow_provider_rivet/a.py", "reportX")) == 0
    assert (
        _check(tmp_path, monkeypatch, one, ("airflow_provider_rivet/a.py", "reportX"), ("airflow_provider_rivet/a.py", "reportX"))
        == 1
    )
    assert _check(tmp_path, monkeypatch, one) == 1
