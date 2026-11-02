"""dev/test_checks.py: the weak-assert kinds it counts, and the suites held to their dated ceilings."""

from __future__ import annotations

import importlib.util
from pathlib import Path

SPEC = importlib.util.spec_from_file_location("test_checks", Path(__file__).resolve().parents[1] / "dev" / "test_checks.py")
checks = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(checks)

SNIPPET = """
import pytest
from airflow_provider_rivet.preflight import PreflightRefusal

def test_it(x, y):
    assert True
    assert x is None or y == 1
    assert x
    assert x.y[0]
    assert x == 1
    assert isinstance(x, int)
    with pytest.raises(Exception):
        pass
    with pytest.raises(PreflightRefusal, match="RIVET_"):
        pass
    with pytest.raises(PreflightRefusal):
        pass
    with pytest.raises(ValueError):
        pass
"""


def test_each_weak_kind_is_found_and_a_strong_assert_or_a_matched_refusal_is_not() -> None:
    found = checks.scan(SNIPPET, checks.refusals())
    assert found == [
        ("assert-true", 6),
        ("or-is-none", 7),
        ("bare-assert", 8),
        ("bare-assert", 9),
        ("raises-no-match", 12),
        ("raises-no-match", 16),
    ]


def test_a_refusal_is_one_the_package_defines_or_a_broad_base() -> None:
    refused = checks.refusals()
    assert {"PreflightRefusal", "ConfigRefusal", "Stopped", "Exception", "AirflowFailException"} <= refused
    assert "ValueError" not in refused and "RivetConfigRefused" not in refused


def test_the_suites_sit_at_their_dated_ceilings() -> None:
    assert checks.problems() == []
