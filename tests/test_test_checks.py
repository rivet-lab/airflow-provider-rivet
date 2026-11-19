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
    with pytest.raises((PreflightRefusal, ValueError)):
        pass
    assert bool(x)
    assert len(x)
    assert not not x
    assert not x
from pytest import raises as _r
import pytest as _pt
with _r(PreflightRefusal):
    pass
with _pt.raises(PreflightRefusal):
    pass
"""


def test_each_weak_kind_is_found_and_a_strong_assert_or_a_matched_refusal_is_not() -> None:
    found = sorted(checks.scan(SNIPPET, checks.refusals()), key=lambda hit: hit[1])
    assert found == [
        ("assert-true", 6),
        ("or-is-none", 7),
        ("bare-assert", 8),
        ("bare-assert", 9),
        ("raises-no-match", 12),
        ("raises-no-match", 16),
        ("raises-no-match", 20),
        ("bare-assert", 22),
        ("bare-assert", 23),
        ("bare-assert", 24),
        ("raises-no-match", 28),
        ("raises-no-match", 30),
    ]


def test_a_refusal_is_one_the_package_defines_or_a_broad_base() -> None:
    refused = checks.refusals()
    assert {"PreflightRefusal", "ConfigRefusal", "Stopped", "Exception", "AirflowFailException"} <= refused
    assert "ValueError" not in refused and "RivetConfigRefused" not in refused


def test_the_suites_sit_at_their_dated_ceilings() -> None:
    assert checks.problems() == []
