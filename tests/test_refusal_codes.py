"""Every refusal code is a `Code` member and documented; its test is held by the failure-surface matrix."""

from __future__ import annotations

from conftest import DOCS

from airflow_provider_rivet.codes import Code

PACKAGE_CODES = sorted(c.value for c in Code)


def test_the_registry_holds_the_codes_the_package_raises() -> None:
    """The registry is not empty: the codes the guide names are members of `Code`."""
    assert len(PACKAGE_CODES) >= 30 and "RIVET_AIRFLOW_BINARY_MISSING" in PACKAGE_CODES


def test_every_refusal_code_is_documented() -> None:
    """A code a user can see in a failed task appears in the README or a docs page."""
    undocumented = [c for c in PACKAGE_CODES if c not in DOCS]
    assert not undocumented, f"refusal codes with no docs row: {undocumented}"
