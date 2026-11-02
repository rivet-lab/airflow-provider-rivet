"""Every refusal code is a `Code` member, documented, and asserted by a test through its code."""

from __future__ import annotations

from conftest import DOCS, PACKAGE_ROOT

from airflow_provider_rivet.codes import Code

PACKAGE_CODES = sorted(c.value for c in Code)
TESTS = "".join(
    p.read_text()
    for p in sorted((PACKAGE_ROOT / "tests").glob("test_*.py")) + sorted((PACKAGE_ROOT / "gate" / "tests").glob("test_*.py"))
)


def test_the_registry_holds_the_codes_the_package_raises() -> None:
    """The registry is not empty: the codes the guide names are members of `Code`."""
    assert len(PACKAGE_CODES) >= 30 and "RIVET_AIRFLOW_BINARY_MISSING" in PACKAGE_CODES


def test_every_refusal_code_is_documented() -> None:
    """A code a user can see in a failed task appears in the README or a docs page."""
    undocumented = [c for c in PACKAGE_CODES if c not in DOCS]
    assert not undocumented, f"refusal codes with no docs row: {undocumented}"


def test_every_refusal_code_is_asserted_by_a_test_through_its_code() -> None:
    """A code no test names is a refusal nothing guards: its text can change, or its raise can go, unseen."""
    unasserted = [c for c in PACKAGE_CODES if c not in TESTS]
    assert not unasserted, f"refusal codes no test asserts by name: {unasserted}"
