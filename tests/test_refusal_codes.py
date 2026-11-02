"""Every refusal code the package names is documented, and a test asserts it by its code."""

from __future__ import annotations

import re

from conftest import DOCS, PACKAGE_ROOT

CODE = re.compile(r"RIVET_AIRFLOW_[A-Z_]+")
PACKAGE_CODES = sorted(set(CODE.findall("".join(p.read_text() for p in (PACKAGE_ROOT / "airflow_provider_rivet").rglob("*.py")))))
TESTS = "".join(
    p.read_text()
    for p in sorted((PACKAGE_ROOT / "tests").glob("test_*.py")) + sorted((PACKAGE_ROOT / "gate" / "tests").glob("test_*.py"))
)


def test_the_scan_finds_the_codes_the_package_raises() -> None:
    """The scan is not vacuous: the codes the guide names are in the package source."""
    assert len(PACKAGE_CODES) >= 30 and "RIVET_AIRFLOW_BINARY_MISSING" in PACKAGE_CODES


def test_every_refusal_code_is_documented() -> None:
    """A code a user can see in a failed task appears in the README or a docs page."""
    undocumented = [c for c in PACKAGE_CODES if c not in DOCS]
    assert not undocumented, f"refusal codes with no docs row: {undocumented}"


def test_every_refusal_code_is_asserted_by_a_test_through_its_code() -> None:
    """A code no test names is a refusal nothing guards: its text can change, or its raise can go, unseen."""
    unasserted = [c for c in PACKAGE_CODES if c not in TESTS]
    assert not unasserted, f"refusal codes no test asserts by name: {unasserted}"
