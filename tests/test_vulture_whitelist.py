"""The vulture whitelist: every entry names its date and its reason."""

from __future__ import annotations

import re

from conftest import PACKAGE_ROOT

ENTRY = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*  # \d{4}-\d{2}-\d{2}: \S.*$")


def test_every_whitelist_entry_has_a_date_and_a_reason() -> None:
    """A line of the whitelist is a name, two spaces, `#`, the date, a colon and a reason."""
    lines = [ln for ln in (PACKAGE_ROOT / "dev" / "vulture_whitelist.py").read_text().splitlines() if ln.strip()]
    entries = [ln for ln in lines if not ln.startswith(('"""', "#"))]
    bad = [ln for ln in entries if not ENTRY.match(ln)]
    assert entries and not bad, f"whitelist lines without a date and a reason: {bad}"
