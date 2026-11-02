"""Every ledger, allowlist, ceiling and ratchet this repository keeps dates each entry it accepts and says why."""

from __future__ import annotations

import importlib.util
import re
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
DATE = re.compile(r"\d{4}-\d{2}-\d{2}")


def _module(path: str):
    """A module loaded from its file, for a ledger that lives beside a script."""
    spec = importlib.util.spec_from_file_location(Path(path).stem, ROOT / path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def _dated(entry: tuple[str, ...]) -> bool:
    """Whether an entry's first field is a YYYY-MM-DD date and its others are non-empty text."""
    return bool(DATE.fullmatch(entry[0])) and all(part.strip() for part in entry[1:])


def test_every_skip_the_ci_jobs_allow_is_dated_with_a_reason_and_what_closes_it() -> None:
    entries = [entry for jobs in _module("dev/skip_census.py").SKIP_ALLOWED.values() for entry in jobs.values()]
    assert entries and all(_dated(entry) for entry in entries)


def test_every_weak_assert_ceiling_is_dated_with_a_reason() -> None:
    ceilings = _module("dev/test_checks.py").CEILING
    assert all(_dated(entry[1:]) for entry in ceilings.values())


def test_every_workflow_fact_the_ledger_records_is_dated_with_a_reason() -> None:
    ledger = _module("tests/test_workflows.py").LEDGER
    assert all(_dated(entry) for entry in ledger.values())


def test_the_mutation_ratchet_value_sits_under_a_dated_entry() -> None:
    lines = (ROOT / ".mutmut-ratchet").read_text().splitlines()
    value = next(i for i, line in enumerate(lines) if line.strip() and not line.startswith("#"))
    block = []
    for line in reversed(lines[:value]):
        if not line.startswith("#"):
            break
        block.append(line)
    assert any(re.match(r"# \d{4}-\d{2}-\d{2}: ", line) for line in block), "the ratchet value has no dated entry above it"


def test_the_mutation_survivors_record_is_dated_in_its_header() -> None:
    header = [line for line in (ROOT / ".mutmut-survivors").read_text().splitlines() if line.startswith("#")]
    assert any(re.match(r"# \d{4}-\d{2}-\d{2}: ", line) for line in header)


def test_a_dev_module_outside_the_tests_names_its_exemption_with_a_date() -> None:
    assert all(_dated(entry) for entry in _module("tests/test_dev_modules.py").DEV_EXEMPT.values())
