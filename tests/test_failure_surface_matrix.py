"""The failure-surface matrix has a row for every refusal code, and every cell is a named test that exists or a dated gap."""

from __future__ import annotations

import ast
import datetime
import re
from pathlib import Path
from typing import Any

import yaml
from test_refusal_codes import PACKAGE_CODES

ROOT = Path(__file__).resolve().parents[1]
MATRIX = ROOT / "docs" / "failure-surface-matrix.yaml"
SURFACES = ("exception", "task_log", "slack", "xcom", "retry")
NON_CODE_ROWS = frozenset({"rivet-exit-nonzero", "rivet-signal", "rivet-timeout", "empty-source", "init-draft-unreadable"})
TEST_CELL = re.compile(r"^test: (tests/[\w/]+\.py)::(\w+)$")
CALLBACK_ENTRY = frozenset({"slack_failure_callback", "posted", "posted_for"})


class _UniqueKeys(yaml.SafeLoader):
    """A YAML loader that refuses a mapping key written twice."""


def _mapping(loader: yaml.SafeLoader, node: yaml.MappingNode) -> dict[Any, Any]:
    """A mapping that raises on a repeated key."""
    seen: dict[Any, Any] = {}
    for key_node, value_node in node.value:
        key = loader.construct_object(key_node, deep=True)
        if key in seen:
            raise ValueError(f"duplicate key {key!r}")
        seen[key] = loader.construct_object(value_node, deep=True)
    return seen


_UniqueKeys.add_constructor(yaml.resolver.BaseResolver.DEFAULT_MAPPING_TAG, _mapping)


def _matrix() -> dict[str, Any]:
    """The matrix as loaded, with a repeated key refused."""
    return yaml.load(MATRIX.read_text(encoding="utf-8"), Loader=_UniqueKeys)


def _defined_names(path: Path) -> set[str]:
    """The top-level function and class names a test file defines, read by AST."""
    tree = ast.parse(path.read_text(encoding="utf-8"), filename=str(path))
    return {node.name for node in tree.body if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef, ast.ClassDef))}


def _gap_problem(gap: Any) -> str | None:
    """Why a gap is malformed, or None when it holds a date, a reason and a closure."""
    if not isinstance(gap, dict) or set(gap) != {"date", "reason", "closes"}:
        return "gap must hold exactly date, reason and closes"
    date = gap["date"]
    if isinstance(date, str):
        try:
            datetime.date.fromisoformat(date)
        except ValueError:
            return f"gap date {date!r} is not an ISO date"
    elif not isinstance(date, datetime.date):
        return f"gap date {date!r} is not a date"
    for field in ("reason", "closes"):
        if not isinstance(gap[field], str) or not gap[field].strip():
            return f"gap {field} is empty"
    return None


def _cell_problem(cell: str) -> str | None:
    """Why a test cell does not name a test that exists, or None when it does."""
    found = TEST_CELL.match(cell)
    if not found:
        return f"cell {cell!r} is not `test: tests/file.py::name`"
    path = ROOT / found.group(1)
    if not path.is_file():
        return f"test file {found.group(1)} does not exist"
    if found.group(2) not in _defined_names(path):
        return f"{found.group(2)} is not defined in {found.group(1)}"
    return None


def _function(path: Path, name: str) -> ast.AST | None:
    """The top-level function or method definition `name` in a file, by AST, or None."""
    tree = ast.parse(path.read_text(encoding="utf-8"), filename=str(path))
    return next((n for n in ast.walk(tree) if isinstance(n, (ast.FunctionDef, ast.AsyncFunctionDef)) and n.name == name), None)


def _names(node: ast.AST) -> set[str]:
    """Every name the node reads, by AST."""
    return {n.id for n in ast.walk(node) if isinstance(n, ast.Name)}


def _slack_problem(cell: str) -> str | None:
    """Why a Slack cell's test does not drive the callback entry point, or None when it does."""
    found = TEST_CELL.match(cell)
    if not found or _cell_problem(cell):
        return None
    test = _function(ROOT / found.group(1), found.group(2))
    if not _names(test) & CALLBACK_ENTRY:
        return f"{found.group(2)} does not drive the Slack callback ({', '.join(sorted(CALLBACK_ENTRY))})"
    return None


def _cells() -> list[tuple[str, str, Any]]:
    """Every (row, surface, cell) of the matrix."""
    return [
        (row, surface, cell)
        for row, surfaces in _matrix().items()
        if isinstance(surfaces, dict)
        for surface, cell in surfaces.items()
    ]


def test_every_refusal_code_has_a_row() -> None:
    """Each refusal code the package raises has a row, and each code row is a code the package raises."""
    rows = set(_matrix())
    codes = set(PACKAGE_CODES)
    assert len(codes) > 0, "the package raises no refusal codes"
    assert codes <= rows, f"no row for {sorted(codes - rows)}"
    assert NON_CODE_ROWS <= rows, f"no row for {sorted(NON_CODE_ROWS - rows)}"
    extra = rows - codes - NON_CODE_ROWS
    assert not extra, f"rows for codes the package does not raise: {sorted(extra)}"


def test_every_row_has_every_surface() -> None:
    """Each row holds exactly the five surfaces, each with a cell."""
    problems: list[str] = []
    for row, surfaces in _matrix().items():
        if not isinstance(surfaces, dict):
            problems.append(f"{row}: not a mapping of surfaces")
            continue
        problems += [f"{row}.{s}: no cell" for s in SURFACES if surfaces.get(s) in (None, "", {})]
        problems += [f"{row}: unexpected surface {s}" for s in surfaces if s not in SURFACES]
    assert not problems, "\n".join(problems)


def test_every_cell_is_a_test_or_a_gap() -> None:
    """Each cell is a `test:` string or a gap mapping, nothing else."""
    problems = [
        f"{row}.{s}: {cell!r} is neither a test nor a gap"
        for row, s, cell in _cells()
        if not isinstance(cell, str) and not (isinstance(cell, dict) and set(cell) == {"gap"})
    ]
    assert not problems, "\n".join(problems)


def test_every_named_test_exists() -> None:
    """Each test cell names a test file that exists and a test defined in it, found by AST."""
    problems = [f"{row}.{s}: {_cell_problem(cell)}" for row, s, cell in _cells() if isinstance(cell, str) and _cell_problem(cell)]
    assert not problems, "\n".join(problems)


def test_every_gap_carries_a_date() -> None:
    """Each gap holds a date, a reason and what closes it."""
    problems = [
        f"{row}.{s}: {_gap_problem(cell.get('gap'))}"
        for row, s, cell in _cells()
        if isinstance(cell, dict) and _gap_problem(cell.get("gap"))
    ]
    assert not problems, "\n".join(problems)


def test_every_slack_cell_names_a_test_that_drives_the_callback() -> None:
    """Each Slack cell names a test that calls `slack_failure_callback`, directly or through the helpers `posted` and `posted_for`."""
    problems = [
        f"{row}.slack: {_slack_problem(cell)}"
        for row, s, cell in _cells()
        if s == "slack" and isinstance(cell, str) and _slack_problem(cell)
    ]
    assert not problems, "\n".join(problems)


def test_the_slack_helpers_call_the_callback() -> None:
    """The helpers the Slack cells name post through the real `slack_failure_callback`, not a formatter."""
    helpers = ROOT / "tests" / "test_slack_alerts.py"
    assert all("slack_failure_callback" in _names(_function(helpers, name)) for name in ("posted", "posted_for"))
