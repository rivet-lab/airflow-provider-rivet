"""Count the weak asserts of the test suites per kind and hold each count to a dated ceiling that only shrinks."""

from __future__ import annotations

import ast
import importlib.util
import sys
from collections import defaultdict
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
_HELPER = importlib.util.spec_from_file_location("ast_names", ROOT / "dev" / "ast_names.py")
ast_names = importlib.util.module_from_spec(_HELPER)
_HELPER.loader.exec_module(ast_names)
SUITES = ("tests", "gate/tests")
BROAD = {"Exception", "BaseException", "AirflowException", "AirflowFailException"}

#: Per kind of weak assert: (the count the suites may hold, date accepted, reason). A count over it fails, and so does a
#: count under it: the ceiling is lowered in the same change, so it only shrinks.
CEILING: dict[str, tuple[int, str, str]] = {
    "assert-true": (0, "2026-11-02", "a literal True asserts nothing"),
    "or-is-none": (0, "2026-11-02", "an assert that accepts None passes a function that returned nothing"),
    "bare-assert": (10, "2026-11-02", "truthiness asserts that name no value; pin each value, then lower this"),
    "raises-no-match": (39, "2026-11-02", "pytest.raises of one of our refusals without match= does not check its code or text"),
}


def _name(node: ast.AST) -> str:
    """The rightmost name of a base class, a called function or a `pytest.raises` owner."""
    return node.id if isinstance(node, ast.Name) else node.attr if isinstance(node, ast.Attribute) else ""


def _is_none(node: ast.AST) -> bool:
    """Whether a comparison is `x is None`."""
    return (
        isinstance(node, ast.Compare)
        and isinstance(node.ops[0], ast.Is)
        and isinstance(node.comparators[0], ast.Constant)
        and node.comparators[0].value is None
    )


def refusals() -> set[str]:
    """Our exception classes (the package's Exception, Error or Refusal subclasses) and the broad bases."""
    names = set(BROAD)
    for path in (ROOT / "airflow_provider_rivet").glob("*.py"):
        for node in ast.walk(ast.parse(path.read_text())):
            if isinstance(node, ast.ClassDef) and any(
                _name(base).endswith(("Exception", "Error", "Refusal")) for base in node.bases
            ):
                names.add(node.name)
    return names


def _is_truthiness(test: ast.AST) -> bool:
    """Whether an assert tests truthiness only: a name, `bool(x)`, `len(x)` or `not not x`."""
    if isinstance(test, (ast.Name, ast.Attribute, ast.Subscript)):
        return True
    if isinstance(test, ast.Call):
        return isinstance(test.func, ast.Name) and test.func.id in ("bool", "len")
    return (
        isinstance(test, ast.UnaryOp)
        and isinstance(test.op, ast.Not)
        and isinstance(test.operand, ast.UnaryOp)
        and isinstance(test.operand.op, ast.Not)
    )


def scan(text: str, refused: set[str]) -> list[tuple[str, int]]:
    """(kind, line) of every weak assert in one test module, its names read through the module's imports."""
    tree = ast.parse(text)
    table = ast_names.import_table(tree)
    found = []
    for node in ast.walk(tree):
        if isinstance(node, ast.Assert):
            test = node.test
            if isinstance(test, ast.Constant) and test.value is True:
                found.append(("assert-true", node.lineno))
            elif isinstance(test, ast.BoolOp) and isinstance(test.op, ast.Or) and _is_none(test.values[0]):
                found.append(("or-is-none", node.lineno))
            elif _is_truthiness(test):
                found.append(("bare-assert", node.lineno))
        elif isinstance(node, ast.Call) and ast_names.dotted(node.func, table) == "pytest.raises" and node.args:
            first = node.args[0]
            classes = first.elts if isinstance(first, ast.Tuple) else [first]
            if any(ast_names.leaf(c, table) in refused for c in classes) and not any(k.arg == "match" for k in node.keywords):
                found.append(("raises-no-match", node.lineno))
    return found


def locate() -> dict[str, list[str]]:
    """The file:line of every weak assert of the suites, by kind."""
    refused = refusals()
    where: dict[str, list[str]] = defaultdict(list)
    for suite in SUITES:
        for path in sorted((ROOT / suite).rglob("*.py")):
            for kind, line in scan(path.read_text(), refused):
                where[kind].append(f"{path.relative_to(ROOT).as_posix()}:{line}")
    return where


def problems() -> list[str]:
    """Every kind whose count is over or under its ceiling, and every kind with no ceiling."""
    where = locate()
    out = [f"{kind}: no ceiling for it" for kind in sorted(where) if kind not in CEILING]
    for kind, (ceiling, _, _) in CEILING.items():
        count = len(where.get(kind, []))
        if count > ceiling:
            out.append(f"{kind}: {count} over the ceiling {ceiling}, e.g. {', '.join(where[kind][:5])}")
        elif count < ceiling:
            out.append(f"{kind}: {count} under the ceiling {ceiling}: lower the ceiling to {count} in CEILING")
    return out


def main(argv: list[str]) -> int:
    """Print the weak-assert counts against their ceilings; exit 1 on any count off its ceiling."""
    where = locate()
    print("weak asserts: " + ", ".join(f"{kind} {len(where.get(kind, []))}/{CEILING[kind][0]}" for kind in CEILING))
    found = problems()
    for problem in found:
        print(f"  {problem}")
    return 1 if found else 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
