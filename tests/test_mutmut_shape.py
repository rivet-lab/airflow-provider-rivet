"""The source-reading guards pass on a mutmut-shaped copy of the package, so a mutant run cannot red them."""

from __future__ import annotations

import ast
import os
import shutil
import subprocess
import sys
from pathlib import Path

from conftest import RIVET_SRC

ROOT = Path(__file__).resolve().parents[1]
GUARDS = (
    "tests/test_refusal_codes.py",
    "tests/test_architecture.py",
    "tests/test_failure_surface_matrix.py",
    "tests/test_contract.py",
    "tests/test_test_checks.py",
)
IGNORED = shutil.ignore_patterns(".git", ".venv", "__pycache__", ".pytest_cache", "mutants", "target", "*.egg-info")


def _functions(tree: ast.Module):
    """(the mutmut name, node) of every module-level function and of every method of a module-level class."""
    for node in tree.body:
        if isinstance(node, ast.FunctionDef) and "__mutmut_" not in node.name:
            yield f"x_{node.name}", node
        elif isinstance(node, ast.ClassDef):
            for sub in node.body:
                if isinstance(sub, ast.FunctionDef) and "__mutmut_" not in sub.name:
                    yield f"xǁ{node.name}ǁ{sub.name}", sub


def _marked(lines: list[str], node: ast.FunctionDef, col: int) -> list[str]:
    """The lines with the first one-line string of the body wrapped in XX inside its quotes, or unchanged."""
    body = node.body[1:] if isinstance(node.body[0], ast.Expr) and isinstance(node.body[0].value, ast.Constant) else node.body
    for stmt in body:
        for sub in ast.walk(stmt):
            if isinstance(sub, ast.Constant) and isinstance(sub.value, str) and sub.lineno == sub.end_lineno:
                row = sub.lineno - node.lineno
                start, end = sub.col_offset - col, sub.end_col_offset - col
                literal = lines[row][start:end]
                quote = next((i for i, ch in enumerate(literal) if ch in "'\""), None)
                if quote is None or len(literal) - quote < 2:
                    continue
                marked = literal[: quote + 1] + "XX" + literal[quote + 1 : -1] + "XX" + literal[-1]
                out = list(lines)
                out[row] = lines[row][:start] + marked + lines[row][end:]
                return out
    return lines


def mutmut_shaped(source: str) -> str:
    """The module's source with a `__mutmut_orig` and a `__mutmut_1` copy of every function appended, as mutmut lays them out."""
    lines = source.splitlines()
    copies = []
    for name, node in _functions(ast.parse(source)):
        col = node.col_offset
        plain = [line[col:] if line[:col].strip() == "" else line.lstrip() for line in lines[node.lineno - 1 : node.end_lineno]]
        for suffix, text in (("__mutmut_orig", plain), ("__mutmut_1", _marked(plain, node, col))):
            text = list(text)
            text[0] = text[0].replace(f"def {node.name}(", f"def {name}{suffix}(", 1)
            copies.append("\n".join(text) + "\n")
    return source.rstrip("\n") + "\n\n\n" + "\n".join(copies)


def test_the_source_guards_pass_on_a_mutmut_shaped_copy(tmp_path: Path) -> None:
    """The guards that read airflow_provider_rivet/ as text or AST pass with the mutant copies beside every function."""
    copy = tmp_path / "repo"
    shutil.copytree(ROOT, copy, ignore=IGNORED)
    for path in sorted((copy / "airflow_provider_rivet").glob("*.py")):
        path.write_text(mutmut_shaped(path.read_text()))
    run = subprocess.run(
        [sys.executable, "-m", "pytest", "-q", "-p", "no:cacheprovider", *GUARDS],
        cwd=copy,
        env={
            **os.environ,
            "PYTHONPATH": str(copy),
            "RIVET_SRC": str(RIVET_SRC),
        },
        capture_output=True,
        text=True,
    )
    assert run.returncode == 0, run.stdout[-4000:] + run.stderr[-2000:]
