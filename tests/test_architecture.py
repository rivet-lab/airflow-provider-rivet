"""Each seam of the package has one owner: docs/seams.yaml names it, and every module is scanned for a bypass of it."""

from __future__ import annotations

import ast
import io
import re
import tokenize
from collections import Counter, defaultdict
from dataclasses import dataclass
from datetime import date
from pathlib import Path
from typing import Any

import pytest
import yaml

ROOT = Path(__file__).resolve().parent.parent
SEAMS = ROOT / "docs" / "seams.yaml"
MODULE = "<module>"
DEFS = (ast.FunctionDef, ast.AsyncFunctionDef, ast.ClassDef)
MUTANT_DEF = re.compile(r"__mutmut_\d+$")
BRACE = re.compile(r"(?<![{$])\{[A-Za-z_]\w*\}(?!\})")


@dataclass(frozen=True)
class Site:
    """One occurrence of a rule's bypass: the file, the def or class around it, and its line."""

    path: str
    scope: str
    line: int
    text: str


def _rules() -> list[dict[str, Any]]:
    """The rules of docs/seams.yaml."""
    return yaml.safe_load(SEAMS.read_text(encoding="utf-8"))["rules"]


def _files(roots: list[str]) -> list[Path]:
    """The Python files under the given directories, sorted."""
    return sorted(path for root in roots for path in (ROOT / root).rglob("*.py"))


def _qualified(node: ast.AST, scope: str) -> str:
    """The qualname of a def or class node that sits in `scope`."""
    name = getattr(node, "name", "")
    return name if scope == MODULE else f"{scope}.{name}"


def _scoped(tree: ast.AST) -> list[tuple[ast.AST, str]]:
    """Every node with the qualname of the def or class it sits in; a mutmut copy (`name__mutmut_N`) is not read."""
    found: list[tuple[ast.AST, str]] = []
    stack: list[tuple[ast.AST, str]] = [(tree, MODULE)]
    while stack:
        node, scope = stack.pop()
        for child in ast.iter_child_nodes(node):
            if isinstance(child, DEFS):
                if MUTANT_DEF.search(child.name):
                    continue
                found.append((child, scope))
                stack.append((child, _qualified(child, scope)))
            else:
                found.append((child, scope))
                stack.append((child, scope))
    return found


def _module_names(tree: ast.Module) -> set[str]:
    """The names a module assigns at its top level."""
    targets = [t for node in tree.body if isinstance(node, ast.Assign) for t in node.targets]
    targets += [node.target for node in tree.body if isinstance(node, ast.AnnAssign)]
    return {t.id for t in targets if isinstance(t, ast.Name)}


def _rendered(node: ast.AST) -> str | None:
    """The text of a string literal, each placeholder of an f-string spelled `{}`; None for any other node."""
    if isinstance(node, ast.Constant) and isinstance(node.value, str):
        return node.value
    if isinstance(node, ast.JoinedStr):
        return "".join(part.value if isinstance(part, ast.Constant) else "{}" for part in node.values)
    return None


def _called(node: ast.AST, dotted: str) -> bool:
    """Whether a node calls the dotted name, such as `json.loads`."""
    if not isinstance(node, ast.Call):
        return False
    parts: list[str] = []
    target: ast.AST = node.func
    while isinstance(target, ast.Attribute):
        parts.append(target.attr)
        target = target.value
    if isinstance(target, ast.Name):
        parts.append(target.id)
    return ".".join(reversed(parts)) == dotted


def _refers_to(node: ast.AST, names: set[str]) -> bool:
    """Whether a node mentions one of the names, as a bare name or an attribute."""
    return any(
        (isinstance(n, ast.Name) and n.id in names) or (isinstance(n, ast.Attribute) and n.attr in names) for n in ast.walk(node)
    )


def _is_hit(rule: dict[str, Any], node: ast.AST, inner: set[int]) -> bool:
    """Whether one node is a bypass of an AST rule (call, raise, get or text)."""
    kind = rule["kind"]
    if kind == "call":
        return _called(node, rule["match"])
    if kind == "raise":
        return isinstance(node, ast.Raise) and node.exc is not None and _refers_to(node.exc, set(rule["names"]))
    if kind == "get":
        return (
            isinstance(node, ast.Call)
            and isinstance(node.func, ast.Attribute)
            and node.func.attr == "get"
            and bool(node.args)
            and isinstance(node.args[0], ast.Constant)
            and node.args[0].value in rule["names"]
        )
    pattern = re.compile(rule["pattern"])
    if isinstance(node, ast.Constant) and isinstance(node.value, int) and not isinstance(node.value, bool):
        return pattern.fullmatch(str(node.value)) is not None
    if isinstance(node, (ast.Constant, ast.JoinedStr)) and id(node) not in inner:
        text = _rendered(node)
        return text is not None and pattern.search(text) is not None
    return False


def _ast_sites(rule: dict[str, Any], path: str, tree: ast.AST, lines: list[str]) -> list[Site]:
    """The occurrences of an AST rule in one module."""
    inner = {id(part) for node in ast.walk(tree) if isinstance(node, ast.JoinedStr) for part in node.values}
    found: list[Site] = []
    for node, scope in _scoped(tree):
        if _is_hit(rule, node, inner):
            line = getattr(node, "lineno", 0)
            found.append(Site(path, scope, line, lines[line - 1].strip()))
    return found


def _literal_groups(source: str) -> list[list[tuple[bool, str, int]]]:
    """Runs of adjacent string literals, each as (is an f-string, text, line); an f-string is one item."""
    groups: list[list[tuple[bool, str, int]]] = []
    current: list[tuple[bool, str, int]] = []
    start: tokenize.TokenInfo | None = None
    depth = 0
    for token in tokenize.generate_tokens(io.StringIO(source).readline):
        if depth:
            if token.type == tokenize.FSTRING_START:
                depth += 1
            elif token.type == tokenize.FSTRING_END:
                depth -= 1
                if depth == 0 and start is not None:
                    current.append((True, "", start.start[0]))
            continue
        if token.type in (tokenize.NL, tokenize.COMMENT):
            continue
        if token.type == tokenize.STRING:
            prefix = token.string[: min(i for i, c in enumerate(token.string) if c in "'\"")].lower()
            current.append(("f" in prefix, token.string, token.start[0]))
        elif token.type == tokenize.FSTRING_START:
            start, depth = token, 1
        elif current:
            groups.append(current)
            current = []
    if current:
        groups.append(current)
    return groups


def _token_sites(path: str, source: str, tree: ast.AST) -> list[Site]:
    """The plain string literals with a `{name}` placeholder that sit beside an f-string in one implicit concatenation."""
    spans = [
        (node.lineno, node.end_lineno or node.lineno, _qualified(node, scope))
        for node, scope in _scoped(tree)
        if isinstance(node, DEFS)
    ]
    lines = source.splitlines()
    found: list[Site] = []
    for group in _literal_groups(source):
        if not any(is_f for is_f, _, _ in group):
            continue
        for is_f, text, line in group:
            if not is_f and BRACE.search(text):
                holding = [span for span in spans if span[0] <= line <= span[1]]
                scope = max(holding, key=lambda span: span[0])[2] if holding else MODULE
                found.append(Site(path, scope, line, lines[line - 1].strip()))
    return found


def _sites(rule: dict[str, Any]) -> list[Site]:
    """Every occurrence of a rule in its scope, in file order."""
    found: list[Site] = []
    for file in _files(rule["scope"]):
        source = file.read_text(encoding="utf-8")
        tree = ast.parse(source)
        path = file.relative_to(ROOT).as_posix()
        if rule["kind"] == "token":
            found += _token_sites(path, source, tree)
        else:
            found += _ast_sites(rule, path, tree, source.splitlines())
    return found


def _allowed(rule: dict[str, Any], site: Site) -> bool:
    """Whether a site is inside the rule's owner or its allowed files and defs: never an exception."""
    spots = {site.path, f"{site.path}::{site.scope}"}
    return bool(spots & set(rule.get("allowed", []) + [rule.get("owner") or ""]))


def _key(at: str) -> tuple[str, str]:
    """An exception's `path::scope` as the (path, scope) it counts; `path::*` counts every scope of the file."""
    path, _, scope = at.partition("::")
    return path, scope or "*"


def _bucket(rule: dict[str, Any], sites: list[Site]) -> Counter[tuple[str, str]]:
    """The unowned sites of a rule, each counted under the exception that holds it, else under its own (path, scope)."""
    expected = {_key(e["at"]) for e in rule.get("exceptions", [])}
    counts: Counter[tuple[str, str]] = Counter()
    for site in sites:
        if _allowed(rule, site):
            continue
        if (site.path, site.scope) not in expected and (site.path, "*") in expected:
            counts[(site.path, "*")] += 1
        else:
            counts[(site.path, site.scope)] += 1
    return counts


RULES = _rules()


@pytest.mark.parametrize("rule", RULES, ids=lambda r: r["id"])
def test_a_seam_is_bypassed_only_at_its_excepted_sites(rule: dict[str, Any]) -> None:
    sites = _sites(rule)
    have_by_key = _bucket(rule, sites)
    expected = {_key(e["at"]): e["count"] for e in rule.get("exceptions", [])}
    for key in sorted(set(have_by_key) | set(expected)):
        have, want = have_by_key.get(key, 0), expected.get(key, 0)
        if have == want:
            continue
        where = [
            f"{s.path}:{s.line}: {s.text}"
            for s in sites
            if not _allowed(rule, s) and s.path == key[0] and key[1] in (s.scope, "*")
        ]
        verdict = "a new bypass of the seam" if have > want else "fewer than excepted: lower the exception's count"
        pytest.fail(f"{rule['id']}: {key[0]}::{key[1]} has {have} site(s), the exceptions say {want} ({verdict}). Sites: {where}")


@pytest.mark.parametrize("rule", [r for r in RULES if r.get("owner")], ids=lambda r: r["id"])
def test_a_seam_names_an_owner_the_package_defines(rule: dict[str, Any]) -> None:
    path, _, name = rule["owner"].partition("::")
    tree = ast.parse((ROOT / path).read_text(encoding="utf-8"))
    defined = {_qualified(node, scope) for node, scope in _scoped(tree) if isinstance(node, DEFS)} | _module_names(tree)
    assert name in defined, f"{rule['id']}: owner {rule['owner']} is not defined in {path}"


@pytest.mark.parametrize("rule", RULES, ids=lambda r: r["id"])
def test_every_exception_is_dated_reasoned_and_closed(rule: dict[str, Any]) -> None:
    for entry in rule.get("exceptions", []):
        date.fromisoformat(str(entry["date"]))
        assert entry["reason"].strip() and entry["closes"].strip(), entry["at"]
        assert "::" in entry["at"] and isinstance(entry["count"], int) and entry["count"] > 0, entry["at"]


def test_a_module_constant_is_defined_in_one_module() -> None:
    owners: dict[str, set[str]] = defaultdict(set)
    for file in _files(["airflow_provider_rivet"]):
        tree = ast.parse(file.read_text(encoding="utf-8"))
        for name in _module_names(tree):
            if name.isupper():
                owners[name].add(file.relative_to(ROOT).as_posix())
    shared = {name: sorted(paths) for name, paths in owners.items() if len(paths) > 1}
    assert not shared, f"a constant is defined in more than one module: {shared}"
