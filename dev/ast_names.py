"""Resolve a module's names through its import table, so a guard reads what a name is bound to, not how it is spelled."""

from __future__ import annotations

import ast

Table = dict[str, str]


def import_table(tree: ast.AST, module: str = "") -> Table:
    """The local names a module binds by import, each mapped to the dotted name it stands for."""
    table: Table = {}
    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            for alias in node.names:
                local = alias.asname or alias.name.split(".")[0]
                table[local] = alias.name if alias.asname else local
        elif isinstance(node, ast.ImportFrom):
            base = _absolute(node, module)
            for alias in node.names:
                table[alias.asname or alias.name] = f"{base}.{alias.name}" if base else alias.name
    return table


def _absolute(node: ast.ImportFrom, module: str) -> str:
    """The absolute module an import names; a relative one is resolved against the importing module."""
    if not node.level:
        return node.module or ""
    parts = module.split(".")[: -node.level]
    return ".".join([*parts, *([node.module] if node.module else [])])


def dotted(node: ast.AST, table: Table) -> str:
    """The dotted name an attribute chain stands for through the import table; '' for any other expression."""
    attrs: list[str] = []
    while isinstance(node, ast.Attribute):
        attrs.append(node.attr)
        node = node.value
    if not isinstance(node, ast.Name):
        return ""
    return ".".join([table.get(node.id, node.id), *reversed(attrs)])


def leaf(node: ast.AST, table: Table) -> str:
    """The last component of an expression's dotted name, so `_E` bound to AirflowException reads as AirflowException."""
    return dotted(node, table).rpartition(".")[2]


def text_of(node: ast.AST) -> str | None:
    """The text a string expression builds: a literal, an f-string with `{}` per placeholder, or `+` of those; None otherwise."""
    if isinstance(node, ast.Constant) and isinstance(node.value, str):
        return node.value
    if isinstance(node, ast.JoinedStr):
        return "".join(part.value if isinstance(part, ast.Constant) else "{}" for part in node.values)
    if isinstance(node, ast.BinOp) and isinstance(node.op, ast.Add):
        left, right = text_of(node.left), text_of(node.right)
        return left + right if left is not None and right is not None else None
    return None


def folded_parts(tree: ast.AST) -> set[int]:
    """The ids of the nodes inside a string expression that folds to one text, so each text is read once, whole."""
    inner: set[int] = set()
    for node in ast.walk(tree):
        if isinstance(node, (ast.BinOp, ast.JoinedStr)) and text_of(node) is not None:
            inner.update(id(part) for part in ast.walk(node) if part is not node)
    return inner
