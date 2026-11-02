"""rivet's stderr file, read in one place: the tail a step classifies and the failure alert quotes."""

from __future__ import annotations

import os
from pathlib import Path

STDERR_TAIL = 1 << 20  # bytes from the end of the file; rivet's error is the last thing it writes
JSON_ERRORS = (ValueError, RecursionError)  # a value nested past the parser's depth is not JSON either


def lines_of(text: str) -> list[str]:
    """The lines of rivet's stderr, cut at "\\n" only, a trailing "\\r" dropped (`str.splitlines` also cuts at U+2028)."""
    pieces = text.split("\n")
    if pieces and pieces[-1] == "":
        pieces.pop()
    return [piece.removesuffix("\r") for piece in pieces]


def tail_lines(path: Path) -> tuple[list[str], bool]:
    """The whole lines of the last STDERR_TAIL bytes, and whether a line cut by the window was lost with no whole one."""
    with open(path, "rb") as handle:
        size = handle.seek(0, os.SEEK_END)
        start = max(0, size - STDERR_TAIL)
        handle.seek(max(0, start - 1))
        data = handle.read()
    if start == 0:
        return lines_of(data.decode(errors="replace")), False
    cut = data[:1] != b"\n"
    body = data[1:]
    if cut:
        newline = body.find(b"\n")
        body = body[newline + 1 :] if newline >= 0 else b""
    return lines_of(body.decode(errors="replace")), cut and not body.strip()
