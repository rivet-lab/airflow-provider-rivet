"""rivet's stderr file, read in one place: the tail a step classifies and the failure alert quotes, and the lines and
heartbeats a running step shows."""

from __future__ import annotations

import os
import threading
import time
from collections.abc import Callable
from pathlib import Path
from typing import Any

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


class Follower:
    """Follows rivet's stderr file while the process runs, so a long step is not silent in the task log: each new whole
    line goes to `on_line` (when the task streams stderr), and every `every` seconds `on_beat(elapsed, size, idle)` says
    how long it has run, how big the file is and how long since it last grew. The lines left when the step ends are
    passed on exit, so a streamed line is never lost; without `on_line` the file is never read, only its size."""

    def __init__(
        self,
        path: Path,
        on_line: Callable[[str], None] | None,
        on_beat: Callable[[float, int, float], None] | None,
        every: float | None,
        poll: float = 1.0,
    ) -> None:
        self.path, self.on_line, self.on_beat, self.every, self.poll = path, on_line, on_beat, every, poll
        self.offset, self.partial, self.size = 0, b"", 0
        self.started = self.grew = self.beat = time.monotonic()
        self.stop = threading.Event()
        self.thread = threading.Thread(target=self._run, name=f"rivet-stderr-{path.name}", daemon=True)

    def __enter__(self) -> Follower:
        if self.on_line or (self.on_beat and self.every):
            self.thread.start()
        return self

    def __exit__(self, *exc: Any) -> None:
        if self.thread.is_alive():
            self.stop.set()
            self.thread.join()
        self._drain(final=True)

    def _run(self) -> None:
        """Drain the file every `poll` seconds and beat every `every`, until the step ends."""
        while not self.stop.wait(self.poll):
            self._drain(final=False)
            now = time.monotonic()
            if self.on_beat and self.every and now - self.beat >= self.every:
                self.beat = now
                self.on_beat(now - self.started, self.size, now - self.grew)

    def _drain(self, final: bool) -> None:
        """Note what the file gained; pass each whole line, and on the `final` drain the unfinished one too."""
        try:
            size = os.stat(self.path).st_size
        except OSError:
            return
        if size > self.size:
            self.size, self.grew = size, time.monotonic()
        if not self.on_line:
            return
        data = b""
        if size > self.offset:
            with open(self.path, "rb") as handle:
                handle.seek(self.offset)
                data = handle.read(size - self.offset)
            self.offset += len(data)
        *whole, self.partial = (self.partial + data).split(b"\n")
        if final and self.partial:
            whole, self.partial = [*whole, self.partial], b""
        for line in whole:
            self.on_line(line.removesuffix(b"\r").decode(errors="replace"))
