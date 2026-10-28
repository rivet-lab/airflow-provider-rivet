"""The installed rivet binary's version, and every fallback the package takes when its output or the worker falls short."""

from __future__ import annotations

import os
import re
import subprocess

MIN_RIVET_VERSION = (0, 32, 2)
PROBE_TIMEOUT = 60
_VERSION_RE = re.compile(r"rivet\s+(\d+)\.(\d+)\.(\d+)")


# Every `degraded` key a task can record, by how it is detected: a member the binary's output lacks, or the worker. What
# each one means and does: docs/capabilities.md.
DEGRADATIONS = {
    "per_unit_error": "output",
    "stop_reason": "output",
    "stderr_temp": "worker",
    "crashed_ledger": "worker",
    "result_missing": "output",
    "result_malformed": "output",
    "exit_zero_failed_unit": "output",
}


class ProbeFailed(RuntimeError):
    """A probe of the binary got no usable answer: it could not start, exited non-zero, timed out or printed no version."""


class Capabilities:
    """The version of one rivet binary, probed from `--version`."""

    def __init__(self, binary: str) -> None:
        out = self._run([binary, "--version"])
        match = _VERSION_RE.search(out)
        if not match:
            raise ProbeFailed(f"`{binary} --version` did not print a rivet version")
        self.version = tuple(int(g) for g in match.groups())
        # The whole version line after `rivet`, so a branch build `0.32.1 (3f337e9e3)` is told apart from the release.
        self.version_text = out[match.start() :].splitlines()[0].split(None, 1)[1].strip()

    @staticmethod
    def _run(argv: list[str]) -> str:
        """Run a probe with a minimal environment and return its stdout; `ProbeFailed` for every probe that does not answer."""
        env = {"PATH": os.environ.get("PATH", ""), "HOME": os.environ.get("HOME", "")}
        shown = " ".join(argv)
        try:  # no `check=`: the exit code is read below
            done = subprocess.run(argv, capture_output=True, text=True, timeout=PROBE_TIMEOUT, env=env)
        except subprocess.TimeoutExpired as exc:
            raise ProbeFailed(f"`{shown}` did not answer within {PROBE_TIMEOUT}s") from exc
        except (OSError, ValueError) as exc:
            raise ProbeFailed(f"`{shown}` could not be run: {type(exc).__name__}") from exc
        if done.returncode != 0:
            raise ProbeFailed(f"`{shown}` exited {done.returncode}")
        return done.stdout

    def new_enough(self) -> bool:
        """True when the binary is at or above the minimum the package was written against."""
        return self.version >= MIN_RIVET_VERSION


_CACHE: dict[tuple, Capabilities] = {}


def probe(binary: str) -> Capabilities:
    """Probe a binary once per process and per file identity."""
    try:
        stat = os.stat(binary)
    except OSError as exc:
        raise ProbeFailed(f"`{binary}` cannot be read: {type(exc).__name__}") from exc
    key = (binary, stat.st_mtime_ns, stat.st_size)
    if key not in _CACHE:
        _CACHE[key] = Capabilities(binary)
    return _CACHE[key]
