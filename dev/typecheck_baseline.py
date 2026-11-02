"""Ratchet basedpyright's diagnostics per file and rule against dev/typecheck_baseline.json: new ones fail, the baseline only shrinks."""

from __future__ import annotations

import argparse
import collections
import json
import subprocess
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
BASELINE = ROOT / "dev" / "typecheck_baseline.json"
BASEDPYRIGHT = "basedpyright==1.31.0"


def counts(report: dict) -> dict[str, int]:
    """Diagnostics of a basedpyright JSON report, counted by `file::rule`."""
    found: collections.Counter[str] = collections.Counter()
    for diag in report["generalDiagnostics"]:
        rel = Path(diag["file"]).resolve().relative_to(ROOT).as_posix()
        found[f"{rel}::{diag.get('rule', '')}"] += 1
    return dict(sorted(found.items()))


def run(python: str) -> dict:
    """basedpyright's JSON report for the package, against the interpreter `python`."""
    done = subprocess.run(["uvx", BASEDPYRIGHT, "--pythonpath", python, "--outputjson"], cwd=ROOT, capture_output=True, text=True)
    return json.loads(done.stdout)


def main(argv: list[str]) -> int:
    """Compare the current report with the baseline; `--write` shrinks the baseline and never grows it."""
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--python", default=sys.executable, help="the interpreter with the package's dependencies")
    parser.add_argument("--json", type=Path, help="a basedpyright --outputjson report to read instead of running it")
    parser.add_argument("--write", action="store_true", help="write the current counts if they do not grow the baseline")
    args = parser.parse_args(argv)
    report = json.loads(args.json.read_text()) if args.json else run(args.python)
    now = counts(report)
    base = json.loads(BASELINE.read_text()) if BASELINE.exists() else {}
    grown = {k: (base.get(k, 0), v) for k, v in now.items() if v > base.get(k, 0)}
    if args.write:
        if grown and BASELINE.exists():
            print(f"refusing to write: the baseline would grow: {grown}", file=sys.stderr)
            return 1
        BASELINE.write_text(json.dumps(now, indent=2, sort_keys=True) + "\n")
        print(f"baseline written: {sum(now.values())} diagnostics")
        return 0
    if grown:
        print(f"new basedpyright diagnostics (file::rule: baseline -> now): {grown}", file=sys.stderr)
        return 1
    stale = {k: (v, now.get(k, 0)) for k, v in base.items() if now.get(k, 0) < v}
    if stale:
        print(f"the baseline is stale; shrink it with `dev/typecheck_baseline.py --write`: {stale}", file=sys.stderr)
        return 1
    print(f"basedpyright: {sum(now.values())} diagnostics, all in the baseline")
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
