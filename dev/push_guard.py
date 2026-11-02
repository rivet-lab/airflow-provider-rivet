"""Refuse a push that deletes more files than the ceiling unless a commit in the range declares the larger number."""

from __future__ import annotations

import argparse
import os
import re
import subprocess
import sys
import tempfile
from pathlib import Path

CEILING = 50
DECLARATION = re.compile(r"^push-guard: allow-deletes (\d+)$", re.M)
ROOT = Path(__file__).resolve().parents[1]


def _clean() -> dict[str, str]:
    """The environment with every GIT_* variable removed."""
    return {k: v for k, v in os.environ.items() if not k.startswith("GIT_")}


def git(repo: Path, *args: str) -> str:
    """Stdout of one git command in `repo`, run with no GIT_* variable."""
    done = subprocess.run(["git", "-C", str(repo), *args], capture_output=True, text=True, env=_clean(), check=True)
    return done.stdout


def check(repo: Path, base: str) -> tuple[int, int]:
    """(files deleted in base..HEAD, the ceiling): a commit in the range may declare a larger ceiling."""
    deleted = [p for p in git(repo, "diff", "--diff-filter=D", "--name-only", f"{base}..HEAD").splitlines() if p]
    messages = git(repo, "log", "--format=%B", f"{base}..HEAD")
    return len(deleted), max([CEILING, *(int(n) for n in DECLARATION.findall(messages))])


def self_test() -> None:
    """The ceiling refuses 51 deletions, passes 50, and passes 51 when a commit declares it."""
    cases = [(51, "", False), (50, "", True), (51, "push-guard: allow-deletes 51", True)]
    for count, note, passes in cases:
        with tempfile.TemporaryDirectory() as tmp:
            repo = Path(tmp)
            ident = ["-c", "user.name=guard", "-c", "user.email=guard@localhost"]
            git(repo, "init", "-q")
            for i in range(60):
                (repo / f"f{i}").write_text("x\n")
            git(repo, "add", "-A")
            git(repo, *ident, "commit", "-qm", "base")
            base = git(repo, "rev-parse", "HEAD").strip()
            for i in range(count):
                (repo / f"f{i}").unlink()
            git(repo, "add", "-A")
            message = "delete\n" + (note + "\n" if note else "")
            git(repo, *ident, "commit", "-qm", message)
            deleted, ceiling = check(repo, base)
            assert (deleted <= ceiling) is passes, (count, note, deleted, ceiling)
    print("push_guard self-test: ok")


def main(argv: list[str]) -> int:
    """Check the range `base..HEAD` of this repository, or run the self-test."""
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--base", default="origin/main")
    parser.add_argument("--self-test", action="store_true")
    args = parser.parse_args(argv)
    if args.self_test:
        self_test()
        return 0
    deleted, ceiling = check(ROOT, args.base)
    if deleted > ceiling:
        print(
            f"push guard: {deleted} files deleted since {args.base}, the ceiling is {ceiling}; a commit may declare "
            "`push-guard: allow-deletes N`",
            file=sys.stderr,
        )
        return 1
    print(f"push guard: {deleted} files deleted since {args.base} (ceiling {ceiling})")
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
