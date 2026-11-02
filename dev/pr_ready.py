"""The mechanical PR rules: trailers, comments, the deletion ceiling, the mutation ratchet and the CHANGELOG."""

from __future__ import annotations

import argparse
import io
import json
import os
import re
import subprocess
import sys
import tempfile
import tokenize
from collections.abc import Callable
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
CEILING = 50
DECLARATION = re.compile(r"^push-guard: allow-deletes (\d+)$", re.M)
TRAILER = re.compile(r"^\s*(co-authored-by|claude-session):", re.I)
RATCHET = ".mutmut-ratchet"
RATCHET_ENTRY = re.compile(r"^# \d{4}-\d{2}-\d{2}: ")
PACKAGE = "airflow_provider_rivet/"
COMMENT_SCOPE = ("airflow_provider_rivet/", "tests/", "dev/", "gate/")
COMMENT_FILES = (".py", ".yml", ".yaml", ".toml")
LEDGER_FILES = {
    "dev/skip_census.py",
    "dev/test_checks.py",
    "dev/vulture_whitelist.py",
    "tests/test_dev_modules.py",
    "tests/test_workflows.py",
}
ALLOWED_COMMENT = re.compile(r"#\s*(pragma|noqa|type:)")
UNRELEASED = re.compile(r"^## Unreleased[ \t]*\n(.*?)(?=^## |\Z)", re.M | re.S)


def _clean() -> dict[str, str]:
    """The environment with every GIT_* variable removed."""
    return {k: v for k, v in os.environ.items() if not k.startswith("GIT_")}


def git(repo: Path, *args: str, check: bool = True) -> str:
    """Stdout of one git command in `repo`."""
    done = subprocess.run(["git", "-C", str(repo), *args], capture_output=True, text=True, env=_clean())
    if check and done.returncode != 0:
        raise SystemExit(f"git {' '.join(args)} failed: {done.stderr.strip()}")
    return done.stdout


def added(diff: str) -> dict[str, list[tuple[int, str]]]:
    """path -> (line number in the new file, text) of every line a -U0 diff adds."""
    out: dict[str, list[tuple[int, str]]] = {}
    path, number = None, 0
    for line in diff.splitlines():
        if line.startswith("+++ "):
            path = line[6:] if line.startswith("+++ b/") else None
        elif line.startswith("@@") and path is not None:
            number = int(re.search(r"\+(\d+)", line).group(1))
        elif line.startswith("+") and path is not None:
            out.setdefault(path, []).append((number, line[1:]))
            number += 1
    return out


def trailer_violations(messages: list[str]) -> list[str]:
    """Every attribution trailer line in the commit messages."""
    return [
        f"commit {i + 1}: {line.strip()}"
        for i, message in enumerate(messages)
        for line in message.splitlines()
        if TRAILER.match(line)
    ]


def comment_violations(diff: str, read: Callable[[str], str]) -> list[str]:
    """Prose comments on the lines a diff adds under the comment scope; `read` gives a file's text at HEAD."""
    out = []
    for path, lines in sorted(added(diff).items()):
        if not path.startswith(COMMENT_SCOPE) or not path.endswith(COMMENT_FILES) or path in LEDGER_FILES:
            continue
        numbers = {n: text for n, text in lines}
        if path.endswith(".py"):
            try:
                found = [
                    (tok.start[0], tok.string)
                    for tok in tokenize.generate_tokens(io.StringIO(read(path)).readline)
                    if tok.type == tokenize.COMMENT
                ]
            except (tokenize.TokenError, SyntaxError) as err:
                out.append(f"{path}: cannot tokenize, comments unchecked: {err}")
                continue
        else:
            found = [(n, text.strip()) for n, text in lines if text.lstrip().startswith("#")]
        for number, text in found:
            if number not in numbers or ALLOWED_COMMENT.match(text) or (number == 1 and text.startswith("#!")):
                continue
            out.append(f"{path}:{number}: prose comment on an added line: {text[:100]}")
    return out


def deletion_verdict(deleted: int, messages: list[str]) -> tuple[str, str]:
    """(verdict, line): a range deletes at most the ceiling, or the larger number a commit declares."""
    ceiling = max([CEILING, *(int(n) for m in messages for n in DECLARATION.findall(m))])
    if deleted > ceiling:
        return "FAIL", f"{deleted} files deleted, the ceiling is {ceiling}; a commit may declare `push-guard: allow-deletes N`"
    return "PASS", f"{deleted} files deleted (ceiling {ceiling})"


def ratchet_value(text: str) -> int:
    """The ratchet: the last line that is neither blank nor a comment, read as an integer."""
    return int([line for line in text.splitlines() if line.strip() and not line.startswith("#")][-1])


def ratchet_verdict(base: int | None, head: int, added_lines: list[str], body: str | None) -> tuple[str, str]:
    """(verdict, line): a raised ratchet needs a new dated entry and a `Ratchets raised` line in the PR body."""
    if base is None or head <= base:
        return "PASS", f"not raised (value {head})"
    if not any(RATCHET_ENTRY.match(line) for line in added_lines):
        return "FAIL", f"raised {base} -> {head} with no new `# YYYY-MM-DD: ` entry in {RATCHET}"
    if body is None:
        return "DECLARE", f"raised {base} -> {head}: declare it in the PR body under `Ratchets raised`"
    section = declared(body)
    if f"{base} -> {head}" not in section:
        return "FAIL", f"raised {base} -> {head} but the PR body's `Ratchets raised` section does not say so"
    return "PASS", f"declared: {base} -> {head}"


def declared(body: str) -> str:
    """The text under the PR body's `Ratchets raised` heading, up to the next heading (empty when absent)."""
    out, on = [], False
    for line in body.splitlines():
        if re.match(r"^\s*(?:#+\s*|\*\*)ratchets raised", line, re.I):
            on = True
        elif on and re.match(r"^\s*(?:#+\s|\*\*[^*]+\*\*\s*$)", line):
            break
        elif on:
            out.append(line)
    return "\n".join(out)


def changelog_verdict(changed: list[str], base_log: str, head_log: str) -> tuple[str, str]:
    """(verdict, line): a change under the package needs an entry in CHANGELOG's Unreleased section."""
    touched = [p for p in changed if p.startswith(PACKAGE)]
    if not touched:
        return "N/A", "no change under airflow_provider_rivet/"
    if _unreleased(base_log) == _unreleased(head_log):
        return "FAIL", f"{len(touched)} package file(s) changed and CHANGELOG.md's Unreleased section has no new entry"
    return "PASS", f"{len(touched)} package file(s) changed; CHANGELOG Unreleased has an entry"


def _unreleased(text: str) -> str:
    """The body of CHANGELOG's `## Unreleased` section."""
    found = UNRELEASED.search(text)
    if found is None:
        raise SystemExit("CHANGELOG.md has no `## Unreleased` section")
    return found.group(1)


def branch_rows(repo: Path, base: str, body: str | None) -> list[tuple[str, str, str]]:
    """(check, verdict, line) for every check, graded off the branch from its merge base with `base`."""
    mb = git(repo, "merge-base", base, "HEAD").strip()
    diff = git(repo, "diff", "-U0", "--no-color", "--no-ext-diff", f"{mb}...HEAD")
    changed = git(repo, "diff", "--name-only", f"{mb}...HEAD").split()
    messages = git(repo, "log", "--no-merges", "--format=%B%x1e", f"{mb}..HEAD").split("\x1e")
    messages = [m for m in messages if m.strip()]
    rows = []
    bad = trailer_violations(messages)
    rows.append(
        ("commit trailers", "FAIL" if bad else "PASS", "; ".join(bad[:5]) or f"{len(messages)} commit(s) without a trailer")
    )
    bad = comment_violations(diff, lambda p: git(repo, "show", f"HEAD:{p}"))
    rows.append(("prose comments", "FAIL" if bad else "PASS", "; ".join(bad[:5]) or "no prose comment on an added line"))
    verdict, line = deletion_verdict(
        len([p for p in git(repo, "diff", "--diff-filter=D", "--name-only", f"{mb}...HEAD").split() if p]), messages
    )
    rows.append(("deletion ceiling", verdict, line))
    head = ratchet_value(git(repo, "show", f"HEAD:{RATCHET}"))
    old = git(repo, "show", f"{mb}:{RATCHET}", check=False)
    base_value = ratchet_value(old) if old else None
    entries = added(diff).get(RATCHET, [])
    verdict, line = ratchet_verdict(base_value, head, [text for _, text in entries], body)
    rows.append(("mutation ratchet", verdict, line))
    verdict, line = changelog_verdict(changed, git(repo, "show", f"{mb}:CHANGELOG.md"), git(repo, "show", "HEAD:CHANGELOG.md"))
    rows.append(("CHANGELOG", verdict, line))
    return rows


def pr_body(args: argparse.Namespace) -> str | None:
    """The PR title and body from the event payload, a file, or `gh`; None in text mode."""
    if args.event:
        pr = json.loads(Path(args.event).read_text())["pull_request"]
        return f"{pr.get('title') or ''}\n\n{pr.get('body') or ''}"
    if args.body:
        return Path(args.body).read_text()
    if args.pr:
        return subprocess.run(
            ["gh", "pr", "view", str(args.pr), "--json", "title,body", "-q", '.title + "\\n\\n" + .body'],
            capture_output=True,
            text=True,
            check=True,
        ).stdout
    return None


def print_rows(title: str, rows: list[tuple[str, str, str]]) -> None:
    """The summary block, one line per check."""
    print(title)
    for name, verdict, line in rows:
        print(f"{name:<20} {verdict:<8} {line}")


def run_ci(args: argparse.Namespace) -> int:
    """The PR-rules job: every check, the PR's own body graded for its declarations."""
    rows = branch_rows(ROOT, args.base, pr_body(args))
    print_rows(f"PR rules (base {args.base[:12]})", rows)
    return 1 if any(verdict in ("FAIL", "DECLARE") for _, verdict, _ in rows) else 0


def run_text(args: argparse.Namespace) -> int:
    """The pre-push run: every check that needs no PR body."""
    if subprocess.run(["git", "-C", str(ROOT), "rev-parse", "--verify", "-q", args.base], capture_output=True).returncode != 0:
        print(f"pr_ready: {args.base} is not fetched; the pre-push checks are skipped", file=sys.stderr)
        return 0
    rows = branch_rows(ROOT, args.base, None)
    print_rows(f"pr_ready text (base {args.base})", rows)
    return 1 if any(verdict == "FAIL" for _, verdict, _ in rows) else 0


def _repo_with(tmp: Path, base: dict[str, str], head: dict[str, str | None]) -> str:
    """A git repository at `tmp` with `base` committed, then `head` (None deletes a file) committed; the base sha."""

    def commit(files: dict[str, str | None], message: str) -> None:
        for path, text in files.items():
            target = tmp / path
            target.parent.mkdir(parents=True, exist_ok=True)
            if text is None:
                target.unlink()
            else:
                target.write_text(text)
        git(tmp, "add", "-A")
        git(tmp, "-c", "user.name=t", "-c", "user.email=t@example.com", "commit", "-qm", message)

    git(tmp, "init", "-q", "-b", "main")
    commit(base, "base")
    sha = git(tmp, "rev-parse", "HEAD").strip()
    commit(head, "delete 51")
    return sha


def self_test() -> int:
    """Each check goes RED on the violation it exists for and stays green on its near miss."""
    assert trailer_violations(["fix\n\nCo-Authored-By: A <a@b>\n"]) == ["commit 1: Co-Authored-By: A <a@b>"]
    assert trailer_violations(["fix\n\nClaude-Session: https://x\n"]), "a Claude-Session trailer must fail"
    assert trailer_violations(["fix\n\nSee the Co-Authored-By mention in the text.\n"]) == [], "an inline mention is no trailer"

    prose = {"airflow_provider_rivet/a.py": "x = 1\n# this is why\ny = 2  # the reason\n"}
    diff = "+++ b/airflow_provider_rivet/a.py\n@@ -0,0 +1,3 @@\n+x = 1\n+# this is why\n+y = 2  # the reason\n"
    got = comment_violations(diff, lambda p: prose[p])
    assert len(got) == 2 and "this is why" in got[0] and "the reason" in got[1], got
    kept = {
        "airflow_provider_rivet/a.py": "#!/usr/bin/env python\nx = 1  # noqa: E501\ny = 2  # pragma: no cover\nz = 3  # type: ignore\ns = '# not a comment'\n"
    }
    diff = "+++ b/airflow_provider_rivet/a.py\n@@ -0,0 +1,5 @@\n" + "".join(
        f"+{line}\n" for line in kept["airflow_provider_rivet/a.py"].splitlines()
    )
    assert comment_violations(diff, lambda p: kept[p]) == [], "pragma, noqa, type and shebang are allowed"
    yaml_diff = "+++ b/tests/ci.yml\n@@ -0,0 +1 @@\n+# why the job waits\n"
    assert comment_violations(yaml_diff, lambda p: ""), "a full-line YAML comment is prose"
    ledger = "+++ b/dev/skip_census.py\n@@ -0,0 +1 @@\n+# a dated reason\n"
    assert comment_violations(ledger, lambda p: "# a dated reason\n") == [], "a dated ledger is exempt"
    outside = "+++ b/docs/x.md\n@@ -0,0 +1 @@\n+# heading\n"
    assert comment_violations(outside, lambda p: "") == [], "docs are outside the scope"

    assert deletion_verdict(51, [])[0] == "FAIL"
    assert deletion_verdict(50, [])[0] == "PASS"
    assert deletion_verdict(51, ["x\n\npush-guard: allow-deletes 51\n"])[0] == "PASS"
    with tempfile.TemporaryDirectory() as t:
        tmp = Path(t)
        sha = _repo_with(tmp, {f"f{i}.txt": "x\n" for i in range(60)}, {f"f{i}.txt": None for i in range(51)})
        deleted = len(git(tmp, "diff", "--diff-filter=D", "--name-only", f"{sha}..HEAD").split())
        messages = git(tmp, "log", "--format=%B%x1e", f"{sha}..HEAD").split("\x1e")
        assert deletion_verdict(deleted, [m for m in messages if m.strip()])[0] == "FAIL", "51 deletions with no declaration fail"

    assert ratchet_value("# c\n# 2026-10-29: note\n6\n") == 6
    entry = ["# 2026-11-02: accepted 3 equivalents"]
    assert ratchet_verdict(6, 6, [], None)[0] == "PASS", "an unchanged ratchet passes"
    assert ratchet_verdict(6, 7, [], "## Ratchets raised\n6 -> 7: x\n")[0] == "FAIL", "a raise with no dated entry fails"
    assert ratchet_verdict(6, 7, entry, None)[0] == "DECLARE", "a raise is declared in the body"
    assert ratchet_verdict(6, 7, entry, "## Why\n6 -> 7 is fine\n")[0] == "FAIL", "a mention outside the section does not declare"
    assert ratchet_verdict(6, 7, entry, "## Ratchets raised\n6 -> 7: three equivalents\n## Evidence\n")[0] == "PASS"

    log = "# C\n\n## Unreleased\n\n### Fixed\n- a\n\n## 0.4.0\n- old\n"
    assert changelog_verdict(["airflow_provider_rivet/x.py"], log, log)[0] == "FAIL", "a package change with no entry fails"
    assert changelog_verdict(["airflow_provider_rivet/x.py"], log, log.replace("- a\n", "- a\n- b\n"))[0] == "PASS"
    assert changelog_verdict(["tests/x.py"], log, log)[0] == "N/A"
    print("pr_ready self-test: ok")
    return 0


def main(argv: list[str]) -> int:
    """Dispatch the modes, or run the self-test."""
    if argv == ["--self-test"]:
        return self_test()
    parser = argparse.ArgumentParser(description=__doc__)
    sub = parser.add_subparsers(dest="cmd", required=True)
    c = sub.add_parser("ci", help="every check, with the PR body from the event, a file or gh")
    c.add_argument("--base", required=True)
    source = c.add_mutually_exclusive_group(required=True)
    source.add_argument("--event", help="the pull_request event payload ($GITHUB_EVENT_PATH)")
    source.add_argument("--body", help="a PR body file")
    source.add_argument("--pr", type=int, help="a PR number, read with gh")
    t = sub.add_parser("text", help="the checks that need no PR body, before a push")
    t.add_argument("--base", default="origin/main")
    args = parser.parse_args(argv)
    return run_ci(args) if args.cmd == "ci" else run_text(args)


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
