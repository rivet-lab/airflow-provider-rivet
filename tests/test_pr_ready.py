"""dev/pr_ready.py: each PR rule is refused on its violation and passes its near miss, on a real branch too."""

from __future__ import annotations

import importlib.util
import subprocess
from pathlib import Path

import pytest
import yaml

ROOT = Path(__file__).resolve().parents[1]
SPEC = importlib.util.spec_from_file_location("pr_ready", ROOT / "dev" / "pr_ready.py")
pr_ready = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(pr_ready)

LOG = "# Changelog\n\n## Unreleased\n\n### Added\n- a\n\n## 0.4.0\n- old\n"


def _diff(path: str, *lines: str) -> str:
    """A -U0 diff that adds `lines` to `path` from line 1."""
    return f"+++ b/{path}\n@@ -0,0 +1,{len(lines)} @@\n" + "".join(f"+{line}\n" for line in lines)


def _branch(tmp: Path, base: dict[str, str], head: dict[str, str | None], message: str = "change") -> tuple[Path, str]:
    """A repository with `base` committed, then `head` committed (None deletes); (path, the base sha)."""
    base = {".mutmut-ratchet": "# 2026-10-29: first full run\n6\n", "CHANGELOG.md": LOG, **base}

    def commit(files: dict[str, str | None], text: str) -> None:
        for path, body in files.items():
            target = tmp / path
            target.parent.mkdir(parents=True, exist_ok=True)
            if body is None:
                target.unlink()
            else:
                target.write_text(body)
        subprocess.run(["git", "-C", str(tmp), "add", "-A"], check=True)
        subprocess.run(
            ["git", "-C", str(tmp), "-c", "user.name=t", "-c", "user.email=t@example.com", "commit", "-qm", text], check=True
        )

    subprocess.run(["git", "init", "-q", "-b", "main", str(tmp)], check=True)
    commit(base, "base")
    sha = pr_ready.git(tmp, "rev-parse", "HEAD").strip()
    commit(head, message)
    return tmp, sha


def _verdicts(rows: list[tuple[str, str, str]]) -> dict[str, str]:
    """The verdict of each check row."""
    return {name: verdict for name, verdict, _ in rows}


def test_the_self_test_of_every_rule_passes() -> None:
    """The module's own RED and GREEN cases hold."""
    assert pr_ready.self_test() == 0


@pytest.mark.parametrize(
    ("message", "refused"),
    [
        ("fix\n\nCo-Authored-By: A <a@b>\n", True),
        ("fix\n\nClaude-Session: https://claude.ai/code/session_x\n", True),
        ("fix\n\nSee the Co-Authored-By mention in the text.\n", False),
    ],
)
def test_an_attribution_trailer_in_a_commit_is_refused(message: str, refused: bool) -> None:
    """A trailer line is refused; the same words inside a sentence are not."""
    assert bool(pr_ready.trailer_violations([message])) is refused


def test_a_prose_comment_on_an_added_line_is_refused_and_the_allowed_markers_are_not() -> None:
    """Comments on the added lines of the scope are refused, except pragma, noqa, type, shebang and a dated ledger."""
    text = "#!/usr/bin/env python\nx = 1  # noqa: E501\ny = 2  # pragma: no cover\nz = 3  # type: ignore\ns = '# not a comment'\n"
    assert pr_ready.comment_violations(_diff("tests/t.py", *text.splitlines()), lambda _: text) == []
    prose = "x = 1\n# why this is so\ny = 2  # the reason\n"
    found = pr_ready.comment_violations(_diff("dev/d.py", *prose.splitlines()), lambda _: prose)
    assert [f.split(":", 2)[1] for f in found] == ["2", "3"]


def test_a_prose_comment_outside_the_scope_or_in_a_dated_ledger_is_allowed() -> None:
    """docs/ is outside the scope; a dated ledger's entries are its reasons."""
    assert pr_ready.comment_violations(_diff("docs/x.md", "# heading"), lambda _: "") == []
    assert pr_ready.comment_violations(_diff("dev/skip_census.py", "# a dated reason"), lambda _: "") == []


def test_a_full_line_comment_in_yaml_under_the_scope_is_refused() -> None:
    """A YAML file under gate/ is graded by its full-line comments."""
    assert pr_ready.comment_violations(_diff("gate/stand/x.yaml", "# why the job waits"), lambda _: "")


def test_a_deletion_over_the_ceiling_is_refused_unless_a_commit_declares_it() -> None:
    """The ceiling is 50; a commit may declare a larger number."""
    assert pr_ready.deletion_verdict(50, [])[0] == "PASS"
    assert pr_ready.deletion_verdict(51, [])[0] == "FAIL"
    assert pr_ready.deletion_verdict(51, ["x\n\npush-guard: allow-deletes 51\n"])[0] == "PASS"


def test_a_raised_ratchet_needs_a_dated_entry_and_a_matching_body_section() -> None:
    """The ratchet's value grows only with a dated entry and a `Ratchets raised` line that names the change."""
    entry = ["# 2026-11-02: three equivalents accepted"]
    assert pr_ready.ratchet_verdict(6, 6, [], None)[0] == "PASS"
    assert pr_ready.ratchet_verdict(6, 7, [], "## Ratchets raised\n6 -> 7: x\n")[0] == "FAIL"
    assert pr_ready.ratchet_verdict(6, 7, entry, None)[0] == "DECLARE"
    assert pr_ready.ratchet_verdict(6, 7, entry, "## Why\n6 -> 7 is fine\n")[0] == "FAIL"
    assert pr_ready.ratchet_verdict(6, 7, entry, "## Ratchets raised\n6 -> 7: three equivalents\n## Evidence\n")[0] == "PASS"
    assert pr_ready.ratchet_verdict(6, 5, [], None)[0] == "PASS", "a lowered ratchet is never refused"


def test_a_package_change_needs_an_unreleased_changelog_entry() -> None:
    """A change under airflow_provider_rivet/ moves CHANGELOG's Unreleased section; a test change does not."""
    assert pr_ready.changelog_verdict(["airflow_provider_rivet/x.py"], LOG, LOG)[0] == "FAIL"
    assert pr_ready.changelog_verdict(["airflow_provider_rivet/x.py"], LOG, LOG.replace("- a\n", "- a\n- b\n"))[0] == "PASS"
    assert pr_ready.changelog_verdict(["tests/x.py"], LOG, LOG)[0] == "N/A"


def test_the_branch_checks_refuse_a_trailer_a_deletion_and_an_unlogged_change(tmp_path: Path) -> None:
    """On a real branch: a trailer commit, 51 deletions and a package change with no entry each fail their row."""
    repo, sha = _branch(tmp_path, {f"f{i}.txt": "x\n" for i in range(60)}, {f"f{i}.txt": None for i in range(51)})
    assert _verdicts(pr_ready.branch_rows(repo, sha, None))["deletion ceiling"] == "FAIL"
    repo, sha = _branch(
        tmp_path / "t",
        {"airflow_provider_rivet/x.py": "x = 1\n"},
        {"airflow_provider_rivet/x.py": "x = 2\n"},
        "fix\n\nCo-Authored-By: A <a@b>",
    )
    rows = _verdicts(pr_ready.branch_rows(repo, sha, None))
    assert rows["commit trailers"] == "FAIL" and rows["CHANGELOG"] == "FAIL"


def test_the_branch_checks_pass_a_clean_change_with_its_declarations(tmp_path: Path) -> None:
    """A package change with a CHANGELOG entry, a dated ratchet raise and its PR-body section passes every row."""
    head = {
        "airflow_provider_rivet/x.py": "x = 2\n",
        "CHANGELOG.md": LOG.replace("- a\n", "- a\n- b\n"),
        ".mutmut-ratchet": "# 2026-10-29: first full run\n# 2026-11-02: accepted one more\n7\n",
    }
    repo, sha = _branch(tmp_path, {"airflow_provider_rivet/x.py": "x = 1\n"}, head, "change the package")
    body = "## Ratchets raised\n6 -> 7: accepted one more\n## Evidence\nnone\n"
    rows = pr_ready.branch_rows(repo, sha, body)
    assert all(verdict in ("PASS", "N/A") for _, verdict, _ in rows), rows


def test_the_pr_rules_job_grades_the_pr_and_the_pre_push_hook_runs_the_text_mode() -> None:
    """The CI job runs `ci` with the event payload and self-tests first; the pre-push hook runs `text`."""
    job = yaml.safe_load((ROOT / ".github" / "workflows" / "pr-rules.yml").read_text())["jobs"]["pr-rules"]
    runs = [step["run"] for step in job["steps"] if "run" in step]
    assert "python3 dev/pr_ready.py --self-test" in runs
    assert any("dev/pr_ready.py ci" in run and "--event" in run for run in runs)
    hooks = yaml.safe_load((ROOT / ".pre-commit-config.yaml").read_text())["repos"][-1]["hooks"]
    assert [h["entry"] for h in hooks if h["id"] == "pr-ready"] == ["python3 dev/pr_ready.py text"]
