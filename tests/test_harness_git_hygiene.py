"""The harness never inherits a git hook's environment and never writes to the real repository."""

from __future__ import annotations

import os
import subprocess
from pathlib import Path

from conftest import PACKAGE_ROOT, real_repo_state


def test_no_git_variable_reaches_a_test() -> None:
    """A test sees no GIT_* variable, whatever the hook that started pytest exported."""
    assert not [k for k in os.environ if k.startswith("GIT_")]


def test_the_state_read_sees_a_commit_made_in_a_scratch_repository(tmp_path: Path) -> None:
    """The guard's read changes when a commit lands in the repository it reads."""
    repo = tmp_path / "scratch"
    repo.mkdir()
    env = {k: v for k, v in os.environ.items() if not k.startswith("GIT_")}
    env.update(GIT_AUTHOR_NAME="t", GIT_AUTHOR_EMAIL="t@t", GIT_COMMITTER_NAME="t", GIT_COMMITTER_EMAIL="t@t")
    subprocess.run(["git", "init", "-q", str(repo)], check=True, env=env)
    (repo / "a").write_text("a\n")
    subprocess.run(["git", "-C", str(repo), "add", "a"], check=True, env=env)
    subprocess.run(["git", "-C", str(repo), "commit", "-qm", "a"], check=True, env=env)
    before = real_repo_state(repo)
    (repo / "b").write_text("b\n")
    subprocess.run(["git", "-C", str(repo), "add", "b"], check=True, env=env)
    subprocess.run(["git", "-C", str(repo), "commit", "-qm", "b"], check=True, env=env)
    assert real_repo_state(repo) != before


def test_the_package_repository_is_a_git_checkout() -> None:
    """The guard watches a real repository, so an absent one would make it pass for nothing."""
    assert real_repo_state(PACKAGE_ROOT)[0]
