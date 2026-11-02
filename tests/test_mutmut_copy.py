"""Every top-level repository file a test names is copied into mutmut's tree, so the stats run reads it there too."""

from __future__ import annotations

import subprocess
import tomllib
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
MUTMUT_ALWAYS = {"pyproject.toml", "tests", "setup.cfg"}


def _copied() -> set[str]:
    """The top-level names mutmut puts in its mutants/ tree."""
    config = tomllib.loads((ROOT / "pyproject.toml").read_text())["tool"]["mutmut"]
    return MUTMUT_ALWAYS | {p.rstrip("/").split("/")[0] for p in config["source_paths"] + config["also_copy"]}


def _named(top: list[str], texts: list[str]) -> set[str]:
    """The top-level names that appear quoted in any of the texts."""
    return {name for name in top if any(f'"{name}"' in text or f"'{name}'" in text for text in texts)}


def test_every_root_file_a_test_names_is_copied_into_the_mutants_tree() -> None:
    top = sorted(
        {
            line.split("/")[0]
            for line in subprocess.run(
                ["git", "ls-files"], cwd=ROOT, capture_output=True, text=True, check=True
            ).stdout.splitlines()
        }
    )
    texts = [path.read_text() for path in (ROOT / "tests").rglob("*.py") if path != Path(__file__).resolve()]
    assert sorted(_named(top, texts) - _copied()) == []


def test_a_named_root_file_left_out_of_the_copy_is_reported() -> None:
    assert _named([".gitleaks.toml", "uv.lock"], ['x = ROOT / ".gitleaks.toml"']) - _copied() == {".gitleaks.toml"}
