"""The CI and release workflows against the defaults and dependencies they name: the live gate's job, the Airflow it runs, publish."""

from __future__ import annotations

import ast
import re
from pathlib import Path

import yaml

ROOT = Path(__file__).resolve().parents[1]
WORKFLOWS = ROOT / ".github" / "workflows"

#: Workflow facts no one has decided yet: (date accepted, reason, what closes it).
LEDGER: dict[str, tuple[str, str, str]] = {
    "release.yml publish needs": (
        "2026-11-02",
        "publish needs build only; gate.yml runs on the same tag with no edge to it",
        "the owner decides whether a release waits for the live gate (harness-3)",
    ),
}


def _workflow(name: str) -> dict:
    """A workflow file, parsed."""
    return yaml.safe_load((WORKFLOWS / name).read_text())


def _airflow_default() -> str:
    """The literal default of the gate's `--airflow` option, read from gate/__main__.py."""
    for node in ast.walk(ast.parse((ROOT / "gate" / "__main__.py").read_text())):
        if isinstance(node, ast.Call) and isinstance(node.func, ast.Attribute) and node.func.attr == "add_argument":
            if node.args and isinstance(node.args[0], ast.Constant) and node.args[0].value == "--airflow":
                default = next(k.value for k in node.keywords if k.arg == "default")
                assert isinstance(default, ast.Constant), "the --airflow default is not a literal"
                return default.value
    raise AssertionError("gate/__main__.py has no --airflow option")


def _versions(text: str) -> set[str]:
    """The Airflow versions a text names."""
    return set(re.findall(r"\d+\.\d+\.\d+", text))


def test_the_live_gate_job_names_exactly_the_airflow_versions_its_default_runs() -> None:
    gate = _workflow("gate.yml")["jobs"]["gate"]
    assert _versions(gate["name"]) == set(_airflow_default().split(","))


def test_the_ci_gate_oracles_matrix_runs_the_versions_the_gate_default_runs() -> None:
    matrix = _workflow("ci.yml")["jobs"]["gate-oracles"]["strategy"]["matrix"]["airflow"]
    assert set(matrix) == set(_airflow_default().split(","))


def test_publish_needs_build_only_as_the_ledger_records() -> None:
    needs = _workflow("release.yml")["jobs"]["publish"]["needs"]
    assert needs in ("build", ["build"])
    accepted, _, _ = LEDGER["release.yml publish needs"]
    assert re.fullmatch(r"\d{4}-\d{2}-\d{2}", accepted)
