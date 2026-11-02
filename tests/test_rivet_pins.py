"""The rivet versions this repository pins, each held to the one place that owns it: the minimum binary
(`MIN_RIVET_VERSION`) and the source the contract is read from (`RIVET_CONTRACT_REF`)."""

from __future__ import annotations

import importlib.util
import os
import re
import subprocess

from conftest import PACKAGE_ROOT, RIVET_SRC

from airflow_provider_rivet.capabilities import MIN_RIVET_VERSION

FLOOR = ".".join(map(str, MIN_RIVET_VERSION))
WORKFLOWS = {p.name: p.read_text() for p in sorted((PACKAGE_ROOT / ".github" / "workflows").glob("*.yml"))}


def test_the_helper_the_workflows_run_prints_the_floor() -> None:
    spec = importlib.util.spec_from_file_location("min_rivet", PACKAGE_ROOT / "dev" / "min_rivet.py")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    assert module.min_rivet() == FLOOR
    done = subprocess.run(["python3", str(PACKAGE_ROOT / "dev" / "min_rivet.py")], capture_output=True, text=True, check=True)
    assert done.stdout.strip() == FLOOR


def test_no_workflow_keeps_a_rivet_version_of_its_own() -> None:
    """The CI matrix once installed `rivet-cli>=0.32.1` and the gate refused below `v0.32.0` while the floor was 0.32.2: a
    workflow takes the floor from `dev/min_rivet.py`, so raising it is one edit."""
    pinned = {
        name: re.findall(r"rivet-cli\s*[=<>~!]=?\s*\d[\w.]*|^\s*min=v?\d[\w.]*", text, re.M) for name, text in WORKFLOWS.items()
    }
    assert {name: found for name, found in pinned.items() if found} == {}
    assert "dev/min_rivet.py" in WORKFLOWS["ci.yml"] and "dev/min_rivet.py" in WORKFLOWS["gate.yml"]


def test_the_dependencies_and_the_quickstart_admit_the_floor() -> None:
    """pyproject cannot read the constant, so it is held to it here; the quickstart image's pin is at or above it."""
    assert f'"rivet-cli>={FLOOR}"' in (PACKAGE_ROOT / "pyproject.toml").read_text()
    pin = re.search(r"^ARG RIVET_VERSION=(\d+)\.(\d+)\.(\d+)$", (PACKAGE_ROOT / "quickstart" / "Dockerfile").read_text(), re.M)
    assert pin and tuple(map(int, pin.groups())) >= MIN_RIVET_VERSION


def test_the_rivet_source_the_tests_read_is_the_contract_ref() -> None:
    """A local run once read `../rivet` at whatever commit it was left on while CI pins the ref, so a green run here said
    nothing about the contract. `git -C <RIVET_SRC> checkout $(cat RIVET_CONTRACT_REF)` fixes a red here."""
    ref = (PACKAGE_ROOT / "RIVET_CONTRACT_REF").read_text().strip()
    env = {k: v for k, v in os.environ.items() if not k.startswith("GIT_")}  # a git hook exports GIT_DIR
    head = subprocess.run(["git", "-C", str(RIVET_SRC), "rev-parse", "HEAD"], capture_output=True, text=True, env=env)
    assert head.stdout.strip() == ref, f"{RIVET_SRC} is at {head.stdout.strip() or head.stderr.strip()}, the contract is {ref}"
