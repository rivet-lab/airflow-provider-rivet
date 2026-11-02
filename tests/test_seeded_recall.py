"""The seeded-defect manifest, its patches and its ledger hold every confirmed bug, and the runner grades itself."""

from __future__ import annotations

import importlib.util
import re
import sys
from pathlib import Path

import yaml

ROOT = Path(__file__).resolve().parents[1]
SEEDED = ROOT / "dev" / "seeded"
DATE = re.compile(r"\d{4}-\d{2}-\d{2}")
CONFIRMED = {
    "seams-1",
    "seams-2",
    "seams-3",
    "seams-4",
    "seams-5",
    "seams-6",
    "seams-7",
    "contract-1",
    "contract-3",
    "harness-1",
    "harness-2",
    "harness-3",
    "harness-4",
    "harness-5",
    "harness-6",
    "harness-7",
    "message-truth-1",
    "message-truth-2",
    "message-truth-3",
    "message-truth-4",
    "hostile-input-1",
    "hostile-input-2",
    "hostile-input-3",
    "hostile-input-4",
    "dag-generation-1",
    "dag-generation-2",
    "dag-generation-3",
    "dag-generation-4",
    "dag-generation-5",
    "vacuous-green-2",
    "vacuous-green-3",
}
PLAUSIBLE = {"lifecycle-1", "lifecycle-2"}


def _runner():
    """The runner module, loaded from its file."""
    spec = importlib.util.spec_from_file_location("seeded_runner", SEEDED / "__main__.py")
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


def _manifest() -> dict:
    """The seeds manifest as YAML."""
    return yaml.safe_load((SEEDED / "seeds.yaml").read_text())


def _ledger() -> dict:
    """The unseeded ledger as YAML."""
    return yaml.safe_load((SEEDED / "unseeded.yaml").read_text())


def test_the_runner_self_test_passes() -> None:
    assert _runner().self_test() == []


def test_every_seed_has_its_patch_and_its_cells_exist_in_the_repository() -> None:
    seeds = _manifest()["seeds"]
    assert seeds and all((SEEDED / s["patch"]).is_file() for s in seeds)
    for s in seeds:
        for cell in s["cells"]:
            path, func = cell.split("::", 1)
            text = (ROOT / path).read_text()
            assert re.search(rf"^def {re.escape(func)}\(", text, re.M), f"{s['name']}: {func} is not defined in {path}"


def test_every_seed_patch_changes_product_code_only() -> None:
    for s in _manifest()["seeds"]:
        for line in (SEEDED / s["patch"]).read_text().splitlines():
            if line.startswith("+++ b/"):
                path = line.removeprefix("+++ b/")
                assert path.startswith(("airflow_provider_rivet/", "gate/")) and "/tests/" not in path, (
                    f"{s['name']} patches {path}"
                )


def test_every_confirmed_bug_is_seeded_or_in_the_ledger_and_nothing_else_is() -> None:
    seeded = {s["bug"].split()[0] for s in _manifest()["seeds"]}
    ledger = _ledger()
    listed = {e["bug"].split()[0] for e in ledger["unseeded"] + ledger["open"]}
    assert seeded.isdisjoint(listed)
    assert (seeded | listed) - PLAUSIBLE == CONFIRMED


def test_every_ledger_entry_is_dated_with_a_reason_and_what_closes_it() -> None:
    ledger = _ledger()
    for entry in ledger["unseeded"] + ledger["open"]:
        assert DATE.fullmatch(str(entry["date"])), entry
        assert entry["reason"].strip() and entry["closes"].strip(), entry
