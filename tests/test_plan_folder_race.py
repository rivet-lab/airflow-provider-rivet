"""A live run's plan folder, with the losing order forced: a sibling task of the same DAG run closes (and prunes) while
an apply has made `plans/<run>/` and is about to run `rivet plan --output` into it. Mutant ids are the mutmut names of
airflow_provider_rivet/invocation.py."""

from __future__ import annotations

import os
import time
from pathlib import Path
from typing import Any

import pytest
from conftest import FakeTI, Rig

from airflow_provider_rivet import invocation
from airflow_provider_rivet.operators import RivetApplyOperator, RivetRunOperator


def test_a_sibling_task_ending_while_an_apply_is_about_to_plan_leaves_its_plan_folder(
    rig: Rig, monkeypatch: pytest.MonkeyPatch
) -> None:
    """_prune_stale: a task of the same DAG run closes between `_apply` creating `plans/<run>/` and `rivet plan` writing
    into it. Unfixed, the sibling's close removed the empty folder and the apply failed for good ('output unwritable')."""
    real = invocation.spawn
    seen: dict[str, Any] = {}

    def spawn(argv: list[str], *rest: Any, **kw: Any) -> int:
        if argv[1] == "plan" and "sibling" not in seen:
            seen["sibling"], _ = rig.run(rig.operator(RivetRunOperator, task_id="sibling", export="users"), FakeTI("sibling"))
            seen["folder"] = Path(argv[argv.index("--output") + 1]).parent.is_dir()
        return real(argv, *rest, **kw)

    monkeypatch.setattr(invocation, "spawn", spawn)
    payload, _ = rig.run(rig.operator(RivetApplyOperator, export="orders"))
    assert seen["folder"], "the folder was there when rivet plan started"
    assert payload["decision"] == "success"
    assert Path(rig.argv("apply")[1]).is_file()


def test_an_apply_into_an_old_empty_plan_folder_touches_it_before_a_sibling_can_prune(
    rig: Rig, monkeypatch: pytest.MonkeyPatch
) -> None:
    """_apply: a folder of this run left empty a day ago is in use again the moment the apply takes it."""
    folder = rig.state_dir / "airflow" / "dag" / "plans" / "manual__2026-10-06"
    folder.mkdir(parents=True)
    day_ago = time.time() - 25 * 3600
    os.utime(folder, (day_ago, day_ago))
    real = invocation.spawn
    seen: dict[str, Any] = {}

    def spawn(argv: list[str], *rest: Any, **kw: Any) -> int:
        if argv[1] == "plan" and "sibling" not in seen:
            seen["sibling"], _ = rig.run(rig.operator(RivetRunOperator, task_id="sibling", export="users"), FakeTI("sibling"))
            seen["folder"] = folder.is_dir()
        return real(argv, *rest, **kw)

    monkeypatch.setattr(invocation, "spawn", spawn)
    payload, _ = rig.run(rig.operator(RivetApplyOperator, export="orders"))
    assert seen["folder"] and payload["decision"] == "success"
