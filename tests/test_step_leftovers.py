"""No file a step creates outlives the step: a step killed or timed out (a BaseException out of the process wait) leaves
none of its summary, result, plan scratch or `.init` log behind, and files left by a killed process are pruned by age
(issue #151)."""

from __future__ import annotations

import os
import stat
from pathlib import Path
from types import SimpleNamespace
from typing import Any

import pytest

from airflow_provider_rivet import invocation, operators
from airflow_provider_rivet.invocation import Request


class Killed(BaseException):
    """What a SIGTERM raises out of the task's wait: a BaseException, not an Exception."""


def session(tmp_path: Path, command: str = "run", plan_file: str | None = None) -> invocation._Session:
    config = tmp_path / "src" / "rivet.yaml"
    config.parent.mkdir(exist_ok=True)
    config.write_text("exports: []\n")
    state_dir = tmp_path / "state"
    state_dir.mkdir(exist_ok=True)
    pf = SimpleNamespace(state_dir=state_dir, state_kind="postgres", config={"exports": []}, binary="rivet")
    req = Request(command, str(config), {}, export="orders", label=("dag", "run", "task", 1), log_keep=None, plan_file=plan_file)
    return invocation._Session(req, pf, lambda e: None, None)  # type: ignore[arg-type]


def arg_after(argv: list[str], flag: str) -> Path:
    return Path(argv[argv.index(flag) + 1])


def kill_after_writing(s: invocation._Session, flag: str, *, to_stdout: bool = False):
    """A step that writes the file its argv (or its stdout) names, then is killed."""

    def step(name: str, argv: list[str], stdout_to: Path | None = None) -> None:
        target = stdout_to if to_stdout else arg_after(argv, flag)
        target.write_text("{}")
        raise Killed()

    s.step = step  # type: ignore[method-assign]


def test_a_killed_run_leaves_no_summary_file_behind(tmp_path: Path) -> None:
    """Issue #151: `step_with_result` unlinked the summary only after `step()` returned."""
    s = session(tmp_path)
    kill_after_writing(s, "--summary-output")
    with pytest.raises(Killed):
        invocation._run(s)
    s.close()
    assert list(s.work.glob("*.summary.json")) == []


def test_a_killed_load_leaves_no_result_file_behind(tmp_path: Path) -> None:
    s = session(tmp_path, "load")
    kill_after_writing(s, "--summary-output")
    with pytest.raises(Killed):
        invocation._load(s)
    s.close()
    assert list(s.work.glob("*.result.json")) == []


def test_a_killed_plan_leaves_no_scratch_file_next_to_the_plan_file(tmp_path: Path) -> None:
    """Issue #151: `<plan_file>.<name>.tmp` stayed next to the user's plan file."""
    target = tmp_path / "out" / "pg.plan.json"
    target.parent.mkdir()
    s = session(tmp_path, "plan", plan_file=str(target))
    kill_after_writing(s, "", to_stdout=True)
    with pytest.raises(Killed):
        invocation._plan(s)
    s.close()
    assert [p.name for p in target.parent.iterdir()] == []


def test_a_plan_scratch_a_killed_process_left_is_pruned_by_age(tmp_path: Path) -> None:
    target = tmp_path / "out" / "pg.plan.json"
    target.parent.mkdir()
    old = target.parent / f"{target.name}.dag__run__task__try1.tmp"
    young = target.parent / f"{target.name}.dag__run__task__try2.tmp"
    for path in (old, young):
        path.write_text("{}")
    os.utime(old, (0, 0))
    s = session(tmp_path, "plan", plan_file=str(target))
    kill_after_writing(s, "", to_stdout=True)
    with pytest.raises(Killed):
        invocation._plan(s)
    assert not old.exists() and young.exists()


def test_prune_removes_old_summary_and_result_files_and_keeps_young_ones(tmp_path: Path) -> None:
    """Issue #151: the age rule covers the summary and result files a killed process left in the DAG's folder."""
    work = tmp_path / "work"
    work.mkdir()
    old = [work / "a.summary.json", work / "b.result.json"]
    young = work / "c.summary.json"
    for path in (*old, young):
        path.write_text("{}")
        os.utime(path, (0, 0) if path in old else None)
    invocation._prune_stale(work)
    assert not any(p.exists() for p in old) and young.exists()


def init_operator(tmp_path: Path) -> operators.RivetInitOperator:
    return operators.RivetInitOperator(
        task_id="init",
        source_env="RIVET_SOURCE_URL",
        config=str(tmp_path / "cfg" / "shop.yaml"),
        rivet_bin=str(tmp_path / "bin" / "rivet"),
        mode="cdc",
        state_dir=str(tmp_path / "state" / "shop"),
        env_from_connections={"RIVET_SOURCE_URL": {"conn_id": "shop"}},
    )


def test_a_killed_init_leaves_neither_its_log_nor_its_draft_behind(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """Issue #151: `.init/init.log` and the draft `.init/<name>.yaml` were never removed."""
    (tmp_path / "state").mkdir()
    (tmp_path / "state" / "shop").mkdir()

    def spawn(argv: list[str], out: Any, err: Any, env: Any, cwd: str, on_process: Any) -> int:
        Path(argv[argv.index("-o") + 1]).write_text("draft")
        raise Killed()

    monkeypatch.setattr(operators, "spawn", spawn)
    op = init_operator(tmp_path)
    with pytest.raises(Killed):
        op._write_config(tmp_path / "cfg" / "shop.yaml", {}, "rivet")
    assert list((tmp_path / "state" / "shop" / ".init").iterdir()) == []


def test_an_init_log_is_opened_private_like_the_other_files_of_the_package(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Issue #151: the `.init` logs hold rivet's full output and must not take the umask's mode."""
    seen: list[int] = []

    def spawn(argv: list[str], out: Any, err: Any, env: Any, cwd: str, on_process: Any) -> int:
        seen.append(stat.S_IMODE(os.fstat(out.fileno()).st_mode))
        return 0

    monkeypatch.setattr(operators, "spawn", spawn)
    op = init_operator(tmp_path)
    (tmp_path / "w").mkdir()
    op._rivet("rivet", ["check"], {}, tmp_path / "w")
    assert seen == [0o600]
