"""Races between tasks of one state directory, each forced into the losing order: a hook runs a second task at the
exact point where the first one is exposed, so the cell loses on the unfixed code every time instead of by timing.
Mutant ids are the mutmut names of airflow_provider_rivet/invocation.py."""

from __future__ import annotations

import fcntl
import json
import os
import threading
import time
from pathlib import Path
from typing import Any

import pytest
from airflow.exceptions import AirflowFailException
from conftest import Rig

from airflow_provider_rivet import invocation
from airflow_provider_rivet.operators import RivetRunOperator

OWNERS = invocation.QUERY_OWNERS


def with_query_file(config: Path, export: str, ref: str = "q.sql") -> None:
    """Point one export of a rig config at a query file."""
    config.write_text(config.read_text().replace(f"query: SELECT * FROM {export}", f"query_file: {ref}"))


def second_config(rig: Rig, name: str = "cfg2") -> Path:
    """A config of another directory over the same state directory; its `users` export reads `q.sql`."""
    other = rig.tmp / name / "pg.yaml"
    other.parent.mkdir(parents=True)
    other.write_text(rig.config.read_text())
    (other.parent / "q.sql").write_text("SELECT 'b'")
    with_query_file(other, "users")
    return other


def sql_of(rig: Rig, export: str) -> list[str]:
    """The SQL each fake rivet process read for one export."""
    return [c["sql"][export] for c in rig.calls() if export in c["sql"]]


def interleave(rig: Rig, monkeypatch: pytest.MonkeyPatch, second: Any, at: Any) -> dict[str, Any]:
    """When `at(path)` first holds for a `_write_atomic`, run `second` (an operator) on its own thread and wait until it
    has finished or is waiting for a lock; the write goes on afterwards. Returns what the second task ended with."""
    ended: dict[str, Any] = {}
    waiting = threading.Event()
    emit = second._emit

    def note(entry: dict) -> None:
        if entry.get("event") == "rivet.lock.wait":
            waiting.set()
        emit(entry)

    second._emit = note
    thread = threading.Thread(target=lambda: ended.update(result=rig.run(second)))
    real = invocation._write_atomic
    fired: list[int] = []

    def hooked(path: Path, data: bytes) -> str:
        if not fired and at(path):
            fired.append(1)
            thread.start()
            deadline = time.monotonic() + 60
            while thread.is_alive() and not waiting.is_set() and time.monotonic() < deadline:
                time.sleep(0.01)
        return real(path, data)

    monkeypatch.setattr(invocation, "_write_atomic", hooked)
    ended["join"] = lambda: thread.join(60) if fired else None
    return ended


def test_two_tasks_claiming_one_query_file_path_cannot_both_pass_the_guard(rig: Rig, monkeypatch: pytest.MonkeyPatch) -> None:
    """_copy_query_files: task B runs whole between task A's read of the owners record and A's write of it (two config
    directories, one state directory, one relative path, export names that share no lock). A is the first claim, so it
    runs its own SQL, and B is refused; unfixed, A ran B's SQL and both reported success."""
    (rig.config_dir / "q.sql").write_text("SELECT 'a'")
    with_query_file(rig.config, "orders")
    other = second_config(rig)
    ended = interleave(
        rig, monkeypatch, rig.operator(RivetRunOperator, export="users", config=str(other)), lambda p: p.name == OWNERS
    )
    a, _ = rig.run(rig.operator(RivetRunOperator, export="orders"))
    ended["join"]()
    b, ti = ended["result"]
    assert a["decision"] == "success" and sql_of(rig, "orders") == ["SELECT 'a'"], "A ran A's SQL"
    assert isinstance(b, AirflowFailException) and "[RIVET_AIRFLOW_QUERY_FILE]" in str(b)
    assert ti.store[("task", "return_value")]["preflight_refusal"] == "RIVET_AIRFLOW_QUERY_FILE"
    assert sql_of(rig, "users") == [], "B started no rivet"
    assert (rig.state_dir / "q.sql").read_text() == "SELECT 'a'"
    assert json.loads((rig.state_dir / OWNERS).read_text()) == {"q.sql": str(rig.config_dir)}


def test_a_task_waits_for_the_owners_lock_by_the_state_directory_only(rig: Rig, monkeypatch: pytest.MonkeyPatch) -> None:
    """_query_files_lock: a second task of ANOTHER state directory (one part of the key differs) is not held back by
    the first one's lock, and a config with no query file takes no such lock at all."""
    (rig.config_dir / "q.sql").write_text("SELECT 'a'")
    with_query_file(rig.config, "orders")
    other = second_config(rig)
    elsewhere = rig.tmp / "state2"
    elsewhere.mkdir()
    ended = interleave(
        rig,
        monkeypatch,
        rig.operator(RivetRunOperator, export="users", config=str(other), state_dir=str(elsewhere)),
        lambda p: p.name == OWNERS,
    )
    rig.run(rig.operator(RivetRunOperator, export="orders"))
    ended["join"]()
    b, _ = ended["result"]
    assert b["decision"] == "success" and sql_of(rig, "users") == ["SELECT 'b'"]
    assert not rig.events("rivet.lock.wait")


def test_a_config_without_a_query_file_never_waits_for_the_owners_lock(rig: Rig) -> None:
    """_copy_query_files is reached under the lock only for a config that has a relative query file."""
    holder = rig.state_dir / "airflow" / "locks" / "state"
    holder.mkdir(parents=True)
    fd = os.open(holder / "query-files.lock", os.O_RDWR | os.O_CREAT, 0o600)
    fcntl.flock(fd, fcntl.LOCK_EX)
    try:
        payload, _ = rig.run(rig.operator(RivetRunOperator, export="orders", lock_wait=0))
    finally:
        fcntl.flock(fd, fcntl.LOCK_UN)
        os.close(fd)
    assert payload["decision"] == "success"


def test_the_owners_lock_held_past_lock_wait_ends_the_task_busy_and_retryable(rig: Rig) -> None:
    """_query_files_lock: a held owners lock is waited for up to `lock_wait`, then the task is Busy, rivet is not started."""
    (rig.config_dir / "q.sql").write_text("SELECT 'a'")
    with_query_file(rig.config, "orders")
    holder = rig.state_dir / "airflow" / "locks" / "state"
    holder.mkdir(parents=True)
    fd = os.open(holder / "query-files.lock", os.O_RDWR | os.O_CREAT, 0o600)
    fcntl.flock(fd, fcntl.LOCK_EX)
    try:
        exc, _ = rig.run(rig.operator(RivetRunOperator, export="orders", lock_wait=0))
    finally:
        fcntl.flock(fd, fcntl.LOCK_UN)
        os.close(fd)
    assert isinstance(exc, Exception) and "RIVET_AIRFLOW_EXPORT_BUSY" in str(exc) and rig.calls() == []


def test_the_owners_lock_is_released_by_an_explicit_unlock_not_by_close(rig: Rig, monkeypatch: pytest.MonkeyPatch) -> None:
    """_query_files_lock: a copy of the descriptor elsewhere (a process spawned meanwhile) keeps a lock released by close
    alone; the unlock is explicit, so the lock is free once the copy is done."""
    (rig.config_dir / "q.sql").write_text("SELECT 'a'")
    with_query_file(rig.config, "orders")
    copies: list[int] = []
    real = os.open

    def open_and_copy(path: Any, *args: Any, **kw: Any) -> int:
        fd = real(path, *args, **kw)
        if str(path).endswith("query-files.lock"):
            copies.append(os.dup(fd))
        return fd

    monkeypatch.setattr(os, "open", open_and_copy)
    rig.run(rig.operator(RivetRunOperator, export="orders"))
    monkeypatch.undo()
    assert len(copies) == 1
    fd = os.open(rig.state_dir / "airflow" / "locks" / "state" / "query-files.lock", os.O_RDWR)
    try:
        fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
    finally:
        os.close(fd)
        os.close(copies[0])


# --- the same config under another revision path ----------------------------------------------------------------------


def git_sync(rig: Rig, revisions: dict[str, str]) -> Path:
    """`dags/repo -> rev-<sha>` as git-sync lays it out; one pipe.yaml and one query file per revision."""
    dags = rig.tmp / "dags"
    for rev, sql in revisions.items():
        (dags / rev).mkdir(parents=True)
        (dags / rev / "q.sql").write_text(sql)
        (dags / rev / "pipe.yaml").write_text(rig.config.read_text())
        with_query_file(dags / rev / "pipe.yaml", "orders")
    return dags


def point(dags: Path, rev: str) -> None:
    """Swap the `repo` symlink to a revision, atomically as git-sync does."""
    link = dags / "repo.new"
    link.symlink_to(dags / rev)
    os.replace(link, dags / "repo")


def test_a_git_sync_revision_flip_is_the_same_config_and_keeps_its_query_file(rig: Rig) -> None:
    """_copy_query_files: the owner is the config path the DAG gave; `repo -> rev-a` flipped to `rev-b` is not another
    config directory. Unfixed, the second run was refused for good as 'two config directories'."""
    dags = git_sync(rig, {"rev-a": "SELECT 'a1'", "rev-b": "SELECT 'b1'"})
    config = str(dags / "repo" / "pipe.yaml")
    point(dags, "rev-a")
    first, _ = rig.run(rig.operator(RivetRunOperator, export="orders", config=config))
    point(dags, "rev-b")
    second, _ = rig.run(rig.operator(RivetRunOperator, export="orders", config=config))
    assert not isinstance(second, Exception), second
    assert first["decision"] == "success" and second["decision"] == "success"
    assert sql_of(rig, "orders") == ["SELECT 'a1'", "SELECT 'b1'"], "the new revision's query runs"
    assert json.loads((rig.state_dir / OWNERS).read_text()) == {"q.sql": str(dags / "repo")}


def test_a_different_config_directory_is_still_refused_after_a_revision_flip(rig: Rig) -> None:
    """_copy_query_files: only the SAME path across revisions is one owner; a config of any other directory is refused."""
    dags = git_sync(rig, {"rev-a": "SELECT 'a1'", "rev-b": "SELECT 'b1'"})
    point(dags, "rev-a")
    rig.run(rig.operator(RivetRunOperator, export="orders", config=str(dags / "repo" / "pipe.yaml")))
    point(dags, "rev-b")
    stranger = second_config(rig)
    with_query_file(stranger, "orders")
    exc, _ = rig.run(rig.operator(RivetRunOperator, export="orders", config=str(stranger)))
    assert isinstance(exc, AirflowFailException) and "[RIVET_AIRFLOW_QUERY_FILE]" in str(exc)
    assert len(rig.calls()) == 1 and (rig.state_dir / "q.sql").read_text() == "SELECT 'a1'", "the stranger started no rivet"


def test_two_spellings_of_one_directory_stay_one_owner(rig: Rig) -> None:
    """_copy_query_files: a symlink alias of the config's directory is the same directory (unchanged from main)."""
    (rig.config_dir / "q.sql").write_text("SELECT 'a'")
    with_query_file(rig.config, "orders")
    alias = rig.tmp / "alias"
    alias.symlink_to(rig.config_dir)
    first, _ = rig.run(rig.operator(RivetRunOperator, export="orders"))
    second, _ = rig.run(rig.operator(RivetRunOperator, export="orders", config=str(alias / "pg.yaml")))
    assert first["decision"] == "success" and second["decision"] == "success"


def test_an_owners_record_written_before_the_upgrade_still_names_its_config(rig: Rig) -> None:
    """_copy_query_files: main recorded the RESOLVED directory (`rev-a`); while `repo` still points there the record is
    this config's, and it is rewritten to the path the DAG gives so the next flip is no flip."""
    dags = git_sync(rig, {"rev-a": "SELECT 'a1'", "rev-b": "SELECT 'b1'"})
    point(dags, "rev-a")
    rig.state_dir.joinpath(OWNERS).write_text(json.dumps({"q.sql": str((dags / "rev-a").resolve())}))
    config = str(dags / "repo" / "pipe.yaml")
    assert rig.run(rig.operator(RivetRunOperator, export="orders", config=config))[0]["decision"] == "success"
    assert json.loads((rig.state_dir / OWNERS).read_text()) == {"q.sql": str(dags / "repo")}
    point(dags, "rev-b")
    assert rig.run(rig.operator(RivetRunOperator, export="orders", config=config))[0]["decision"] == "success"


def test_a_record_of_a_revision_that_is_gone_is_refused_with_a_remedy_that_works(rig: Rig) -> None:
    """_copy_query_files: a deployment stuck by the old behaviour (record names `rev-a`, the link now points to `rev-b`)
    cannot be told from another config; the refusal says how to start over, and doing it works with no other change."""
    dags = git_sync(rig, {"rev-a": "SELECT 'a1'", "rev-b": "SELECT 'b1'"})
    point(dags, "rev-b")
    owners = rig.state_dir / OWNERS
    owners.write_text(json.dumps({"q.sql": str(dags / "rev-a")}))
    config = str(dags / "repo" / "pipe.yaml")
    exc, _ = rig.run(rig.operator(RivetRunOperator, export="orders", config=config))
    assert isinstance(exc, AirflowFailException)
    assert f"delete {owners}" in str(exc) and "the next task claims the path" in str(exc)
    owners.unlink()
    again, _ = rig.run(rig.operator(RivetRunOperator, export="orders", config=config))
    assert again["decision"] == "success" and sql_of(rig, "orders") == ["SELECT 'b1'"]
