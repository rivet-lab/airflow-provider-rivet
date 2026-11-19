"""The locks of each scope overlap: a coarse scope (the whole config, an export) takes every lock of the finer scopes
inside it, so a load of one table and a load of the whole config that holds it never run side by side, and two configs
with one export name but different sources do not wait for each other (issue #152)."""

from __future__ import annotations

from pathlib import Path
from types import SimpleNamespace

import pytest

from airflow_provider_rivet import invocation
from airflow_provider_rivet.invocation import Busy

EXPORTS = [{"name": "orders", "tables": ["users", "items"]}, {"name": "clicks", "table": "events"}]
SOURCE = {"type": "postgres", "url_env": "SRC_URL"}


def scope_session(
    tmp_path: Path, command: str, export: str | None = None, table: str | None = None, source: dict | None = SOURCE
) -> SimpleNamespace:
    config = tmp_path / "rivet.yaml"
    config.write_text("")
    (tmp_path / "state").mkdir(exist_ok=True)
    pf = {"exports": EXPORTS, "source": source}
    return SimpleNamespace(
        pf=SimpleNamespace(state_dir=tmp_path / "state", config=pf),
        req=SimpleNamespace(command=command, export=export, table=table, config=str(config), lock_wait=0),
        emit=lambda record: None,
        stopped=None,
        lock_fds=(),
    )


def holds(s: SimpleNamespace, other: SimpleNamespace) -> bool:
    """True when `other` is Busy while `s` holds its locks."""
    with invocation._export_lock(s):
        try:
            with invocation._export_lock(other):
                return False
        except Busy:
            return True


@pytest.mark.parametrize(
    ("coarse", "fine"),
    [
        (("load", None, None), ("load", "orders", "users")),
        (("compact", None, None), ("compact", "orders", "items")),
        (("load", "orders", None), ("load", "orders", "users")),
        (("load", None, None), ("load", "clicks", "events")),
        (("load", "orders", None), ("load", "orders", None)),
    ],
)
def test_a_coarse_load_scope_and_a_finer_one_it_covers_exclude_each_other(tmp_path: Path, coarse, fine) -> None:
    """Issue #152: a whole-config load and a per-table load of the same table lose the race every time, in either order."""
    whole = scope_session(tmp_path, *coarse)
    table = scope_session(tmp_path, *fine)
    assert holds(whole, table)
    assert holds(table, whole)


def test_two_loads_of_different_tables_of_one_export_do_not_wait_for_each_other(tmp_path: Path) -> None:
    assert not holds(scope_session(tmp_path, "load", "orders", "users"), scope_session(tmp_path, "load", "orders", "items"))


def test_a_run_of_the_whole_config_and_a_run_of_its_export_exclude_each_other(tmp_path: Path) -> None:
    assert holds(scope_session(tmp_path, "run"), scope_session(tmp_path, "run", "orders"))


def test_run_locks_are_keyed_by_export_and_source(tmp_path: Path) -> None:
    """Issue #152: two configs with one export name and two sources do not serialise; the same source does."""
    other_source = {"type": "postgres", "url_env": "OTHER_URL"}
    assert not holds(scope_session(tmp_path, "run", "orders"), scope_session(tmp_path, "run", "orders", source=other_source))
    assert holds(scope_session(tmp_path, "run", "orders"), scope_session(tmp_path, "run", "orders"))


def test_the_locks_of_a_scope_are_taken_in_one_order_and_never_twice(tmp_path: Path) -> None:
    paths = [p for p, _ in invocation._lock_paths(scope_session(tmp_path, "load", None, None))]
    assert paths == sorted(paths) and len(paths) == len(set(paths))
