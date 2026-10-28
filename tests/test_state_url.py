"""`RIVET_STATE_URL` by rivet's own rule (src/state/mod.rs at RIVET_CONTRACT_REF): a PostgreSQL URL or a `sqlite:` file is
a state rivet opens, an empty value keeps the file beside the config, anything else rivet refuses. A worker's value that
names a state and is not passed to rivet used to be dropped silently when it was a `sqlite:` file: rivet ran on an
empty state in `state_dir` and read every table again."""

from __future__ import annotations

import json
from pathlib import Path

import pytest
from airflow.exceptions import AirflowFailException
from conftest import SECRET, FakeTI, Rig

from airflow_provider_rivet import preflight
from airflow_provider_rivet.invocation import undeclared_state_variables
from airflow_provider_rivet.operators import RivetApplyOperator, RivetRunOperator

PG_URL = "postgresql://state:st4te-pw@state-db/rivet_state"
REFUSED = "RIVET_AIRFLOW_STATE_ENV_NOT_PASSED"


def declared(rig: Rig, url: str, **kw: object) -> object:
    """A run task whose own environment declares `RIVET_STATE_URL`."""
    return rig.operator(RivetRunOperator, export="orders", env={"RIVET_PG_URL": SECRET, "RIVET_STATE_URL": url}, **kw)


def refusal_code(rig: Rig, op: object) -> str:
    """Run a task that must be refused before rivet starts; the refusal code."""
    exc, ti = rig.run(op)
    assert isinstance(exc, AirflowFailException), exc
    assert not rig.calls(), "rivet was not started"
    return ti.store[("task", "return_value")]["preflight_refusal"]


@pytest.mark.parametrize(
    ("url", "kind", "selected"),
    [
        ("postgres://h/db", "postgres", True),
        ("postgresql://h/db", "postgres", True),
        ("sqlite:/data/s.db", "sqlite", True),
        ("sqlite:///data/s.db", "sqlite", True),
        ("sqlite:s.db", "sqlite", True),
        ("", "sqlite", False),
        (" ", "sqlite", False),
        ("mysql://h/db", "sqlite", False),
        ("Postgres://h/db", "sqlite", False),
        (" postgresql://h/db", "sqlite", False),
        ("postgres-state.db", "sqlite", False),
        ("SQLite:/data/s.db", "sqlite", False),
    ],
)
def test_the_state_url_is_classified_by_rivets_scheme_rule(url: str, kind: str, selected: bool) -> None:
    assert preflight.state_kind({"RIVET_STATE_URL": url}) == kind
    assert preflight.selects_state({"RIVET_STATE_URL": url}) is selected
    assert undeclared_state_variables({}, {"RIVET_STATE_URL": url}) == (["RIVET_STATE_URL"] if selected else [])
    assert undeclared_state_variables({"RIVET_STATE_URL": url}, {"RIVET_STATE_URL": url}) == []


def test_no_state_url_at_all_selects_nothing() -> None:
    assert preflight.state_kind({}) == "sqlite" and not preflight.selects_state({}) and undeclared_state_variables({}, {}) == []


@pytest.mark.parametrize(
    ("url", "expected"),
    [
        ("", "/st/.rivet_state.db"),
        (PG_URL, "/st/.rivet_state.db"),
        ("sqlite:/data/s.db", "/data/s.db"),
        ("sqlite:///data/s.db", "/data/s.db"),
        ("sqlite://data/s.db", "/st/data/s.db"),
        ("sqlite:s.db", "/st/s.db"),
        ("sqlite:sub/s.db", "/st/sub/s.db"),
    ],
)
def test_the_sqlite_file_is_the_one_rivet_opens_for_a_config_in_the_state_directory(url: str, expected: str) -> None:
    """rivet's `sqlite_state_path`: an absolute path as written (after `sqlite:` and an optional `//`), a relative one
    against the config's directory, which for the copy in `state_dir` is `state_dir`."""
    assert preflight.sqlite_state_file({"RIVET_STATE_URL": url}, Path("/st")) == Path(expected)


@pytest.mark.parametrize("url", ["sqlite:/data/rivet/state.db", "sqlite:///data/rivet/state.db", "sqlite:state.db"])
def test_a_worker_sqlite_state_the_task_does_not_pass_on_is_refused_before_rivet_starts(
    rig: Rig, monkeypatch: pytest.MonkeyPatch, caplog: pytest.LogCaptureFixture, url: str
) -> None:
    monkeypatch.setenv("RIVET_STATE_URL", url)
    op = rig.operator(RivetRunOperator, export="orders")
    for try_number in (1, 2):
        exc, ti = rig.run(op, FakeTI(try_number=try_number))
        assert isinstance(exc, AirflowFailException), exc
        assert ti.store[("task", "return_value")]["preflight_refusal"] == REFUSED
        text = str(exc)
        assert 'env_passthrough=["RIVET_STATE_URL"]' in text and "`sqlite:` file" in text and "env_from_connections" in text
        blob = text + json.dumps(ti.store[("task", "return_value")]) + json.dumps(rig.records) + caplog.text
        assert "/data/rivet" not in blob and "state.db" not in blob, "the value is never shown"
    assert not rig.calls() and not list(rig.state_dir.iterdir()), "rivet is not started on an empty state"


def test_a_worker_sqlite_state_the_task_passes_on_reaches_rivet_as_written(rig: Rig, monkeypatch: pytest.MonkeyPatch) -> None:
    url = f"sqlite:{rig.tmp / 'worker-state.db'}"
    (rig.tmp / "worker-state.db").write_bytes(b"state")
    monkeypatch.setenv("RIVET_STATE_URL", url)
    payload, _ = rig.run(rig.operator(RivetRunOperator, export="orders", env_passthrough=["RIVET_STATE_URL"]))
    assert payload["decision"] == "success", payload
    assert rig.calls()[-1]["env"]["RIVET_STATE_URL"] == url
    other = f"sqlite:{rig.tmp / 'task-state.db'}"
    (rig.tmp / "task-state.db").write_bytes(b"state")
    rig.run(declared(rig, other))
    assert rig.calls()[-1]["env"]["RIVET_STATE_URL"] == other, "the task's own value wins over the worker's"


@pytest.mark.parametrize(
    "ignored", ["", " ", "mysql://state-db/x", "Postgres://state-db/x", " postgresql://state-db/x", "postgres-state.db"]
)
def test_a_worker_value_that_selects_no_state_is_not_refused_and_not_passed(
    rig: Rig, monkeypatch: pytest.MonkeyPatch, ignored: str
) -> None:
    monkeypatch.setenv("RIVET_STATE_URL", ignored)
    payload, _ = rig.run(rig.operator(RivetRunOperator, export="orders"))
    assert payload["decision"] == "success", payload
    assert rig.argv("run")[2] == str(rig.copy()), "SQLite state beside the copy in state_dir"
    assert rig.calls()[-1]["env"].get("RIVET_STATE_URL") is None


def test_a_sqlite_state_declared_by_the_task_is_checked_where_rivet_opens_it(rig: Rig) -> None:
    """Before: the check looked at `state_dir/.rivet_state.db` whatever file the URL named, so a state that was at the
    named file was refused as left behind, and a state left beside the config went unseen when the default file existed."""
    left = rig.config_dir / ".rivet_state.db"
    left.write_bytes(b"state")
    target = rig.tmp / "elsewhere" / "state.db"
    target.parent.mkdir()
    op = declared(rig, f"sqlite:{target}")
    assert refusal_code(rig, op) == "RIVET_AIRFLOW_STATE_BESIDE_CONFIG"
    text = rig.events("rivet.refused")[-1]["text"]
    assert str(left.resolve()) in text and f"{target} does not" in text and f"to {target}" in text
    assert not target.exists(), "a refusal writes nothing that would lift it"
    target.write_bytes(b"state")
    payload, _ = rig.run(op)
    assert payload["decision"] == "success", payload
    assert rig.calls()[-1]["env"]["RIVET_STATE_URL"] == f"sqlite:{target}"
    (rig.state_dir / ".rivet_state.db").unlink(missing_ok=True)


def test_a_relative_sqlite_state_is_the_file_in_the_state_directory(rig: Rig) -> None:
    (rig.config_dir / ".rivet_state.db").write_bytes(b"state")
    op = declared(rig, "sqlite:named.db")
    assert refusal_code(rig, op) == "RIVET_AIRFLOW_STATE_BESIDE_CONFIG"
    assert f"{rig.state_dir / 'named.db'} does not" in rig.events("rivet.refused")[-1]["text"]
    (rig.state_dir / "named.db").write_bytes(b"state")
    payload, _ = rig.run(op)
    assert payload["decision"] == "success", payload


def test_the_single_host_warning_names_the_file_rivet_opens(rig: Rig) -> None:
    target = rig.tmp / "elsewhere.db"
    payload, _ = rig.run(declared(rig, f"sqlite:{target}"))
    assert payload["decision"] == "success", payload
    assert [w["text"] for w in rig.events("rivet.warning")][0].startswith(f"SQLite state in {target}: this setup is single-host")
    rig.records.clear()
    rig.run(rig.operator(RivetRunOperator, export="orders"))
    assert [w["text"] for w in rig.events("rivet.warning")][0].startswith(f"SQLite state in {rig.state_dir / '.rivet_state.db'}:")


def test_a_pod_refuses_a_sqlite_state_url_as_it_refuses_the_default_file(rig: Rig) -> None:
    op = declared(rig, "sqlite:/mnt/pv/state.db", deployment="pod")
    assert refusal_code(rig, op) == "RIVET_AIRFLOW_STATE_SQLITE_ON_POD"


def test_sqlite_state_declared_by_the_task_is_still_refused_under_an_executor_of_several_hosts(
    rig: Rig, monkeypatch: pytest.MonkeyPatch
) -> None:
    from airflow_provider_rivet import operators

    monkeypatch.setattr(operators, "executor_spreading_hosts", lambda: "CeleryExecutor")
    assert refusal_code(rig, declared(rig, "sqlite:/mnt/shared/state.db")) == "RIVET_AIRFLOW_STATE_SQLITE_MULTI_HOST"
    payload, _ = rig.run(declared(rig, "sqlite:/mnt/shared/state.db", single_host=True))
    assert payload["decision"] == "success", payload


def test_apply_tasks_refuse_a_worker_sqlite_state_the_same_way(rig: Rig, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("RIVET_STATE_URL", "sqlite:/data/rivet/state.db")
    assert refusal_code(rig, rig.operator(RivetApplyOperator, export="orders")) == REFUSED
