"""What the task log, the XCom payload, the Airflow skip text and the Slack title say, against what rivet reported.

Each test feeds the package rivet 0.32.2's own shapes (`rivet_shapes`, the contract fixtures) and compares the words it
writes with the facts in the feed: the reason rivet gave for a skip, the lock that timed out, the error that decided the
task, the retry that will or will not happen.
"""
# ruff: noqa: F811  (the `log` fixture is imported and taken by name)

from __future__ import annotations

import logging
from typing import Any

import pytest
from airflow.exceptions import AirflowException, AirflowFailException, AirflowSkipException
from conftest import FakeTI, Rig, fixture
from rivet_shapes import load_row, run_entry, run_summary
from test_task_log import Recorder, log  # noqa: F401  (the `log` fixture)

from airflow_provider_rivet import tasklog
from airflow_provider_rivet.callbacks import format_title
from airflow_provider_rivet.operators import (
    RivetApplyOperator,
    RivetCdcRunOperator,
    RivetCompactOperator,
    RivetLoadOperator,
    RivetRunOperator,
)
from airflow_provider_rivet.reading import Reading, _Step

REASON = "no new rows since cursor 'updated_at'"


def payload_of(ti: FakeTI) -> dict[str, Any]:
    return ti.store[("task", "return_value")]


# --- the skip reason rivet gives a run / cdc_run / apply entry reaches XCom and the log -------------------------------


@pytest.mark.parametrize(
    ("operator", "export", "sub"),
    [(RivetRunOperator, "users", "run"), (RivetApplyOperator, "users", "apply"), (RivetCdcRunOperator, "orders_cdc", "run")],
)
def test_the_skip_reason_of_a_run_entry_reaches_the_payload_and_the_headline(
    rig: Rig, log: Recorder, operator: type, export: str, sub: str
) -> None:
    entry = run_entry(export, "skipped", skip_reason=REASON)
    rig.scenario(**{sub: {"summary": run_summary([entry])}})
    payload, _ = rig.run(rig.operator(operator, export=export))
    assert payload["decision"] == "success" and payload["skip_reason"] == REASON
    assert log.shown()[0].startswith(f"INFO rivet {sub} {export} → SKIPPED: nothing to do ({REASON}) in ")


def test_the_contract_fixture_of_a_skipped_run_entry_is_read_through_run_units() -> None:
    """tests/fixtures/scheduler/run_entry_skipped.json is rivet's own entry: every field the package reads is read."""
    entry = fixture("run_entry_skipped.json")
    unit = Reading(set(), {}, "run").run_units(_Step(0, None, None), {"per_export": [entry]}, None, "countries")[0]
    assert (unit.status, unit.skip_reason) == ("skipped", entry["skip_reason"])


def test_an_entry_without_a_skip_reason_has_none_and_one_of_the_wrong_type_is_null_and_recorded(rig: Rig) -> None:
    """A success entry has no reason (as on main); a reason that is not text is null and recorded malformed."""
    rig.scenario(run={"summary": run_summary([run_entry("users", "success")])})
    payload, _ = rig.run(rig.operator(RivetRunOperator, export="users"))
    assert payload["skip_reason"] is None and payload["degraded"] == []
    rig.scenario(run={"summary": run_summary([run_entry("users", "skipped", skip_reason=5)])})
    payload, _ = rig.run(rig.operator(RivetRunOperator, export="users"))
    assert payload["skip_reason"] is None and payload["degraded"] == ["result_malformed"]


# --- the skip reason of several units ----------------------------------------------------------------------------------


def compact_summary(*rows: dict[str, Any]) -> dict[str, Any]:
    return {"run_id": "c1", "per_table": list(rows)}


def skipped(table: str, reason: str) -> dict[str, Any]:
    return load_row("orders", table, "skipped", skip_reason=reason)


def test_two_skipped_tables_with_one_reason_are_an_airflow_skip_that_says_why(rig: Rig, log: Recorder) -> None:
    rig.scenario(compact={"summary": compact_summary(skipped("t1", "nothing_to_compact"), skipped("t2", "nothing_to_compact"))})
    exc, ti = rig.run(rig.operator(RivetCompactOperator, export="orders"))
    assert isinstance(exc, AirflowSkipException) and str(exc) == "compact skipped: nothing_to_compact"
    assert payload_of(ti)["skip_reason"] == "nothing_to_compact"
    shown = log.shown()
    assert shown[0].startswith("INFO rivet compact orders → SKIPPED: nothing to do (nothing_to_compact) in ")
    assert "INFO   table    orders.t1 → p.d.t1: skipped (nothing_to_compact), 0 rows (run c1)" in shown


def test_skipped_tables_with_different_reasons_name_each_reason_once(rig: Rig) -> None:
    rows = (skipped("t1", "full_load"), skipped("t2", "nothing_to_compact"), skipped("t3", "full_load"))
    rig.scenario(compact={"summary": compact_summary(*rows)})
    exc, ti = rig.run(rig.operator(RivetCompactOperator, export="orders"))
    assert str(exc) == "compact skipped: full_load; nothing_to_compact"
    assert payload_of(ti)["skip_reason"] == "full_load; nothing_to_compact"


def test_a_skip_with_no_reason_does_not_print_none(rig: Rig) -> None:
    rig.scenario(compact={"summary": compact_summary(load_row("orders", "t1", "skipped"), load_row("orders", "t2", "skipped"))})
    exc, ti = rig.run(rig.operator(RivetCompactOperator, export="orders"))
    assert isinstance(exc, AirflowSkipException) and str(exc) == "compact skipped"
    assert payload_of(ti)["skip_reason"] is None


def test_one_unit_keeps_its_reason_and_a_mixed_load_is_still_a_success(rig: Rig) -> None:
    """Unchanged neighbours: one skipped table says its reason as before; a load where one table skipped and one loaded is
    no skip, and its XCom carries the skipped table's reason (the payload key never was a verdict)."""
    rig.scenario(compact={"summary": compact_summary(skipped("t1", "full_load"))})
    exc, ti = rig.run(rig.operator(RivetCompactOperator, export="orders", table="t1"))
    assert str(exc) == "compact skipped: full_load" and payload_of(ti)["skip_reason"] == "full_load"
    rig.scenario(load={"summary": compact_summary(skipped("t1", "up_to_date"), load_row("orders", "t2", "loaded", rows=3))})
    payload, _ = rig.run(rig.operator(RivetLoadOperator, export="orders"))
    assert payload["decision"] == "success" and [u["status"] for u in payload["units"]] == ["skipped", "loaded"]


# --- the title's retry word is the deciding error's ---------------------------------------------------------------------


def test_a_final_refusal_among_a_retryable_crash_is_titled_final(rig: Rig) -> None:
    """json_errors_mixed.json: `events` crashed (retryable), `orders_cdc` is a CDC log gap (a refusal, final), exit 5. The
    task fails without a retry; the Slack title named the first failed unit's retry word: `crashed (retryable)`."""
    line = fixture("json_errors_mixed.json")
    entries = [
        run_entry("events", "failed", error_message="exited with status signal"),
        run_entry("orders_cdc", "failed", error_message="the checkpoint position is no longer in the source log"),
    ]
    rig.scenario(run={"exit": 5, "line": line, "summary": run_summary(entries)})
    exc, ti = rig.run(rig.operator(RivetRunOperator))
    assert isinstance(exc, AirflowFailException)
    payload = payload_of(ti)
    assert [u["error"]["class"] for u in payload["units"]] == ["crashed", "refusal"]
    assert format_title(payload, dag_id="d", task_id="run") == "d.run — RIVET_SOURCE_CDC_LOG_GAP (final) +1 more"


def unit(code: str | None, cls: str, retryable: bool, export: str) -> dict[str, Any]:
    error = {"code": code, "kind": None, "class": cls, "exit_code": 2, "retryable": retryable, "action": None}
    return {"export": export, "table": None, "status": "failed", "error": error}


def test_the_title_reads_the_deciding_error_and_counts_the_other_failed_units() -> None:
    crashed, gap = unit(None, "crashed", True, "a"), unit("RIVET_SOURCE_CDC_LOG_GAP", "refusal", False, "b")
    gap_error = gap["error"]
    deciding = {"units": [crashed, gap], "error": gap_error}
    assert format_title(deciding, dag_id="d", task_id="t") == "d.t — RIVET_SOURCE_CDC_LOG_GAP (final) +1 more"
    timeout = unit("RIVET_SOURCE_TIMEOUT", "retryable", True, "a")["error"]
    synthetic = {"decision": "fail", "units": [unit("RIVET_SOURCE_TIMEOUT", "retryable", True, "a"), gap], "error": gap_error}
    assert format_title(synthetic, dag_id="d", task_id="t") == "d.t — RIVET_SOURCE_CDC_LOG_GAP (final) +1 more"
    # unchanged: a payload with no process-level error is titled by its first failed unit, as before
    assert format_title({"units": [crashed, gap]}, dag_id="d", task_id="t") == "d.t — crashed (retryable) +1 more"
    # unchanged: a process-level error with no failed unit is the only failure
    assert format_title({"units": [], "error": timeout}, dag_id="d", task_id="t") == "d.t — RIVET_SOURCE_TIMEOUT (retryable)"


# --- the lock a headline names ----------------------------------------------------------------------------------------


def wait(unit: str, lock_wait: int = 1) -> dict[str, Any]:
    return {"event": "rivet.lock.wait", "lock": f"/x/{unit}", "unit": unit, "lock_wait": lock_wait}


def acquired(unit: str, waited: float) -> dict[str, Any]:
    return {"event": "rivet.lock.acquired", "lock": f"/x/{unit}", "unit": unit, "waited": waited}


def render(events: list[dict[str, Any]], payload: dict[str, Any], clock_ticks: list[float]) -> list[tuple[int, str]]:
    ticks = iter(clock_ticks)
    original = tasklog.clock
    tasklog.clock = lambda: next(ticks)
    try:
        task = tasklog.TaskLog("run", None, None, lambda level, text: None)
        for event in events:
            task.record(event)
        return task.render(payload, 1, 2, False)
    finally:
        tasklog.clock = original


def test_the_busy_headline_names_the_lock_that_timed_out_not_the_first_one_waited_for() -> None:
    """A run of the whole config takes one lock per export: `countries` was held for a while and released, `users` stayed
    held. The exception names `users`; the headline named `countries`, which was free."""
    events = [wait("export `countries` of the config"), acquired("export `countries` of the config", 0.3)]
    events.append(wait("export `users` of the config"))
    payload = {"command": "run", "decision": "retry", "busy": "RIVET_AIRFLOW_EXPORT_BUSY", "units": []}
    (level, text), *_ = render(events, payload, [0.0, 2.0])
    assert level == logging.WARNING
    assert text == (
        "rivet run → BUSY [RIVET_AIRFLOW_EXPORT_BUSY]: the lock of export `users` of the config stayed held for 1 s; "
        "rivet was not started, Airflow retries it (try 1 of 3)"
    )


def test_the_busy_headline_of_one_lock_is_unchanged() -> None:
    payload = {"command": "run", "decision": "retry", "busy": "RIVET_AIRFLOW_EXPORT_BUSY", "units": []}
    (_, text), *_ = render([wait("export `users`", 5)], payload, [0.0, 5.0])
    assert text == (
        "rivet run → BUSY [RIVET_AIRFLOW_EXPORT_BUSY]: the lock of export `users` stayed held for 5 s; "
        "rivet was not started, Airflow retries it (try 1 of 3)"
    )


def test_the_success_headline_counts_the_wait_for_every_lock_it_took() -> None:
    """`countries` released after 0.9 s and `users` after 1.5 s: 2.4 s of the 4.1 s task was waiting, not work."""
    one, two = "export `countries` of the config", "export `users` of the config"
    events = [wait(one, 10), acquired(one, 0.9), wait(two, 10), acquired(two, 1.5)]
    payload = {"command": "run", "decision": "success", "units": [{"export": "countries", "status": "success", "rows": 1}]}
    (_, text), *_ = render(events, payload, [0.0, 4.1])
    assert text == f"rivet run → OK: 1 row in 1.7 s, after 2.4 s waiting for 2 locks ({one}, {two})"


def test_the_success_headline_of_one_lock_is_unchanged() -> None:
    one = "export `countries`"
    payload = {"command": "run", "decision": "success", "units": [{"export": "countries", "status": "success", "rows": 1}]}
    (_, text), *_ = render([wait(one, 10), acquired(one, 0.9)], payload, [0.0, 4.0])
    assert text == "rivet run → OK: 1 row in 3.1 s, after 0.9 s waiting for the lock of export `countries`"


# --- a stop before rivet starts names a rivet subcommand ---------------------------------------------------------------


def test_a_cdc_run_stopped_while_waiting_for_its_lock_names_rivet_run(rig: Rig, log: Recorder) -> None:
    import fcntl
    import os

    from airflow_provider_rivet.invocation import path_name

    op = rig.operator(RivetCdcRunOperator, export="orders_cdc", lock_wait=5)
    locks = rig.state_dir / "airflow" / "locks" / "run"
    locks.mkdir(parents=True, exist_ok=True)
    holder = os.open(locks / f"{path_name('orders_cdc')}.lock", os.O_RDWR | os.O_CREAT)
    fcntl.flock(holder, fcntl.LOCK_EX)
    op._stop_requested = True
    try:
        exc, ti = rig.run(op)
    finally:
        os.close(holder)
    assert isinstance(exc, AirflowException)
    assert str(exc) == "the task was stopped before rivet run started; rivet run was not run"
    assert payload_of(ti)["stopped_before"] == "run"
    assert log.shown()[0] == "WARNING rivet run orders_cdc → STOPPED by Airflow before rivet run started; rivet run was not run"


# --- a load or compact result that names no table ----------------------------------------------------------------------


@pytest.mark.parametrize("operator", [RivetLoadOperator, RivetCompactOperator])
@pytest.mark.parametrize("doc", [{"run_id": "r1", "per_table": []}, {"run_id": "r1"}])
def test_a_result_with_no_table_entry_is_not_read_as_done_without_a_note(rig: Rig, operator: type, doc: dict) -> None:
    rig.scenario(**{operator.command: {"summary": doc}})
    payload, _ = rig.run(rig.operator(operator, export="orders", table="t"))
    assert payload["decision"] == "success" and payload["degraded"] == ["result_malformed"]


@pytest.mark.parametrize("operator", [RivetLoadOperator, RivetCompactOperator])
def test_a_result_with_a_table_entry_carries_no_note_and_a_failed_load_none_either(rig: Rig, operator: type) -> None:
    """Unchanged neighbours: a well-formed result is not degraded, and a failed command whose result is empty is a failure."""
    done = "loaded" if operator is RivetLoadOperator else "compacted"
    rig.scenario(**{operator.command: {"summary": {"run_id": "r1", "per_table": [load_row("orders", "t", done, rows=1)]}}})
    payload, _ = rig.run(rig.operator(operator, export="orders", table="t"))
    assert payload["degraded"] == []
    rig.scenario(**{operator.command: {"exit": 1, "line": {"error": "boom", "exit_class": 1}, "summary": {"per_table": []}}})
    exc, ti = rig.run(rig.operator(operator, export="orders", table="t"))
    assert isinstance(exc, AirflowFailException) and "result_malformed" not in payload_of(ti)["degraded"]


# --- a crash with no Airflow try left is not a retry --------------------------------------------------------------------


def test_a_crash_on_the_last_airflow_try_says_no_retry_in_xcom_and_the_log(rig: Rig, log: Recorder) -> None:
    rig.scenario(run={"signal": "SIGKILL"})
    ti = FakeTI(task_id="task", try_number=1, max_tries=0)
    exc, _ = rig.run(rig.operator(RivetRunOperator, export="users"), ti)
    assert isinstance(exc, AirflowFailException), "Airflow itself will not retry: retries=0"
    assert payload_of(ti)["crashed_retry"] is False
    assert log.shown()[0] == "ERROR rivet run users → CRASHED: rivet ended by signal 9, final: no try left (try 1 of 1)"


def test_a_crash_with_a_try_left_and_one_with_the_budget_used_up_are_unchanged(rig: Rig, log: Recorder) -> None:
    rig.scenario(run={"signal": "SIGKILL"})
    one = FakeTI(task_id="task", try_number=1, max_tries=2)
    exc, _ = rig.run(rig.operator(RivetRunOperator, export="users"), one)
    assert type(exc) is AirflowException and payload_of(one)["crashed_retry"] is True
    assert (
        "WARNING rivet run users → CRASHED: rivet ended by signal 9; Airflow retries it (try 1 of 3) under the crash budget"
        in log.shown()
    )
    two = FakeTI(task_id="task", try_number=2, max_tries=2)
    exc, _ = rig.run(rig.operator(RivetRunOperator, export="users"), two)
    assert type(exc) is AirflowFailException and payload_of(two)["crashed_retry"] is False
    assert "ERROR rivet run users → CRASHED: rivet ended by signal 9, final: the crash budget is used up" in log.shown()
