"""Each failure the operators can publish reaches the operator's surfaces: the exception, the task log, the Slack text, the XCom payload and the retry decision."""

from __future__ import annotations

from collections.abc import Callable
from typing import Any

import pytest
from airflow.exceptions import AirflowException, AirflowFailException
from conftest import FakeTI, Rig
from test_refusal_codes import PACKAGE_CODES

from airflow_provider_rivet import operators
from airflow_provider_rivet.callbacks import format_failure, format_title
from airflow_provider_rivet.operators import RivetApplyOperator, RivetBaseOperator, RivetConfigRefused, RivetRunOperator
from airflow_provider_rivet.preflight import PreflightRefusal


class _TaskLog:
    """The task log as the operator writes it: each line of `logging`, formatted."""

    def __init__(self) -> None:
        self.lines: list[str] = []

    def record(self, fmt: str, *args: Any) -> None:
        """One line of the task log."""
        self.lines.append(fmt % args if args else fmt)

    debug = info = warning = error = record


def _raising(exc: Exception) -> Callable[..., Any]:
    """An `invoke` that raises before any rivet process starts."""

    def invoke(*_: Any) -> Any:
        raise exc

    return invoke


@pytest.mark.parametrize("code", PACKAGE_CODES)
def test_a_refusal_before_rivet_is_on_every_surface(rig: Rig, monkeypatch: pytest.MonkeyPatch, code: str) -> None:
    """A refusal made before rivet starts: a final exception, the XCom payload, a task-log block and the Slack text."""
    refusal = PreflightRefusal(code, "refused for the surface test")
    lines = _TaskLog()
    monkeypatch.setattr(operators, "invoke", _raising(refusal))
    monkeypatch.setattr(RivetBaseOperator, "log", property(lambda self: lines))
    exc, ti = rig.run(rig.operator(RivetApplyOperator, export="orders"))
    assert type(exc) is AirflowFailException and str(exc) == str(refusal)
    payload = ti.store[("task", "return_value")]
    assert payload == {"command": "apply", "decision": "fail", "preflight_refusal": code, "units": []}
    assert any(f"REFUSED [{code}]" in line for line in lines.lines)
    text = format_failure(payload, dag_id=ti.dag_id, task_id=ti.task_id, run_id=ti.run_id)
    assert f"refused before rivet started: `{code}`" in text


@pytest.mark.parametrize("code", PACKAGE_CODES)
def test_a_config_refused_dag_publishes_its_refusal_and_fails_without_retry(code: str) -> None:
    """A config no DAG can be built from: its one task fails without retry, publishes the code and reads on Slack."""
    refusal = PreflightRefusal(code, "refused for the surface test")
    ti = FakeTI(task_id="config", try_number=1, max_tries=0)
    task = RivetConfigRefused(task_id="config", refusal=str(refusal), code=code, retries=0)
    with pytest.raises(AirflowFailException, match="refused for the surface test") as exc:
        task.execute({"ti": ti, "run_id": ti.run_id})
    assert str(exc.value) == str(refusal)
    payload = ti.store[("config", "return_value")]
    assert payload["preflight_refusal"] == code and payload["decision"] == "fail"
    text = format_failure(payload, dag_id="dag", task_id="config", run_id=ti.run_id)
    assert f"refused before rivet started: `{code}`" in text


def test_a_source_timeout_is_retried_and_says_so_on_every_surface(rig: Rig, monkeypatch: pytest.MonkeyPatch) -> None:
    """A rivet timeout (`RIVET_SOURCE_TIMEOUT`, retryable): a retry exception, the XCom error, the log line and the title."""
    line = {
        "error": "source did not answer",
        "exit_class": 2,
        "code": "RIVET_SOURCE_TIMEOUT",
        "class": "retryable",
        "retryable": True,
    }
    rig.scenario(run={"exit": 2, "line": line})
    lines = _TaskLog()
    monkeypatch.setattr(RivetBaseOperator, "log", property(lambda self: lines))
    exc, ti = rig.run(rig.operator(RivetRunOperator, export="orders"), FakeTI(try_number=1, max_tries=2))
    assert type(exc) is AirflowException
    payload = ti.store[("task", "return_value")]
    assert payload["decision"] == "retry" and payload["error"]["code"] == "RIVET_SOURCE_TIMEOUT"
    assert any("RIVET_SOURCE_TIMEOUT" in line and "retryable" in line for line in lines.lines)
    title = format_title(payload, dag_id="dag", task_id="task", try_number=1, max_tries=2)
    assert title == "dag.task — RIVET_SOURCE_TIMEOUT (retryable)"


def test_an_empty_source_is_a_success_on_every_surface(rig: Rig, monkeypatch: pytest.MonkeyPatch) -> None:
    """A source that delivers no rows: a success with no exception, no error line in the log, and a success payload."""
    entry = {"export_name": "orders", "status": "success", "run_id": "orders_1", "rows": 0, "files": 0, "mode": "full"}
    rig.scenario(apply={"summary": {"per_export": [{**entry, "error": None, "stop_reason": None, "tables": None}]}})
    lines = _TaskLog()
    monkeypatch.setattr(RivetBaseOperator, "log", property(lambda self: lines))
    payload, _ = rig.run(rig.operator(RivetApplyOperator, export="orders"))
    assert isinstance(payload, dict) and payload["decision"] == "success"
    assert payload["units"][0]["status"] == "success" and payload["units"][0]["rows"] == 0
    assert not any(line.startswith(("ERROR", "WARNING")) for line in lines.lines)
