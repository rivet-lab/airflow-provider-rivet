"""The Slack title says `retryable` exactly when the task log says Airflow runs another try, for any failure rivet reports."""

from __future__ import annotations

from typing import Any

from hypothesis import given
from hypothesis import strategies as st

from airflow_provider_rivet.callbacks import format_title
from airflow_provider_rivet.tasklog import TaskLog

ERROR = st.fixed_dictionaries(
    {"class": st.sampled_from(["retryable", "generic", "crashed", "refusal"]), "retryable": st.booleans()},
    optional={"code": st.sampled_from(["RIVET_SOURCE_X", "RIVET_BUSY_LOCK"])},
)


def _payload(error: dict[str, Any], decision: str, crashed_retry: bool) -> dict[str, Any]:
    """The XCom payload of a failed try: the error on the process and on its one failed unit."""
    payload: dict[str, Any] = {
        "decision": decision,
        "error": error,
        "exit_status": 137 if decision == "retry_crashed" else 2,
        "units": [{"export": "orders", "table": None, "status": "failed", "error": error}],
    }
    if decision == "retry_crashed":
        payload["crashed_retry"] = crashed_retry
    return payload


@given(
    ERROR,
    st.sampled_from(["retry", "fail", "retry_crashed"]),
    st.booleans(),
    st.integers(min_value=1, max_value=4),
    st.integers(min_value=0, max_value=3),
)
def test_the_slack_title_says_retryable_exactly_when_airflow_runs_another_try(
    error: dict[str, Any], decision: str, crashed_retry: bool, try_number: int, max_tries: int
) -> None:
    """The title's word and the log's `Airflow retries it` agree for every decision, budget and try."""
    payload = _payload(error, decision, crashed_retry)
    title = format_title(payload, dag_id="d", task_id="t", try_number=try_number, max_tries=max_tries)
    written: list[tuple[int, str]] = []
    TaskLog("run", "orders", None, lambda level, text: written.append((level, text))).finish(
        payload, try_number, max_tries, False
    )
    log_says_retry = any("Airflow retries it" in text for _, text in written)
    assert ("(retryable)" in title) == log_says_retry, (title, written)
