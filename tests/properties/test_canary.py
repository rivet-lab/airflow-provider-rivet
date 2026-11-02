"""A secret canary in every credential-bearing input reaches no log record, exception, Slack text or XCom value."""

from __future__ import annotations

import logging
from types import SimpleNamespace
from typing import Any

import pytest
from conftest import FakeTI, Rig

from airflow_provider_rivet.callbacks import format_error_text, format_failure, format_title
from airflow_provider_rivet.connections import connection_url
from airflow_provider_rivet.operators import RivetRunOperator

CANARY = "c4n4ry-s3cr3t-7f1e9a"
URL = connection_url(
    SimpleNamespace(
        conn_id="pg",
        conn_type="postgres",
        host="db.internal",
        login="rivet",
        password=CANARY,
        port=5432,
        schema="app",
        extra_dejson={},
    )
)
PARAMS = ["-p", f"TOKEN={CANARY}", f"--param=KEY={CANARY}"]
SCENARIOS = {
    "success": {},
    "retryable": {"run": {"exit": 2, "line": {"error": "connection reset by peer", "exit_class": 2}}},
    "final": {"run": {"exit": 5, "line": {"error": "bad export", "exit_class": 5, "code": "RIVET_SOURCE_X"}}},
    "crashed": {"run": {"exit": 137}},
    "rivet echoes its url": {"run": {"exit": 2, "line": {"error": "auth failed", "exit_class": 2}}, "leak_env": ["RIVET_PG_URL"]},
}


@pytest.mark.parametrize("spec", list(SCENARIOS.values()), ids=list(SCENARIOS))
def test_the_canary_reaches_no_text_the_provider_emits(
    rig: Rig, spec: dict[str, Any], caplog: pytest.LogCaptureFixture, capfd: pytest.CaptureFixture
) -> None:
    """Whatever rivet does with the canary in the URL and the extra arguments, every emitted text is free of it."""
    caplog.set_level(logging.DEBUG)
    rig.scenario(**spec)
    op = rig.operator(RivetRunOperator, export="orders", extra_args=PARAMS, env={"RIVET_PG_URL": URL})
    result, ti = rig.run(op, FakeTI())
    emitted = [repr(rig.records), repr(ti.store), str(result), caplog.text]
    captured = capfd.readouterr()
    emitted += [captured.out, captured.err]
    if isinstance(result, dict):
        emitted += [
            format_title(result, dag_id="dag", task_id="task", try_number=1, max_tries=2),
            format_failure(result, dag_id="dag", task_id="task", run_id="run"),
            format_error_text(result, limit=3500),
        ]
    for text in emitted:
        assert CANARY not in text
