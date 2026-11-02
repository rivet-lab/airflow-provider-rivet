"""A `-p` / `--param` value is a secret until proven otherwise: no log record or task log line carries it."""

from __future__ import annotations

import json
import logging

import pytest
from conftest import FakeTI, Rig

from airflow_provider_rivet._yaml import redacted_argv
from airflow_provider_rivet.operators import RivetRunOperator

PARAMS = ["-p", "DB_PASSWORD=hunter2-secret-value", "--param=TOKEN=tok-secret-xyz", "-pSEC=sec-inline-val", "-p=EQ=eq-secret"]
SECRETS = ("hunter2-secret-value", "tok-secret-xyz", "sec-inline-val", "eq-secret")


def test_a_param_value_reaches_rivet_but_no_log_record_or_task_log_line(
    rig: Rig, caplog: pytest.LogCaptureFixture, capfd: pytest.CaptureFixture
) -> None:
    caplog.set_level(logging.DEBUG)
    payload, ti = rig.run(rig.operator(RivetRunOperator, export="orders", extra_args=PARAMS), FakeTI())
    assert payload["decision"] == "success"
    assert rig.argv("run")[-len(PARAMS) - 3 : -3] == PARAMS or all(p in rig.argv("run") for p in PARAMS), (
        "rivet itself gets the real values"
    )
    seen = json.dumps(rig.records) + json.dumps(ti.store, default=str) + caplog.text
    captured = capfd.readouterr()
    seen += captured.out + captured.err
    for secret in SECRETS:
        assert secret not in seen, f"{secret!r} reached a log record, the task log or the process output"
    starts = [r for r in rig.records if r.get("event") == "rivet.start"]
    assert starts and "DB_PASSWORD=***" in starts[0]["argv"] and "--param=TOKEN=***" in starts[0]["argv"], "the keys stay"


def test_redacted_argv_masks_every_spelling_of_a_param_value_and_keeps_the_key() -> None:
    """redacted_argv: each `-p` / `--param` spelling keeps its key and masks its value; other arguments stay."""
    args = ["run", "--config", "c.yaml", "-p", "A=1", "--param", "B=2=3", "-pC=4", "--param=D=5", "-p=E=6", "--json-errors"]
    assert redacted_argv(args) == [
        "run",
        "--config",
        "c.yaml",
        "-p",
        "A=***",
        "--param",
        "B=***",
        "-pC=***",
        "--param=D=***",
        "-p=E=***",
        "--json-errors",
    ]
    assert redacted_argv(["-p", "NOEQ"]) == ["-p", "***"], "a value with no key is masked whole"
    assert redacted_argv(["-p", ""]) == ["-p", ""] and redacted_argv(["-p"]) == ["-p"], "nothing to mask"
    assert redacted_argv(["-p", "", "x"]) == ["-p", "", "x"], "an empty value is not masked, whatever comes after it"
    assert args[4] == "A=1", "the input list is not changed"
