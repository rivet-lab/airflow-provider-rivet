"""Slack alerts: the title, the error text, how rivet's error line is read from the stderr file, the cap, and the task instance, hook and response the callback reads."""

from __future__ import annotations

import json
import logging
import sys
import types
from pathlib import Path
from typing import Any

import pytest

from airflow_provider_rivet import callbacks
from airflow_provider_rivet.callbacks import format_error_text, format_title, slack_failure_callback
from airflow_provider_rivet.stderr import STDERR_TAIL


def err(code: str | None = "RIVET_X", cls: str = "refusal", retryable: bool = False, action: str | None = None) -> dict:
    return {"code": code, "kind": None, "class": cls, "exit_code": 5, "retryable": retryable, "action": action}


def failed(export: str | None, error: dict, table: str | None = None) -> dict:
    return {"export": export, "table": table, "status": "failed", "error": error}


def test_the_title_counts_the_other_failed_units_and_falls_back_to_the_class() -> None:
    """x_format_title: the first failed unit's code, else its class, else `rivet failed`; `+N more` for the rest."""
    three = {
        "decision": "retry_crashed",
        "crashed_retry": True,
        "units": [failed("a", err(None, "crashed", True)), failed("b", err()), failed("c", err())],
    }
    assert format_title(three, dag_id="d", task_id="t", try_number=1, max_tries=1) == "d.t — crashed (retryable) +2 more"
    bare = {"units": [failed("a", {"code": None, "class": None, "retryable": False})]}
    assert format_title(bare, dag_id="d", task_id="t", try_number=1, max_tries=1) == "d.t — rivet failed (final)"
    assert (
        format_title({}, dag_id="d", task_id="t", try_number=1, max_tries=1, exception="not an exception")
        == "d.t — failed, no rivet result"
    )


@pytest.mark.parametrize(
    ("decision", "crashed_retry", "try_number", "word"),
    [
        ("retry", None, 1, "retryable"),
        ("retry", None, 2, "final"),
        ("fail", None, 1, "final"),
        ("retry_crashed", True, 1, "retryable"),
        ("retry_crashed", True, 2, "final"),
        ("retry_crashed", False, 1, "final"),
    ],
)
def test_the_title_says_retryable_only_when_airflow_runs_another_try(
    decision: str, crashed_retry: bool | None, try_number: int, word: str
) -> None:
    """message-truth-1: the title's retry word is the decision Airflow acts on, as the task log's is (max_tries 1)."""
    payload = {"decision": decision, "units": [failed("a", err(None, "crashed", True))]}
    if crashed_retry is not None:
        payload["crashed_retry"] = crashed_retry
    assert format_title(payload, dag_id="d", task_id="t", try_number=try_number, max_tries=1) == f"d.t — crashed ({word})"


@pytest.mark.parametrize(("retryable", "word"), [(True, "retryable"), (False, "final")])
def test_a_title_asked_as_0_3_0_asked_it_takes_the_error_s_own_retry_flag(retryable: bool, word: str) -> None:
    payload = {"decision": "fail", "units": [failed("a", err("RIVET_X", "transient", retryable))]}
    assert format_title(payload, dag_id="d", task_id="t") == f"d.t — RIVET_X ({word})"
    assert format_title(payload, dag_id="d", task_id="t", try_number=1) == f"d.t — RIVET_X ({word})"


# --- rivet's error line from the stderr file ------------------------------------------------------------------------


def stderr(tmp_path: Path, *lines: str | bytes) -> dict:
    path = tmp_path / "step.stderr.log"
    path.write_bytes(b"".join((line if isinstance(line, bytes) else line.encode()) + b"\n" for line in lines))
    return {"stderr_path": str(path)}


def test_the_error_object_gives_its_text_with_its_code_once(tmp_path: Path) -> None:
    """x__rivet_errors: `error`, else `message`; the code is prefixed unless the text already holds it, or it is not a
    non-empty string; an object without text, a JSON value that is not an object, and other lines are skipped."""
    payload = stderr(
        tmp_path,
        "warning: single-host state",
        json.dumps({"error": "plain", "code": "RIVET_A"}),
        json.dumps({"error": "[RIVET_B] already coded", "code": "RIVET_B"}),
        json.dumps({"message": "from message", "code": ""}),
        json.dumps({"error": 7, "message": "error is not text", "code": 9}),
        json.dumps({"error": "", "code": "RIVET_EMPTY"}),
        json.dumps({"code": "RIVET_NO_TEXT"}),
        json.dumps(["Error: a list is not an object"]),
        "Error: the human line repeats the object",
    )
    assert format_error_text(payload, limit=3500).split("\n") == [
        "[RIVET_A] plain",
        "[RIVET_B] already coded",
        "from message",
        "error is not text",
    ]


def test_without_an_object_the_error_lines_are_taken_whatever_their_case_and_indent(tmp_path: Path) -> None:
    payload = stderr(tmp_path, "  ERROR: upper and indented  ", "Errors: not an error line", "error:lower", "note: x")
    assert format_error_text(payload, limit=3500) == "ERROR: upper and indented\nerror:lower"


def test_only_the_end_of_a_long_stderr_file_is_read_and_bad_bytes_do_not_stop_it(tmp_path: Path) -> None:
    """An error line before the last STDERR_TAIL bytes is not read; invalid UTF-8 is replaced, not raised."""
    early = "Error: written before the tail"
    filler = ["x" * 1000] * (STDERR_TAIL // 1000 + 1)
    payload = stderr(tmp_path, early, *filler, b"Error: bad byte \xff here")
    assert format_error_text(payload, limit=3500) == "Error: bad byte � here"
    short = stderr(tmp_path, early, "Error: last")
    assert format_error_text(short, limit=3500) == f"{early}\nError: last"


def test_an_unreadable_stderr_path_falls_back_to_the_exception(tmp_path: Path) -> None:
    assert format_error_text({"stderr_path": str(tmp_path)}, exception=ValueError("boom"), limit=3500) == "boom"
    assert format_error_text({"stderr_path": ""}, exception=ValueError("boom"), limit=3500) == "boom"


def test_the_fallback_texts_name_what_there_is(tmp_path: Path) -> None:
    """format_error_text: an exception without a message gives its type; with neither, the sentence says so."""
    assert format_error_text(None, exception=KeyError(), limit=3500) == "KeyError"
    assert format_error_text({}, limit=3500) == "no error text: the task failed without a rivet result or an exception"


def test_the_cap_keeps_a_text_of_exactly_the_limit_and_never_goes_negative() -> None:
    exact = "y" * 50
    assert format_error_text(None, exception=RuntimeError(exact), limit=50) == exact
    cut = format_error_text(None, exception=RuntimeError("y" * 51), limit=50)
    assert cut == "y" * 14 + "\n… truncated, full log: the task log" and len(cut) == 50
    assert format_error_text(None, exception=RuntimeError("y" * 51), limit=5) == "\n… truncated, full log: the task log"


def test_slack_control_characters_are_escaped_and_a_fence_cannot_close_the_block() -> None:
    assert callbacks._code_block("a<b>&```c") == "```\na&lt;b&gt;&amp;'''c\n```"


# --- the callback: task instance, hooks, response -------------------------------------------------------------------


class TI:
    """A task instance with what the callback reads; `pull` decides what `xcom_pull` does."""

    def __init__(self, pull: Any, map_index: Any = -1, log_url: Any = "http://af/l", run_id: str = "ti_run") -> None:
        self.dag_id, self.task_id, self.map_index, self.log_url, self.run_id = "d", "apply.users", map_index, log_url, run_id
        self.try_number, self.max_tries = 1, 1
        self.pull, self.pulled = pull, []

    def xcom_pull(self, **kwargs: Any) -> Any:
        self.pulled.append(kwargs)
        if isinstance(self.pull, BaseException):
            raise self.pull
        return self.pull


def hooks(monkeypatch: pytest.MonkeyPatch, response: Any = None, top: dict | None = None) -> list:
    """Both Slack hooks replaced; every construction and call recorded in order."""
    seen: list = []

    class Client:
        def chat_postMessage(self, **kwargs: Any) -> dict:
            seen.append(("post", kwargs))
            return top if top is not None else {"ok": True, "channel": "C9", "ts": "1.1"}

    class SlackHook:
        def __init__(self, **kwargs: Any) -> None:
            seen.append(("bot", kwargs))
            self.client = Client()

    class SlackWebhookHook:
        def __init__(self, **kwargs: Any) -> None:
            seen.append(("webhook", kwargs))

        def send(self, **kwargs: Any) -> Any:
            seen.append(("send", kwargs))
            return response

    for name, hook in (
        ("airflow.providers.slack.hooks.slack", SlackHook),
        ("airflow.providers.slack.hooks.slack_webhook", SlackWebhookHook),
    ):
        module = types.ModuleType(name)
        setattr(module, hook.__name__, hook)
        monkeypatch.setitem(sys.modules, name, module)
    return seen


PAYLOAD = {"units": [failed("users", err("RIVET_A"))]}


def test_a_mapped_task_pulls_its_own_index_and_names_it(monkeypatch: pytest.MonkeyPatch) -> None:
    """x_slack_failure_callback: `map_indexes` is the task's own index and the title names it; an unmapped one is not
    suffixed; the run id comes from the context, else the task instance."""
    seen = hooks(monkeypatch, response=types.SimpleNamespace(status_code=200, body="ok"))
    ti = TI(PAYLOAD, map_index=3)
    slack_failure_callback("hook_conn")({"ti": ti, "run_id": "ctx_run"})
    assert ti.pulled == [{"task_ids": "apply.users", "key": "return_value", "map_indexes": 3}]
    text = seen[1][1]["text"]
    assert text.startswith(":x: *d.apply.users[3] — RIVET_A (final)*\nrun `ctx_run` · <http://af/l|task log>\n")
    seen.clear()
    slack_failure_callback("hook_conn")({"ti": TI(PAYLOAD, map_index=0), "run_id": None})
    assert seen[1][1]["text"].startswith(":x: *d.apply.users[0] — RIVET_A (final)*\nrun `ti_run` ·")
    seen.clear()
    slack_failure_callback("hook_conn")({"ti": TI(PAYLOAD, map_index=None)})
    assert seen[1][1]["text"].startswith(":x: *d.apply.users — RIVET_A (final)*\nrun `ti_run` ·")


@pytest.mark.parametrize("pull", [RuntimeError("xcom backend down"), ["not", "a", "dict"]])
def test_an_unreadable_or_foreign_payload_still_alerts_with_the_exception(monkeypatch: pytest.MonkeyPatch, pull: Any) -> None:
    seen = hooks(monkeypatch, response=types.SimpleNamespace(status_code=200, body="ok"))
    slack_failure_callback("hook_conn")({"ti": TI(pull), "run_id": "r", "exception": TimeoutError("slow")})
    assert seen[1][1]["text"].split("\n") == [
        ":x: *d.apply.users — TimeoutError*",
        "run `r` · <http://af/l|task log>",
        "```",
        "slow",
        "```",
    ]


def test_a_task_instance_without_a_log_url_sends_no_link(monkeypatch: pytest.MonkeyPatch) -> None:
    seen = hooks(monkeypatch, response=types.SimpleNamespace(status_code=200, body="ok"))
    slack_failure_callback("hook_conn")({"ti": TI(PAYLOAD, log_url=None), "run_id": "r"})
    assert seen[1][1]["text"].split("\n")[1] == "run `r`"


def test_the_webhook_hook_gets_the_connection_and_the_extra_arguments(
    monkeypatch: pytest.MonkeyPatch, caplog: pytest.LogCaptureFixture
) -> None:
    """_post_webhook: the hook's connection and kwargs; the log line names the answer, or says there was none."""
    caplog.set_level(logging.INFO)
    seen = hooks(monkeypatch, response=None)
    slack_failure_callback("hook_conn", timeout=7)({"ti": TI(PAYLOAD), "run_id": "r"})
    assert seen[0] == ("webhook", {"slack_webhook_conn_id": "hook_conn", "timeout": 7})
    assert [name for name, _ in seen] == ["webhook", "send"] and set(seen[1][1]) == {"text"}
    seen = hooks(monkeypatch, response=types.SimpleNamespace(status_code=204, body=""))
    slack_failure_callback()({"ti": TI(PAYLOAD), "run_id": "r"})
    assert seen[0] == ("webhook", {"slack_webhook_conn_id": "slack_default"})
    assert [r.getMessage() for r in caplog.records if r.name == "airflow.task.rivet_slack"] == [
        "Slack alert for d.apply.users sent through connection `hook_conn`: no response object",
        "Slack alert for d.apply.users sent through connection `slack_default`: HTTP 204",
    ]


def test_the_webhook_message_is_capped_below_slacks_limit(monkeypatch: pytest.MonkeyPatch) -> None:
    seen = hooks(monkeypatch, response=None)
    slack_failure_callback("hook_conn")({"ti": TI(PAYLOAD), "run_id": "r", "exception": RuntimeError("z" * 9000)})
    block = seen[1][1]["text"].split("```\n", 1)[1]
    assert len(block) == len("\n```") + callbacks.WEBHOOK_TEXT_LIMIT and block.endswith("full log: http://af/l\n```")


def test_the_bot_threads_under_the_channel_slack_answered_and_passes_the_extra_arguments(
    monkeypatch: pytest.MonkeyPatch, caplog: pytest.LogCaptureFixture
) -> None:
    """_post_threaded: the hook's connection and kwargs; the reply goes to the channel Slack named (its id, not the
    name the DAG gave), else the one asked for; the thread text is capped at THREAD_TEXT_LIMIT."""
    caplog.set_level(logging.INFO)
    seen = hooks(monkeypatch, top={"ok": True, "channel": "C777", "ts": "9.9"})
    slack_failure_callback("bot_conn", channel="#alerts", timeout=3)(
        {"ti": TI(PAYLOAD), "run_id": "r", "exception": RuntimeError("q" * 9000)}
    )
    assert seen[0] == ("bot", {"slack_conn_id": "bot_conn", "timeout": 3})
    (_, top), (_, reply) = seen[1], seen[2]
    assert top["channel"] == "#alerts" and (reply["channel"], reply["thread_ts"]) == ("C777", "9.9")
    assert len(reply["text"]) == len("```\n\n```") + callbacks.THREAD_TEXT_LIMIT
    seen = hooks(monkeypatch, top={"ok": True, "ts": "8.8"})
    slack_failure_callback("bot_conn", channel="C1")({"ti": TI(PAYLOAD), "run_id": "r"})
    assert (seen[2][1]["channel"], seen[2][1]["thread_ts"]) == ("C1", "8.8")
    assert [r.getMessage() for r in caplog.records if r.name == "airflow.task.rivet_slack"][-1] == (
        "Slack alert for d.apply.users posted to channel C1 (ts 8.8) through connection `bot_conn`, its error text in the thread"
    )


class BareTI:
    """A task instance with no `map_index`, `run_id` or `log_url` attribute at all (a custom or older runtime)."""

    dag_id, task_id = "d", "apply.users"
    try_number, max_tries = 1, 1

    def __init__(self) -> None:
        self.pulled: list = []

    def xcom_pull(self, **kwargs: Any) -> Any:
        self.pulled.append(kwargs)
        return PAYLOAD


def test_a_task_instance_without_the_optional_attributes_alerts_unmapped_with_no_run_and_no_link(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """x_slack_failure_callback 9/12/15/16 (map_index default -1: its own index, no suffix), 52/55/58 (run_id default
    empty), 65/68 (log_url default empty): the alert still goes out and says only what it knows."""
    seen = hooks(monkeypatch, response=types.SimpleNamespace(status_code=200, body="ok"))
    ti = BareTI()
    slack_failure_callback("hook_conn")({"ti": ti})
    assert ti.pulled == [{"task_ids": "apply.users", "key": "return_value", "map_indexes": -1}]
    assert seen[1][1]["text"].split("\n")[:2] == [":x: *d.apply.users — RIVET_A (final)*", "run ``"]


def test_a_webhook_answer_without_a_body_is_logged_by_its_status(
    monkeypatch: pytest.MonkeyPatch, caplog: pytest.LogCaptureFixture
) -> None:
    """_post_webhook 24/27/30: a response with a status and no `body` reads `HTTP 200`, nothing after it."""
    caplog.set_level(logging.INFO)
    hooks(monkeypatch, response=types.SimpleNamespace(status_code=200))
    slack_failure_callback("hook_conn")({"ti": TI(PAYLOAD), "run_id": "r"})
    assert [r.getMessage() for r in caplog.records if r.name == "airflow.task.rivet_slack"] == [
        "Slack alert for d.apply.users sent through connection `hook_conn`: HTTP 200"
    ]


def test_the_tail_starts_exactly_stderr_tail_bytes_before_the_end(tmp_path: Path) -> None:
    """The window starts exactly STDERR_TAIL bytes before the end: the line there is whole, and the earlier error line
    is not read."""
    head = b"Error: before the tail\n" + b"x" * 76 + b"\n"
    line = b"Error: first line of the tail\n"
    body = line + b"y" * (STDERR_TAIL - len(line) - 1) + b"\n"
    path = tmp_path / "exact.stderr.log"
    path.write_bytes(head + body)
    assert len(body) == STDERR_TAIL
    assert format_error_text({"stderr_path": str(path)}, limit=3500) == "Error: first line of the tail"
