"""Failure alerting through Airflow: a one-line Slack title per failed task, and its error text in a thread.

With a bot token (`SlackHook`, a `slack` Connection) the channel gets the title, the run id and the task-log link; the
error text (rivet's error line of the failed step, else the exception's message; redacted, capped) is a reply in that
message's thread. An incoming webhook (`SlackWebhookHook`) cannot thread: it posts title and error text as one message.
"""

from __future__ import annotations

import logging
from collections.abc import Callable, Mapping
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from .operators import XCOM_KEY
from .reading import _json_of
from .result import REFUSED, airflow_retries, redact
from .stderr import tail_lines

# the thread's code block; a webhook message carries title and link too, and stays under Slack's 4000
THREAD_TEXT_LIMIT = 3500
WEBHOOK_TEXT_LIMIT = 3000
# a child of Airflow's task logger: the line lands in the task log on Airflow 2 and 3 alike
log = logging.getLogger("airflow.task.rivet_slack")


def _failed_units(payload: Mapping[str, Any]) -> list[Mapping[str, Any]]:
    """The units that carry an error; the process-level error when no unit does."""
    failed = [u for u in payload.get("units") or [] if u.get("error")]
    if not failed and payload.get("error"):
        failed = [{"error": payload["error"]}]  # its name is read with `.get`: `(process)`, no table
    return failed


def format_title(
    payload: Mapping[str, Any] | None,
    *,
    dag_id: str,
    task_id: str,
    try_number: int | None = None,
    max_tries: int | None = None,
    exception: Any = None,
) -> str:
    """`dag.task — <error type>`: the rivet code and final / retryable, else the exception's class; no error text.
    Without `try_number` and `max_tries` (a 0.3.0 call), retryable is the error's own flag."""
    payload = payload or {}
    if payload.get(REFUSED):
        kind = f"{payload[REFUSED]} (final)"
    elif failed := _failed_units(payload):
        error = payload.get("error") or failed[0]["error"]  # the error that decided the task, else the first failed unit's
        name = error.get("code") or error.get("class") or "rivet failed"
        retry = (
            error.get("retryable") if try_number is None or max_tries is None else airflow_retries(payload, try_number, max_tries)
        )
        kind = f"{name} ({'retryable' if retry else 'final'})"
        if len(failed) > 1:
            kind += f" +{len(failed) - 1} more"
    elif isinstance(exception, BaseException):
        kind = type(exception).__name__
    else:
        kind = "failed, no rivet result"
    return f"{dag_id}.{task_id} — {kind}"


def _rivet_errors(payload: Mapping[str, Any]) -> list[tuple[str | None, str]]:
    """rivet's error lines in the failed step's stderr file as (code, text): its `--json-errors` objects, else `Error:` lines."""
    path = payload.get("stderr_path")
    if not path:
        return []
    try:
        lines, _truncated = tail_lines(Path(path))
    except OSError:
        return []
    found: list[tuple[str | None, str]] = []
    plain: list[tuple[str | None, str]] = []
    for line in lines:
        obj = _json_of(line)
        if obj is None:
            if line.lstrip().lower().startswith("error:"):
                plain.append((None, line.strip()))
            continue
        if isinstance(obj, dict):
            text = obj.get("error") if isinstance(obj.get("error"), str) else obj.get("message")
            if isinstance(text, str) and text:
                code = obj.get("code")
                found.append((code if isinstance(code, str) and code and code not in text else None, text))
    return found or plain  # the human `Error:` line repeats the object when both are printed


def _actions(payload: Mapping[str, Any]) -> list[str]:
    """Each failed unit's remedy as an `action:` line, once each, in unit order."""
    return list(dict.fromkeys(f"action: {u['error']['action']}" for u in _failed_units(payload) if u["error"].get("action")))


def format_error_text(payload: Mapping[str, Any] | None, *, exception: Any = None, limit: int, log_url: str = "") -> str:
    """The thread's text: the remedies, then rivet's error line(s) or the exception's message; redacted, capped."""
    payload = payload or {}
    errors = [f"[{code}] {text}" if code else text for code, text in _rivet_errors(payload)]
    if not errors and exception is not None:
        errors = [str(exception) or type(exception).__name__]
    text = redact(
        "\n".join([*_actions(payload), *errors]) or "no error text: the task failed without a rivet result or an exception"
    )
    if len(text) <= limit:
        return text
    tail = f"\n… truncated, full log: {log_url or 'the task log'}"
    return text[: max(limit - len(tail), 0)] + tail


def _mrkdwn(text: str) -> str:
    """Slack's three control characters escaped, and no fence the text could close early."""
    return text.replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;").replace("```", "'''")


def _code_block(text: str) -> str:
    return f"```\n{_mrkdwn(text)}\n```"


@dataclass(frozen=True)
class _Alert:
    """One failed task instance, ready to post."""

    dag_id: str
    task: str  # task id, `[map_index]` added for a mapped one
    run_id: str
    log_url: str
    payload: Mapping[str, Any] | None
    exception: Any
    try_number: int
    max_tries: int

    @property
    def title(self) -> str:
        return redact(
            format_title(
                self.payload,
                dag_id=self.dag_id,
                task_id=self.task,
                try_number=self.try_number,
                max_tries=self.max_tries,
                exception=self.exception,
            )
        )

    @property
    def context_line(self) -> str:
        link = f" · <{self.log_url}|task log>" if self.log_url else ""
        return f"run `{_mrkdwn(self.run_id)}`{link}"

    def error_text(self, limit: int) -> str:
        return format_error_text(self.payload, exception=self.exception, limit=limit, log_url=self.log_url)


def slack_failure_callback(
    conn_id: str = "slack_default", *, channel: str | None = None, **hook_kwargs: Any
) -> Callable[[Any], None]:
    """An `on_failure_callback` that posts a failed rivet task to Slack.

    With `channel` (a channel id, `C…`): `conn_id` is a `slack` Connection holding a bot token (scope `chat:write`);
    the title goes to the channel and the error text into its thread. Without it: `conn_id` is a `slackwebhook`
    Connection and title and error text go out as one message.
    """

    def notify(context: Any) -> None:
        """Format this task instance's rivet result and send it."""
        ti = context["ti"]
        map_index = getattr(ti, "map_index", -1)
        try:
            # this index's own payload: without `map_indexes` a mapped task's pull returns every index's (D11)
            payload = ti.xcom_pull(task_ids=ti.task_id, key=XCOM_KEY, map_indexes=map_index)
        except Exception:
            payload = None  # pragma: no mutate - any non-dict here is dropped two lines below, `""` alike
        log_url = getattr(ti, "log_url", "") or ""  # pragma: no mutate - a `None` default is caught by `or ""`
        alert = _Alert(
            dag_id=ti.dag_id,
            task=ti.task_id + (f"[{map_index}]" if isinstance(map_index, int) and map_index >= 0 else ""),
            run_id=str(context.get("run_id") or getattr(ti, "run_id", "")),
            log_url=log_url,
            payload=payload if isinstance(payload, dict) else None,
            exception=context.get("exception"),
            try_number=ti.try_number,
            max_tries=ti.max_tries,
        )
        if channel:
            _post_threaded(alert, conn_id, channel, hook_kwargs)
        else:
            _post_webhook(alert, conn_id, hook_kwargs)

    return notify


def _post_webhook(alert: _Alert, conn_id: str, hook_kwargs: Mapping[str, Any]) -> None:
    """Title, context and the error text as one message: a webhook returns no `ts` to thread under."""
    from airflow.providers.slack.hooks.slack_webhook import SlackWebhookHook

    text = f":x: *{_mrkdwn(alert.title)}*\n{alert.context_line}\n{_code_block(alert.error_text(WEBHOOK_TEXT_LIMIT))}"
    # the hook raises on any answer but 200; the line says the alert was posted, and what Slack answered
    response = SlackWebhookHook(slack_webhook_conn_id=conn_id, **hook_kwargs).send(text=text)
    status = getattr(response, "status_code", None)
    answer = f"HTTP {status} {getattr(response, 'body', '')}".rstrip() if status is not None else "no response object"
    log.info("Slack alert for %s.%s sent through connection `%s`: %s", alert.dag_id, alert.task, conn_id, answer)


def _post_threaded(alert: _Alert, conn_id: str, channel: str, hook_kwargs: Mapping[str, Any]) -> None:
    """The title to the channel, then the error text as a code block in its thread."""
    from airflow.providers.slack.hooks.slack import SlackHook

    client = SlackHook(slack_conn_id=conn_id, **hook_kwargs).client
    title = alert.title
    blocks = [
        {"type": "section", "text": {"type": "mrkdwn", "text": f":x: *{_mrkdwn(title)}*"}},
        {"type": "context", "elements": [{"type": "mrkdwn", "text": alert.context_line}]},
    ]
    # slack_sdk raises SlackApiError on any `ok: false`: reaching the next line means Slack accepted the message
    top = client.chat_postMessage(channel=channel, text=title, blocks=blocks)
    ts, where = top["ts"], top.get("channel") or channel
    client.chat_postMessage(channel=where, thread_ts=ts, text=_code_block(alert.error_text(THREAD_TEXT_LIMIT)))
    log.info(
        "Slack alert for %s.%s posted to channel %s (ts %s) through connection `%s`, its error text in the thread",
        alert.dag_id,
        alert.task,
        where,
        ts,
        conn_id,
    )
