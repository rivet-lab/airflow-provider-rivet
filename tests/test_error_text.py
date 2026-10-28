"""rivet's error text where an operator reads it (see #100): the one-line, redacted, bounded form (`error_text`), the
exception text, the task log's failure lines, and `format_failure`. The text is operator and source text, so hostile."""

from __future__ import annotations

import json
import time
from pathlib import Path
from typing import Any

import pytest

from airflow_provider_rivet.callbacks import format_failure
from airflow_provider_rivet.result import ERROR_TEXT_LIMIT, ErrorObject, error_text

PLANTED = "hunter2-planted-by-a-connection"


@pytest.fixture
def planted() -> str:
    """A value registered with Airflow's secrets masker, as a Connection's password is when a task reads it."""
    try:
        from airflow.sdk.log import mask_secret
    except ImportError:
        from airflow.utils.log.secrets_masker import mask_secret
    mask_secret(PLANTED)
    return PLANTED


# --- error_text -----------------------------------------------------------------------------------------------------


@pytest.mark.parametrize("message", [None, 5, b"bytes", ["a"], "", "   ", "\n\n", " \n \t \n"])
def test_no_text_is_no_text(message: Any) -> None:
    assert error_text(message) is None


def test_lines_are_cut_at_newline_trimmed_and_joined() -> None:
    message = "1 export(s) could not be typed:\n  - export 'orders': no spec\n\n  - export 'users': no spec\n"
    assert error_text(message) == "1 export(s) could not be typed: | - export 'orders': no spec | - export 'users': no spec"


@pytest.mark.parametrize("char", ["\r", "\x0b", "\x0c", "\x1c", "\x1d", "\x1e", "\x85", " ", " "])
def test_only_newline_ends_a_line_and_every_other_separator_is_shown_as_an_escape(char: str) -> None:
    """D01: `str.splitlines` also splits on these; rivet's line end is `\\n` alone, and a log line must stay one line."""
    text = error_text(f"a{char}b")
    assert text == f"a\\u{{{ord(char):02x}}}b", text
    assert len((text or "").splitlines()) == 1


def test_a_terminal_escape_cannot_reach_the_log_as_one() -> None:
    assert error_text("\x1b[31mred\x1b[0m\x00") == "\\u{1b}[31mred\\u{1b}[0m\\u{00}"


def test_a_tab_is_a_space_and_text_beyond_ascii_is_kept() -> None:
    assert error_text("a\tb — Straße 数据") == "a b — Straße 数据"


def test_the_bound_is_the_limit_and_the_cut_says_so() -> None:
    assert error_text("x" * ERROR_TEXT_LIMIT) == "x" * ERROR_TEXT_LIMIT
    cut = error_text("x" * (ERROR_TEXT_LIMIT + 1))
    assert cut == "x" * (ERROR_TEXT_LIMIT - 1) + "…" and len(cut) == ERROR_TEXT_LIMIT
    assert error_text("abcdef", limit=4) == "abc…"


def test_a_credential_is_masked_before_the_cut_so_no_part_of_it_is_left_by_the_cut(planted: str) -> None:
    """The cut falls inside the credential: cutting first would leave `hunt…`, a prefix the masker no longer matches."""
    message = "x" * (ERROR_TEXT_LIMIT - 5) + planted + " tail"
    text = error_text(message)
    assert text is not None and planted not in text and planted[:4] not in text
    assert text.startswith("x" * (ERROR_TEXT_LIMIT - 5) + "***")


def test_a_credential_on_the_first_of_two_lines_is_masked(planted: str) -> None:
    assert error_text(f"the value {planted}\nsecond line") == "the value *** | second line"


def test_a_megabyte_of_hostile_text_is_read_once_and_cheaply(planted: str) -> None:
    started = time.monotonic()
    for hostile in ("\n" * 1_000_000, "a\n" * 500_000, ("\r" * 50 + "\x1b") * 20_000, ("é" * 7 + planted) * 70_000):
        text = error_text(hostile)
        assert text is None or len(text) <= ERROR_TEXT_LIMIT
        assert text is None or planted not in text
    assert time.monotonic() - started < 10


# --- the exception text -----------------------------------------------------------------------------------------------


def test_the_exception_text_is_code_class_action_unit_and_the_one_line_text() -> None:
    error = ErrorObject("RIVET_X", None, "refusal", 5, False, "do the thing", "first\nsecond")
    assert error.describe("orders") == "[RIVET_X] refusal: do the thing (orders): first | second"
    assert error.describe(None) == "[RIVET_X] refusal: do the thing: first | second"


def test_an_error_without_text_reads_as_it_did_before() -> None:
    assert ErrorObject("RIVET_X", None, "refusal", 5, False, None, None).describe("orders") == "[RIVET_X] refusal (orders)"
    assert ErrorObject(None, None, "generic", 1, False, None, " \n").describe(None) == "generic"


# --- format_failure ---------------------------------------------------------------------------------------------------


def stderr_of(tmp_path: Path, *lines: str) -> dict:
    path = tmp_path / "step.stderr.log"
    path.write_bytes(b"".join(line.encode() + b"\n" for line in lines))
    return {"stderr_path": str(path)}


def test_the_failure_message_says_rivets_reason_on_one_line(tmp_path: Path) -> None:
    payload = stderr_of(
        tmp_path, json.dumps({"error": "table 'orders':\nBigQuery job X failed", "code": "RIVET_LOAD_WAREHOUSE_REJECTED"})
    )
    text = format_failure(payload, dag_id="d", task_id="t", run_id="r")
    assert text.split("\n")[:2] == [":x: *d.t* failed (run `r`)", "rivet said: table 'orders': | BigQuery job X failed"]


def test_the_failure_message_masks_the_credential_and_bounds_the_reason(tmp_path: Path, planted: str) -> None:
    payload = stderr_of(tmp_path, json.dumps({"error": f"{planted} " + "y" * 1000}))
    text = format_failure(payload, dag_id="d", task_id="t", run_id="r")
    reason = text.split("\n")[1]
    assert planted not in text and reason.startswith("rivet said: *** yyy") and reason.endswith("…")
    assert len(reason) == len("rivet said: ") + ERROR_TEXT_LIMIT


def test_the_failure_message_without_a_stderr_file_is_what_it_was() -> None:
    assert format_failure({}, dag_id="d", task_id="t", run_id="r").split("\n")[0] == ":x: *d.t* failed (run `r`)"
    assert "rivet said" not in format_failure({"stderr_path": "/nonexistent/step.log"}, dag_id="d", task_id="t", run_id="r")


def test_a_raw_unicode_separator_inside_the_json_line_does_not_split_it(tmp_path: Path) -> None:
    """D01: the stderr tail is read by `\\n`, so a raw U+2028 / U+0085 in a JSON string cannot cut the line in two."""
    line = json.dumps({"error": "cell a b\x85c\x0bd", "code": "RIVET_X"}, ensure_ascii=False)
    text = format_failure(stderr_of(tmp_path, line), dag_id="d", task_id="t", run_id="r")
    assert "rivet said: cell a\\u{2028}b\\u{85}c\\u{0b}d" in text.split("\n")
