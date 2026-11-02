"""rivet's error text where an operator reads it: the one-line, redacted, bounded form (`error_text`), the Slack text and the task log's failure lines. Operator and source text, so hostile."""

from __future__ import annotations

import json
import time
from pathlib import Path
from typing import Any

import pytest
from test_slack_alerts import posted

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


def test_a_format_character_that_reorders_text_is_shown_as_an_escape() -> None:
    """Category Cf: U+202E (right-to-left override) and U+200B (zero-width space) hide or reverse what follows."""
    assert error_text("file\u202egpj.exe\u200b") == "file\\u{202e}gpj.exe\\u{200b}"


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
    for hostile, empty in (
        ("\n" * 1_000_000, True),
        ("a\n" * 500_000, False),
        (("\r" * 50 + "\x1b") * 20_000, False),
        (("é" * 7 + planted) * 70_000, False),
    ):
        text = error_text(hostile)
        if empty:
            assert text is None
        else:
            assert text is not None and len(text) <= ERROR_TEXT_LIMIT and planted not in text
    assert time.monotonic() - started < 10


# --- the exception text -----------------------------------------------------------------------------------------------


def test_the_exception_text_is_code_class_action_unit_and_the_one_line_text() -> None:
    error = ErrorObject("RIVET_X", None, "refusal", 5, False, "do the thing", "first\nsecond")
    assert error.describe("orders") == "[RIVET_X] refusal: do the thing (orders): first | second"
    assert error.describe(None) == "[RIVET_X] refusal: do the thing: first | second"


def test_an_error_without_text_reads_as_it_did_before() -> None:
    assert ErrorObject("RIVET_X", None, "refusal", 5, False, None, None).describe("orders") == "[RIVET_X] refusal (orders)"
    assert ErrorObject(None, None, "generic", 1, False, None, " \n").describe(None) == "generic"


def stderr_of(tmp_path: Path, *lines: str) -> dict:
    path = tmp_path / "step.stderr.log"
    path.write_bytes(b"".join(line.encode() + b"\n" for line in lines))
    return {"stderr_path": str(path)}


def test_the_posted_text_says_rivets_reason_on_its_error_line(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    payload = stderr_of(
        tmp_path, json.dumps({"error": "table 'orders':\nBigQuery job X failed", "code": "RIVET_LOAD_WAREHOUSE_REJECTED"})
    )
    assert posted(monkeypatch, payload).split("\n")[2:] == [
        "```",
        "[RIVET_LOAD_WAREHOUSE_REJECTED] table 'orders':",
        "BigQuery job X failed",
        "```",
    ]


def test_the_posted_text_masks_the_credential(tmp_path: Path, monkeypatch: pytest.MonkeyPatch, planted: str) -> None:
    payload = stderr_of(tmp_path, json.dumps({"error": f"{planted} " + "y" * 1000}))
    text = posted(monkeypatch, payload)
    assert planted not in text and "*** yyy" in text


def test_without_a_rivet_result_or_error_line_the_posted_text_says_so(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    for payload in ({}, {"stderr_path": "/nonexistent/step.log"}):
        assert "no error text: the task failed without a rivet result or an exception" in posted(monkeypatch, payload)


def test_a_raw_unicode_separator_inside_the_json_line_does_not_split_it(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """D01: the stderr tail is read by `\\n`, so a raw U+2028 / U+0085 in a JSON string cannot cut the line in two."""
    line = json.dumps({"error": "cell a b\x85c\x0bd", "code": "RIVET_X"}, ensure_ascii=False)
    assert "[RIVET_X] cell a b\x85c\x0bd" in posted(monkeypatch, stderr_of(tmp_path, line)).split("\n")
