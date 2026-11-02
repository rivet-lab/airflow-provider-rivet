"""The stderr reader: a long last line, nested JSON, the contract shape without `exit_class`, one window for both readers."""

from __future__ import annotations

import json
from pathlib import Path

import pytest
from conftest import Rig, fixture

from airflow_provider_rivet import callbacks, invocation, reading, stderr
from airflow_provider_rivet.operators import RivetRunOperator
from airflow_provider_rivet.result import error_from_line

TAIL = stderr.STDERR_TAIL


def test_a_last_line_longer_than_the_tail_is_recorded_malformed_not_silently_generic(rig: Rig) -> None:
    """contract-1: the error line is longer than the 1 MiB window, so it cannot be read whole; the step records
    `result_malformed` instead of building a generic error from the exit alone."""
    long_line = json.dumps({"exit_class": 2, "error": "e" * (TAIL + 100), "code": "RIVET_SOURCE_CONNECT"})
    rig.scenario(run={"line_raw": long_line + "\n", "exit": 1})
    rig.run(rig.operator(RivetRunOperator, export="orders"))
    [result] = rig.events("rivet.result")
    assert "result_malformed" in result["degraded"]


def test_a_last_line_that_fits_the_tail_is_read_whole_after_a_longer_earlier_one(tmp_path: Path) -> None:
    """contract-1 (no regression): the window cut lands in an earlier long line; the last short line is still read."""
    path = tmp_path / "e.log"
    short = {"exit_class": 2, "error": "x"}
    path.write_bytes(b"y" * (TAIL + 10) + b"\n" + json.dumps(short).encode() + b"\n")
    assert invocation._error_line(stderr.tail_lines(path)[0]) == short


def _too_deep(text: str, *args: object, **kwargs: object) -> object:
    raise RecursionError("maximum recursion depth exceeded while decoding a JSON array from a unicode string")


def test_a_value_too_deep_for_the_parser_is_not_json_for_any_reader(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    """hostile-input-3: `json.loads` raises RecursionError past the parser's depth (Python 3.14 parses deeper, so the
    error is simulated); each reader treats that line as not JSON, and the next error line is still read."""
    monkeypatch.setattr(json, "loads", _too_deep)
    line = '{"exit_class": 2, "error": "x"}'
    assert invocation._error_line([line]) is None
    assert invocation._has_broken_object([line]) is True
    assert reading._json_of(b"{}") is None
    path = tmp_path / "e.stderr.log"
    path.write_text(line + "\nError: after the deep line\n")
    assert callbacks._rivet_errors({"stderr_path": str(path)}) == [(None, "Error: after the deep line")]


def test_a_deep_stderr_line_never_raises_from_a_reader() -> None:
    """hostile-input-3: a line nested 10000 levels deep is read as JSON or not, and no reader raises."""
    deep = '{"exit_class": 2, "error": ' + "[" * 10000 + "]" * 10000 + "}"
    found = invocation._error_line([deep])
    if found is not None:
        assert found["exit_class"] == 2
    assert invocation._has_broken_object([deep]) in (True, False)
    doc = reading._json_of(deep.encode())
    if doc is not None:
        assert isinstance(doc, dict)


@pytest.mark.parametrize(
    "name",
    [
        "json_errors.json",
        "json_errors_uncoded.json",
        "json_errors_crashed.json",
        "json_errors_mixed.json",
        "json_errors_load.json",
    ],
)
def test_a_contract_shaped_error_line_without_exit_class_is_read_as_the_contract_says(name: str, tmp_path: Path) -> None:
    """contract-3: a line printed with the contract's keys (`class`, `retryable`) and no `exit_class` is read by the
    stderr path too, and classified as the fixture says."""
    line = {key: value for key, value in fixture(name).items() if key != "exit_class"}
    path = tmp_path / "e.stderr.log"
    path.write_text(json.dumps(line) + "\n")
    found = invocation._error_line(stderr.tail_lines(path)[0])
    assert found is not None
    error = error_from_line(found)
    assert error is not None
    assert (error.class_, error.retryable, error.exit_code) == (line["class"], line["retryable"], line["exit_code"])
    assert error.code == line.get("code")


def test_an_error_line_of_100_kib_is_read_by_the_alert_and_by_the_classifier(tmp_path: Path) -> None:
    """seams-1: both readers see the same window, so a 100 KiB error line is the alert's text and the step's error."""
    path = tmp_path / "e.stderr.log"
    message = "connection reset " + "r" * (100 * 1024)
    path.write_text(json.dumps({"exit_class": 2, "error": message, "code": "RIVET_SOURCE_CONNECT"}) + "\n")
    assert callbacks._rivet_errors({"stderr_path": str(path)}) == [("RIVET_SOURCE_CONNECT", message)]
    found = invocation._error_line(stderr.tail_lines(path)[0])
    assert found is not None and found["error"] == message


@pytest.mark.parametrize("line", [{"class": "crashed"}, {"retryable": True}, {"exit_class": 2}])
def test_each_key_that_marks_an_error_object_is_enough_to_find_it(line: dict) -> None:
    """contract-3: an object with any one of `exit_class`, `class`, `retryable` is the error line."""
    assert invocation._error_line([json.dumps(line)]) == line
