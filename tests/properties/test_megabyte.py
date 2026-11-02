"""Each reader of rivet's text takes a megabyte of it within a few seconds: no quadratic scan of its input."""

from __future__ import annotations

import json
import signal
from collections.abc import Iterator
from contextlib import contextmanager
from pathlib import Path
from types import SimpleNamespace

from airflow_provider_rivet._yaml import resolve_and_load
from airflow_provider_rivet.callbacks import format_error_text
from airflow_provider_rivet.connections import connection_url
from airflow_provider_rivet.dags import task_name
from airflow_provider_rivet.invocation import _error_line
from airflow_provider_rivet.stderr import lines_of, tail_lines

MEGABYTE = 1 << 20
SECONDS = 5


@contextmanager
def within(seconds: int) -> Iterator[None]:
    """Fail with TimeoutError when the block runs past `seconds`."""

    def expired(*_: object) -> None:
        raise TimeoutError(f"ran past {seconds} s: a reader scans its input more than once per byte")

    previous = signal.signal(signal.SIGALRM, expired)
    signal.setitimer(signal.ITIMER_REAL, seconds)
    try:
        yield
    finally:
        signal.setitimer(signal.ITIMER_REAL, 0)
        signal.signal(signal.SIGALRM, previous)


def test_the_tail_reader_reads_a_window_of_short_lines_and_flags_one_long_line(tmp_path: Path) -> None:
    """A window of a megabyte of short lines gives every whole line in it; a window of one longer line gives none, flagged."""
    short = tmp_path / "short.txt"
    short.write_bytes(b"x\n" * (MEGABYTE // 2 + 1))
    long_line = tmp_path / "long.txt"
    long_line.write_bytes(b"y" * (MEGABYTE + 1) + b"\n")
    with within(SECONDS):
        lines, cut = tail_lines(short)
        assert len(lines) == MEGABYTE // 2 and set(lines) == {"x"} and cut is False
        assert tail_lines(long_line) == ([], True)


def test_the_line_splitter_and_the_placeholder_resolver_scan_a_megabyte_once() -> None:
    """A megabyte of lines is split in one pass, and a megabyte of placeholders is resolved in one."""
    with within(SECONDS):
        assert len(lines_of("line\n" * (MEGABYTE // 5))) == MEGABYTE // 5
        doc, unresolved = resolve_and_load("k: " + "${A}" * (MEGABYTE // 4) + "\n", lambda name: "v")
        assert unresolved == [] and doc["k"] == "v" * (MEGABYTE // 4)


def test_the_classifier_and_the_error_text_take_a_megabyte_line(tmp_path: Path) -> None:
    """A megabyte of nested brackets is not an error object, and a megabyte error text is capped in time."""
    deep = "{" + "[" * (MEGABYTE // 2) + "]" * (MEGABYTE // 2) + "}"
    error = tmp_path / "err.txt"
    error.write_text(json.dumps({"exit_class": 2, "error": "e" * MEGABYTE, "code": "RIVET_X"}) + "\n")
    with within(SECONDS):
        assert _error_line([deep]) is None
        assert len(format_error_text({"stderr_path": str(error)}, limit=3500)) <= 3500


def test_the_connection_url_and_the_task_id_take_a_megabyte_of_text() -> None:
    """A megabyte password is encoded and a megabyte name is hashed, each in time."""
    conn = SimpleNamespace(
        conn_id="c", conn_type="postgres", host="db", login="u", password="p" * MEGABYTE, port=None, schema="app", extra_dejson={}
    )
    with within(SECONDS):
        assert len(connection_url(conn)) > MEGABYTE
        assert len(task_name("n" * MEGABYTE + " ")) == MEGABYTE + 10
