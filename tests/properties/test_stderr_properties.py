"""The stderr reader is total: any bytes, any window, any depth give lines or a flag, never an exception."""

from __future__ import annotations

import json
from pathlib import Path

import pytest
from conftest import FIXTURES, fixture
from hypothesis import given
from hypothesis import strategies as st

from airflow_provider_rivet import stderr
from airflow_provider_rivet.invocation import _error_line
from airflow_provider_rivet.result import error_from_line
from airflow_provider_rivet.stderr import lines_of, tail_lines

TEXT = st.text(alphabet=st.characters(blacklist_categories=("Cs", "Cc")), max_size=60)


@given(st.text(max_size=300))
def test_lines_are_cut_at_newline_only(text: str) -> None:
    """Rivet's lines end at "\\n" alone: no other separator splits one, and the count is the newlines plus a last piece."""
    lines = lines_of(text)
    assert all("\n" not in line for line in lines)
    assert len(lines) == text.count("\n") + (1 if text and not text.endswith("\n") else 0)


@given(data=st.binary(max_size=600), window=st.integers(min_value=1, max_value=200))
def test_the_tail_reader_returns_lines_and_a_flag_for_any_bytes(
    data: bytes, window: int, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Any bytes in any window give strings with no newline and a bool; a file within the window is read whole."""
    monkeypatch.setattr(stderr, "STDERR_TAIL", window)
    path = tmp_path / "err.txt"
    path.write_bytes(data)
    lines, cut = tail_lines(path)
    assert all(isinstance(line, str) and "\n" not in line for line in lines)
    assert isinstance(cut, bool)
    if len(data) <= window:
        assert lines == lines_of(data.decode(errors="replace")) and cut is False


@given(
    window=st.integers(min_value=8, max_value=120),
    length=st.integers(min_value=1, max_value=400),
    earlier=st.lists(TEXT, max_size=3),
)
def test_the_last_line_is_read_whole_when_it_fits_the_window_and_flagged_when_it_does_not(
    window: int, length: int, earlier: list[str], tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """A last line of up to the window less its newline is read whole; a longer one is not, and the flag says so."""
    monkeypatch.setattr(stderr, "STDERR_TAIL", window)
    last = '{"error": "' + "e" * length + '"}'
    text = "".join(line.replace("\n", "") + "\n" for line in earlier) + last + "\n"
    path = tmp_path / "err.txt"
    path.write_bytes(text.encode())
    lines, cut = tail_lines(path)
    if len(last.encode()) + 1 <= window:
        assert lines[-1] == last and cut is False
    else:
        assert last not in lines and cut is True


@given(depth=st.integers(min_value=1, max_value=20_000), opener=st.sampled_from(["[", '{"x": [']))
def test_a_value_nested_to_any_depth_is_skipped_by_the_classifier_never_raised(depth: int, opener: str) -> None:
    """A line nested past the parser's depth is not an error object, and the classifier reads it as None, not a crash."""
    closer = "]" * depth + ("}" if opener.startswith("{") else "")
    line = '{"exit_class": 2, "error": ' + opener * depth + closer + "}"
    found = _error_line([line])
    if found is not None:
        assert "exit_class" in found


@given(TEXT)
def test_the_classifier_is_total_on_any_text_after_a_brace(text: str) -> None:
    """The classifier returns an error object or None for any text that starts like one."""
    found = _error_line(["{" + text])
    assert isinstance(found, (dict, type(None)))


@pytest.mark.parametrize("name", sorted(p.name for p in FIXTURES.glob("json_errors*.json")))
def test_a_contract_error_object_read_from_stderr_is_read_as_the_direct_reader_reads_it(name: str) -> None:
    """The stderr path finds every contract error object, also one without exit_class, and classifies it alike."""
    obj = fixture(name)
    if "class" in obj:
        obj.pop("exit_class", None)
    line = f"  {json.dumps(obj)}  "
    found = _error_line([line])
    assert found == obj
    assert error_from_line(found, obj.get("exit_code")) == error_from_line(obj, obj.get("exit_code"))
