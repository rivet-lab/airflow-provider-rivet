"""`_yaml` at its edges: the scalars rivet reads as written, and the `${name}` scan.
Mutant ids (`x_<fn>__mutmut_<n>`) are those of the full run on main 6ddc6d9."""

from __future__ import annotations

import math

import pytest

from airflow_provider_rivet import _yaml

# --- scalars ---------------------------------------------------------------------------------------------------------


def test_prefixed_and_signed_ints_read_their_value_and_keep_their_text() -> None:
    """A sign before a base prefix (_int 8, 14, 16, 17, 18); an explicit `!!int` that is no int stays text (_int 3)."""
    doc = _yaml.load("a: -0x1F\nb: +0x10\nc: +0o17\nd: -0b11\ne: !!int nope\n")
    assert doc == {"a": -31, "b": 16, "c": 15, "d": -3, "e": "nope"}
    assert [_yaml.text_of(doc[k]) for k in "abcd"] == ["-0x1F", "+0x10", "+0o17", "-0b11"]


@pytest.mark.parametrize(
    ("text", "value"),
    [(".inf", math.inf), ("-.inf", -math.inf), ("+.Inf", math.inf), ("-.INF", -math.inf), (".NaN", math.nan)],
)
def test_the_special_floats_read_with_their_sign_and_keep_their_text(text: str, value: float) -> None:
    """_float 4, 5, 6, 7, 10-23: every spelling of .inf / .nan, with its sign."""
    got = _yaml.load(f"v: {text}")["v"]
    assert (math.isnan(got) if math.isnan(value) else got == value) and _yaml.text_of(got) == text


def test_a_float_keeps_the_text_it_was_written_as() -> None:
    """_float 29: `1.50` is the string field `1.50`, not `1.5`."""
    assert _yaml.text_of(_yaml.load("v: 1.50")["v"]) == "1.50"


def test_text_of_spells_each_value_as_rivets_string_field_reads_it() -> None:
    """text_of 1 (None stays None), 3-8 (bools), 17 (a number with no written text)."""
    assert _yaml.text_of(None) is None
    assert (_yaml.text_of(True), _yaml.text_of(False)) == ("true", "false")
    assert _yaml.text_of(5) == "5"


# --- the `${name}` scan --------------------------------------------------------------------------------------------


def test_a_placeholder_at_the_very_start_is_substituted() -> None:
    """substitute 10, 11: the first `${` at offset 0."""
    assert _yaml.substitute("${A}x", {"A": "v"}.get) == ("vx", [])


def test_an_unresolved_placeholder_reads_back_spelled_as_written() -> None:
    """unmasked 9, 10, 12: the mask is undone in a string, in a list and in a mapping key."""
    doc, unresolved = _yaml.resolve_and_load("a: x${U}y\nb: [${V}]\n${W}: 1\n", lambda name: None)
    assert doc == {"a": "x${U}y", "b": ["${V}"], "${W}": 1} and unresolved == ["U", "V", "W"]


def test_a_trailing_param_flag_with_no_value_is_ignored() -> None:
    """params_of 12, 15, 17: `-p` / `--param` as the last argument."""
    assert _yaml.params_of(["-p", "A=1", "-p"]) == {"A": "1"}
    assert _yaml.params_of(["--param"]) == {}
