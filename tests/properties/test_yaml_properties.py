"""resolve_and_load gives back exactly the text a placeholder resolved to, whatever characters the text holds."""

from __future__ import annotations

from hypothesis import given
from hypothesis import strategies as st

from airflow_provider_rivet._yaml import resolve_and_load

CHAR = st.characters(blacklist_categories=("Cs", "Cc", "Zl", "Zp"), blacklist_characters='"\\￾￿')
TOKEN = st.sampled_from(["${", "}", "$", "{", "", "", "", "${PW}"])
VALUE = st.lists(st.one_of(CHAR, TOKEN), max_size=12).map("".join)


@given(VALUE)
def test_a_resolved_value_comes_back_as_it_was_written(value: str) -> None:
    """A value spliced into a quoted scalar is the scalar, with no placeholder spelled back and nothing unresolved."""
    doc, unresolved = resolve_and_load('name: "${PW}"\n', lambda name: value if name == "PW" else None)
    assert unresolved == []
    assert doc == {"name": value}


@given(name=st.from_regex(r"[A-Z]{1,6}", fullmatch=True), value=st.lists(CHAR, max_size=12).map("".join))
def test_an_unresolved_placeholder_is_spelled_back_and_reported(name: str, value: str) -> None:
    """A placeholder nothing resolves stays `${name}` in the document and is reported by name, whatever else the text holds."""
    doc, unresolved = resolve_and_load(f'name: "${{{name}}}"\nother: "{value}"\n', lambda _: None)
    assert unresolved == [name]
    assert doc == {"name": "${" + name + "}", "other": value}
