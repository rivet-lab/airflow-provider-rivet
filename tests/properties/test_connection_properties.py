"""A Connection's URL names its host, login and password as written, or is refused with a coded reason."""

from __future__ import annotations

from types import SimpleNamespace
from urllib.parse import unquote, urlsplit

from hypothesis import given
from hypothesis import strategies as st

from airflow_provider_rivet.connections import UNSUPPORTED, connection_url
from airflow_provider_rivet.preflight import PreflightRefusal

HOST = st.text(alphabet="abcXYZ019.-_@/?#\\ ", min_size=1, max_size=12)
FIELD = st.one_of(st.none(), st.text(alphabet=st.characters(blacklist_categories=("Cs",)), max_size=30))
DANGEROUS = "@/?#\\ "


@given(HOST, FIELD, FIELD, st.one_of(st.none(), st.integers(min_value=1, max_value=65535)))
def test_the_url_holds_the_host_and_the_credentials_or_refuses_the_host(
    host: str, login: str | None, password: str | None, port: int | None
) -> None:
    """A host with a character a URL reads as a delimiter is refused as UNSUPPORTED; any other host, login and password
    come back out of the URL as written."""
    conn = SimpleNamespace(
        conn_id="c", conn_type="postgres", host=host, login=login, password=password, port=port, schema="app", extra_dejson={}
    )
    if any(char in host for char in DANGEROUS):
        try:
            connection_url(conn)
        except PreflightRefusal as refusal:
            assert refusal.code == UNSUPPORTED
            return
        raise AssertionError(f"host {host!r} holds a URL delimiter and was not refused")
    parts = urlsplit(connection_url(conn))
    assert parts.hostname == host.lower()
    assert (parts.port or None) == port
    assert unquote(parts.username or "") == (login or "")
    assert unquote(parts.password or "") == (password or "")
