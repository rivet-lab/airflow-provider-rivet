"""The one reader of an export as rivet reads it: mode default, load strategy and the layout rivet resolves.

The table mirrors rivet's own resolver (src/plan/load_settings.rs `load_mode_of`, `resolved_layout`, `cdc_layout`; ADR-0034 D7)."""

from __future__ import annotations

from typing import Any

import pytest

from airflow_provider_rivet import _yaml
from airflow_provider_rivet.exports import compacts, entries_of, entry_named, load_mode, mode_of, name_of, tables_of

BQ = "load:\n  target: bigquery\n  project: p\n  dataset: d\n"


def doc(text: str) -> dict[str, Any]:
    return _yaml.load(text)


@pytest.mark.parametrize(
    ("config", "mode", "layout", "table", "compacted"),
    [
        # a written base_buffer: every accumulating mode, never `full`
        (BQ + "  layout: base_buffer\n", "incremental", None, None, True),
        (BQ + "  layout: base_buffer\n", "cdc", None, None, True),
        (BQ + "  layout: base_buffer\n", "full", None, None, False),
        (BQ + "  layout: base_buffer\n", None, None, None, False),  # no `mode:` is `full`
        (BQ + "  layout: base_buffer\n", "chunked", None, None, False),
        (BQ + "  layout: base_buffer\n", "time_window", None, None, False),
        # nothing written: a stream with a `backfill:` is a base and a buffer, anything else a change log
        (BQ, "cdc", None, None, False),
        (BQ, "incremental", None, None, False),
        (BQ + "  layout: log_view\n", "cdc", None, None, False),
        # the export's own `load:` over the section, the captured table's over both
        (BQ + "  layout: base_buffer\n", "cdc", "      layout: log_view\n", None, False),
        (BQ + "  layout: log_view\n", "cdc", "      layout: base_buffer\n", None, True),
        (BQ + "  layout: base_buffer\n", "cdc", "      tables:\n        orders: {layout: log_view}\n", "orders", False),
        (BQ + "  layout: base_buffer\n", "cdc", "      tables:\n        orders: {layout: log_view}\n", "customers", True),
        # a warehouse that cannot compact
        ("load:\n  target: clickhouse\n  database: d\n", "cdc", None, None, False),
        ("load:\n  target: snowflake\n  database: d\n", "cdc", None, None, False),
        ("", "incremental", None, None, False),
    ],
)
def test_the_layout_resolves_as_rivet_resolves_it(
    config: str, mode: str | None, layout: str | None, table: str | None, compacted: bool
) -> None:
    export = "exports:\n  - name: orders\n    table: orders\n" + (f"    mode: {mode}\n" if mode else "")
    if mode == "cdc":
        export += "    tables: [orders, customers]\n"
    export += f"    load:\n{layout}" if layout else ""
    text = config + export
    entry = entries_of(doc(text))[0]
    assert compacts(doc(text), entry, table) is compacted


def test_a_stream_with_a_backfill_and_no_layout_is_a_base_and_a_buffer_on_a_compacting_warehouse() -> None:
    stream = "exports:\n  - name: s\n    mode: cdc\n    tables: [t]\n    cdc:\n      backfill: auto\n"
    assert compacts(doc(BQ + stream), entries_of(doc(BQ + stream))[0]) is True  # rivet cdc_layout: (Cdc, None) with a backfill
    other = "load:\n  target: clickhouse\n  database: d\n"
    assert compacts(doc(other + stream), entries_of(doc(other + stream))[0]) is False
    written = BQ + "  layout: log_view\n" + stream
    assert compacts(doc(written), entries_of(doc(written))[0]) is False  # a written layout decides
    assert compacts(doc(BQ + stream.replace("backfill: auto", "checkpoint: x")), entries_of(doc(BQ + stream))[0]) is True


def test_a_full_export_that_continues_past_a_key_is_an_incremental_load() -> None:
    """rivet `continued_key`: a keyset-incremental chunked export, and a MongoDB full export with paging and resume."""
    keyset = "exports:\n  - name: k\n    mode: chunked\n    chunk_by_key: id\n    keyset_incremental: true\n"
    plain = "exports:\n  - name: k\n    mode: chunked\n    chunk_by_key: id\n"
    mongo = "source:\n  type: mongo\n  mongo: {page_size: 100, resume: true}\n" + "exports:\n  - name: k\n    table: k\n"
    unpaged = "source:\n  type: mongo\n  mongo: {resume: true}\n" + "exports:\n  - name: k\n    table: k\n"
    for text, mode in [(keyset, "incremental"), (plain, "full"), (mongo, "incremental"), (unpaged, "full")]:
        config = doc(text)
        assert load_mode(config, entries_of(config)[0]) == mode, text
    config = doc(BQ + "  layout: base_buffer\n" + keyset)
    assert compacts(config, entries_of(config)[0]) is True
    config = doc(BQ + "  layout: base_buffer\n" + plain)
    assert compacts(config, entries_of(config)[0]) is False


def test_names_modes_and_tables_read_as_rivet_reads_the_scalars() -> None:
    config = doc(
        "exports:\n  - name: 2024\n  - name: 0x10\n    mode: cdc\n    table: 007\n  - just a string\n  - name: t\n    tables: [1, a.b]\n"
    )
    assert [name_of(e) for e in entries_of(config)] == ["2024", "0x10", "t"]
    assert [mode_of(e) for e in entries_of(config)] == ["full", "cdc", "full"]
    assert [tables_of(e) for e in entries_of(config)] == [[], ["007"], ["1", "a.b"]]
    assert entry_named(config, "0x10") is entries_of(config)[1] and entry_named(config, "none") is None
