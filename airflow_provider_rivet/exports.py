"""The one reader of an export as rivet reads it: its name and mode (rivet's defaults), the load strategy its mode maps to
and the warehouse layout it resolves to (src/plan/load_settings.rs `load_mode_of`, `resolved_layout`, `cdc_layout`)."""

from __future__ import annotations

from collections.abc import Mapping
from typing import Any

from . import _yaml

COMPACTING_TARGETS = ("bigquery",)  # rivet's `LoadTarget::compacts`: the warehouses `rivet compact` can merge a buffer in


def entries_of(config: Mapping[str, Any]) -> list[Mapping[str, Any]]:
    """The export entries of a config document, in order; an item that is not a mapping is not an export."""
    return [e for e in config.get("exports") or [] if isinstance(e, Mapping)]


def name_of(entry: Mapping[str, Any]) -> str:
    """The export's name as rivet reads the scalar (D07: `name: 2024` is `"2024"`)."""
    return str(_yaml.text_of(entry.get("name")))


def mode_of(entry: Mapping[str, Any]) -> str:
    """The export's `mode`; rivet's default for an absent one is `full` (src/config/export.rs `default_mode`)."""
    return str(_yaml.text_of(entry.get("mode")) or "full")


def tables_of(entry: Mapping[str, Any]) -> list[str]:
    """The tables the export names, as written: `tables:`, else the one `table:`."""
    named = entry.get("tables") or ([entry["table"]] if entry.get("table") else [])
    return [str(_yaml.text_of(t)) for t in named]


def entry_named(config: Mapping[str, Any], export: str | None) -> Mapping[str, Any] | None:
    """The export entry with this name, or None."""
    return next((e for e in entries_of(config) if name_of(e) == export), None)


def load_mode(config: Mapping[str, Any], entry: Mapping[str, Any]) -> str:
    """How a load treats the export: `cdc`, `incremental` (a delta since a cursor or a continued key) or `full`."""
    mode = mode_of(entry)
    if mode in ("cdc", "incremental"):
        return mode
    source = config.get("source")
    source = source if isinstance(source, Mapping) else {}
    mongo = source.get("mongo")
    keyset = mode == "chunked" and entry.get("keyset_incremental") is True and bool(entry.get("chunk_by_key"))
    paged = (
        mode == "full"
        and _yaml.text_of(source.get("type")) == "mongo"
        and isinstance(mongo, Mapping)
        and mongo.get("resume") is True
        and mongo.get("page_size") is not None
    )
    return "incremental" if keyset or paged else "full"


def layout_of(config: Mapping[str, Any], entry: Mapping[str, Any], table: str | None = None) -> str | None:
    """The `load.layout` written for the export: the section's, over it the export's `load:`, over it the captured table's."""
    export_load = entry.get("load")
    per_table = export_load.get("tables") if isinstance(export_load, Mapping) else None
    layers = [config.get("load"), export_load, per_table.get(table) if isinstance(per_table, Mapping) and table else None]
    written = None
    for layer in layers:
        if isinstance(layer, Mapping) and layer.get("layout") is not None:
            written = str(_yaml.text_of(layer["layout"]))
    return written


def compacts(config: Mapping[str, Any], entry: Mapping[str, Any], table: str | None = None) -> bool:
    """Whether rivet resolves the export (and captured `table`, as written) to a base and a buffer that `rivet compact`
    merges: a compacting warehouse, a load that accumulates, and `layout: base_buffer` written or, for a stream, `backfill:`."""
    load = config.get("load")
    if not isinstance(load, Mapping) or _yaml.text_of(load.get("target")) not in COMPACTING_TARGETS:
        return False
    mode = load_mode(config, entry)
    if mode == "full":
        return False
    written = layout_of(config, entry, table)
    if written is not None:
        return written == "base_buffer"
    cdc = entry.get("cdc")
    return mode == "cdc" and isinstance(cdc, Mapping) and cdc.get("backfill") is not None
