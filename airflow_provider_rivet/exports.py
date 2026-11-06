"""The one reader of an export as rivet reads it: its name and mode (rivet's defaults), the load strategy its mode maps to
and the warehouse layout it resolves to (src/plan/load_settings.rs `load_mode_of`, `resolved_layout`, `cdc_layout`)."""

from __future__ import annotations

from collections.abc import Mapping
from pathlib import Path
from typing import Any

from . import _yaml

COMPACTING_TARGETS = ("bigquery",)  # rivet's `LoadTarget::compacts`: the warehouses `rivet compact` can merge a buffer in


def entries_of(config: Mapping[str, Any]) -> list[Mapping[str, Any]]:
    """The export entries of a config document, in order; an item that is not a mapping is not an export."""
    return [e for e in config.get("exports") or [] if isinstance(e, Mapping)]


def name_of(entry: Mapping[str, Any]) -> str:
    """The export's name as rivet reads the scalar (D07: `name: 2024` is `"2024"`)."""
    return str(_yaml.text_of(entry.get("name")))


def checkpoints(config: Mapping[str, Any], base: Path) -> dict[str, Path]:
    """The checkpoint file each export of the config names under `cdc.checkpoint`, by export name; relative to `base`."""
    out = {}
    for entry in entries_of(config):
        cdc = entry.get("cdc")
        ref = cdc.get("checkpoint") if isinstance(cdc, Mapping) else None
        if ref:
            path = Path(str(_yaml.text_of(ref)))
            out[name_of(entry)] = path if path.is_absolute() else base / path
    return out


def server_id_of(config: Mapping[str, Any], default: int) -> int | None:
    """The MySQL replica id of the config's first CDC export (`cdc.server_id`, else rivet's `default`); None for none."""
    for entry in entries_of(config):
        if mode_of(entry) == "cdc":
            cdc = _mapping(entry.get("cdc"))
            value = _yaml.text_of(cdc.get("server_id"))
            return int(value) if value not in (None, "") else default
    return None


def staging_of(config: Mapping[str, Any]) -> dict[str, str]:
    """The GCS bucket and prefix of the config's first export that stages in one, and the BigQuery project and dataset
    it loads into: the keys `bucket`, `prefix`, `project`, `dataset` that the config names."""
    out: dict[str, str] = {}
    for entry in entries_of(config):
        dest = _mapping(entry.get("destination"))
        if _yaml.text_of(dest.get("type")) == "gcs" and dest.get("bucket"):
            out = {"bucket": str(_yaml.text_of(dest["bucket"])), "prefix": str(_yaml.text_of(dest.get("prefix")) or "")}
            break
    load = _mapping(config.get("load"))
    if _yaml.text_of(load.get("target")) == "bigquery":
        out.update({k: str(_yaml.text_of(load[k])) for k in ("project", "dataset") if load.get(k)})
    return out


def mode_of(entry: Mapping[str, Any]) -> str:
    """The export's `mode`; rivet's default for an absent one is `full` (src/config/export.rs `default_mode`)."""
    return str(_yaml.text_of(entry.get("mode")) or "full")


def may_be_cdc(mode: str) -> bool:
    """Whether an export's mode (`mode_of`) is, or may resolve to, `cdc`: a mode a `${...}` still holds may."""
    return mode == "cdc" or "${" in mode


def tables_of(entry: Mapping[str, Any]) -> list[str]:
    """The tables the export names, as written: `tables:`, else the one `table:`."""
    named = entry.get("tables") or ([entry["table"]] if entry.get("table") else [])
    return [str(_yaml.text_of(t)) for t in named]


def lists_tables(entry: Mapping[str, Any]) -> bool:
    """Whether the export names its tables as a `tables:` list (a stream of several), not one `table:`."""
    return bool(entry.get("tables"))


def load_tables(config: Mapping[str, Any]) -> list[str]:
    """The tables the `load:` section configures one by one (`load.tables.<table>`), by name."""
    load = config.get("load")
    tables = load.get("tables") if isinstance(load, Mapping) else None
    return [str(_yaml.text_of(t)) for t in tables] if isinstance(tables, Mapping) else []


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


def _mapping(value: Any) -> Mapping[str, Any]:
    """The value as a mapping, or an empty one."""
    return value if isinstance(value, Mapping) else {}


def _held(value: Any) -> bool:
    """Whether a value is text that still holds a `${...}`."""
    return isinstance(value, str) and "${" in value


def shape_placeholders(config: Mapping[str, Any]) -> list[str]:
    """The fields that decide which tasks a DAG has, by name, that still hold a `${...}` after resolution."""
    source = _mapping(config.get("source"))
    mongo = _mapping(source.get("mongo"))
    fields = [
        ("load", config.get("load")),
        ("source.type", source.get("type")),
        ("source.url_env", source.get("url_env")),
        ("source.mongo.resume", mongo.get("resume")),
        ("source.mongo.page_size", mongo.get("page_size")),
    ]
    for entry in entries_of(config):
        fields += [
            ("name", entry.get("name")),
            ("keyset_incremental", entry.get("keyset_incremental")),
            ("chunk_by_key", entry.get("chunk_by_key")),
        ]
        if mode_of(entry) == "cdc":
            tables = entry.get("tables")
            listed = tables if isinstance(tables, list) else []
            cdc = _mapping(entry.get("cdc"))
            fields += [
                ("table", entry.get("table")),
                ("tables", tables),
                ("cdc.backfill", cdc.get("backfill")),
                *(("tables", t) for t in listed),
            ]
    return list(dict.fromkeys(field for field, value in fields if _held(value)))


def placeholder_decides_compaction(config: Mapping[str, Any], entry: Mapping[str, Any], table: str | None = None) -> bool:
    """Whether a `${...}` in `load.target` or the layout written for the export (or table) may decide whether it compacts."""
    load = config.get("load")
    if not isinstance(load, Mapping) or load_mode(config, entry) == "full":
        return False
    target = str(_yaml.text_of(load.get("target")))
    if target not in COMPACTING_TARGETS and "${" not in target:
        return False
    return "${" in target or "${" in str(layout_of(config, entry, table))


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
