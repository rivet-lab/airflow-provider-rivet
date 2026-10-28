"""The JSON documents rivet 0.32.2 writes, in one place: the fake binary (copied into the rig beside this file) writes them
and the tests build theirs with the same functions, so a shape changes once.

Read from rivet at RIVET_CONTRACT_REF: `RunAggregate` / `RunAggregateEntry` (src/state/run_aggregate.rs, built by
src/pipeline/aggregate.rs `build`) for `rivet run` / `apply --summary-output`, `CommandResult` / `TableResult`
(src/load/result.rs) for `rivet load` / `compact`, and the `--json-errors` line (`error`, `exit_class`, `code` when coded).
"""

ENTRY_KEYS = (
    "export_name",
    "status",
    "run_id",
    "answered_by_run",
    "rows",
    "files",
    "bytes",
    "bytes_read",
    "duration_ms",
    "mode",
    "error_message",
    "skip_reason",
)


def run_entry(name, status="success", **more):
    """One `per_export` entry as 0.32.2 serialises it: every key, `null` where rivet has none. A failed entry carries
    `error_message` (a `skipped` one `skip_reason`), never the `error` / `stop_reason` of a shape no binary writes."""
    entry = {
        "export_name": name,
        "status": status,
        "run_id": f"{name}_1",
        "answered_by_run": None,
        "rows": 0,
        "files": 0,
        "bytes": 0,
        "bytes_read": 0,
        "duration_ms": 0,
        "mode": "full",
        "error_message": None,
        "skip_reason": None,
    }
    return {**entry, **more}


def run_summary(entries):
    """The `--summary-output` document of `rivet run` / `apply`: the aggregate around its entries."""
    counts = {s: sum(e["status"] == s for e in entries) for s in ("success", "failed")}
    return {
        "run_aggregate_id": "agg_20261006T000000000",
        "started_at": "2026-10-06T00:00:00+00:00",
        "finished_at": "2026-10-06T00:00:00+00:00",
        "duration_ms": 0,
        "config_path": None,
        "parallel_mode": "sequential",
        "total_exports": len(entries),
        "success_count": counts["success"],
        "failed_count": counts["failed"],
        "skipped_count": len(entries) - counts["success"] - counts["failed"],
        "total_rows": sum(e["rows"] for e in entries),
        "total_files": sum(e["files"] for e in entries),
        "total_bytes": sum(e["bytes"] for e in entries),
        "total_bytes_read": sum(e["bytes_read"] for e in entries),
        "per_export": list(entries),
    }


def load_summary(rows=()):
    """The `--summary-output` document of `rivet load` / `compact`."""
    return {"run_id": "load_1", "per_table": list(rows)}


def error_line(text, exit_class, code=None):
    """The `--json-errors` line on stderr: `code` is there when rivet coded the refusal."""
    return {"error": text, "exit_class": exit_class, **({"code": code} if code else {})}
