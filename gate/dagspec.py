"""The gate's DAG specs: the one place a leg says which DAGs the stack runs.

A leg decides everything about its DAGs on the host: the ids, the config and state_dir, the connections the tasks read,
the extras one run needs (a pause window, a queue, a retry). It writes those as specs to `configs/dag_specs_<leg>.json`,
and `stand/dags/rivet_gate.py` builds exactly them with the provider's builders. The host grades the task ids it gets
from the same spec (`task_ids`), so what is built and what is graded cannot drift: neither re-derives the other.

A spec is the arguments of one builder call, as data:

    {"dag_id", "kind": "batch" | "cdc" | "load", "build": {...builder kwargs...},
     "default_args": {...}, "task_env": {task_id: env}, "task_queue": {task_id: queue}}

`default_args["retry_delay"]` is in seconds (the DAG file makes the timedelta). `task_env` and `task_queue` are set on the
built DAG's tasks (they are not operator kwargs of the builders).
"""

from __future__ import annotations

import json
import os
from collections.abc import Mapping, Sequence
from pathlib import Path
from typing import Any

#: Where the stack mounts the gate's directory.
ROOT = "/opt/rivet"
#: The spec files, beside the configs they name.
SPEC_PREFIX = "dag_specs_"
#: The source every scenario DAG reads, and the Connection it comes from (stand.py `connections`).
SOURCE_ENV = {"RIVET_GATE_SOURCE_URL": "rivet_gate_source"}
KINDS = ("batch", "cdc", "load")


def env_key(stand: Any) -> bool:
    """`--gcp-auth env-file` (the client's): no GCP Connection; the key file the compose env names reaches rivet through
    `cloud_credentials=["gcp"]` (GOOGLE_APPLICATION_CREDENTIALS and the other GOOGLE_* variables)."""
    return bool(getattr(stand, "gcp_key_file", None))


def gcp(stand: Any) -> dict[str, Any]:
    """How a DAG's tasks reach GCP: the key file the compose env names, else the Connection."""
    return {"cloud_credentials": ["gcp"]} if env_key(stand) else {"connections": ["gate_gcp"]}


def _spec(
    kind: str,
    dag_id: str,
    build: dict[str, Any],
    retries: int | None,
    retry_delay: int | None,
    task_env: Mapping[str, Mapping[str, str]] | None,
    task_queue: Mapping[str, str] | None,
) -> dict[str, Any]:
    default_args = {
        **({} if retries is None else {"retries": retries}),
        **({} if retry_delay is None else {"retry_delay": retry_delay}),
    }
    return {
        "dag_id": dag_id,
        "kind": kind,
        "build": build,
        "default_args": default_args,
        "task_env": {t: dict(env) for t, env in (task_env or {}).items()},
        "task_queue": dict(task_queue or {}),
    }


def batch(
    dag_id: str,
    *,
    config: str,
    state_dir: str,
    exports: Sequence[str],
    operator_kwargs: Mapping[str, Any],
    tags: Sequence[str],
    load: bool = True,
    compact_exports: Sequence[str] = (),
    retries: int | None = None,
    retry_delay: int | None = None,
    task_env: Mapping[str, Mapping[str, str]] | None = None,
    task_queue: Mapping[str, str] | None = None,
) -> dict[str, Any]:
    """`build_batch_dag`: apply (and, with `load`, load and the named compactions) per export."""
    build = {
        "config": config,
        "state_dir": state_dir,
        "exports": list(exports),
        "load": load,
        "compact_exports": list(compact_exports),
        "operator_kwargs": dict(operator_kwargs),
        "tags": list(tags),
    }
    return _spec("batch", dag_id, build, retries, retry_delay, task_env, task_queue)


def cdc(
    dag_id: str,
    *,
    config: str,
    state_dir: str,
    export: str,
    tables: Sequence[str],
    operator_kwargs: Mapping[str, Any],
    tags: Sequence[str],
    load: bool = True,
    compact: bool = True,
    retries: int | None = None,
    retry_delay: int | None = None,
    task_env: Mapping[str, Mapping[str, str]] | None = None,
    task_queue: Mapping[str, str] | None = None,
) -> dict[str, Any]:
    """`build_cdc_dag`: the stream's `run`, then a load (and a compaction) per captured table."""
    build = {
        "config": config,
        "state_dir": state_dir,
        "export": export,
        "tables": list(tables),
        "load": load,
        "compact": compact,
        "operator_kwargs": dict(operator_kwargs),
        "tags": list(tags),
    }
    return _spec("cdc", dag_id, build, retries, retry_delay, task_env, task_queue)


def load_only(
    dag_id: str,
    *,
    config: str,
    state_dir: str,
    export: str,
    operator_kwargs: Mapping[str, Any],
    tags: Sequence[str],
    retries: int | None = None,
) -> dict[str, Any]:
    """One `load` task of one export, nothing before it (a DAG of its own, not a builder's)."""
    build = {
        "config": config,
        "state_dir": state_dir,
        "export": export,
        "operator_kwargs": dict(operator_kwargs),
        "tags": list(tags),
    }
    return _spec("load", dag_id, build, retries, None, None, None)


def task_ids(spec: Mapping[str, Any]) -> list[str]:
    """The rivet tasks the builder makes for a spec (not its plan, barriers or watcher): what the host grades."""
    build = spec["build"]
    if spec["kind"] == "load":
        return ["load"]
    if spec["kind"] == "batch":
        compacted = set(build["compact_exports"])
        return [
            f"{group}.{export}"
            for export in build["exports"]
            for group in ("apply", *(("load",) if build["load"] else ()), *(("compact",) if export in compacted else ()))
        ]
    groups = (*(("load",) if build["load"] else ()), *(("compact",) if build["load"] and build["compact"] else ()))
    return ["run", *(f"{group}.{table}" for table in build["tables"] for group in groups)]


def write(rivet_dir: Path, leg: str, specs: Sequence[Mapping[str, Any]]) -> None:
    """Publish a leg's specs for the stack's DAG processor (whole or not at all: a half-written file is a parse error)."""
    target = rivet_dir / "configs" / f"{SPEC_PREFIX}{leg}.json"
    ids = [s["dag_id"] for s in specs]
    if len(set(ids)) != len(ids):
        raise ValueError(f"{leg}: DAG ids repeat in {ids}")
    for spec in specs:
        if spec["kind"] not in KINDS:
            raise ValueError(f"{leg}: {spec['dag_id']} has kind {spec['kind']!r}, expected one of {KINDS}")
    staged = target.with_suffix(".tmp")
    staged.write_text(json.dumps({"dags": list(specs)}))
    os.replace(staged, target)
