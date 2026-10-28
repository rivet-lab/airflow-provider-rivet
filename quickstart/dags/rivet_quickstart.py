"""rivet quickstart: one DAG per pipeline, each built from its own config in dags/rivet/<pipeline>.yaml.

A pipeline reads its source through the Connection named in PIPELINES (the config's `url_env: RIVET_SOURCE_URL`),
keeps its state in the database behind `rivet_state_<pipeline>` and its files in `state_dir`, stages Parquet in the
bucket its config names (credentials from the `storage` Connection) and loads ClickHouse (password from the
`warehouse` Connection). A config with `mode: cdc` exports becomes a CDC DAG (a group per stream, then a load per table);
any other becomes a batch DAG (an extract and a load per export). To add a pipeline, follow docs/quickstart.md: generate
its config with `rivet init`, then add one line here.
"""

from __future__ import annotations

from datetime import datetime
from pathlib import Path

import yaml

from airflow_provider_rivet.dags import build_batch_dag, build_cdc_streams_dag
from airflow_provider_rivet.exports import entries_of, mode_of, name_of, tables_of

#: pipeline name -> its Connections: `source` (required), `storage` and `warehouse` (default: the stack's own)
PIPELINES = {
    "mysql_cdc": {"source": "rivet_mysql"},
    "mysql_batch": {"source": "rivet_mysql"},
    "postgres_cdc": {"source": "rivet_postgres"},
    "postgres_batch": {"source": "rivet_postgres"},
}
CONFIG_DIR = Path(__file__).resolve().parent / "rivet"
STATE_ROOT = Path("/opt/airflow/rivet-state")
SCHEDULE = "*/10 * * * *"

for pipeline, conns in PIPELINES.items():
    config = CONFIG_DIR / f"{pipeline}.yaml"
    exports = entries_of(yaml.safe_load(config.read_text()))
    streams = [e for e in exports if mode_of(e) == "cdc"]
    common = {
        "config": str(config),
        "state_dir": str(STATE_ROOT / pipeline),
        "schedule": SCHEDULE,
        "start_date": datetime(2026, 1, 1),  # in the past: a future start_date gives runs with no tasks
        "catchup": False,
        "operator_kwargs": {
            "env_from_connections": {"RIVET_SOURCE_URL": conns["source"], "RIVET_STATE_URL": f"rivet_state_{pipeline}"},
            "connections": [conns.get("storage", "rivet_s3"), conns.get("warehouse", "rivet_clickhouse")],
        },
    }
    if streams:
        # ClickHouse collapses a change log by key itself: there is nothing to compact.
        # init writes one stream per table outside PostgreSQL `public`, on SQL Server, Oracle and MongoDB: a group each
        dag = build_cdc_streams_dag(
            f"rivet_{pipeline}", streams=[(name_of(e), tables_of(e)) for e in streams], compact=False, **common
        )
    else:
        dag = build_batch_dag(f"rivet_{pipeline}", exports=[name_of(e) for e in exports], **common)
    globals()[dag.dag_id] = dag
