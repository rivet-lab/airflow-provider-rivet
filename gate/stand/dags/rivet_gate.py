"""Gate DAGs: one DAG per spec a gate leg wrote (gate/dagspec.py), built as the spec says.

Every decision (ids, configs, state_dirs, Connections, pause windows, queues, retries) is the leg's, made on the host
where it also grades the result; this file only turns each spec into the provider's DAG. A leg that is not in the run has
no spec file and so no DAG.
"""

from __future__ import annotations

import glob
import json
from datetime import datetime, timedelta

from airflow_provider_rivet._compat import DAG
from airflow_provider_rivet.dags import build_batch_dag, build_cdc_dag
from airflow_provider_rivet.operators import RivetLoadOperator

SPECS = "/opt/rivet/configs/dag_specs_*.json"
BUILDERS = {"batch": build_batch_dag, "cdc": build_cdc_dag}


def build(spec: dict) -> DAG:
    """The DAG of one spec."""
    default_args = dict(spec["default_args"])
    if "retry_delay" in default_args:
        default_args["retry_delay"] = timedelta(seconds=default_args["retry_delay"])
    extra = {"default_args": default_args} if default_args else {}
    args = spec["build"]
    if spec["kind"] == "load":
        # One load of one export, nothing around it; nothing retries unless the spec says so.
        with DAG(
            dag_id=spec["dag_id"], schedule=None, start_date=datetime(2026, 1, 1), catchup=False, max_active_runs=1,
            tags=args["tags"], **extra,
        ) as dag:  # fmt: skip
            RivetLoadOperator(
                task_id="load",
                export=args["export"],
                config=args["config"],
                state_dir=args["state_dir"],
                **args["operator_kwargs"],
            )
        return dag
    dag = BUILDERS[spec["kind"]](spec["dag_id"], **args, **extra)
    for task_id, env in spec["task_env"].items():
        dag.get_task(task_id).env = env
    for task in dag.tasks:
        if task.task_id in spec["task_queue"]:
            task.queue = spec["task_queue"][task.task_id]
    return dag


for spec_file in sorted(glob.glob(SPECS)):
    with open(spec_file) as handle:
        for spec in json.load(handle)["dags"]:
            globals()[spec["dag_id"]] = build(spec)
