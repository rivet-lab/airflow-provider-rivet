"""quickstart/: the DAG file builds one DAG per pipeline from its config, and the files that name a pipeline's
resources (compose services, demo SQL, configs, PIPELINES) name the same ones."""

from __future__ import annotations

import importlib.util
import re
import shutil
from datetime import datetime
from pathlib import Path

import pytest
import yaml

from airflow_provider_rivet.dags import build_cdc_dag

QUICKSTART = Path(__file__).resolve().parents[1] / "quickstart"
DAG_FILE = QUICKSTART / "dags" / "rivet_quickstart.py"
CONFIGS = QUICKSTART / "dags" / "rivet"
COMPOSE = {v: QUICKSTART / f"airflow-{v}" / "docker-compose.yaml" for v in ("2.10", "3.3")}
AIRFLOW_SERVICES = {
    "postgres",
    "airflow-init",
    "airflow-scheduler",
    "airflow-webserver",
    "airflow-apiserver",
    "airflow-dag-processor",
}


def load(dags: Path):
    spec = importlib.util.spec_from_file_location(f"rivet_quickstart_{id(dags)}", dags / "rivet_quickstart.py")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def config(name: str) -> dict:
    return yaml.safe_load((CONFIGS / f"{name}.yaml").read_text())


def compose(version: str) -> dict:
    return yaml.safe_load(COMPOSE[version].read_text())


def test_each_pipeline_is_a_dag_of_its_kind_with_its_own_state_and_connections() -> None:
    module = load(DAG_FILE.parent)
    assert set(module.PIPELINES) == {p.stem for p in CONFIGS.glob("*.yaml")}
    for pipeline, conns in module.PIPELINES.items():
        dag = getattr(module, f"rivet_{pipeline}")
        tasks = {t.task_id for t in dag.tasks}
        if pipeline.endswith("_cdc"):
            assert tasks == {"run", "load.customers", "load.orders", "watcher"}, pipeline  # ClickHouse: no compact
        else:
            assert tasks == {"apply.customers", "apply.orders", "load.customers", "load.orders", "watcher"}, pipeline
        for task in dag.tasks:
            if task.task_id == "watcher":
                continue
            assert task.config == str(CONFIGS / f"{pipeline}.yaml")
            assert task.state_dir == f"/opt/airflow/rivet-state/{pipeline}"
            assert task.env_from_connections == {
                "RIVET_SOURCE_URL": conns["source"],
                "RIVET_STATE_URL": f"rivet_state_{pipeline}",
            }
            assert tuple(task.connections) == ("rivet_s3", "rivet_clickhouse")


def test_a_cdc_config_of_one_table_and_own_storage_and_warehouse(tmp_path: Path) -> None:
    dags = tmp_path / "dags"
    shutil.copytree(DAG_FILE.parent, dags)
    (dags / "rivet" / "billing_cdc.yaml").write_text(
        "source: {type: postgres, url_env: RIVET_SOURCE_URL}\n"
        "exports:\n  - name: invoices_cdc\n    table: invoices\n    mode: cdc\n"
        "load: {target: clickhouse, url: 'http://clickhouse:8123', database: billing_cdc}\n"
    )
    text = (dags / "rivet_quickstart.py").read_text()
    line = '    "billing_cdc": {"source": "rivet_billing", "storage": "rivet_s3_aws", "warehouse": "rivet_clickhouse_own"},\n'
    (dags / "rivet_quickstart.py").write_text(text.replace("PIPELINES = {\n", "PIPELINES = {\n" + line, 1))
    dag = load(dags).rivet_billing_cdc
    assert {t.task_id for t in dag.tasks} == {"run", "load.invoices", "watcher"}
    assert tuple(dag.get_task("run").connections) == ("rivet_s3_aws", "rivet_clickhouse_own")


def test_a_config_of_several_streams_builds_a_group_per_stream_with_each_tables_load(tmp_path: Path) -> None:
    dags = tmp_path / "dags"
    shutil.copytree(DAG_FILE.parent, dags)
    (dags / "rivet" / "erp_cdc.yaml").write_text(
        "source: {type: sqlserver, url_env: RIVET_SOURCE_URL}\n"
        "exports:\n"
        "  - {name: dbo_orders, tables: [dbo.orders, dbo.lines], mode: cdc}\n"
        "  - {name: dbo_customers, table: dbo.customers, mode: cdc}\n"
        "load: {target: clickhouse, url: 'http://clickhouse:8123', database: erp_cdc}\n"
    )
    text = (dags / "rivet_quickstart.py").read_text()
    (dags / "rivet_quickstart.py").write_text(
        text.replace("PIPELINES = {\n", 'PIPELINES = {\n    "erp_cdc": {"source": "rivet_erp"},\n', 1)
    )
    dag = load(dags).rivet_erp_cdc
    assert set(dag.task_group.children) == {"dbo_orders", "dbo_customers", "watcher"}
    assert set(dag.task_ids) == {
        "dbo_orders.run",
        "dbo_orders.load.dbo_orders",
        "dbo_orders.load.dbo_lines",
        "dbo_customers.run",
        "dbo_customers.load.dbo_customers",
        "watcher",
    }  # ClickHouse: no compact, as for one stream; the warehouse names, as build_source_dags builds them
    for stream, tables in (("dbo_orders", ("dbo_orders", "dbo_lines")), ("dbo_customers", ("dbo_customers",))):
        run = dag.get_task(f"{stream}.run")
        assert run.export == stream and run.upstream_task_ids == set()
        for table in tables:
            task = dag.get_task(f"{stream}.load.{table}")
            assert (task.export, task.table) == (stream, table)
            assert task.upstream_task_ids == {f"{stream}.run"}, "a stream's load waits for its own drain"
    assert dag.get_task("watcher").upstream_task_ids == set(dag.task_ids) - {"watcher"}


def test_a_single_stream_builds_the_dag_build_cdc_dag_built_before_every_stream_was_built(tmp_path: Path) -> None:
    module = load(DAG_FILE.parent)
    for pipeline in ("mysql_cdc", "postgres_cdc"):
        stream = next(
            e for e in yaml.safe_load(CONFIGS.joinpath(f"{pipeline}.yaml").read_text())["exports"] if e.get("mode") == "cdc"
        )
        conns = module.PIPELINES[pipeline]
        before = build_cdc_dag(  # the call the quickstart made for its one stream before it built every stream
            f"rivet_{pipeline}",
            config=str(CONFIGS / f"{pipeline}.yaml"),
            export=stream["name"],
            tables=stream.get("tables") or [stream["table"]],
            compact=False,
            state_dir=f"/opt/airflow/rivet-state/{pipeline}",
            schedule="*/10 * * * *",
            start_date=datetime(2026, 1, 1),
            catchup=False,
            operator_kwargs={
                "env_from_connections": {"RIVET_SOURCE_URL": conns["source"], "RIVET_STATE_URL": f"rivet_state_{pipeline}"},
                "connections": ["rivet_s3", "rivet_clickhouse"],
            },
        )
        assert shape(getattr(module, f"rivet_{pipeline}")) == shape(before), pipeline
        assert set(before.task_group.children) == {"run", "load", "watcher"}, "one stream has no group"


def shape(dag) -> dict:
    """What a DAG is to Airflow: its settings, its groups, and each task with its edges and its arguments."""
    tasks = {
        t.task_id: {
            "class": type(t).__name__,
            "group": t.task_group.group_id if t.task_group else None,
            "upstream": sorted(t.upstream_task_ids),
            "downstream": sorted(t.downstream_task_ids),
            "trigger_rule": t.trigger_rule,
            "export": getattr(t, "export", None),
            "table": getattr(t, "table", None),
            "config": getattr(t, "config", None),
            "state_dir": getattr(t, "state_dir", None),
            "env_from_connections": getattr(t, "env_from_connections", None),
            "connections": tuple(getattr(t, "connections", None) or ()),
        }
        for t in dag.tasks
    }
    return {
        "dag_id": dag.dag_id,
        "schedule": timetable(dag.timetable),
        "start_date": dag.start_date,
        "catchup": dag.catchup,
        "max_active_runs": dag.max_active_runs,
        "tags": sorted(dag.tags),
        "default_args": dict(dag.default_args),
        "groups": sorted(dag.task_group.children),
        "tasks": tasks,
    }


def timetable(t) -> tuple:
    """A timetable by value: Airflow 2's repr is its address, and the cron fields are private there."""
    return (
        type(t).__name__,
        getattr(t, "expression", None) or getattr(t, "_expression", None),
        str(getattr(t, "timezone", None) or getattr(t, "_timezone", None)),
    )


def test_a_stream_without_a_written_mode_or_with_a_numeric_name_is_read_as_rivet_reads_it(tmp_path: Path) -> None:
    dags = tmp_path / "dags"
    shutil.copytree(DAG_FILE.parent, dags)
    (dags / "rivet" / "num_batch.yaml").write_text(
        "source: {type: postgres, url_env: RIVET_SOURCE_URL}\nexports:\n  - {name: 2024, table: t}\n"
        "load: {target: clickhouse, url: 'http://clickhouse:8123', database: num_batch}\n"
    )
    text = (dags / "rivet_quickstart.py").read_text()
    (dags / "rivet_quickstart.py").write_text(
        text.replace("PIPELINES = {\n", 'PIPELINES = {\n    "num_batch": {"source": "rivet_n"},\n', 1)
    )
    assert {t.task_id for t in load(dags).rivet_num_batch.tasks} == {"apply.2024", "load.2024", "watcher"}


def test_every_config_reads_the_source_the_dag_fills_and_batch_declares_its_key() -> None:
    for path in CONFIGS.glob("*.yaml"):
        doc = config(path.stem)
        assert doc["source"]["url_env"] == "RIVET_SOURCE_URL", path.name
        assert doc["load"]["target"] == "clickhouse" and doc["load"]["database"] == path.stem, path.name
        if path.stem.endswith("_batch"):
            # rivet#679: a `query:` export has no recorded key on a state `init` did not write
            assert doc["load"]["pk"] == ["id"], path.name


def test_the_two_compose_files_differ_only_in_airflow() -> None:
    a, b = compose("2.10"), compose("3.3")
    shared = lambda d: {k: v for k, v in d["services"].items() if k not in AIRFLOW_SERVICES}  # noqa: E731
    assert shared(a) == shared(b)
    conns = lambda d: {k: v for k, v in d["x-airflow-common"]["environment"].items() if k.startswith("AIRFLOW_CONN_")}  # noqa: E731
    assert conns(a) == conns(b)
    assert a["volumes"] == b["volumes"]


def test_every_pipeline_has_its_bucket_state_database_and_clickhouse_database() -> None:
    pipelines = set(load(DAG_FILE.parent).PIPELINES)
    services = compose("3.3")["services"]
    buckets = {arg.split("/", 1)[1] for arg in services["minio-bucket"]["entrypoint"] if arg.startswith("quickstart/")}
    assert buckets == {e["destination"]["bucket"] for p in pipelines for e in config(p)["exports"]}
    sql = (QUICKSTART / "demo" / "rivet-state.sql").read_text()
    assert set(re.findall(r"CREATE DATABASE rivet_state_(\w+)", sql)) == pipelines
    assert (
        set(re.findall(r"CREATE DATABASE IF NOT EXISTS (\w+)", (QUICKSTART / "demo" / "clickhouse.sql").read_text())) == pipelines
    )
    env = compose("3.3")["x-airflow-common"]["environment"]
    assert {
        k.removeprefix("AIRFLOW_CONN_RIVET_STATE_").lower() for k in env if k.startswith("AIRFLOW_CONN_RIVET_STATE_")
    } == pipelines
    init = " ".join(services["airflow-init"]["command"])
    assert set(re.search(r"rivet-state/\{([^}]*)\}", init).group(1).split(",")) == pipelines


@pytest.mark.parametrize("version", sorted(COMPOSE))
def test_compose_pins_its_airflow_version(version: str) -> None:
    common = compose(version)["x-airflow-common"]
    full = common["build"]["args"]["AIRFLOW_VERSION"]
    assert full.startswith(version + ".") and common["image"] == f"rivet-quickstart:{full}"


@pytest.mark.skipif(shutil.which("node") is None, reason="the command builder runs under node")
def test_every_placeholder_on_the_builder_page_is_filled_by_the_builder() -> None:
    import json
    import subprocess

    docs = Path(__file__).resolve().parents[1] / "docs"
    used = set(re.findall(r"\{\{([A-Z_]+)\}\}", (docs / "your-database.md").read_text()))
    script = "console.log(JSON.stringify(Object.keys(require(process.argv[1]).tokens({}))))"
    filled = set(
        json.loads(
            subprocess.run(
                ["node", "-e", script, str(docs / "your-database.js")], capture_output=True, text=True, check=True
            ).stdout
        )
    )
    assert used and used <= filled, sorted(used - filled)
