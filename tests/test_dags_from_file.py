"""build_dags_from_file: one DAG per source of a sources file; the init's
config-folder check; the state database a moved stream would open empty."""

from __future__ import annotations

import importlib.util
import re
import shutil
import sys
import types
from pathlib import Path
from typing import Any

import pytest
from test_source_dags import CDC_CONFIG, Init, connection, lookup

from airflow_provider_rivet._compat import AirflowFailException
from airflow_provider_rivet.dags import build_cdc_dag, build_dags_from_file, build_source_dags
from airflow_provider_rivet.operators import RivetCdcRunOperator
from airflow_provider_rivet.preflight import PreflightRefusal
from airflow_provider_rivet.sources import SourcesFileError

ROOT = Path(__file__).resolve().parents[1]
STREAM = """\
source:
  type: mysql
  url_env: RIVET_SOURCE_URL
  tls: {{mode: disable}}
load:
  target: bigquery
  project: my-project
  dataset: rivet_{name}
  layout: base_buffer
exports:
  - name: {name}_cdc
    mode: cdc
    tables: [{tables}]
    cdc: {{checkpoint: ./cdc/{name}_cdc.ckpt}}
    destination: {{type: gcs, bucket: my-bucket, prefix: {name}/cdc/}}
"""
SOURCES_FILE = """\
version: 1
config_dir: rivet
state_root: /opt/airflow/rivet-state
state_connection: rivet_state_shop
source_connection: "rivet_{name}"
template: shop
defaults:
  bigquery_dataset: "rivet_{name}"
  schedule: "0 4-18/2 * * *"
  start_date: "2026-10-01"
  tls: disable
  cloud_credentials: [gcp]
sources:
  shop: {}
  billing: {}
  analytics:
    include: [pageviews, visitors]
"""


def dags_folder(tmp_path: Path, spec: str = SOURCES_FILE, **configs: str) -> Path:
    folder = tmp_path / "dags"
    (folder / "rivet").mkdir(parents=True)
    (folder / "rivet_sources.yaml").write_text(spec)
    for name, tables in configs.items():
        (folder / "rivet" / f"{name}.yaml").write_text(STREAM.format(name=name, tables=tables))
    (folder / "rivet_cdc.py").write_text(
        "from pathlib import Path\nfrom airflow_provider_rivet.dags import build_dags_from_file\n"
        'globals().update(build_dags_from_file(Path(__file__).with_name("rivet_sources.yaml")))\n'
    )
    return folder


def parse(folder: Path, file: str = "rivet_cdc.py") -> dict[str, Any]:
    spec = importlib.util.spec_from_file_location(f"dagfile_{abs(hash(folder))}", folder / file)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return {k: v for k, v in vars(module).items() if k.startswith("rivet_") and k.endswith(("_cdc", "_batch"))}


def test_the_one_line_dag_file_builds_a_dag_per_source_from_the_file_beside_it(tmp_path: Path) -> None:
    folder = dags_folder(tmp_path, shop="a, b", analytics="pageviews, visitors")
    dags = parse(folder)
    assert sorted(dags) == ["rivet_analytics_cdc", "rivet_billing_cdc", "rivet_shop_cdc"]
    assert sorted(t.task_id for t in dags["rivet_shop_cdc"].tasks) == [
        "compact.a",
        "compact.b",
        "load.a",
        "load.b",
        "preflight",
        "run",
        "watcher",
    ]
    preflight = dags["rivet_shop_cdc"].get_task("preflight")
    assert preflight.downstream_task_ids == {"run", "watcher"} and preflight.retries == 0
    init = dags["rivet_billing_cdc"].get_task("init")
    assert init.init_args == [
        "--tls",
        "disable",
        "--gcs-bucket",
        "my-bucket",
        "--bigquery-project",
        "my-project",
        "--bigquery-dataset",
        "rivet_billing",
    ]
    assert init.env_from_connections == {
        "RIVET_SOURCE_URL": {"conn_id": "rivet_billing", "odbc_ini": "/etc/odbc.ini", "dsn": "billing"},
        "RIVET_STATE_URL": {"conn_id": "rivet_state_shop", "database": "rivet_state_billing"},
    }
    assert init.cloud_credentials == ("gcp",) and init.state_dir == "/opt/airflow/rivet-state/billing"
    run = dags["rivet_shop_cdc"].get_task("run")
    assert run.env_from_connections["RIVET_STATE_URL"] == {
        "conn_id": "rivet_state_shop",
        "database": "rivet_state_shop",
    }
    assert run.config == str(folder / "rivet" / "shop.yaml")
    for dag in dags.values():
        assert getattr(dag, "is_paused_upon_creation", None) is True
        assert dag.start_date.year == 2026 and dag.start_date.month == 10


def test_preflight_carries_the_include_and_exclude_it_compares_the_config_with(tmp_path: Path) -> None:
    folder = dags_folder(tmp_path, shop="a", analytics="pageviews, visitors, clicks")
    check = parse(folder)["rivet_analytics_cdc"].get_task("preflight")
    assert (check.include, check.exclude) == (["pageviews", "visitors"], [])
    assert check.downstream_task_ids == {"run", "watcher"}
    spec = SOURCES_FILE.replace("    include: [pageviews, visitors]", "    exclude: [clicks]")
    check = parse(dags_folder(tmp_path / "x", spec, shop="a", analytics="pageviews"))["rivet_analytics_cdc"].get_task("preflight")
    assert (check.include, check.exclude) == ([], ["clicks"])


def test_a_relative_sources_file_is_refused_naming_the_absolute_form(tmp_path: Path) -> None:
    with pytest.raises(ValueError, match=r"rivet_sources.yaml is relative; .*Path\(__file__\).with_name\('rivet_sources.yaml'\)"):
        build_dags_from_file("rivet_sources.yaml")


def test_a_source_with_no_config_waits_for_the_template_s_config(tmp_path: Path) -> None:
    dags = parse(dags_folder(tmp_path))
    dag = dags["rivet_shop_cdc"]
    assert sorted(dag.task_ids) == ["init", "preflight"] and dag.get_task("init").upstream_task_ids == {"preflight"}
    refused = dags["rivet_analytics_cdc"].get_task("config")
    assert refused.code == "RIVET_AIRFLOW_SOURCES_FILE_REFUSED"
    assert "template `shop`" in refused.refusal and "run the DAG of `shop` first" in refused.refusal


def test_a_file_this_reader_refuses_is_an_import_error_naming_the_line(tmp_path: Path) -> None:
    folder = dags_folder(tmp_path, spec=SOURCES_FILE.replace("tls: disable", "tls: off"))
    with pytest.raises(SourcesFileError, match=r"rivet_sources.yaml:11:8: `defaults.tls`: `off` is not one of"):
        parse(folder)


def test_the_example_dag_file_builds_every_source_of_the_example_yaml(tmp_path: Path) -> None:
    folder = tmp_path / "dags"
    (folder / "rivet").mkdir(parents=True)
    for name in ("rivet_sources.py", "rivet_sources.yaml"):
        shutil.copy(ROOT / "examples" / name, folder / name)
    (folder / "rivet" / "shop.yaml").write_text(STREAM.format(name="shop", tables="orders"))
    dags = parse(folder, "rivet_sources.py")
    assert sorted(dags) == ["rivet_analytics_cdc", "rivet_billing_cdc", "rivet_shop_cdc"]
    init = dags["rivet_billing_cdc"].get_task("init")
    assert init.init_args == [
        "--tls",
        "disable",
        "--gcs-bucket",
        "my-bucket",
        "--bigquery-project",
        "my-project",
        "--bigquery-dataset",
        "billing",
        "--exclude",
        "*_tmp",
        "audit_log",
    ]


def test_state_database_none_keeps_the_connection_s_own_database(tmp_path: Path) -> None:
    dag = build_source_dags(
        ["shop"], mode="cdc", config_dir=str(tmp_path), state_root="/s", state_connection="rivet_state_shop", state_database=None
    )["rivet_shop_cdc"]
    assert dag.get_task("init").env_from_connections["RIVET_STATE_URL"] == {"conn_id": "rivet_state_shop"}


def test_globs_are_given_once(tmp_path: Path) -> None:
    with pytest.raises(ValueError, match=r"init_args set \['--include'\] beside `include`/`exclude`"):
        build_source_dags(
            ["shop"],
            mode="cdc",
            config_dir=str(tmp_path),
            state_root="/s",
            state_connection="s",
            include=["a"],
            init_args=["--include=b"],
        )


def test_init_refuses_a_config_folder_it_cannot_write_before_anything_runs(init: Init, monkeypatch: pytest.MonkeyPatch) -> None:
    def denied(*args: Any, **kwargs: Any) -> Any:
        raise PermissionError(13, "Permission denied")

    monkeypatch.setattr("airflow_provider_rivet.operators.tempfile.mkstemp", denied)
    out = init.run()
    assert isinstance(out, AirflowFailException), out
    text = str(out)
    assert f"{init.config.parent} is not writable by this worker" in text and "Permission denied" in text
    assert "sudo chown -R" in text and "chmod -R g+w" in text and "777" not in text
    assert init.calls() == [] and not init.config.exists()


@pytest.fixture
def init(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Init:
    return Init(tmp_path, monkeypatch)


class Tables:
    """A PostgreSQL server whose every database has `count` tables."""

    def __init__(self, count: int, ran: int = 0) -> None:
        self.count, self.ran, self.dsn = count, ran, []

    def connect(self, dsn: str) -> Any:
        self.dsn.append(dsn)
        return types.SimpleNamespace(cursor=lambda: _Cursor(self.count, self.ran), close=lambda: None)


class _Cursor:
    def __init__(self, count: int, ran: int = 0) -> None:
        self.count, self.ran, self.found = count, ran, count

    def __enter__(self) -> _Cursor:
        return self

    def __exit__(self, *exc: Any) -> None:
        return None

    def execute(self, sql: str, params: tuple[str]) -> None:
        assert "information_schema.tables" in sql
        self.found = self.ran if params == ("export_state",) else self.count

    def fetchone(self) -> tuple[int]:
        return (self.found,)


def cdc_run(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    own: str | None,
    count: int,
    checkpoint: bool,
    slot: bool = False,
    ran: int = 1,
) -> Any:
    tables = Tables(count, ran)
    monkeypatch.setitem(sys.modules, "psycopg2", types.SimpleNamespace(connect=tables.connect, Error=Exception))
    lookup(monkeypatch, {"rivet_state_aa": connection("rivet_state_aa", "postgres", schema=own)})
    config = tmp_path / "cfg" / "shop.yaml"
    config.parent.mkdir()
    text = CDC_CONFIG.format(env="RIVET_SOURCE_URL")
    config.write_text(re.sub(r"\n *checkpoint:[^\n]*", "", text) if slot else text)
    state = tmp_path / "state" / "shop"
    (state / "cdc").mkdir(parents=True)
    if checkpoint:
        (state / "cdc" / "shop_cdc.ckpt").write_text("mysql-bin.000042:4")
    op = RivetCdcRunOperator(
        task_id="run",
        config=str(config),
        export="shop_cdc",
        state_dir=str(state),
        env_from_connections={"RIVET_STATE_URL": {"conn_id": "rivet_state_aa", "database": "rivet_state_shop"}},
    )
    try:
        op._refuse_empty_state_of_a_started_stream()
    except PreflightRefusal as refusal:
        return refusal, tables
    return None, tables


def test_an_empty_state_database_beside_a_started_stream_s_checkpoint_is_refused(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    refusal, tables = cdc_run(tmp_path, monkeypatch, own="rivet_state_aa", count=0, checkpoint=True)
    assert refusal is not None and refusal.code == "RIVET_AIRFLOW_STATE_DATABASE_MISMATCH"
    text = str(refusal)
    assert "`rivet_state_shop`" in text and "names the database `rivet_state_aa`" in text
    assert "state_database: connection" in text and "pg_dump rivet_state_aa | psql rivet_state_shop" in text
    assert tables.dsn == ["postgresql://u:pw@rivet_state_aa.host/rivet_state_shop"]


@pytest.mark.parametrize(
    ("own", "count", "checkpoint"),
    [
        ("rivet_state_aa", 7, True),
        ("rivet_state_aa", 0, False),
        ("rivet_state_shop", 0, True),
        (None, 0, True),
    ],
)
def test_a_state_database_with_tables_a_first_run_or_the_connection_s_own_passes(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, own: str | None, count: int, checkpoint: bool
) -> None:
    assert cdc_run(tmp_path, monkeypatch, own, count, checkpoint)[0] is None


def test_an_empty_state_database_beside_a_slot_stream_that_ran_on_the_connection_s_own_is_refused(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    refusal, tables = cdc_run(tmp_path, monkeypatch, own="rivet_state_aa", count=0, checkpoint=False, slot=True)
    assert refusal is not None and "own database `rivet_state_aa` holds rivet's state (`export_state`)" in str(refusal)
    assert tables.dsn == [
        "postgresql://u:pw@rivet_state_aa.host/rivet_state_aa",
        "postgresql://u:pw@rivet_state_aa.host/rivet_state_shop",
    ]
    (tmp_path / "fresh").mkdir()
    fresh = cdc_run(tmp_path / "fresh", monkeypatch, own="rivet_state_aa", count=0, checkpoint=False, slot=True, ran=0)
    assert fresh[0] is None


def test_without_psycopg2_the_state_database_is_not_checked(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    refusal, _ = cdc_run(tmp_path, monkeypatch, own="rivet_state_aa", count=0, checkpoint=True)
    assert refusal is not None
    monkeypatch.setitem(sys.modules, "psycopg2", None)
    op = RivetCdcRunOperator(
        task_id="run",
        config=str(tmp_path / "cfg" / "shop.yaml"),
        export="shop_cdc",
        state_dir=str(tmp_path / "state" / "shop"),
        env_from_connections={"RIVET_STATE_URL": {"conn_id": "rivet_state_aa", "database": "rivet_state_shop"}},
    )
    op._refuse_empty_state_of_a_started_stream()


def test_build_cdc_dag_and_the_source_builder_keep_one_task_id_per_table(tmp_path: Path) -> None:
    config = tmp_path / "shop.yaml"
    config.write_text(STREAM.format(name="shop", tables="orders, customers"))
    before = build_cdc_dag("rivet_shop_cdc", config=str(config), export="shop_cdc", tables=["orders", "customers"])
    after = build_source_dags(["shop"], mode="cdc", config_dir=str(tmp_path), state_root="/s", state_connection="s")
    assert sorted(before.task_ids) == sorted(after["rivet_shop_cdc"].task_ids)
