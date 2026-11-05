"""build_dags_from_file: one DAG per source of a sources file; the drift check of a config against it; the init's
config-folder check; the state database a moved stream would open empty."""

from __future__ import annotations

import importlib.util
import shutil
import sys
import types
from pathlib import Path
from typing import Any

import pytest
from conftest import DagRig, FakeTI
from test_source_dags import CDC_CONFIG, FAKE_RIVET, Init, connection, lookup

from airflow_provider_rivet._compat import AirflowFailException, AirflowSkipException
from airflow_provider_rivet.dags import DRIFT_GATE_ID, DRIFT_ID, build_cdc_dag, build_source_dags
from airflow_provider_rivet.operators import RivetCdcRunOperator, RivetSpecDriftOperator
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
        'from airflow_provider_rivet.dags import build_dags_from_file; globals().update(build_dags_from_file("rivet_sources.yaml"))\n'
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
        "run",
        "watcher",
    ]
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
    assert DRIFT_ID not in dags["rivet_analytics_cdc"].task_ids


def test_a_config_that_captures_a_table_the_file_leaves_out_gets_the_drift_check(tmp_path: Path) -> None:
    folder = dags_folder(tmp_path, shop="a", analytics="pageviews, visitors, clicks")
    dag = parse(folder)["rivet_analytics_cdc"]
    check, gate = dag.get_task(DRIFT_ID), dag.get_task(DRIFT_GATE_ID)
    assert isinstance(check, RivetSpecDriftOperator) and check.retries == 0
    assert check.include == ["pageviews", "visitors"] and check.exclude == []
    assert check.init_args[-3:] == ["--include", "pageviews", "visitors"]
    assert check.env_from_connections["RIVET_SOURCE_URL"]["conn_id"] == "rivet_analytics"
    assert gate.trigger_rule == "none_skipped" and check.downstream_task_ids == {DRIFT_GATE_ID, "watcher"}
    assert gate.downstream_task_ids == {"run", "watcher"}
    assert dag.get_task("run").upstream_task_ids == {DRIFT_GATE_ID}


def test_a_source_with_no_config_waits_for_the_template_s_config(tmp_path: Path) -> None:
    dags = parse(dags_folder(tmp_path))
    assert [t.task_id for t in dags["rivet_shop_cdc"].tasks] == ["init"]
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


STATE_FAKE = FAKE_RIVET + (
    'elif argv[0] == "state":\n'
    '    print("ledger read", file=sys.stderr)\n'
    '    print(json.dumps(spec.get("runs", [])))\n'
    '    sys.exit(spec.get("state_exit", 0))\n'
)
OLD = CDC_CONFIG.format(env="RIVET_SOURCE_URL").replace("tables: [orders, customers]", "tables: [orders, customers, payments]")


class Drift(Init):
    """The init fake, which also answers `rivet state runs`, and a config that captures `payments` beside the two
    tables the sources file includes."""

    def __init__(self, tmp: Path, monkeypatch: pytest.MonkeyPatch) -> None:
        super().__init__(tmp, monkeypatch)
        (tmp / "bin" / "rivet").write_text(f"#!{sys.executable}\n{STATE_FAKE}")
        self.config.parent.mkdir(parents=True)
        self.config.write_text(OLD)

    def run(self, **kwargs: Any) -> Any:
        op = RivetSpecDriftOperator(
            task_id=DRIFT_ID,
            mode="cdc",
            source_env="RIVET_SOURCE_URL",
            include=["orders", "customers"],
            init_args=["--include", "orders", "customers"],
            config=str(self.config),
            state_dir=str(self.tmp / "state" / "shop"),
            rivet_bin=str(self.tmp / "bin" / "rivet"),
            env_from_connections={"RIVET_SOURCE_URL": {"conn_id": "shop", "odbc_ini": str(self.tmp / "odbc.ini")}},
            **kwargs,
        )
        try:
            return op.execute({"ti": FakeTI(task_id=DRIFT_ID), "run_id": "r"})
        except Exception as exc:
            return exc

    def aside(self) -> list[Path]:
        return sorted((self.config.parent / ".replaced").glob("*.yaml"))


@pytest.fixture
def drifted(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Drift:
    return Drift(tmp_path, monkeypatch)


def test_a_stream_that_has_not_started_is_written_again_and_the_run_skipped(drifted: Drift) -> None:
    out = drifted.run()
    assert isinstance(out, AirflowSkipException), out
    assert "written again by `rivet init`" in str(out) and "customers, orders" in str(out)
    assert drifted.config.read_text() == CDC_CONFIG.format(env="RIVET_SOURCE_URL")
    [aside] = drifted.aside()
    assert aside.read_text() == OLD and aside.name.startswith("shop.") and str(aside) in str(out)
    argv = [c["argv"] for c in drifted.calls()]
    assert argv[0][:3] == ["state", "runs", "--config"] and argv[0][-3:] == ["--json", "--last", "1"]
    assert argv[1][0] == "init" and argv[1][-3:] == ["--include", "orders", "customers"]
    assert argv[2][0] == "check"


@pytest.mark.parametrize(
    ("setup", "said"),
    [
        ("checkpoint", "the checkpoint"),
        ("runs", "rivet's run ledger has a run (r-1)"),
        ("state_exit", "rivet's run ledger could not be read"),
    ],
)
def test_a_stream_that_has_started_is_not_written_and_the_task_names_each_place(drifted: Drift, setup: str, said: str) -> None:
    if setup == "checkpoint":
        (drifted.tmp / "state" / "shop" / "cdc").mkdir(parents=True)
        (drifted.tmp / "state" / "shop" / "cdc" / "shop_cdc.ckpt").write_text("pos")
    else:
        drifted.spec(**({"runs": [{"run_id": "r-1"}]} if setup == "runs" else {"state_exit": 2}))
    out = drifted.run()
    assert isinstance(out, AirflowFailException), out
    assert said in str(out) and "captures payments" in str(out) and "`tables:` of export `shop_cdc`" in str(out)
    assert drifted.config.read_text() == OLD and not drifted.aside()
    assert [c["argv"][0] for c in drifted.calls()] == ["state"]


def test_a_failed_rewrite_puts_the_old_config_back(drifted: Drift) -> None:
    drifted.spec(init_exit=1)
    out = drifted.run()
    assert isinstance(out, AirflowFailException) and "no config was written" in str(out)
    assert drifted.config.read_text() == OLD


def test_a_config_that_matches_runs_nothing(drifted: Drift) -> None:
    drifted.config.write_text(CDC_CONFIG.format(env="RIVET_SOURCE_URL"))
    assert drifted.run() == {"config": str(drifted.config), "drift": False}
    assert drifted.calls() == []


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

    def __init__(self, count: int) -> None:
        self.count, self.dsn = count, []

    def connect(self, dsn: str) -> Any:
        self.dsn.append(dsn)
        return types.SimpleNamespace(cursor=lambda: _Cursor(self.count), close=lambda: None)


class _Cursor:
    def __init__(self, count: int) -> None:
        self.count = count

    def __enter__(self) -> _Cursor:
        return self

    def __exit__(self, *exc: Any) -> None:
        return None

    def execute(self, sql: str) -> None:
        assert "information_schema.tables" in sql

    def fetchone(self) -> tuple[int]:
        return (self.count,)


def cdc_run(tmp_path: Path, monkeypatch: pytest.MonkeyPatch, own: str | None, count: int, checkpoint: bool) -> Any:
    tables = Tables(count)
    monkeypatch.setitem(sys.modules, "psycopg2", types.SimpleNamespace(connect=tables.connect, Error=Exception))
    lookup(monkeypatch, {"rivet_state_aa": connection("rivet_state_aa", "postgres", schema=own)})
    config = tmp_path / "cfg" / "shop.yaml"
    config.parent.mkdir()
    config.write_text(CDC_CONFIG.format(env="RIVET_SOURCE_URL"))
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


DRIFT_DAG = """\
import os
from airflow_provider_rivet.dags import _with_drift_check, build_cdc_dag
kwargs = {kwargs!r}
kwargs['operator_kwargs']['env'] = {{'RIVET_PG_URL': os.environ['RIVET_PG_URL']}}
dag = build_cdc_dag({dag_id!r}, **kwargs)
_with_drift_check(dag, 'cdc', (['orders'], [], []), {{
    'config': kwargs['config'], 'state_dir': kwargs['state_dir'],
    'operator_kwargs': {{'env_from_connections': {{'RIVET_SOURCE_URL': 'x'}}}},
}})
"""


def drift_run(rig: DagRig, monkeypatch: pytest.MonkeyPatch, dag_id: str, outcome: type[Exception]) -> tuple[str, dict]:
    def execute(self: Any, context: Any, **kwargs: Any) -> None:
        (rig.home / "drift_ran").write_text("skip" if outcome is AirflowSkipException else "fail")
        raise outcome("drift")

    monkeypatch.setattr(RivetSpecDriftOperator, "execute", execute)
    rig.config.write_text("source:\n  type: postgres\n  url_env: RIVET_PG_URL\nexports:\n  - {name: orders_cdc, mode: cdc}\n")
    kwargs = {
        "config": str(rig.config),
        "state_dir": str(rig.state_dir),
        "export": "orders_cdc",
        "tables": ["orders"],
        "default_args": {"retries": 0},
        "operator_kwargs": {"rivet_bin": str(rig.bin / "rivet"), "deployment": "local"},
    }
    return rig.run_source(dag_id, DRIFT_DAG.format(kwargs=kwargs, dag_id=dag_id))


def test_a_rewritten_config_skips_the_rest_of_the_run(dag_rig: DagRig, monkeypatch: pytest.MonkeyPatch) -> None:
    state, tasks = drift_run(dag_rig, monkeypatch, "drift_rewritten", AirflowSkipException)
    assert tasks == {t: "skipped" for t in tasks} and state == "success", tasks
    assert dag_rig.ran() == []


def test_a_drift_that_cannot_be_fixed_fails_the_run_and_the_stream_goes_on(
    dag_rig: DagRig, monkeypatch: pytest.MonkeyPatch
) -> None:
    state, tasks = drift_run(dag_rig, monkeypatch, "drift_started", AirflowFailException)
    assert tasks[DRIFT_ID] == "failed" and tasks[DRIFT_GATE_ID] == "success"
    assert (dag_rig.home / "drift_ran").read_text() == "fail"
    assert (tasks["run"], tasks["load.orders"]) == ("success", "success")
    assert tasks["watcher"] == "failed" and state == "failed"


def test_build_cdc_dag_and_the_source_builder_keep_one_task_id_per_table(tmp_path: Path) -> None:
    config = tmp_path / "shop.yaml"
    config.write_text(STREAM.format(name="shop", tables="orders, customers"))
    before = build_cdc_dag("rivet_shop_cdc", config=str(config), export="shop_cdc", tables=["orders", "customers"])
    after = build_source_dags(["shop"], mode="cdc", config_dir=str(tmp_path), state_root="/s", state_connection="s")
    assert sorted(before.task_ids) == sorted(after["rivet_shop_cdc"].task_ids)
