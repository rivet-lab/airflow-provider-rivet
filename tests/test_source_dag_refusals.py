"""build_source_dags: one source's config that cannot be built fails its own DAG, never the DAG file (F3, F5, F6, F9).

A DAG file is imported whole: an exception from one source's config used to drop every DAG the call builds. The
source whose config is refused now has a DAG of one task, `config`, which fails naming the refusal.
"""

from __future__ import annotations

from pathlib import Path
from typing import Any

import pytest
from conftest import FakeTI

from airflow_provider_rivet._compat import AirflowFailException
from airflow_provider_rivet.dags import build_batch_dag, build_source_dags
from airflow_provider_rivet.preflight import ConfigRefusal, PreflightRefusal, read_config_source

GOOD_CDC = """\
source: {type: mysql, url_env: RIVET_SOURCE_URL}
load: {target: bigquery, project: p, dataset: shop, layout: base_buffer}
exports:
  - name: shop_cdc
    mode: cdc
    tables: [orders]
    cdc: {checkpoint: ./cdc/shop_cdc.ckpt}
"""
GOOD_BATCH = """\
source: {type: mysql, url_env: RIVET_SOURCE_URL}
load: {target: bigquery, project: p, dataset: shop, layout: base_buffer}
exports:
  - name: orders
    table: orders
    mode: incremental
"""
BROKEN = "source:\n  type: mysql\nexports:\n  - name: orders\n   mode: full\n"


def build(tmp_path: Path, sources: list[str], mode: str, **kwargs: Any) -> dict:
    return build_source_dags(
        sources, mode=mode, config_dir=str(tmp_path), state_root="/srv/state", state_connection="st", **kwargs
    )


def fail(dag: Any) -> tuple[str, dict]:
    """Run the one task of a refused DAG: (the exception text, the XCom it published)."""
    ti = FakeTI(task_id="config")
    with pytest.raises(AirflowFailException) as exc:
        dag.get_task("config").execute({"ti": ti})
    return str(exc.value), ti.store[("config", "return_value")]


def test_a_malformed_config_names_its_line_and_column_and_what_is_wrong(tmp_path: Path) -> None:
    cfg = tmp_path / "c.yaml"
    cfg.write_text(BROKEN)
    with pytest.raises(PreflightRefusal) as exc:
        read_config_source(str(cfg))
    assert str(exc.value) == (
        f"[RIVET_AIRFLOW_CONFIG_UNREADABLE] cannot read config {cfg}: ParserError at line 5, column 4: "
        "expected <block end>, but found '<block mapping start>'"
    )


def test_a_config_that_is_not_yaml_text_still_names_only_its_type(tmp_path: Path) -> None:
    cfg = tmp_path / "c.yaml"
    cfg.write_bytes(b"\xff\xfe")
    with pytest.raises(PreflightRefusal) as exc:
        read_config_source(str(cfg))
    assert str(exc.value) == f"[RIVET_AIRFLOW_CONFIG_UNREADABLE] cannot read config {cfg}: UnicodeDecodeError"


@pytest.mark.parametrize("mode", ["cdc", "batch"])
def test_a_malformed_config_fails_its_own_dag_and_the_others_are_built(tmp_path: Path, mode: str) -> None:
    (tmp_path / "shop.yaml").write_text(GOOD_CDC if mode == "cdc" else GOOD_BATCH)
    (tmp_path / "billing.yaml").write_text(BROKEN)
    dags = build(tmp_path, ["shop", "billing", "crm"], mode)
    assert sorted(dags) == [f"rivet_billing_{mode}", f"rivet_crm_{mode}", f"rivet_shop_{mode}"]
    assert [t.task_id for t in dags[f"rivet_crm_{mode}"].tasks] == ["init"], "a source with no config yet is still init"
    assert "watcher" in dags[f"rivet_shop_{mode}"].task_ids
    refused = dags[f"rivet_billing_{mode}"]
    assert [t.task_id for t in refused.tasks] == ["config"]
    assert sorted(refused.tags) == sorted(["rivet", mode, "refused"])
    assert getattr(refused, "is_paused_upon_creation", None) is True and refused.max_active_runs == 1
    text, payload = fail(refused)
    assert text.startswith(
        f"[RIVET_AIRFLOW_CONFIG_UNREADABLE] cannot read config {tmp_path / 'billing.yaml'}: ParserError at line 5"
    )
    assert payload == {
        "command": "config",
        "decision": "fail",
        "preflight_refusal": "RIVET_AIRFLOW_CONFIG_UNREADABLE",
        "units": [],
    }
    assert refused.get_task("config").retries == 0


REFUSED = {
    "no_cdc_export_in_a_cdc_dag": (
        "cdc",
        GOOD_BATCH,
        "RIVET_AIRFLOW_CONFIG_NO_CDC_EXPORT",
        "{cfg} has no export with `mode: cdc`, so there is no stream for a CDC DAG; build this source with mode='batch', or "
        "delete the file to have `init` write it again with the CDC mode",
    ),
    "no_export_at_all_in_a_batch_dag": (
        "batch",
        "exports: []\n",
        "RIVET_AIRFLOW_CONFIG_NO_EXPORT",
        "{cfg} has no export; delete the file to have `init` write it again, or add the exports",
    ),
    "all_cdc_in_a_batch_dag": (
        "batch",
        GOOD_CDC,
        "RIVET_AIRFLOW_CONFIG_HAS_CDC_EXPORT",
        "{cfg} has `mode: cdc` exports (shop_cdc): a batch DAG over it would run on the config and the state database of "
        "the CDC DAG, and load the same tables beside it. A source in both a CDC and a batch DAG needs two names, or a "
        "`config_dir` and a `state_root` per mode: build the batch DAG from its own",
    ),
    "a_stream_beside_batch_exports_in_a_batch_dag": (
        "batch",
        GOOD_BATCH + "  - name: shop_cdc\n    mode: cdc\n    tables: [orders]\n",
        "RIVET_AIRFLOW_CONFIG_HAS_CDC_EXPORT",
        "{cfg} has `mode: cdc` exports (shop_cdc): a batch DAG over it would run on the config and the state database of "
        "the CDC DAG, and load the same tables beside it. A source in both a CDC and a batch DAG needs two names, or a "
        "`config_dir` and a `state_root` per mode: build the batch DAG from its own",
    ),
}


@pytest.mark.parametrize("mode, text, code, message", REFUSED.values(), ids=REFUSED.keys())
def test_a_config_the_builder_refuses_is_a_red_task_with_the_cause_and_the_remedy(
    tmp_path: Path, mode: str, text: str, code: str, message: str
) -> None:
    (tmp_path / "shop.yaml").write_text(text)
    (tmp_path / "other.yaml").write_text(GOOD_CDC if mode == "cdc" else GOOD_BATCH)
    dags = build(tmp_path, ["other", "shop"], mode)
    exc_text, payload = fail(dags[f"rivet_shop_{mode}"])
    assert exc_text == f"[{code}] " + message.format(cfg=tmp_path / "shop.yaml")
    assert payload["preflight_refusal"] == code
    assert "config" not in dags[f"rivet_other_{mode}"].task_ids


def test_a_config_shape_nobody_foresaw_is_shown_as_what_it_is_and_ends_no_dag_file(tmp_path: Path) -> None:
    (tmp_path / "shop.yaml").write_text("source: 5\nexports:\n  - name: orders\n    mode: full\n")
    (tmp_path / "billing.yaml").write_text(GOOD_BATCH)
    dags = build(tmp_path, ["billing", "shop"], "batch")
    text, payload = fail(dags["rivet_shop_batch"])
    assert text.startswith(
        f"[RIVET_AIRFLOW_CONFIG_UNBUILDABLE] cannot build a DAG from {tmp_path / 'shop.yaml'}: AttributeError: "
    )
    assert payload["preflight_refusal"] == "RIVET_AIRFLOW_CONFIG_UNBUILDABLE"
    assert "apply.orders" in dags["rivet_billing_batch"].task_ids


def test_a_numeric_export_name_in_one_config_leaves_the_other_sources_built(tmp_path: Path) -> None:
    (tmp_path / "shop.yaml").write_text(GOOD_BATCH.replace("name: orders", "name: 2024"))
    (tmp_path / "billing.yaml").write_text(GOOD_BATCH)
    dags = build(tmp_path, ["billing", "shop"], "batch")
    assert "apply.2024" in dags["rivet_shop_batch"].task_ids and "apply.orders" in dags["rivet_billing_batch"].task_ids


def test_a_refusal_the_caller_of_a_builder_caught_as_value_error_is_still_one() -> None:
    assert issubclass(ConfigRefusal, ValueError) and issubclass(ConfigRefusal, PreflightRefusal)
    assert str(ConfigRefusal("CODE", "text")) == "[CODE] text"


def test_a_wrong_dag_argument_is_still_an_import_error_not_a_red_dag(tmp_path: Path) -> None:
    (tmp_path / "shop.yaml").write_text(BROKEN)
    with pytest.raises(TypeError):
        build(tmp_path, ["shop"], "cdc", no_such_dag_argument=1)


# --- F5: an export named like an ordering point ---


def test_an_export_named_like_an_ordering_point_keeps_its_id_and_the_barrier_moves(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr(
        "airflow_provider_rivet.dags.read_plan_layout",
        lambda _: [
            {"wave": 1, "exports": ["sales", "after_sales", "wave_1_done"], "heavy": ["sales"]},
            {"wave": 2, "exports": ["tail"], "heavy": []},
        ],
    )
    dag = build_batch_dag("d", config="c", plan_file="p", load=False)
    ids = sorted(t for t in dag.task_ids if t.startswith("apply."))
    assert ids == [
        "apply.after_sales",
        "apply.after_sales_barrier",
        "apply.sales",
        "apply.tail",
        "apply.wave_1_done",
        "apply.wave_1_done_barrier",
    ]
    assert dag.get_task("apply.after_sales").export == "after_sales" and dag.get_task("apply.wave_1_done").export == "wave_1_done"
    assert dag.get_task("apply.after_sales_barrier").upstream_task_ids == {"apply.sales"}
    assert dag.get_task("apply.after_sales").upstream_task_ids == {"plan", "apply.after_sales_barrier"}
    assert dag.get_task("apply.tail").upstream_task_ids == {"plan", "apply.wave_1_done_barrier"}


def test_barrier_ids_a_dag_without_a_clash_are_what_they_were(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(
        "airflow_provider_rivet.dags.read_plan_layout",
        lambda _: [
            {"wave": 1, "exports": ["big", "a"], "heavy": ["big"]},
            {"wave": 2, "exports": ["b"], "heavy": []},
        ],
    )
    dag = build_batch_dag("d", config="c", plan_file="p", load=False)
    assert sorted(t for t in dag.task_ids if t.startswith("apply.")) == [
        "apply.a",
        "apply.after_big",
        "apply.b",
        "apply.big",
        "apply.wave_1_done",
    ]
