"""Task ids are one naming for every builder: a safe id is the name, any other is a hash-tagged id, and CDC and config agree."""

from __future__ import annotations

import re
from pathlib import Path
from typing import Any

import yaml
from hypothesis import assume, given
from hypothesis import strategies as st

from airflow_provider_rivet.dags import build_cdc_dag, build_config_dag, task_name

SAFE = re.compile(r"[A-Za-z0-9_.-]+")
STATE = "/state/shop"
TABLE = st.from_regex(r"[A-Za-z][A-Za-z0-9_]{0,6}(\.[A-Za-z][A-Za-z0-9_]{0,6})?", fullmatch=True)
EXPORT = st.one_of(
    st.from_regex(r"[a-z][a-z0-9_]{0,10}", fullmatch=True),
    st.sampled_from(["watcher", "run", "load", "compact", "wave_1_done", "after_x", "daily orders", "orders:v2"]),
)


def _captured_ids(dag: Any) -> list[str]:
    """The load and compact task ids of a DAG, sorted."""
    return sorted(t for t in dag.task_ids if t.startswith(("load.", "compact.")))


def _write_config(path: Path, exports: list[dict[str, Any]]) -> str:
    """A rivet config of the given exports, with a BigQuery load block and the base_buffer layout."""
    doc = {
        "source": {"type": "mysql", "url_env": "RIVET_SOURCE_URL"},
        "load": {"target": "bigquery", "project": "p", "dataset": "d", "layout": "base_buffer"},
        "exports": exports,
    }
    path.write_text(yaml.safe_dump(doc, sort_keys=False))
    return str(path)


def _cdc(name: str, table: str, checkpoint: str) -> dict[str, Any]:
    """A CDC export of one table."""
    return {"name": name, "mode": "cdc", "tables": [table], "cdc": {"checkpoint": checkpoint}}


@given(st.text(max_size=40))
def test_a_task_id_part_is_the_name_when_safe_and_hash_tagged_otherwise(name: str) -> None:
    """A safe name is its own id; any other name gives an id of the safe alphabet that is not the name."""
    result = task_name(name)
    assert SAFE.fullmatch(result)
    if name and SAFE.fullmatch(name):
        assert result == name


@given(st.text(max_size=20), st.text(max_size=20))
def test_two_different_names_never_share_a_task_id(first: str, second: str) -> None:
    """The hash is of the real name, so `daily orders` and `daily_orders` never meet in one id."""
    assume(first != second)
    assert task_name(first) != task_name(second)


@given(export=st.from_regex(r"[a-z][a-z0-9_]{0,8}", fullmatch=True), table=TABLE)
def test_the_cdc_and_config_builders_name_a_captured_table_alike(export: str, table: str, tmp_path: Path) -> None:
    """A captured table gets the same load and compact task ids from build_cdc_dag and from build_config_dag."""
    config = _write_config(tmp_path / "shop.yaml", [_cdc(export, table, "./cdc/x.ckpt")])
    from_config = build_config_dag("d", config=config, state_dir=STATE)
    from_cdc = build_cdc_dag("d", config=config, state_dir=STATE, export=export, tables=[table])
    assert _captured_ids(from_config) == _captured_ids(from_cdc) and _captured_ids(from_cdc)


@given(export=EXPORT)
def test_any_export_name_builds_a_dag_or_a_coded_refusal(export: str, tmp_path: Path) -> None:
    """Two CDC streams under any export name build a DAG, or a DAG that refuses with a code; no builder raises."""
    assume(export != "other_stream")
    config = _write_config(
        tmp_path / "shop.yaml", [_cdc(export, "orders", "./cdc/a.ckpt"), _cdc("other_stream", "t2", "./cdc/b.ckpt")]
    )
    dag = build_config_dag("d", config=config, state_dir=STATE)
    if dag.task_ids == ["config"]:
        assert dag.get_task("config").code.startswith("RIVET_AIRFLOW_CONFIG_")
    else:
        assert len(dag.task_ids) == len(set(dag.task_ids))


def test_an_export_named_watcher_beside_another_cdc_stream_is_refused_by_its_own_code(tmp_path: Path) -> None:
    """The watcher id is taken by an export beside another CDC stream: refused as RESERVED_EXPORT_NAME, not a generic code."""
    config = _write_config(
        tmp_path / "shop.yaml", [_cdc("watcher", "orders", "./cdc/a.ckpt"), _cdc("other_stream", "t2", "./cdc/b.ckpt")]
    )
    dag = build_config_dag("d", config=config, state_dir=STATE)
    assert dag.task_ids == ["config"]
    assert dag.get_task("config").code == "RIVET_AIRFLOW_CONFIG_RESERVED_EXPORT_NAME"
