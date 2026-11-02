"""Every public DAG builder keeps one contract: an owned key is refused, a valid build gives its tasks the builder's own, and a `${VAR}` builds the DAG of its resolved value or a coded refusal."""

from __future__ import annotations

from pathlib import Path
from typing import Any

import pytest
import yaml

from airflow_provider_rivet.dags import OWNED_BY_BUILDERS, build_batch_dag, build_cdc_dag, build_config_dag, build_source_dags
from airflow_provider_rivet.operators import _RivetTask

STATE = "/state/shop"
BUILDERS = ["batch", "cdc", "config", "sources"]
OWNED_VALUES = {"trigger_rule": "all_done", "config": "/other.yaml", "state_dir": "/other", "max_active_tis_per_dag": 4}
PLACEHOLDER_CODES = {"RIVET_AIRFLOW_CONFIG_UNRESOLVED", "RIVET_AIRFLOW_CONFIG_HAS_CDC_EXPORT"}
BROKEN = "source:\n  type: mysql\nexports:\n  - name: orders\n   mode: full\n"


def config_text(
    kind: str,
    *,
    mode: Any = None,
    target: Any = "bigquery",
    top_layout: Any = "base_buffer",
    layout: Any = None,
    name: Any = None,
    load: Any = None,
) -> str:
    """A rivet config of one batch export, or of a CDC stream and a second one whose `mode` is given."""
    if kind == "cdc":
        first: dict[str, Any] = {
            "name": name or "shop_cdc",
            "mode": "cdc",
            "tables": ["orders"],
            "cdc": {"checkpoint": "./cdc/shop_cdc.ckpt"},
        }
        exports = [first, {"name": "other", "mode": mode or "cdc", "tables": ["t2"], "cdc": {"checkpoint": "./cdc/o.ckpt"}}]
        source = {"type": "mysql", "url_env": "RIVET_SOURCE_URL"}
    else:
        first = {"name": name or "orders", "table": "orders", "mode": mode or "incremental"}
        exports = [first]
        source = {"type": "postgres", "url_env": "RIVET_SOURCE_URL"}
    if layout is not None:
        first["load"] = {"layout": layout}
    section: dict[str, Any] = {"target": target, "project": "p", "dataset": "d"}
    if top_layout is not None:
        section["layout"] = top_layout
    doc = {"source": source, "load": section if load is None else load, "exports": exports}
    return yaml.safe_dump(doc, sort_keys=False)


def built(builder: str, root: Path, operator_kwargs: dict[str, Any] | None = None) -> tuple[list[Any], str]:
    """The DAGs one builder returns for a valid batch config, and the config path its tasks must carry."""
    root.mkdir(parents=True, exist_ok=True)
    cfg = root / "shop.yaml"
    cfg.write_text(config_text("batch"))
    if builder == "batch":
        return [build_batch_dag("d", config=str(cfg), state_dir=STATE, exports=["orders"], operator_kwargs=operator_kwargs)], str(
            cfg
        )
    if builder == "cdc":
        dag = build_cdc_dag(
            "d", config=str(cfg), state_dir=STATE, export="shop_cdc", tables=["orders"], operator_kwargs=operator_kwargs
        )
        return [dag], str(cfg)
    if builder == "config":
        return [build_config_dag("d", config=str(cfg), state_dir=STATE, operator_kwargs=operator_kwargs)], str(cfg)
    dags = build_source_dags(
        ["shop"],
        mode="batch",
        config_dir=str(root),
        state_root="/state",
        state_connection="st",
        operator_kwargs=operator_kwargs,
    )
    return list(dags.values()), str(cfg)


def dag_of(builder: str, kind: str, root: Path, text: str) -> Any:
    """The one DAG a config-reading builder returns for a config text of a batch or CDC export."""
    root.mkdir(parents=True, exist_ok=True)
    (root / "shop.yaml").write_text(text)
    if builder == "config":
        return build_config_dag("d", config=str(root / "shop.yaml"), state_dir=STATE)
    dags = build_source_dags(
        ["shop"],
        mode="cdc" if kind == "cdc" else "batch",
        config_dir=str(root),
        state_root="/state",
        state_connection="st",
    )
    (dag,) = dags.values()
    return dag


def refusal_of(dag: Any) -> str | None:
    """The code of a refused DAG (one task, `config`), or None for a DAG that builds."""
    return dag.get_task("config").code if dag.task_ids == ["config"] else None


def shape(dag: Any) -> tuple[list[str], list[tuple[str, str]]]:
    """The task ids and the (upstream, downstream) edges of a DAG."""
    return sorted(dag.task_ids), sorted((t.task_id, d) for t in dag.tasks for d in t.downstream_task_ids)


CASES = [
    pytest.param("batch", {}, {"mode": "${MODE}"}, id="batch-export-mode"),
    pytest.param("batch", {}, {"target": "${TARGET}"}, id="batch-load-target"),
    pytest.param("batch", {}, {"top_layout": "${LAYOUT}"}, id="batch-load-layout"),
    pytest.param(
        "batch",
        {"top_layout": None, "layout": "base_buffer"},
        {"top_layout": None, "layout": "${LAYOUT}"},
        id="batch-export-layout",
    ),
    pytest.param("cdc", {}, {"mode": "${MODE}"}, id="cdc-export-mode"),
    pytest.param("cdc", {"layout": "base_buffer"}, {"layout": "${LAYOUT}"}, id="cdc-export-layout"),
    pytest.param("batch", {}, {"name": "${NAME}"}, id="batch-export-name"),
    pytest.param("batch", {"load": False}, {"load": "${LOAD}"}, id="batch-load-flag"),
]

LOAD_BQ = "load: {target: bigquery, project: p, dataset: d}\n"
MYSQL = "source: {type: mysql, url_env: RIVET_SOURCE_URL}\n"
RAW_FIELDS = [
    pytest.param(
        "cdc",
        MYSQL + LOAD_BQ + "exports:\n  - {name: s, mode: cdc, table: '${TABLE}', cdc: {checkpoint: ./s.ckpt}}\n",
        "`table`",
        id="cdc-export-table",
    ),
    pytest.param(
        "cdc",
        MYSQL + LOAD_BQ + "exports:\n  - {name: s, mode: cdc, tables: ['${TABLE}'], cdc: {checkpoint: ./s.ckpt}}\n",
        "`tables`",
        id="cdc-export-tables-item",
    ),
    pytest.param(
        "cdc",
        MYSQL + LOAD_BQ + "exports:\n  - {name: s, mode: cdc, tables: '${TABLES}', cdc: {checkpoint: ./s.ckpt}}\n",
        "`tables`",
        id="cdc-export-tables-whole",
    ),
    pytest.param(
        "cdc",
        MYSQL
        + LOAD_BQ
        + "exports:\n  - {name: s, mode: cdc, tables: [t], cdc: {checkpoint: ./s.ckpt, backfill: '${BACKFILL}'}}\n",
        "`cdc.backfill`",
        id="cdc-backfill",
    ),
    pytest.param(
        "batch",
        MYSQL
        + LOAD_BQ
        + "exports:\n  - {name: s, table: t, mode: chunked, chunk_by_key: [id], keyset_incremental: '${KEYSET}'}\n",
        "`keyset_incremental`",
        id="batch-keyset-incremental",
    ),
    pytest.param(
        "batch",
        MYSQL
        + LOAD_BQ
        + "exports:\n  - {name: s, table: t, mode: chunked, chunk_by_key: '${CHUNK}', keyset_incremental: true}\n",
        "`chunk_by_key`",
        id="batch-chunk-by-key",
    ),
    pytest.param(
        "batch",
        "source: {type: postgres, url_env: '${SOURCE_ENV}'}\n" + LOAD_BQ + "exports:\n  - {name: s, table: t}\n",
        "`source.url_env`",
        id="source-url-env",
    ),
    pytest.param(
        "batch",
        "source: {type: '${TYPE}', url_env: RIVET_SOURCE_URL}\n" + LOAD_BQ + "exports:\n  - {name: s, table: t}\n",
        "`source.type`",
        id="source-type",
    ),
    pytest.param(
        "batch",
        "source: {type: mongo, url_env: RIVET_SOURCE_URL, mongo: {resume: '${RESUME}', page_size: 10}}\n"
        + LOAD_BQ
        + "exports:\n  - {name: s, table: t}\n",
        "`source.mongo.resume`",
        id="source-mongo-resume",
    ),
    pytest.param(
        "batch",
        "source: {type: mongo, url_env: RIVET_SOURCE_URL, mongo: {resume: true, page_size: '${PAGE}'}}\n"
        + LOAD_BQ
        + "exports:\n  - {name: s, table: t}\n",
        "`source.mongo.page_size`",
        id="source-mongo-page-size",
    ),
    pytest.param("batch", MYSQL + LOAD_BQ + "exports:\n  - {name: '${NAME}', table: t}\n", "`name`", id="batch-export-name"),
    pytest.param("batch", MYSQL + "load: '${LOAD}'\nexports:\n  - {name: s, table: t}\n", "`load`", id="batch-load-flag"),
    pytest.param(
        "batch", MYSQL + "load: '${LOAD}'\nexports:\n  - {name: '${NAME}', table: t}\n", "`load`, `name`", id="two-fields"
    ),
    pytest.param(
        "batch",
        MYSQL + LOAD_BQ + "exports:\n  - {name: '${NAME}', table: t}\n  - {name: s, table: u}\n",
        "`name`",
        id="placeholder-in-first-export",
    ),
]


def test_the_owned_keys_checked_here_are_the_builders_owned_keys() -> None:
    """Every key a builder owns is in the table below, so a new owned key is tested on every builder."""
    assert set(OWNED_VALUES) == set(OWNED_BY_BUILDERS)


@pytest.mark.parametrize("builder", BUILDERS)
def test_a_valid_build_gives_every_rivet_task_the_builders_config_folder_and_gate(tmp_path: Path, builder: str) -> None:
    """Every rivet task carries the builder's config and state folder, one slot, the gate and the operator_kwargs."""
    dags, cfg = built(builder, tmp_path, {"retries": 3})
    assert len(dags) >= 1
    for dag in dags:
        assert "watcher" in dag.task_ids
        rivet = [t for t in dag.tasks if isinstance(t, _RivetTask)]
        assert len(rivet) >= 1
        for t in rivet:
            assert (t.config, t.state_dir, t.max_active_tis_per_dag, t.trigger_rule, t.retries) == (
                cfg,
                STATE,
                1,
                "all_success",
                3,
            )


@pytest.mark.parametrize("key", sorted(OWNED_VALUES))
@pytest.mark.parametrize("builder", BUILDERS)
def test_an_owned_key_in_operator_kwargs_is_refused_on_every_builder(tmp_path: Path, builder: str, key: str) -> None:
    """The key is refused by name, never overridden: the builder raises ValueError and builds nothing."""
    with pytest.raises(ValueError) as exc:
        built(builder, tmp_path, {key: OWNED_VALUES[key]})
    assert "operator_kwargs may not set" in str(exc.value)
    assert key in str(exc.value)


def test_a_broken_config_in_build_config_dag_is_a_refused_dag_not_an_exception(tmp_path: Path) -> None:
    """A config that cannot be read is one DAG of one task, `config`, that carries its refusal code."""
    (tmp_path / "shop.yaml").write_text(BROKEN)
    dag = build_config_dag("d", config=str(tmp_path / "shop.yaml"), state_dir=STATE)
    assert dag.task_ids == ["config"]
    assert dag.get_task("config").code == "RIVET_AIRFLOW_CONFIG_UNREADABLE"


def test_a_broken_source_config_fails_its_own_dag_and_not_the_others(tmp_path: Path) -> None:
    """One source's unreadable config is a refused DAG of that source; the other sources of the DAG file are built."""
    (tmp_path / "shop.yaml").write_text(BROKEN)
    (tmp_path / "ok.yaml").write_text(config_text("batch"))
    dags = build_source_dags(["shop", "ok"], mode="batch", config_dir=str(tmp_path), state_root="/state", state_connection="st")
    assert sorted(dags) == ["rivet_ok_batch", "rivet_shop_batch"]
    assert dags["rivet_shop_batch"].task_ids == ["config"]
    assert "watcher" in dags["rivet_ok_batch"].task_ids


@pytest.mark.parametrize("builder", ["config", "sources"])
@pytest.mark.parametrize(("kind", "resolved", "placeholder"), CASES)
def test_a_placeholder_builds_the_dag_of_its_resolved_value_or_a_coded_refusal(
    tmp_path: Path, builder: str, kind: str, resolved: dict[str, Any], placeholder: dict[str, Any]
) -> None:
    """A scalar written as `${VAR}` builds the same task graph as its resolved value, or is refused with a coded reason."""
    literal = dag_of(builder, kind, tmp_path / "resolved", config_text(kind, **resolved))
    assert refusal_of(literal) is None
    written = dag_of(builder, kind, tmp_path / "placeholder", config_text(kind, **placeholder))
    code = refusal_of(written)
    if code is not None:
        assert code in PLACEHOLDER_CODES
    else:
        assert shape(written) == shape(literal)


@pytest.mark.parametrize("builder", ["config", "sources"])
@pytest.mark.parametrize(("kind", "text", "fields"), RAW_FIELDS)
def test_a_placeholder_in_a_field_that_decides_the_tasks_is_refused_coded(
    tmp_path: Path, builder: str, kind: str, text: str, fields: str
) -> None:
    """A `${...}` the parse cannot resolve in a field that decides which tasks a DAG has is refused as UNRESOLVED, naming it."""
    dag = dag_of(builder, kind, tmp_path, text)
    pronoun = "them" if "," in fields else "it"
    assert dag.task_ids == ["config"]
    assert dag.get_task("config").refusal == (
        f"[RIVET_AIRFLOW_CONFIG_UNRESOLVED] {tmp_path / 'shop.yaml'} has a `${{...}}` in {fields}, which decides which tasks "
        f"the DAG has and cannot be resolved when the DAG is parsed: write {pronoun} as a literal"
    )


@pytest.mark.parametrize("builder", ["config", "sources"])
def test_a_bare_dollar_in_a_shape_field_is_a_name_and_builds(tmp_path: Path, builder: str) -> None:
    """A `$` without `{` is text, not a placeholder: the export keeps its name and the DAG builds."""
    text = "source: {type: mysql, url_env: RIVET_SOURCE_URL}\nload: {target: bigquery, project: p, dataset: d}\n"
    text += "exports:\n  - {name: 'a$b', table: t, mode: incremental}\n"
    assert refusal_of(dag_of(builder, "batch", tmp_path, text)) is None
