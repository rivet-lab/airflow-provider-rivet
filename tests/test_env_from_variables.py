"""`env_from_variables`: a source's environment variable filled from an Airflow Variable, read when a task runs.

The Variable of each source holds the Secret Manager name rivet reads (`url_secret: ${RIVET_SOURCE_URL_SECRET}`). It is
read by the task, never at DAG parse; a Variable no backend defines is a coded refusal of that source's tasks only; and
a source whose config names its secret reads no source Connection.
"""

from __future__ import annotations

from pathlib import Path
from typing import Any

import pytest
from conftest import CONFIG, FakeTI, Rig

from airflow_provider_rivet._compat import AirflowFailException, Variable
from airflow_provider_rivet.dags import build_source_dags
from airflow_provider_rivet.operators import RivetRunOperator
from airflow_provider_rivet.preflight import read_config

SECRET_VAR = "RIVET_SOURCE_URL_SECRET"
URL_SECRET_CONFIG = CONFIG.replace("  url_env: RIVET_PG_URL\n", "  url_secret: ${RIVET_SOURCE_URL_SECRET}\n")
CDC_URL_SECRET = """\
source:
  type: mysql
  url_secret: ${RIVET_SOURCE_URL_SECRET}
load:
  target: bigquery
  project: p
  dataset: shop
  layout: base_buffer
exports:
  - name: shop_cdc
    mode: cdc
    tables: [orders]
    cdc: {checkpoint: ./cdc/shop_cdc.ckpt}
"""
BATCH_PASSWORD_SECRET = """\
source:
  type: postgres
  host: db.internal
  user: rivet
  database: shop
  password_secret: projects/p/secrets/shop-pw
load:
  target: bigquery
  project: p
  dataset: shop
exports:
  - name: orders
    query: SELECT * FROM orders
    mode: full
"""


def stored(monkeypatch: pytest.MonkeyPatch, values: dict[str, str]) -> None:
    """`airflow.sdk.Variable` (or `airflow.models.Variable`, Airflow 2.10) answering from `values`, as `Variable.get` does."""

    def get(cls: Any, key: str, default: Any = None, *args: Any, **kwargs: Any) -> Any:
        return values.get(key, default)

    monkeypatch.setattr(Variable, "get", classmethod(get))


def refusing_at_parse(monkeypatch: pytest.MonkeyPatch) -> None:
    """Parse reads no Variable: any read while the DAG file is built is a failure."""

    def get(cls: Any, key: str, *args: Any, **kwargs: Any) -> Any:
        raise AssertionError(f"Variable {key} was read at parse")

    monkeypatch.setattr(Variable, "get", classmethod(get))


def build(tmp_path: Path, variables: dict[str, str], configs: dict[str, str], **kwargs: Any) -> dict:
    for name, text in configs.items():
        (tmp_path / f"{name}.yaml").write_text(text)
    return build_source_dags(
        list(configs),
        mode=kwargs.pop("mode", "cdc"),
        config_dir=str(tmp_path),
        state_root="/srv/state",
        state_connection="rivet_state",
        env_from_variables=variables,
        **kwargs,
    )


def test_parse_builds_every_source_with_its_variables_and_reads_none(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    refusing_at_parse(monkeypatch)
    dags = build(
        tmp_path,
        {SECRET_VAR: "rivet_{name}_url_secret"},
        {"alpha": CDC_URL_SECRET, "beta": CDC_URL_SECRET},
    )
    assert {d: dags[d].get_task("run").env_from_variables for d in dags} == {
        "rivet_alpha_cdc": {SECRET_VAR: "rivet_alpha_url_secret"},
        "rivet_beta_cdc": {SECRET_VAR: "rivet_beta_url_secret"},
    }


def test_a_source_reads_its_own_variable_and_the_config_resolves_it_for_rivet(
    tmp_path: Path, rig: Rig, monkeypatch: pytest.MonkeyPatch
) -> None:
    stored(monkeypatch, {"rivet_shop_url_secret": "projects/rivet-data-tool/secrets/shop-url"})
    rig.config.write_text(URL_SECRET_CONFIG)
    op = rig.operator(RivetRunOperator, export="orders", env_from_variables={SECRET_VAR: "rivet_shop_url_secret"})
    env = op._environment(tmp_path)
    assert env[SECRET_VAR] == "projects/rivet-data-tool/secrets/shop-url"
    # the preflight reads the config with the environment the task builds: `${...}` resolves as rivet resolves it
    assert read_config(str(rig.config), env)["source"]["url_secret"] == "projects/rivet-data-tool/secrets/shop-url"


def test_rivet_is_started_with_the_resolved_secret_and_a_secret_it_cannot_read_is_its_coded_refusal(
    rig: Rig, monkeypatch: pytest.MonkeyPatch
) -> None:
    name = "projects/rivet-data-tool/secrets/shop-url"
    stored(monkeypatch, {"rivet_shop_url_secret": name})
    rig.config.write_text(URL_SECRET_CONFIG)
    rig.scenario(version="0.33.0", secrets={name: 403}, record_env=["RIVET_PG_URL", SECRET_VAR])
    exc, _ = rig.run(rig.operator(RivetRunOperator, export="orders", env_from_variables={SECRET_VAR: "rivet_shop_url_secret"}))
    assert isinstance(exc, AirflowFailException) and "RIVET_CONFIG_GCP_SECRET_UNREADABLE" in str(exc), exc
    assert rig.calls()[-1]["env"][SECRET_VAR] == name


def test_a_missing_variable_is_refused_before_rivet_starts_with_its_remedy(rig: Rig, monkeypatch: pytest.MonkeyPatch) -> None:
    stored(monkeypatch, {})
    rig.config.write_text(URL_SECRET_CONFIG)
    op = rig.operator(RivetRunOperator, export="orders", env_from_variables={SECRET_VAR: "rivet_shop_url_secret"})
    exc, ti = rig.run(op)
    assert isinstance(exc, AirflowFailException)
    assert str(exc) == (
        "[RIVET_AIRFLOW_VARIABLE_MISSING] airflow variable `rivet_shop_url_secret` is not defined in the metadata database or "
        "any secrets backend this worker reads; it fills `RIVET_SOURCE_URL_SECRET`, which the config reads as "
        "`${RIVET_SOURCE_URL_SECRET}`: set it with `airflow variables set rivet_shop_url_secret "
        "projects/<project>/secrets/<secret>`, or AIRFLOW_VAR_rivet_shop_url_secret in the worker's environment; a retry "
        "cannot create it"
    )
    assert ti.store[("task", "return_value")]["preflight_refusal"] == "RIVET_AIRFLOW_VARIABLE_MISSING"
    assert not rig.calls(), "a refused task must not start rivet"


def test_an_empty_variable_is_a_missing_one(rig: Rig, monkeypatch: pytest.MonkeyPatch) -> None:
    stored(monkeypatch, {"rivet_shop_url_secret": ""})
    op = rig.operator(RivetRunOperator, export="orders", env_from_variables={SECRET_VAR: "rivet_shop_url_secret"})
    exc, _ = rig.run(op)
    assert "[RIVET_AIRFLOW_VARIABLE_MISSING]" in str(exc)


def test_a_missing_variable_fails_only_its_own_source(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    stored(monkeypatch, {"rivet_alpha_url_secret": "projects/p/secrets/alpha-url"})
    dags = build(
        tmp_path,
        {SECRET_VAR: "rivet_{name}_url_secret"},
        {"alpha": CDC_URL_SECRET, "beta": CDC_URL_SECRET},
    )
    alpha = dags["rivet_alpha_cdc"].get_task("run")
    assert alpha._variables() == {SECRET_VAR: "projects/p/secrets/alpha-url"}
    beta = dags["rivet_beta_cdc"].get_task("run")
    ti = FakeTI(task_id="run")
    with pytest.raises(AirflowFailException) as refused:
        beta.execute({"ti": ti, "run_id": ti.run_id})
    assert "[RIVET_AIRFLOW_VARIABLE_MISSING]" in str(refused.value) and "`rivet_beta_url_secret`" in str(refused.value)


def test_a_source_with_a_password_secret_and_no_url_env_reads_no_source_connection(tmp_path: Path) -> None:
    dags = build(tmp_path, {}, {"shop": BATCH_PASSWORD_SECRET}, mode="batch")
    assert set(dags["rivet_shop_batch"].get_task("apply.orders").env_from_connections) == {"RIVET_STATE_URL"}


def test_a_password_secret_with_url_env_still_reads_the_source_connection(tmp_path: Path) -> None:
    text = BATCH_PASSWORD_SECRET.replace(
        "  host: db.internal\n  user: rivet\n  database: shop\n", "  url_env: RIVET_SOURCE_URL\n"
    )
    dags = build(tmp_path, {}, {"shop": text}, mode="batch")
    assert set(dags["rivet_shop_batch"].get_task("apply.orders").env_from_connections) == {"RIVET_SOURCE_URL", "RIVET_STATE_URL"}


def test_operator_kwargs_may_not_set_the_variables(tmp_path: Path) -> None:
    with pytest.raises(ValueError) as refused:
        build_source_dags(
            ["shop"],
            mode="cdc",
            config_dir=str(tmp_path),
            state_root="/s",
            state_connection="st",
            operator_kwargs={"env_from_variables": {"X": "y"}},
        )
    assert str(refused.value) == "operator_kwargs may not set ['env_from_variables']: each source's DAG sets them"


def test_a_variable_and_an_env_value_may_not_name_the_same_variable() -> None:
    with pytest.raises(ValueError) as refused:
        RivetRunOperator(
            task_id="run",
            config="/c.yaml",
            export="orders",
            env={SECRET_VAR: "projects/p/secrets/s"},
            env_from_variables={SECRET_VAR: "rivet_shop_url_secret"},
        )
    assert str(refused.value) == f"env and env_from_variables both set ['{SECRET_VAR}']: a variable has one value"
