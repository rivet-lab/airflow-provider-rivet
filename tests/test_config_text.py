"""Config text fidelity: the package reads a config the way rivet reads it (D06, D07)."""

from __future__ import annotations

from types import SimpleNamespace

import pytest
from airflow.exceptions import AirflowFailException
from conftest import FakeTI, Rig

from airflow_provider_rivet import _yaml
from airflow_provider_rivet.operators import RivetCdcRunOperator, RivetRunOperator

PG_STATE = {"RIVET_STATE_URL": "postgresql://state:pw@state-db/rivet_state"}
POD = {"deployment": "pod"}


def refused(rig: Rig, op: object) -> str:
    """Run an operator that must be refused; return the refusal code."""
    exc, ti = rig.run(op, FakeTI(task_id=op.task_id))
    assert isinstance(exc, AirflowFailException), exc
    return ti.store[(ti.task_id, "return_value")].get("preflight_refusal")


# ---- D09: the load/compact copy is cut out of the original text, never re-serialised ----

BLOCK = """\
# the pipeline's config: comments, quoting and anchors are the operator's
source:
  type: postgres
  url_env: RIVET_PG_URL
exports:
  - name: orders            # first
    note: '${NOTE}'
    mode: full
  # a comment about users
  - name: users
    query: "SELECT 1"
    note: '${NOTE}'
    mode: incremental
  - name: countries
    mode: full
    note: '${NOTE}'
destination: {type: local, path: ./out/}
"""

FLOW = """\
source: {type: postgres, url_env: RIVET_PG_URL}
exports: [{name: orders, mode: full, note: '${NOTE}'},
  {name: users, mode: incremental, note: '${NOTE}'}, {name: countries, mode: full}]
"""

HOSTILE = ["@abc", "*abc", "!abc", "pa': 'ss", "abc #1", "a: b"]


ANCHORED = """\
source: {type: postgres, url_env: RIVET_PG_URL}
exports:
  - &base
    name: orders
    mode: full
  - <<: *base
    name: users
"""


# ---- D07: YAML 1.2 scalars as serde_yaml_ng types them; names compared as their text ----


def test_d07_leading_zero_digits_are_strings_and_prefixed_ints_are_ints() -> None:
    """D07: PyYAML's 1.1 int read `08` / `09` as a bad octal (ValueError) and `017` as 15; serde_yaml_ng keeps them
    strings (`digits_but_not_number`) and reads `0x` / `0o` / `0b` as ints."""
    doc = _yaml.load("a: 017\nb: 08\nc: 09\nd: -08\ne: 0x1F\nf: 0o17\ng: 0b101\nh: 0\ni: -12\nj: 1.5\nk: 08.5\n")
    assert doc == {"a": "017", "b": "08", "c": "09", "d": "-08", "e": 31, "f": 15, "g": 5, "h": 0, "i": -12, "j": 1.5, "k": 8.5}


def test_d07_a_leading_zero_value_does_not_crash_the_preflight(rig: Rig) -> None:
    """D07: `chunk_size: 09` raised a ValueError traceback from preflight, which Airflow retried."""
    rig.config.write_text(rig.config.read_text().replace("    mode: full\n", "    mode: full\n    chunk_size: 09\n", 1))
    payload, _ = rig.run(rig.operator(RivetRunOperator, export="orders"))
    assert isinstance(payload, dict) and payload["decision"] == "success", payload


# ---- D06: `${...}` in mode / name / cdc.checkpoint resolved with the task env and `-p`, or fail closed ----


@pytest.mark.parametrize("env", [{"RIVET_MODE": "cdc"}, {}])
def test_d06_a_templated_cdc_mode_is_refused_on_a_pod(rig: Rig, env: dict) -> None:
    """D06: `mode: ${RIVET_MODE}` was read unresolved, so the CDC-on-pod guard never fired; set, it resolves to cdc;
    unset, the guard fails closed and treats it as cdc."""
    rig.config.write_text(rig.config.read_text().replace("    mode: full\n", "    mode: ${RIVET_MODE}\n", 1))
    op = rig.operator(RivetRunOperator, export="orders", env={**PG_STATE, **env}, **POD)
    assert refused(rig, op) == "RIVET_AIRFLOW_CDC_ON_POD"
    assert not rig.calls()


def test_a_templated_batch_mode_still_runs_on_a_pod(rig: Rig) -> None:
    rig.config.write_text(rig.config.read_text().replace("    mode: full\n", "    mode: ${RIVET_MODE}\n", 1))
    payload, _ = rig.run(rig.operator(RivetRunOperator, export="orders", env={**PG_STATE, "RIVET_MODE": "full"}, **POD))
    assert isinstance(payload, dict) and payload["decision"] == "success", payload


@pytest.mark.parametrize("how", ["env", "param"])
def test_d06_a_templated_export_name_resolves_with_the_task_env_or_a_param(rig: Rig, how: str) -> None:
    """D06: `name: orders_${ENV}` was refused `EXPORT_UNKNOWN`; rivet resolves it from `-p` or the env."""
    rig.config.write_text(rig.config.read_text().replace("  - name: orders\n", "  - name: orders_${ENV}\n"))
    extra = {"env": {"RIVET_PG_URL": "x", "ENV": "prod"}} if how == "env" else {"extra_args": ["-p", "ENV=prod"]}
    payload, _ = rig.run(rig.operator(RivetRunOperator, export="orders_prod", **extra))
    assert isinstance(payload, dict) and payload["decision"] == "success", payload


def test_d06_an_unresolved_export_name_is_refused_with_its_own_code(rig: Rig) -> None:
    rig.config.write_text(rig.config.read_text().replace("  - name: orders\n", "  - name: orders_${ENV}\n"))
    assert refused(rig, rig.operator(RivetRunOperator, export="orders_prod")) == "RIVET_AIRFLOW_CONFIG_UNRESOLVED"
    assert not rig.calls()


def test_d06_a_templated_checkpoint_is_looked_for_where_it_resolves(rig: Rig) -> None:
    """D06: `cdc.checkpoint: ${CKPT}` sent the beside-config guard looking for a file literally named `${CKPT}`, so a
    checkpoint left beside the config was missed and the stream re-anchored; unset, the guard refuses with a code."""
    rig.config.write_text(
        rig.config.read_text().replace("    mode: cdc\n", "    mode: cdc\n    cdc:\n      checkpoint: ${CKPT}\n")
    )
    (rig.config_dir / "ckpt").mkdir()
    (rig.config_dir / "ckpt" / "orders.ckpt").write_text("lsn")
    op = rig.operator(RivetCdcRunOperator, export="orders_cdc", env={"RIVET_PG_URL": "x", "CKPT": "ckpt/orders.ckpt"})
    assert refused(rig, op) == "RIVET_AIRFLOW_CHECKPOINT_BESIDE_CONFIG"
    unset = rig.operator(RivetCdcRunOperator, export="orders_cdc")
    assert refused(rig, unset) == "RIVET_AIRFLOW_CONFIG_UNRESOLVED"
    assert not rig.calls()


def test_the_placeholder_grammar_is_rivets() -> None:
    """The grammar D06's resolution uses, one scan as `resolve.rs`: `${}` is empty, an unclosed `${` ends the scan, a value is never scanned again."""
    values = {"A": "${B}", "B": "no"}.get
    assert _yaml.substitute("x${A}y${}z ${C} ${open", values) == ("x${B}yz C ${open", ["C"])
    assert _yaml.params_of(["-p", "A=1", "--param", "B=2=3", "-pC=4", "--param=D=5", "-p=E=6"]) == {
        "A": "1",
        "B": "2=3",
        "C": "4",
        "D": "5",
        "E": "6",
    }


@pytest.mark.parametrize("how", ["env", "param"])
def test_d06_a_templated_load_password_env_resolves_for_the_connection(
    rig: Rig, monkeypatch: pytest.MonkeyPatch, how: str
) -> None:
    """D06 on the Connection side: `_config_doc` read `load.password_env: ${CH_PW_VAR}` unresolved, so the ClickHouse
    Connection's password went to a variable literally named `${CH_PW_VAR}` and never to the one rivet reads."""
    ch = SimpleNamespace(
        conn_id="ch", conn_type="clickhouse", login="loader", password="pw", host="ch", port=None, schema=None, extra_dejson={}
    )
    monkeypatch.setattr("airflow_provider_rivet.operators.BaseHook.get_connection", lambda conn_id: ch)
    rig.config.write_text(rig.config.read_text() + "load:\n  target: clickhouse\n  password_env: ${CH_PW_VAR}\n")
    rig.scenario(record_env=["CH_PASSWORD"], leak_env=[])
    env = {"RIVET_PG_URL": "x", "CH_PW_VAR": "CH_PASSWORD"} if how == "env" else {"RIVET_PG_URL": "x"}
    extra = {} if how == "env" else {"extra_args": ["-p", "CH_PW_VAR=CH_PASSWORD"]}
    payload, _ = rig.run(rig.operator(RivetRunOperator, export="orders", connections=["ch"], env=env, **extra))
    assert isinstance(payload, dict), payload
    assert rig.calls()[-1]["env"]["CH_PASSWORD"] == "pw", "the password must reach the variable the config names"
