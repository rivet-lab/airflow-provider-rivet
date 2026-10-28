"""RivetInitOperator.init_args: the flags the operator owns are refused however clap would spell them, and a value that
happens to read `cdc` is not a request for the CDC mode (F4)."""

from __future__ import annotations

from pathlib import Path

import pytest
from test_source_dags import init_operator

OWNED_TEXT = "init_args may not set {taken}: the operator sets the source, the output and the CDC mode"
BATCH_CDC_TEXT = "init_args may not ask for `--mode cdc` in a batch DAG: use mode='cdc'"


@pytest.mark.parametrize("mode", ["cdc", "batch"])
@pytest.mark.parametrize(
    "args",
    [
        ["--bigquery-dataset", "cdc"],
        ["--gcs-bucket", "cdc", "--tls", "disable"],
        ["--include", "cdc", "orders"],
        ["--table", "cdc"],
    ],
    ids=["dataset", "bucket", "include-glob", "table"],
)
def test_a_value_named_cdc_is_not_a_request_for_the_cdc_mode(tmp_path: Path, mode: str, args: list[str]) -> None:
    assert init_operator(tmp_path, mode=mode, init_args=args).init_args == args


@pytest.mark.parametrize("spelled", [["--mode=cdc"], ["--mode", "cdc"], ["--tls", "disable", "--mode=cdc"]])
def test_a_batch_dag_refuses_the_cdc_mode_however_it_is_spelled(tmp_path: Path, spelled: list[str]) -> None:
    with pytest.raises(ValueError) as exc:
        init_operator(tmp_path, mode="batch", init_args=spelled)
    assert str(exc.value) == BATCH_CDC_TEXT


@pytest.mark.parametrize(
    "owned",
    ["--source=x", "--source-env=X", "--source-file=/f", "--output=out.yaml", "-oout.yaml", "-o=out.yaml", "--mode=full"],
)
@pytest.mark.parametrize("mode", ["cdc", "batch"])
def test_a_flag_the_operator_owns_is_refused_in_its_attached_spelling_too(tmp_path: Path, mode: str, owned: str) -> None:
    if mode == "batch" and owned == "--mode=full":
        assert init_operator(tmp_path, mode=mode, init_args=[owned]).init_args == [owned]
        return
    with pytest.raises(ValueError) as exc:
        init_operator(tmp_path, mode=mode, init_args=["--tls", "disable", owned])
    assert str(exc.value) == OWNED_TEXT.format(taken=f"['{owned}']")


def test_what_was_accepted_and_refused_before_is_unchanged(tmp_path: Path) -> None:
    ok = ["--gcs-bucket", "b", "--bigquery-dataset", "rivet_shop", "--schema", "sales", "--include", "o*", "u*"]
    assert init_operator(tmp_path, mode="cdc", init_args=ok).init_args == ok
    assert init_operator(tmp_path, mode="batch", init_args=["--mode", "incremental", *ok]).init_args == [
        "--mode",
        "incremental",
        *ok,
    ]
    for owned in ("--source", "--source-env", "--source-file", "-o", "--output", "--mode"):
        with pytest.raises(ValueError) as exc:
            init_operator(tmp_path, mode="cdc", init_args=[owned, "x"])
        assert str(exc.value) == OWNED_TEXT.format(taken=f"['{owned}']")
    with pytest.raises(ValueError) as exc:
        init_operator(tmp_path, mode="cdc", init_args=["--mode", "cdc", "-o", "x"])
    assert str(exc.value) == OWNED_TEXT.format(taken="['--mode', '-o']")
