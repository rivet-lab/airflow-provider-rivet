"""The sources file: read strictly, with the file, line and column of every refusal, and the drift of a config from it."""

from __future__ import annotations

import json
import re
from pathlib import Path

import jsonschema
import pytest
import yaml

from airflow_provider_rivet.sources import (
    INIT_FLAGS,
    SCHEMA_FILE,
    SourcesFileError,
    drift,
    glob_matches,
    kept,
    read_sources_file,
    schema,
    skipped_by_init,
)

ROOT = Path(__file__).resolve().parents[1]
HEAD = "version: 1\nstate_root: /opt/airflow/rivet-state\nstate_connection: rivet_state\n"
TEMPLATE_CONFIG = """\
source:
  type: mysql
  url_env: RIVET_SOURCE_URL
  tls: {mode: disable}
load:
  target: bigquery
  project: my-project
  dataset: rivet_shop
exports:
  - name: shop_cdc
    mode: cdc
    tables: [a, b]
    destination: {type: gcs, bucket: my-bucket, prefix: shop/cdc/}
"""


def write(tmp_path: Path, text: str) -> Path:
    path = tmp_path / "rivet_sources.yaml"
    path.write_text(text, encoding="utf-8")
    return path


def refusal(tmp_path: Path, text: str) -> str:
    with pytest.raises(SourcesFileError, match=re.escape(str(tmp_path / "rivet_sources.yaml"))) as caught:
        read_sources_file(write(tmp_path, text))
    return str(caught.value).replace(str(tmp_path / "rivet_sources.yaml"), "F")


def test_each_source_takes_the_template_under_the_defaults_under_its_own(tmp_path: Path) -> None:
    (tmp_path / "rivet").mkdir()
    (tmp_path / "rivet" / "shop.yaml").write_text(TEMPLATE_CONFIG)
    spec = read_sources_file(
        write(
            tmp_path,
            HEAD
            + 'template: shop\ndefaults:\n  bigquery_dataset: "rivet_{name}"\n  tls: require\n'
            + "sources:\n  shop:\n  analytics:\n    tls: disable\n    include: [visitors]\n",
        )
    )
    assert spec.config_dir == tmp_path / "rivet" and spec.mode == "cdc" and spec.source_connection == "{name}"
    analytics = spec.sources["analytics"]
    assert analytics.settings == {
        "gcs_bucket": "my-bucket",
        "bigquery_project": "my-project",
        "tls": "disable",
        "bigquery_dataset": "rivet_analytics",
        "include": ["visitors"],
    }
    assert analytics.init_args() == [
        "--tls",
        "disable",
        "--gcs-bucket",
        "my-bucket",
        "--bigquery-project",
        "my-project",
        "--bigquery-dataset",
        "rivet_analytics",
    ]
    assert spec.sources["shop"].settings == {"bigquery_dataset": "rivet_shop", "tls": "require"}
    assert analytics.state_database() == "rivet_state_analytics"


def test_a_source_s_own_init_args_flag_wins_over_the_template_s_setting(tmp_path: Path) -> None:
    (tmp_path / "rivet").mkdir()
    (tmp_path / "rivet" / "shop.yaml").write_text(TEMPLATE_CONFIG)
    text = HEAD + "template: shop\nsources:\n  shop:\n  b:\n    init_args: [--gcs-bucket, other]\n"
    b = read_sources_file(write(tmp_path, text)).sources["b"]
    assert "gcs_bucket" not in b.settings and b.settings["bigquery_project"] == "my-project"
    assert b.init_args().count("--gcs-bucket") == 1 and "other" in b.init_args()
    text = HEAD + "template: shop\ndefaults:\n  init_args: [--gcs-bucket, other]\nsources:\n  shop:\n  b: {}\n"
    assert "gcs_bucket" not in read_sources_file(write(tmp_path, text)).sources["b"].settings


def test_state_database_connection_names_the_connection_s_own(tmp_path: Path) -> None:
    spec = read_sources_file(write(tmp_path, HEAD + "sources:\n  shop:\n    state_database: connection\n  b: {}\n"))
    assert spec.sources["shop"].state_database() is None
    assert spec.sources["b"].state_database() == "rivet_state_b"


def test_a_template_config_writing_to_two_buckets_names_both_and_asks_for_one(tmp_path: Path) -> None:
    (tmp_path / "rivet").mkdir()
    second = "  - name: shop_b\n    mode: cdc\n    tables: [c]\n    destination: {type: gcs, bucket: other, prefix: b/}\n"
    (tmp_path / "rivet" / "shop.yaml").write_text(TEMPLATE_CONFIG + second)
    config = tmp_path / "rivet" / "shop.yaml"
    said = f"{config} writes to more than one GCS bucket (my-bucket, other): name `gcs_bucket` in `defaults`"
    with pytest.raises(SourcesFileError, match=f"^{re.escape(said)}$"):
        read_sources_file(write(tmp_path, HEAD + "template: shop\nsources:\n  shop:\n  b: {}\n"))


def test_name_is_replaced_in_the_keys_of_a_mapping_too(tmp_path: Path) -> None:
    text = HEAD + 'defaults:\n  env_from_variables:\n    "X_{name}": "v_{name}"\nsources:\n  shop: {}\n'
    assert read_sources_file(write(tmp_path, text)).sources["shop"].settings == {"env_from_variables": {"X_shop": "v_shop"}}


def test_a_byte_order_mark_at_the_start_is_read_past(tmp_path: Path) -> None:
    spec = read_sources_file(write(tmp_path, "\ufeff" + HEAD + "mode: batch\nsources:\n  shop: {}\n"))
    assert (spec.mode, list(spec.sources)) == ("batch", ["shop"])
    text = "\ufeffversion: 2\nstate_root: /x\nstate_connection: s\nsources:\n  a: {}\n"
    assert refusal(tmp_path, text) == "F:1:10: `version`: is 2; the only one is 1"


def test_a_template_with_no_config_yet_is_reported_not_refused(tmp_path: Path) -> None:
    spec = read_sources_file(write(tmp_path, HEAD + "template: a\nsources:\n  a: {}\n  b: {}\n"))
    assert spec.template_missing and "template `a`" in spec.template_missing


HEREDOC = (
    "A heredoc (`cat <<EOF`) pasted together with the commands after it writes them into the file; "
    "keep only the YAML here and run the commands in the shell"
)
ALIAS = 'starts with `*`, which YAML reads as an alias, not text: quote the glob, "*_tmp"'


@pytest.mark.parametrize(
    ("text", "said"),
    [
        (HEAD + "sources:\n  a:\n    exclude: [*_tmp]\n", f"F:6:15: `*_tmp` {ALIAS}"),
        (HEAD + "sources:\n  a:\n    exclude:\n      - *_tmp\n", f"F:7:9: `*_tmp` {ALIAS}"),
        (
            HEAD + "defaults: &defs\n  tls: disable\nsources:\n  a: *defs\n",
            "F:7:6: `*defs` is a YAML alias of the anchor `&defs`; a sources file reads no anchors or aliases: write the "
            "value out",
        ),
        (
            HEAD + "defaults: &d\n  tls: disable\nsources:\n  a:\n    exclude: [*]\n",
            'F:8:15: `*` starts with `*`, which YAML reads as an alias, not text: quote the glob, "*"',
        ),
        (HEAD + "sources:\n  a:\n    datset: x\n", "F:6:5: unknown key `datset`; did you mean `bigquery_dataset`?"),
        (HEAD + "sources:\n  a:\n    dataset: x\n", "F:6:5: unknown key `dataset`; did you mean `bigquery_dataset`?"),
        (HEAD + "sources:\n  a:\n    tables: [x]\n", "F:6:5: unknown key `tables`; did you mean `include`?"),
        (HEAD + "sources:\n  a:\n    bucket: x\n", "F:6:5: unknown key `bucket`; did you mean `gcs_bucket`?"),
        (
            HEAD + "sources:\n  a:\n    tls: off\n",
            "F:6:10: `sources.a.tls`: `off` is not one of disable, require, verify-ca, verify-full",
        ),
        (
            HEAD + 'defaults:\n  bigquery_dataset: "rivet_{dsn}"\nsources:\n  a: {}\n',
            "F:5:21: `{dsn}` is no placeholder of this file; the one placeholder is `{name}`, the source's name",
        ),
        (HEAD + "sources:\n  A-b: {}\n", "F:5:3: `sources`: `A-b` is not a name of lower-case letters, digits and `_`"),
        (
            "version: 1\nstate_root: \u201c/x\u201d\nstate_connection: s\nsources:\n  a: {}\n",
            "F:2:13: `\u201c` is a typographic quote (copied from a messenger or a word processor); "
            "YAML quotes with a straight \" or '",
        ),
        (
            HEAD + "sources:\n  a:\u00a0{}\n",
            "F:5:5: a no-break space (U+00A0), which a messenger or a word processor puts in copied text; "
            "retype the line with plain spaces",
        ),
        (HEAD + "sources:\n\ta: {}\n", "F:5:1: a tab in the indentation; YAML indents with spaces only"),
        (
            HEAD + "sources:\n  a: {}\nEOF\ndocker compose up -d\n",
            f"F:6:1: this line looks like a shell command, not YAML: `EOF`. {HEREDOC}\n"
            f"F:7:1: this line looks like a shell command, not YAML: `docker compose up -d`. {HEREDOC}",
        ),
        (
            HEAD + "sources:\n  a: {}\ncat <<EOF > dags/x.py\n",
            f"F:6:1: this line looks like a shell command, not YAML: `cat <<EOF > dags/x.py`. {HEREDOC}",
        ),
        (
            HEAD + "sources:\n  a: {}\n  a: {}\n",
            "F:6:3: `a` is written twice in one mapping; YAML would keep only the last one silently; write it once",
        ),
        (
            HEAD + "sources:\n  a:\n    include: orders\n",
            "F:6:14: `sources.a.include`: is str 'orders', expected array: write [orders] or one `- orders` per line",
        ),
        ("version: 2\nstate_root: /x\nstate_connection: s\nsources:\n  a: {}\n", "F:1:10: `version`: is 2; the only one is 1"),
        ("version: 1\nstate_root: /x\nsources:\n  a: {}\n", "F:1:1: `(top)`: `state_connection` is required"),
        (HEAD + "sources: {}\n", "F:4:10: `sources`: names no source; add one under `sources:`"),
        (
            HEAD + 'sources:\n  a:\n    tls: disable\n    init_args: ["--tls=require"]\n',
            "F:6:5: source `a`: init_args sets --tls, which its settings already set; keep one",
        ),
        (HEAD + "template: z\nsources:\n  a: {}\n", "F:4:11: template `z` is not a source of this file (a)"),
        (HEAD + "sources:\n  a: [\n", "F:6:1: not YAML: expected the node content, but found '<stream end>'"),
        ("", "F: the file is empty; see docs/dags-from-file.md for its shape"),
    ],
)
def test_a_refusal_names_the_line_the_column_and_what_was_meant(tmp_path: Path, text: str, said: str) -> None:
    assert refusal(tmp_path, text) == said


def test_a_comment_may_hold_what_a_value_may_not(tmp_path: Path) -> None:
    text = HEAD + '# docker compose up -d, \u201cquoted\u201d, [*_tmp]\nsources:\n  a:\n    exclude: ["*_tmp"]  # *_old\n'
    assert read_sources_file(write(tmp_path, text)).sources["a"].settings == {"exclude": ["*_tmp"]}


def test_a_cron_schedule_with_stars_reads_unquoted_and_quoted(tmp_path: Path) -> None:
    text = HEAD + 'defaults:\n  schedule: 0 4-18/2 * * *\nsources:\n  a:\n    schedule: "*/30 * * * *"\n  b: {}\n'
    spec = read_sources_file(write(tmp_path, text))
    assert spec.sources["a"].settings["schedule"] == "*/30 * * * *" and spec.sources["b"].settings["schedule"] == "0 4-18/2 * * *"


def test_every_error_of_a_file_is_reported_at_once(tmp_path: Path) -> None:
    said = refusal(tmp_path, HEAD + 'defaults:\n  bigquery_dataset: "{dsn}"\nsources:\n  a:\n    datset: x\n    tls: off\n')
    assert said.count("\n") == 2 and "{dsn}" in said and "datset" in said and "`off`" in said


def test_the_schema_is_a_valid_json_schema_with_one_key_per_init_flag() -> None:
    doc = json.loads(SCHEMA_FILE.read_text())
    jsonschema.Draft202012Validator.check_schema(doc)
    assert set(INIT_FLAGS) <= set(doc["$defs"]["settings"]["properties"])
    assert schema() == doc


def test_the_example_validates_against_the_schema_as_an_editor_reads_it() -> None:
    example = yaml.safe_load((ROOT / "examples" / "rivet_sources.yaml").read_text())
    validator = jsonschema.Draft202012Validator(schema())
    assert [e.message for e in validator.iter_errors(example)] == []
    assert sorted(example["sources"]) == ["analytics", "billing", "shop"]
    example["sources"]["billing"]["datset"] = "x"
    [error] = validator.iter_errors(example)
    assert list(error.absolute_path) == ["sources", "billing"] and "'datset': 'x'" in error.message


@pytest.mark.parametrize(
    ("pattern", "name", "matches"),
    [
        ("orders", "orders", True),
        ("orders", "orders2", False),
        ("ord*", "orders", True),
        ("*_tmp", "x_tmp", True),
        ("*_tmp", "x_tmp_old", False),
        ("ord?rs", "orders", True),
        ("Orders", "orders", False),
        ("a[1]", "a[1]", True),
        ("a.b", "axb", False),
        ("*", "", True),
    ],
)
def test_a_glob_matches_as_rivet_init_matches_it(pattern: str, name: str, matches: bool) -> None:
    assert glob_matches(pattern, name) is matches


def test_exclude_wins_and_no_include_keeps_all() -> None:
    assert kept("orders", (), ()) and not kept("orders", ("orders",), ("ord*",)) and not kept("users", ("orders",), ())


STREAM = {
    "load": {"target": "bigquery", "tables": {"clicks": {"layout": "base_buffer"}}},
    "exports": [
        {"name": "analytics_cdc", "mode": "cdc", "tables": ["pageviews", "visitors", "clicks"]},
        {"name": "snap", "table": "snapshots", "mode": "full"},
    ],
}


def test_drift_names_each_extra_table_and_where_it_is_written() -> None:
    found = drift(STREAM, "cdc", ["pageviews", "visitors", "sessions"], [])
    assert list(found.extra) == ["clicks"] and list(found.missing) == ["sessions"]
    assert found.places["clicks"] == ["`tables:` of export `analytics_cdc`", "`load.tables.clicks`"]
    assert found.text(Path("/c/analytics.yaml")) == (
        "/c/analytics.yaml captures clicks, which the sources file leaves out "
        "(`clicks`: `tables:` of export `analytics_cdc`, `load.tables.clicks`); "
        "the sources file includes sessions, which /c/analytics.yaml does not capture"
    )


def test_no_drift_when_the_config_captures_what_the_file_says() -> None:
    assert not drift(STREAM, "cdc", ["*"], [])
    assert not drift(STREAM, "cdc", [], ["*_old"])
    assert drift(STREAM, "cdc", [], ["clicks"]).extra == ["clicks"]
    assert not drift(STREAM, "cdc", ["pageviews", "visitors", "clicks", "gone"], ["gone"])


def test_a_table_init_left_out_of_the_stream_is_not_missing() -> None:
    text = "exports:\n  # SKIPPED user-events: its name cannot be a `table:` shortcut\n  # SKIPPED 'Orders': its name\n"
    assert skipped_by_init(text) == ["user-events", "Orders"]
    assert skipped_by_init("  # SKIPPED 'Xmas-sales': its name\n") == ["Xmas-sales"]
    found = drift(STREAM, "cdc", ["pageviews", "visitors", "clicks", "user-events", "sessions"], [], skipped_by_init(text))
    assert list(found.missing) == ["sessions"]


def test_a_table_named_with_its_database_and_schema_is_compared_by_its_last_part() -> None:
    doc = {"exports": [{"name": "c", "mode": "cdc", "tables": ["shop.dbo.orders"]}]}
    assert not drift(doc, "cdc", ["orders"], [])


def test_a_mongo_collection_with_a_dot_is_compared_whole() -> None:
    doc = {"source": {"type": "mongo"}, "exports": [{"name": "c", "mode": "cdc", "tables": ["events.2024"]}]}
    assert not drift(doc, "cdc", ["events.2024"], []) and not drift(doc, "cdc", ["events.*"], [])
    assert drift(doc, "cdc", ["2024"], []).extra == ["events.2024"]


def test_a_batch_source_compares_its_batch_exports_by_schema_less_name() -> None:
    doc = {"exports": [{"name": "dbo_orders", "table": "dbo.orders"}, {"name": "dbo_users", "table": "dbo.users"}]}
    found = drift(doc, "batch", ["orders"], [])
    assert list(found.extra) == ["dbo.users"] and found.places["dbo.users"] == ["export `dbo_users`"]
    assert list(found.missing) == []
