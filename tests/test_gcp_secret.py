"""A source that names its credential by a Google Secret Manager secret (`url_secret`, `password_secret`; rivet #717).

The provider's part: a rivet that cannot read the key is refused with the version it needs, before any rivet process
starts; a secret rivet cannot read is passed through as rivet's coded refusal; and the fake binary teaches rivet's
side of the contract (0.32.2 refuses the key as an unknown field; from #717 it reads the key and refuses a bad
reference or an unreadable secret).
"""

from __future__ import annotations

import subprocess

import pytest
from conftest import CONFIG, Rig

from airflow_provider_rivet._compat import AirflowFailException
from airflow_provider_rivet.operators import RivetRunOperator

URL_SECRET = CONFIG.replace("  url_env: RIVET_PG_URL\n", "  url_secret: projects/rivet-data-tool/secrets/pg-url\n")
PASSWORD_SECRET = CONFIG.replace(
    "  url_env: RIVET_PG_URL\n",
    "  host: db.internal\n  user: rivet\n  database: shop\n  password_secret: projects/rivet-data-tool/secrets/pg-pw/versions/3\n",
)


@pytest.mark.parametrize("config", [URL_SECRET, PASSWORD_SECRET], ids=["url_secret", "password_secret"])
def test_a_secret_source_is_refused_by_a_rivet_too_old_to_read_it_naming_the_version(rig: Rig, config: str) -> None:
    from airflow_provider_rivet.capabilities import GCP_SECRET_MIN_RIVET_VERSION

    rig.config.write_text(config)
    rig.scenario(version="0.32.2")
    exc, ti = rig.run(rig.operator(RivetRunOperator, export="orders"))
    assert isinstance(exc, AirflowFailException), exc
    assert ti.store[("task", "return_value")]["preflight_refusal"] == "RIVET_AIRFLOW_BINARY_TOO_OLD"
    assert ".".join(map(str, GCP_SECRET_MIN_RIVET_VERSION)) in str(exc) and "0.32.2" in str(exc)
    assert not rig.calls(), "a refused task must not start rivet"


def test_a_secret_source_runs_on_a_rivet_that_reads_it(rig: Rig) -> None:
    rig.config.write_text(URL_SECRET)
    rig.scenario(version="0.33.0")
    payload, _ = rig.run(rig.operator(RivetRunOperator, export="orders"))
    assert isinstance(payload, dict) and payload["decision"] == "success", payload


def test_a_secret_rivet_cannot_read_is_passed_through_as_its_coded_refusal(rig: Rig) -> None:
    rig.config.write_text(URL_SECRET)
    rig.scenario(version="0.33.0", secrets={"projects/rivet-data-tool/secrets/pg-url": 403})
    exc, ti = rig.run(rig.operator(RivetRunOperator, export="orders"))
    assert isinstance(exc, AirflowFailException) and "RIVET_CONFIG_GCP_SECRET_UNREADABLE" in str(exc), exc
    assert len(rig.calls()) == 1, "the package does not retry a secret rivet cannot read: one rivet call"


def test_the_fake_refuses_a_secret_source_as_rivet_0_32_2_does(rig: Rig) -> None:
    rig.config.write_text(URL_SECRET)
    rig.scenario(version="0.32.2")
    done = subprocess.run([str(rig.bin / "rivet"), "run", "--config", str(rig.config)], capture_output=True, text=True)
    assert done.returncode == 1 and "RIVET_CONFIG_YAML_INVALID" in done.stderr and "unknown field `url_secret`" in done.stderr


def test_the_fake_refuses_a_malformed_reference_and_an_unreadable_secret_as_rivet_1_does(rig: Rig) -> None:
    rig.scenario(version="0.33.0")
    rig.config.write_text(CONFIG.replace("  url_env: RIVET_PG_URL\n", "  url_secret: shop/pg-url\n"))
    bad = subprocess.run([str(rig.bin / "rivet"), "run", "--config", str(rig.config)], capture_output=True, text=True)
    assert "RIVET_CONFIG_SOURCE_CONNECTION_INVALID" in bad.stderr and "is not a Secret Manager secret name" in bad.stderr
    rig.scenario(version="0.33.0", secrets={"projects/rivet-data-tool/secrets/pg-url": 404})
    rig.config.write_text(URL_SECRET)
    missing = subprocess.run([str(rig.bin / "rivet"), "run", "--config", str(rig.config)], capture_output=True, text=True)
    assert "RIVET_CONFIG_GCP_SECRET_UNREADABLE" in missing.stderr and "(HTTP 404)" in missing.stderr


def test_the_floor_stays_and_the_secret_version_is_a_separate_constant() -> None:
    from airflow_provider_rivet.capabilities import GCP_SECRET_MIN_RIVET_VERSION, MIN_RIVET_VERSION

    assert MIN_RIVET_VERSION == (0, 32, 2)
    assert GCP_SECRET_MIN_RIVET_VERSION > MIN_RIVET_VERSION


def test_a_binary_reads_secret_sources_from_the_named_version_only() -> None:
    from airflow_provider_rivet.capabilities import GCP_SECRET_MIN_RIVET_VERSION, Capabilities

    def caps(version: tuple[int, ...]) -> Capabilities:
        made = object.__new__(Capabilities)
        made.version = version
        return made

    assert not caps((0, 32, 2)).at_least(GCP_SECRET_MIN_RIVET_VERSION)
    assert caps(GCP_SECRET_MIN_RIVET_VERSION).at_least(GCP_SECRET_MIN_RIVET_VERSION)


def test_secret_sources_lists_the_keys_a_config_names() -> None:
    from airflow_provider_rivet.preflight import secret_sources

    assert secret_sources({"source": {"password_secret": "p", "url_secret": "u"}}) == ["url_secret", "password_secret"]
    assert secret_sources({"source": {"url_env": "X"}}) == []
    assert secret_sources({}) == []
