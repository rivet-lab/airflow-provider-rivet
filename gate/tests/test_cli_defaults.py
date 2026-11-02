"""The live gate's command-line defaults."""

from __future__ import annotations

import argparse

import pytest

from gate.__main__ import main


class Parsed(Exception):
    """Stops `main` once its arguments are parsed."""


def test_the_airflow_default_is_the_two_stacks_the_workflow_and_docs_name(monkeypatch: pytest.MonkeyPatch) -> None:
    """Mutant: the default is every flavor in stand.FLAVORS, so the job also ran 2.10.2 under its 90-minute timeout."""
    parsed: list[argparse.Namespace] = []
    real = argparse.ArgumentParser.parse_args

    def stop_after_parsing(self: argparse.ArgumentParser, args=None, namespace=None) -> argparse.Namespace:
        parsed.append(real(self, args, namespace))
        raise Parsed

    monkeypatch.setattr(argparse.ArgumentParser, "parse_args", stop_after_parsing)
    with pytest.raises(Parsed):
        main(["--rivet-bin", "rivet"])
    assert parsed[0].airflow == "2.10.5,3.3.2"
