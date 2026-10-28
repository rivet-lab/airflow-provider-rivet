"""The fake rivet answers like rivet 0.32.2: each subcommand offers the flags the installed rivet-cli lists for it, and a
flag or a subcommand it does not offer is refused with exit 2, as clap refuses it."""

from __future__ import annotations

import ast
import json
import re
import subprocess
from pathlib import Path

import rivet_cli
from conftest import CONFIG, Rig, fixture
from rivet_shapes import ENTRY_KEYS, load_summary, run_entry, run_summary

from airflow_provider_rivet.capabilities import MIN_RIVET_VERSION, probe

FAKE = ast.parse((Path(__file__).parent / "fake_rivet.py").read_text())  # the fake runs `main()` on import
BASE_FLAGS = next(ast.literal_eval(n.value) for n in FAKE.body if isinstance(n, ast.Assign) and n.targets[0].id == "BASE_FLAGS")


OPTION = re.compile(r"^  (?:-(\w), |    )--([\w-]+)( <[^>]+>)?(?:\s|$)", re.M)


def options(help_text: str) -> dict[str, tuple[str | None, bool]]:
    """{long flag: (short, takes a value)} of a `--help` text, as clap prints it (`-h`/`-V` left out)."""
    return {long: (short or None, bool(value)) for short, long, value in OPTION.findall(help_text) if long != "help"}


def test_the_fake_offers_each_subcommand_the_flags_rivet_0_32_2_does(rig: Rig) -> None:
    real = rivet_cli.path()
    assert probe(real).version >= MIN_RIVET_VERSION, "the installed rivet-cli is below the floor the fake models"
    exact = probe(real).version == MIN_RIVET_VERSION  # a newer rivet-cli may have grown flags the floor lacks
    for sub in BASE_FLAGS:
        want = options(subprocess.run([real, sub, "--help"], capture_output=True, text=True, check=True).stdout)
        got = options(subprocess.run([str(rig.bin / "rivet"), sub, "--help"], capture_output=True, text=True, check=True).stdout)
        assert want, f"no options parsed from `rivet {sub} --help`"
        if exact:
            assert got == want, f"rivet {sub}: the fake lacks {want.keys() - got.keys()}, offers {got.keys() - want.keys()}"
        else:
            assert got.items() <= want.items(), f"rivet {sub}: the fake offers {got.items() - want.items()}"


def test_an_unknown_flag_or_subcommand_is_refused_as_clap_refuses_it(rig: Rig) -> None:
    rivet, cfg = str(rig.bin / "rivet"), str(rig.config)
    for argv, said in (
        (["run", "--config", cfg, "--bogus"], "unexpected argument '--bogus'"),
        (["plan", "--CONFIG", cfg], "unexpected argument '--CONFIG'"),
        (["XXplanXX", "--config", cfg], "unrecognized subcommand 'XXplanXX'"),
    ):
        done = subprocess.run([rivet, *argv], capture_output=True, text=True, cwd=rig.tmp)
        assert (done.returncode, said in done.stderr) == (2, True), (argv, done.stderr)
    for sub in ("run", "plan"):  # `-p` is a flag of run, plan and check only
        done = subprocess.run([rivet, sub, "--config", cfg, "-p", "A=1", "--param=B=2", "-pC=3", "--json-errors"], cwd=rig.tmp)
        assert done.returncode == 0, sub
    for sub in ("load", "compact", "apply", "metrics"):
        argv = [sub, cfg, "-p", "A=1"] if sub == "apply" else [sub, "--config", cfg, "-p", "A=1"]  # apply takes a file
        done = subprocess.run([rivet, *argv], capture_output=True, text=True, cwd=rig.tmp)
        assert (done.returncode, "unexpected argument '-p'" in done.stderr) == (2, True), sub


def test_the_rig_config_is_one_the_installed_rivet_reads(tmp_path: Path) -> None:
    """F5: the rig config lacked `format`, so no behavioural test could ever point at the real binary. `check` reads the
    config before it connects: a config rivet refuses is a `RIVET_CONFIG_*` code, a valid one fails at the dial."""
    config = tmp_path / "rig.yaml"
    config.write_text(CONFIG)
    env = {"PATH": "/usr/bin:/bin", "RIVET_PG_URL": "postgresql://u:p@127.0.0.1:1/db"}  # port 1: nothing listens
    done = subprocess.run(
        [rivet_cli.path(), "check", "--json-errors", "--config", str(config)],
        capture_output=True,
        text=True,
        cwd=tmp_path,
        env=env,
    )
    assert "RIVET_CONFIG_" not in done.stderr, done.stderr


def test_the_summary_shapes_the_fake_writes_are_the_pinned_contracts() -> None:
    """F4: an entry carries exactly the keys rivet's `RunAggregateEntry` serialises (the pinned `run_entry.json` holds those
    first, then the provider's own), the aggregate and a load document the keys of theirs."""
    pinned = list(fixture("run_entry.json"))
    assert list(run_entry("x")) == pinned[: len(ENTRY_KEYS)] and tuple(run_entry("x")) == ENTRY_KEYS
    assert set(load_summary()) == set(fixture("load_result.json"))
    summary = run_summary([run_entry("x"), run_entry("y", "failed"), run_entry("z", "skipped")])
    assert (summary["total_exports"], summary["success_count"], summary["failed_count"], summary["skipped_count"]) == (3, 1, 1, 1)
    assert summary["per_export"][1]["error_message"] is None, (
        "a failed entry's text is the caller's: the shape has no `error` key"
    )


def test_the_fake_writes_a_summary_on_exit_0_and_on_exit_2_and_a_coded_refusal_line(rig: Rig, tmp_path: Path) -> None:
    """F4: rivet writes `--summary-output` for a run that ended 0 or 2; a refusal line carries its `code`."""
    out = tmp_path / "summary.json"
    for exit_code, status in ((0, "success"), (2, "failed")):
        rig.scenario(run={"exit": exit_code})
        subprocess.run(
            [str(rig.bin / "rivet"), "run", "--config", str(rig.config), "--export", "orders", "--summary-output", str(out)],
            cwd=rig.tmp,
        )
        [entry] = json.loads(out.read_text())["per_export"]
        assert (entry["status"], tuple(entry)) == (status, ENTRY_KEYS)
        out.unlink()
    rig.scenario(run={"exit": 5})
    subprocess.run([str(rig.bin / "rivet"), "run", "--config", str(rig.config), "--summary-output", str(out)], cwd=rig.tmp)
    assert not out.exists(), "an exit other than 0 and 2 writes no summary"
    rig.config.write_text("a: [\n")
    done = subprocess.run(
        [str(rig.bin / "rivet"), "run", "--config", str(rig.config)], capture_output=True, text=True, cwd=rig.tmp
    )
    assert json.loads(done.stderr.strip().splitlines()[-1])["code"] == "RIVET_CONFIG_YAML_INVALID"
