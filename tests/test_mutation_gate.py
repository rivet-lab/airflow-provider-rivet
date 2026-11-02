"""dev/mutants_gate.py: a pull request fails only on a survivor of a line it changed."""

from __future__ import annotations

import importlib.util
import json
import os
import subprocess
from pathlib import Path

import pytest

SPEC = importlib.util.spec_from_file_location("mutants_gate", Path(__file__).resolve().parents[1] / "dev" / "mutants_gate.py")
gate = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(gate)

SOURCE = "class K:\n    @classmethod\n    def m(cls, a):\n        b = a\n        return b + 1\n"
MUTATED = (
    "class K:\n"
    "    def xǁKǁm__mutmut_orig(cls, a):\n        b = a\n        return b + 1\n"
    "    def xǁKǁm__mutmut_1(cls, a):\n        b = a\n        return b - 1\n"
)


def test_a_mutant_is_placed_on_the_source_line_it_changes(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    for root, text in ((tmp_path, SOURCE), (tmp_path / "mutants", MUTATED)):
        (root / gate.PACKAGE).mkdir(parents=True)
        (root / gate.PACKAGE / "mod.py").write_text(text)
    monkeypatch.setattr(gate, "ROOT", tmp_path)
    assert gate.mutated_line(f"{gate.PACKAGE}.mod.xǁKǁm__mutmut_1") == (f"{gate.PACKAGE}/mod.py", 5)
    assert gate.mutated_line(f"{gate.PACKAGE}.mod.xǁKǁm__mutmut_9") is None


def test_a_mutant_of_a_package_init_function_is_placed_in_init(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    (tmp_path / gate.PACKAGE).mkdir(parents=True)
    (tmp_path / gate.PACKAGE / "__init__.py").write_text('"""doc."""\n\ndef get():\n    return 1\n')
    (tmp_path / "mutants" / gate.PACKAGE).mkdir(parents=True)
    (tmp_path / "mutants" / gate.PACKAGE / "__init__.py").write_text(
        "def x_get__mutmut_orig():\n    return 1\ndef x_get__mutmut_1():\n    return 2\n"
    )
    monkeypatch.setattr(gate, "ROOT", tmp_path)
    assert gate.mutated_line(f"{gate.PACKAGE}.x_get__mutmut_1") == (f"{gate.PACKAGE}/__init__.py", 4)


PKG = gate.PACKAGE


def _history(tmp_path: Path, monkeypatch: pytest.MonkeyPatch, base: dict[str, str], head: dict[str, str]) -> str:
    """A repository at `tmp_path` with `base` committed, then `head` on top; returns the base commit."""
    for name in [k for k in os.environ if k.startswith("GIT_")]:
        monkeypatch.delenv(name)

    def git(*args: str) -> str:
        return subprocess.run(["git", *args], cwd=tmp_path, check=True, capture_output=True, text=True).stdout

    def commit(files: dict[str, str]) -> None:
        for path, text in files.items():
            (tmp_path / path).parent.mkdir(parents=True, exist_ok=True)
            (tmp_path / path).write_text(text)
        git("add", "-A")
        git("-c", "user.name=t", "-c", "user.email=t@example.com", "commit", "-qm", "c")

    git("init", "-q", "-b", "main")
    commit(base)
    base_sha = git("rev-parse", "HEAD").strip()
    commit(head)
    monkeypatch.setattr(gate, "ROOT", tmp_path)
    return base_sha


def test_changed_lines_marks_each_changed_line_and_the_line_after_a_deletion(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    base = _history(
        tmp_path,
        monkeypatch,
        {f"{PKG}/mod.py": "a = 1\nb = 2\nc = 3\nd = 4\ne = 5\n"},
        {f"{PKG}/mod.py": "a = 1\nB = 2\nc = 3\ne = 5\n", "tests/x.py": "x = 1\n"},
    )
    assert gate.changed_lines(base) == {f"{PKG}/mod.py": {2, 3}}
    assert gate.changed_lines(base, spec="tests/*.py") == {"tests/x.py": {1}}


def test_changed_globs_names_the_mutmut_functions_the_diff_touches(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    source = "def a():\n    return 1\n\n\nclass K:\n    def m(self):\n        return 2\n\n\ndef b():\n    return 3\n"
    base = _history(
        tmp_path,
        monkeypatch,
        {f"{PKG}/mod.py": source, f"{PKG}/__init__.py": "def get():\n    return 1\n"},
        {f"{PKG}/mod.py": source.replace("return 2", "return 5"), f"{PKG}/__init__.py": "def get():\n    return 2\n"},
    )
    assert gate.changed_globs(base) == [f"{PKG}.x_get__mutmut_*", f"{PKG}.mod.xǁKǁm__mutmut_*"]


def test_mode_targets_a_pull_request_only_when_package_code_changed(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    base = _history(tmp_path, monkeypatch, {f"{PKG}/mod.py": "a = 1\n", "tests/t.py": "b = 1\n"}, {"tests/t.py": "b = 2\n"})
    assert gate.mode(base)[0] == "none"
    (tmp_path / f"{PKG}/mod.py").write_text("a = 2\n")
    subprocess.run(["git", "-C", str(tmp_path), "-c", "user.name=t", "-c", "user.email=t@e", "commit", "-qam", "q"], check=True)
    assert gate.mode(base) == ("targeted", [f"{PKG}/mod.py changed"])


def test_judge_passes_at_the_ratchet_and_fails_one_above(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture
) -> None:
    ratchet = tmp_path / "ratchet"
    ratchet.write_text("# the ratchet\n\n6\n")
    monkeypatch.setattr(gate, "RATCHET", ratchet)
    survivors = {f"m{i}": "survived" for i in range(7)}
    assert gate.judge(dict(list(survivors.items())[:6])) == 0
    assert "survivors 6 <= ratchet 6" in capsys.readouterr().out
    assert gate.judge(survivors) == 1
    assert "survivors 7 > ratchet 6" in capsys.readouterr().out
    assert gate.judge(dict(list(survivors.items())[:4])) == 0
    assert "lower `.mutmut-ratchet` to 4" in capsys.readouterr().out


def _shard(folder: Path, shard: int, **over) -> None:
    doc = {
        "shard": shard,
        "total_mutants": 30,
        "mutants": 3,
        "tested": [f"s{shard}-{j}" for j in range(3)],
        "killed": 2,
        "not_killed": {},
    } | over
    (folder / f"shard-{shard}.json").write_text(json.dumps(doc))


def _ten_shards(folder: Path) -> None:
    for shard in range(10):
        _shard(folder, shard)


def test_gate_ratchet_accepts_ten_shards_that_grade_every_mutant_once(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture
) -> None:
    ratchet = tmp_path / "ratchet"
    ratchet.write_text("6\n")
    monkeypatch.setattr(gate, "RATCHET", ratchet)
    _ten_shards(tmp_path)
    assert gate.gate_ratchet(tmp_path) == 0
    assert "sum(tested) == total mutants == 30" in capsys.readouterr().out
    assert (tmp_path / "mutmut-survivors.txt").read_text() == ""


def test_gate_ratchet_fails_on_a_missing_shard_or_a_mismatch_of_totals_or_grades(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture
) -> None:
    ratchet = tmp_path / "ratchet"
    ratchet.write_text("6\n")
    monkeypatch.setattr(gate, "RATCHET", ratchet)
    _ten_shards(tmp_path)
    (tmp_path / "shard-9.json").unlink()
    assert gate.gate_ratchet(tmp_path) == 1
    assert "shards [9] reported nothing" in capsys.readouterr().out

    _ten_shards(tmp_path)
    _shard(tmp_path, 4, total_mutants=31)
    assert gate.gate_ratchet(tmp_path) == 1
    assert "different totals" in capsys.readouterr().out

    _ten_shards(tmp_path)
    _shard(tmp_path, 1, tested=["s0-0", "s1-1", "s1-2"])
    assert gate.gate_ratchet(tmp_path) == 1
    out = capsys.readouterr().out
    assert "1 mutants were graded by more than one shard" in out
    assert "sum(tested) 29 != total mutants 30" in out


def test_gate_ratchet_judges_the_survivors_of_all_shards(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture
) -> None:
    ratchet = tmp_path / "ratchet"
    ratchet.write_text("6\n")
    monkeypatch.setattr(gate, "RATCHET", ratchet)
    _ten_shards(tmp_path)
    _shard(tmp_path, 0, not_killed={f"n{i}": "survived" for i in range(4)})
    _shard(tmp_path, 5, not_killed={f"k{i}": "timeout" for i in range(3)})
    assert gate.gate_ratchet(tmp_path) == 1
    assert "survivors 7 > ratchet 6" in capsys.readouterr().out
    assert (tmp_path / "mutmut-survivors.txt").read_text().splitlines() == sorted(
        [f"n{i}" for i in range(4)] + [f"k{i}" for i in range(3)]
    )


def _gate_pr_setup(
    monkeypatch: pytest.MonkeyPatch, globs: list[str], survivor_lines: dict[str, int], returncode: int = 0, stdout: str = ""
) -> None:
    monkeypatch.setattr(gate, "changed_globs", lambda base: globs)
    monkeypatch.setattr(
        gate, "run_mutmut", lambda args, label="", globs=None: subprocess.CompletedProcess([], returncode, stdout, "")
    )
    monkeypatch.setattr(
        gate, "statuses", lambda: {f"{PKG}.mod.x_f__mutmut_{n}": s for n, s in {1: "survived", 2: "killed", 3: "timeout"}.items()}
    )
    monkeypatch.setattr(gate, "changed_lines", lambda base, spec=f"{PKG}/*.py": {f"{PKG}/mod.py": {5}})
    monkeypatch.setattr(
        gate, "mutated_line", lambda name: (f"{PKG}/mod.py", survivor_lines[name[-1]]) if name[-1] in survivor_lines else None
    )
    monkeypatch.setattr(gate, "show", lambda names: None)


def test_gate_pr_fails_on_a_survivor_of_a_changed_line_and_lists_the_others(
    monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture
) -> None:
    _gate_pr_setup(monkeypatch, [f"{PKG}.mod.x_f__mutmut_*"], {"1": 5, "3": 9})
    assert gate.gate_pr("main", None) == 1
    out = capsys.readouterr().out
    assert "1 mutant(s) on changed lines not killed" in out
    assert f"survived   {PKG}.mod.x_f__mutmut_1" in out
    assert f"  timeout    {PKG}.mod.x_f__mutmut_3  (line 9, not changed by this pull request)" in out


def test_gate_pr_passes_when_every_survivor_is_off_the_changed_lines(
    monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture
) -> None:
    _gate_pr_setup(monkeypatch, [f"{PKG}.mod.x_f__mutmut_*"], {"1": 9, "3": 9})
    assert gate.gate_pr("main", None) == 0
    assert "every mutant on a changed line is killed" in capsys.readouterr().out


def test_gate_pr_with_no_changed_function_runs_nothing(monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture) -> None:
    _gate_pr_setup(monkeypatch, [], {})
    monkeypatch.setattr(gate, "run_mutmut", lambda *a, **k: pytest.fail("mutmut must not run"))
    assert gate.gate_pr("main", None) == 0
    assert "touches no function" in capsys.readouterr().out


def test_gate_pr_passes_a_shard_that_has_no_mutants_and_fails_any_other_mutmut_error(
    monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture
) -> None:
    _gate_pr_setup(monkeypatch, [f"{PKG}.mod.x_f__mutmut_*"], {}, returncode=1, stdout="nothing matches")
    assert gate.gate_pr("main", 3) == 0
    _gate_pr_setup(monkeypatch, [f"{PKG}.mod.x_f__mutmut_*"], {}, returncode=2, stdout="boom")
    assert gate.gate_pr("main", None) == 1
    assert "mutmut run exited 2" in capsys.readouterr().out
