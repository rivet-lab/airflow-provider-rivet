"""Seeded-defect recall: re-introduce each fixed bug as a product patch and grade whether its cells go red."""

from __future__ import annotations

import argparse
import os
import shutil
import subprocess
import sys
import tempfile
import xml.etree.ElementTree as ET
from dataclasses import dataclass
from pathlib import Path

import yaml

HERE = Path(__file__).resolve().parent
ROOT = HERE.parents[1]
MANIFEST = HERE / "seeds.yaml"
CAUGHT, MISSED, STALE, BROKEN, NO_CONTROL = "CAUGHT", "MISSED", "STALE", "BROKEN", "NO-CONTROL"
PASS, FAIL, SKIP, ABSENT = "PASS", "FAIL", "SKIP", "ABSENT"
IGNORED = shutil.ignore_patterns(".git", ".venv", "__pycache__", ".pytest_cache", "target", "mutants")


@dataclass(frozen=True)
class Seed:
    """One seed as the manifest declares it."""

    name: str
    bug: str
    fix: str
    what: str
    patch: Path
    cells: tuple[str, ...]


@dataclass(frozen=True)
class Verdict:
    """A graded seed: its status and the evidence for it."""

    seed: Seed
    status: str
    detail: str


def load_seeds(manifest: Path = MANIFEST) -> list[Seed]:
    """The manifest's seeds; ValueError names the first malformed entry."""
    seeds, names = [], set()
    for s in yaml.safe_load(manifest.read_text())["seeds"]:
        name = s.get("name")
        if not name or name in names:
            raise ValueError(f"seed {name!r}: missing or duplicate name")
        names.add(name)
        for key in ("bug", "fix", "what", "patch"):
            if not str(s.get(key) or "").strip():
                raise ValueError(f"seed {name}: `{key}` is required")
        if not (manifest.parent / s["patch"]).is_file():
            raise ValueError(f"seed {name}: patch {s['patch']} does not exist")
        cells = tuple(s.get("cells") or ())
        if not cells or any("::" not in c for c in cells):
            raise ValueError(f"seed {name}: needs `cells:` as path::test, at least one")
        seeds.append(Seed(name, s["bug"], s["fix"], s["what"], manifest.parent / s["patch"], cells))
    if not seeds:
        raise ValueError("the manifest declares no seeds")
    return seeds


def patch_files(patch_text: str) -> list[str]:
    """The repo-relative files a unified diff changes."""
    return [line[6:] for line in patch_text.splitlines() if line.startswith("+++ b/")]


def junit_outcomes(xml_path: Path, cells: tuple[str, ...]) -> dict[str, str]:
    """Each cell's outcome in a pytest junit file: PASS, FAIL, SKIP (every parametrization skipped) or ABSENT."""
    cases = [tc for tc in ET.parse(xml_path).iter("testcase")]
    out = {}
    for cell in cells:
        path, func = cell.split("::", 1)
        stem = Path(path).stem
        hits = [
            tc
            for tc in cases
            if tc.get("classname", "").split(".")[-1] == stem
            and (tc.get("name") == func or tc.get("name", "").startswith(func + "["))
        ]
        if not hits:
            out[cell] = ABSENT
        elif any(tc.find("failure") is not None or tc.find("error") is not None for tc in hits):
            out[cell] = FAIL
        elif all(tc.find("skipped") is not None for tc in hits):
            out[cell] = SKIP
        else:
            out[cell] = PASS
    return out


def run_cells(tree: Path, cells: tuple[str, ...], env: dict[str, str], xml: Path) -> dict[str, str]:
    """Run exactly these cells in the tree, one pytest session per test directory, and grade each from its junit file."""
    out: dict[str, str] = {}
    groups: dict[str, list[str]] = {}
    for cell in cells:
        groups.setdefault(str(Path(cell.split("::", 1)[0]).parent), []).append(cell)
    for i, group in enumerate(groups.values()):
        part = xml.with_name(f"{xml.stem}-{i}.xml")
        part.unlink(missing_ok=True)
        subprocess.run(
            [sys.executable, "-m", "pytest", "-q", "-p", "no:cacheprovider", f"--junitxml={part}", *group],
            cwd=tree,
            env=env,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            text=True,
        )
        if part.exists():
            out.update(junit_outcomes(part, tuple(group)))
        else:
            out.update({c: ABSENT for c in group})
    return out


def grade(seed: Seed, control: dict[str, str], seeded: dict[str, str] | None) -> Verdict:
    """CAUGHT iff a declared cell green on the clean tree is red on the seeded one."""
    if seeded is None:
        return Verdict(
            seed,
            STALE,
            f"seed {seed.name} no longer applies to {', '.join(patch_files(seed.patch.read_text()))}: update the patch",
        )
    gone = [c for c in seed.cells if seeded.get(c) == ABSENT or control.get(c) == ABSENT]
    if gone:
        return Verdict(seed, BROKEN, f"declared cells do not exist: {', '.join(gone)}")
    red_before = [c for c in seed.cells if control.get(c) != PASS]
    if red_before:
        said = ", ".join(f"{c}={control[c]}" for c in red_before)
        return Verdict(seed, NO_CONTROL, f"a declared cell is not green on the clean tree ({said})")
    red = [c for c in seed.cells if seeded.get(c) == FAIL]
    if red:
        return Verdict(seed, CAUGHT, ", ".join(red))
    said = ", ".join(f"{c}={seeded[c]}" for c in seed.cells)
    return Verdict(seed, MISSED, f"every declared cell stayed green under the seed ({said})")


def apply_seed(tree: Path, patch: Path) -> dict[Path, bytes] | None:
    """Apply a patch in the tree; the snapshot of the files it touches, or None when it does not apply."""
    snap = {tree / f: (tree / f).read_bytes() for f in patch_files(patch.read_text())}
    if subprocess.run(["git", "apply", "--check", str(patch)], cwd=tree, capture_output=True).returncode != 0:
        return None
    subprocess.run(["git", "apply", str(patch)], cwd=tree, check=True)
    return snap


def restore(snap: dict[Path, bytes]) -> None:
    """Write each snapshotted file back, so the next seed starts from the clean tree."""
    for f, data in snap.items():
        f.write_bytes(data)


def run(seeds: list[Seed], root: Path = ROOT, base: Path | None = None) -> list[Verdict]:
    """Grade each seed on a copy of the tree: the clean tree first, then each seed applied and its cells run."""
    work = Path(tempfile.mkdtemp(prefix="seeded-", dir=base))
    tree = work / "tree"
    shutil.copytree(root, tree, ignore=IGNORED, symlinks=True)
    env = {**os.environ, "PYTHONPATH": str(tree)}
    if "RIVET_SRC" in env:
        env["RIVET_SRC"] = str(Path(env["RIVET_SRC"]).resolve())
    union = tuple(dict.fromkeys(c for s in seeds for c in s.cells))
    control = run_cells(tree, union, env, work / "control.xml")
    verdicts = []
    for seed in seeds:
        snap = apply_seed(tree, seed.patch)
        try:
            seeded = None if snap is None else run_cells(tree, seed.cells, env, work / f"{seed.name}.xml")
        finally:
            if snap is not None:
                restore(snap)
        verdicts.append(grade(seed, control, seeded))
    shutil.rmtree(work, ignore_errors=True)
    return verdicts


def shard_of(seeds: list[Seed], spec: str) -> list[Seed]:
    """The `i/n` shard of the seeds: every n-th seed starting at i."""
    i, n = (int(x) for x in spec.split("/"))
    return seeds[i::n]


def self_test(base: Path | None = None) -> list[str]:
    """Problems in the grading on a synthetic tree: a seed caught, a seed with no effect missed, a stale patch stale."""
    problems = []
    root = Path(tempfile.mkdtemp(prefix="seeded-self-", dir=base))
    (root / "pkg").mkdir()
    (root / "tests").mkdir()
    (root / "pkg" / "__init__.py").write_text("")
    (root / "pkg" / "calc.py").write_text("def double(x):\n    return x * 2\n")
    (root / "tests" / "test_calc.py").write_text(
        "from pkg.calc import double\n\n\ndef test_double() -> None:\n    assert double(3) == 6\n"
    )
    (root / "seeds").mkdir()
    (root / "seeds" / "caught.patch").write_text(
        "--- a/pkg/calc.py\n+++ b/pkg/calc.py\n@@ -1,2 +1,2 @@\n def double(x):\n-    return x * 2\n+    return x + x + 1\n"
    )
    (root / "seeds" / "no-effect.patch").write_text(
        "--- a/pkg/calc.py\n+++ b/pkg/calc.py\n@@ -1,2 +1,2 @@\n-def double(x):\n+def double(x):  # same behaviour\n     return x * 2\n"
    )
    (root / "seeds" / "stale.patch").write_text(
        "--- a/pkg/calc.py\n+++ b/pkg/calc.py\n@@ -1,2 +1,2 @@\n def triple(x):\n-    return x * 3\n+    return x * 4\n"
    )
    cell = "tests/test_calc.py::test_double"
    seeds = [
        Seed(n, "self-test", "none", "self-test", root / "seeds" / f"{n}.patch", (cell,))
        for n in ("caught", "no-effect", "stale")
    ]
    expected = {"caught": CAUGHT, "no-effect": MISSED, "stale": STALE}
    for verdict in run(seeds, root=root, base=base):
        if verdict.status != expected[verdict.seed.name]:
            problems.append(f"self-test {verdict.seed.name}: {verdict.status}, want {expected[verdict.seed.name]}")
    shutil.rmtree(root, ignore_errors=True)
    return problems


def main(argv: list[str] | None = None) -> int:
    """Grade the seeds (or run the self-test) and print one line per seed; exit 1 on any problem."""
    ap = argparse.ArgumentParser(prog="python -m dev.seeded", description=__doc__.splitlines()[0])
    ap.add_argument("--seeds", help="comma-separated seed names")
    ap.add_argument("--shard", help="i/n: this shard of the seeds")
    ap.add_argument("--self-test", action="store_true", help="grade a synthetic tree instead of the repository")
    args = ap.parse_args(argv)
    if args.self_test:
        problems = self_test()
        print("self-test: " + ("ok" if not problems else "; ".join(problems)))
        return 1 if problems else 0
    seeds = load_seeds()
    if args.seeds:
        wanted = args.seeds.split(",")
        seeds = [s for s in seeds if s.name in wanted]
    if args.shard:
        seeds = shard_of(seeds, args.shard)
    verdicts = run(seeds)
    for v in verdicts:
        print(f"{v.status:<11} {v.seed.name:<18} {v.seed.bug:<34} {v.detail}")
    bad = [v for v in verdicts if v.status != CAUGHT]
    print(f"{len(verdicts) - len(bad)} of {len(verdicts)} seeds CAUGHT")
    return 1 if bad else 0


if __name__ == "__main__":
    sys.exit(main())
