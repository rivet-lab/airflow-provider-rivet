"""Every module in dev/ is held by a test that loads it, a `--self-test`, or a dated exemption below."""

from __future__ import annotations

from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]

#: Modules no test loads by name: (date accepted, reason, what closes it).
DEV_EXEMPT: dict[str, tuple[str, str, str]] = {
    "mutmut_isolation.py": (
        "2026-11-02",
        "a pytest plugin loaded by mutmut's `-p` option in pyproject.toml, only inside a mutation run",
        "a test that loads it as a plugin in a pytest subprocess",
    ),
}


def _held(name: str, tests: str) -> bool:
    """Whether a dev module is named by a test, or carries a `--self-test` of its own."""
    return name in tests or "--self-test" in (ROOT / "dev" / name).read_text()


def test_every_dev_module_is_loaded_by_a_test_or_has_a_self_test_or_a_dated_exemption() -> None:
    tests = "\n".join(
        p.read_text() for suite in ("tests", "gate/tests") for p in (ROOT / suite).rglob("*.py") if p.name != Path(__file__).name
    )
    unheld = [p.name for p in sorted((ROOT / "dev").glob("*.py")) if not _held(p.name, tests) and p.name not in DEV_EXEMPT]
    assert unheld == []


def test_an_exempt_module_is_still_a_module_of_dev() -> None:
    assert all((ROOT / "dev" / name).is_file() for name in DEV_EXEMPT)
