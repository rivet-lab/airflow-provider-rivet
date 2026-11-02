"""Grade one CI job's pytest junit report: every skip is allowed by the job's dated ledger entry, and the count stays under its ceiling."""

from __future__ import annotations

import argparse
import sys
import xml.etree.ElementTree as ET
from pathlib import Path

XFAILED_ON_MAIN: dict[str, tuple[str, str, str]] = {
    "tests.test_operators::test_a_task_applies_its_own_plan_when_rivet_records_another_spelling_of_the_config[case]": (
        "2026-11-02",
        "this file system tells `STATE` from `state`",
        "the test runs on a case-sensitive file system",
    ),
    "tests.test_operators::test_a_task_applies_its_own_plan_when_rivet_records_another_spelling_of_the_config[unicode-form]": (
        "2026-11-02",
        "this file system tells the two Unicode forms of one name apart",
        "the test runs on a file system that folds Unicode forms",
    ),
}
SLACK_HOOK_SHAPE = "tests.test_slack_alerts::test_the_fake_slack_hooks_have_the_shape_of_the_slack_providers_hooks"
SKIP_ALLOWED: dict[str, dict[str, tuple[str, str, str]]] = {
    "locked": {
        **XFAILED_ON_MAIN,
        SLACK_HOOK_SHAPE: (
            "2026-11-02",
            "`uv sync --locked` installs no slack extra, so the provider's Slack hooks cannot be imported",
            "the locked job syncs the slack extra",
        ),
    },
    "release": {
        **XFAILED_ON_MAIN,
        SLACK_HOOK_SHAPE: (
            "2026-11-02",
            "the release build syncs the locked environment, which installs no slack extra",
            "the release build syncs the slack extra",
        ),
    },
    "airflow": dict(XFAILED_ON_MAIN),
    "gate-oracles": {},
}
SKIP_CEILING = {"locked": 3, "release": 3, "airflow": 2, "gate-oracles": 0}


def skipped(junit: Path) -> dict[str, str]:
    """The test ids a junit report skipped, each with the skip's message."""
    found = {}
    for case in ET.parse(junit).iter("testcase"):
        for skip in case.findall("skipped"):
            found[f"{case.get('classname')}::{case.get('name')}"] = skip.get("message", "")
    return found


def grade(skips: dict[str, str], job: str) -> list[str]:
    """The problems with one job's skips: an unallowed skip, a stale ledger entry, or a count over the ceiling."""
    allowed = SKIP_ALLOWED[job]
    problems = [f"unallowed skip {test}: {why}" for test, why in sorted(skips.items()) if test not in allowed]
    problems += [
        f"stale ledger entry {test}: no run of {job} skips it any more, remove it"
        for test in sorted(allowed)
        if test not in skips
    ]
    if len(skips) > SKIP_CEILING[job]:
        problems.append(f"{len(skips)} skips over the {job} ceiling of {SKIP_CEILING[job]}")
    return problems


def main(argv: list[str]) -> int:
    """Grade the junit report of `--job`; exit 1 with every problem found."""
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("junit", type=Path, help="the pytest --junitxml report of the job")
    parser.add_argument("--job", required=True, choices=sorted(SKIP_ALLOWED), help="the CI job the report is from")
    args = parser.parse_args(argv)
    skips = skipped(args.junit)
    problems = grade(skips, args.job)
    print(
        f"skip census {args.job}: {len(skips)} skipped, ceiling {SKIP_CEILING[args.job]}, {len(SKIP_ALLOWED[args.job])} allowed"
    )
    for problem in problems:
        print(f"  {problem}")
    return 1 if problems else 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
