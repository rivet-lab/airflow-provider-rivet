"""Print the minimum rivet version, `0.32.2`, from the one place that holds it: `MIN_RIVET_VERSION` in
airflow_provider_rivet/capabilities.py. The CI and gate workflows run this instead of keeping a number of their own;
it reads the file as text, so it needs neither Airflow nor the package installed."""

import ast
from pathlib import Path

SOURCE = Path(__file__).resolve().parents[1] / "airflow_provider_rivet" / "capabilities.py"


def min_rivet() -> str:
    """The minimum version as `X.Y.Z`."""
    for node in ast.parse(SOURCE.read_text()).body:
        if isinstance(node, ast.Assign) and [t.id for t in node.targets if isinstance(t, ast.Name)] == ["MIN_RIVET_VERSION"]:
            return ".".join(str(part) for part in ast.literal_eval(node.value))
    raise SystemExit(f"MIN_RIVET_VERSION is not assigned in {SOURCE}")


if __name__ == "__main__":
    print(min_rivet())
