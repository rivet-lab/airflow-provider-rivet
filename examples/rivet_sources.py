"""Example: every source database of examples/rivet_sources.yaml as its own DAG (docs/dags-from-file.md).

Put both files in the DAGs folder; adding a source is one line in the YAML file, no Python.
"""

from pathlib import Path

from airflow_provider_rivet.dags import build_dags_from_file

globals().update(build_dags_from_file(Path(__file__).with_name("rivet_sources.yaml")))
