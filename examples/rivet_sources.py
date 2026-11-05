"""Example: every source database of examples/rivet_sources.yaml as its own DAG (docs/dags-from-file.md).

Put both files in the DAGs folder; adding a source is one line in the YAML file, no Python.
"""

from airflow_provider_rivet.dags import build_dags_from_file

globals().update(build_dags_from_file("rivet_sources.yaml"))
