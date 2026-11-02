"""Names vulture cannot see used. Every entry names its date and its reason; `closes:` names what removes the entry."""

get_provider_info  # 2026-11-02: Airflow reads it through the apache_airflow_provider entry point
slack_failure_callback  # 2026-11-02: public API, passed by DAG authors as on_failure_callback (docs/slack.md)
build_cdc_dag  # 2026-11-02: public DAG builder (docs/dag-builders.md)
build_source_dags  # 2026-11-02: public DAG builder (docs/dag-builders.md)
build_config_dag  # 2026-11-02: public DAG builder (docs/dag-builders.md)
cloud_environment  # 2026-11-02: public helper, asserted by tests/test_connections.py; no product caller
DEGRADATIONS  # 2026-11-02: the degradation vocabulary; tests/test_contract.py holds it to docs/capabilities.md
retries  # 2026-11-02: Airflow 3 reads the watcher's retries attribute; set to 0 on purpose (dags.py)
template_fields  # 2026-11-02: Airflow operator attribute, read by Airflow's templating
ui_color  # 2026-11-02: Airflow operator attribute, read by the Airflow UI
on_kill  # 2026-11-02: Airflow calls it when a task is killed
autocommit  # 2026-11-02: psycopg2 connection attribute, read by the driver
