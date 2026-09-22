from airflow.sdk import (
    DAG,  # noqa: F401 — garante detecção pelo dag_discovery_safe_mode
)

from lakehouse.pipelines.silver_dag import build_silver_dag

silver_transformations = build_silver_dag()
