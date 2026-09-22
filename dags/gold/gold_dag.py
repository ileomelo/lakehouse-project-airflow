from airflow.sdk import (
    DAG,  # noqa: F401 — garante detecção pelo dag_discovery_safe_mode
)

from lakehouse.pipelines.gold_dag import build_gold_dag

gold_transformations = build_gold_dag()
