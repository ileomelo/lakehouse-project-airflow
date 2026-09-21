from airflow.sdk import (
    DAG,  # noqa: F401 — garante detecção pelo dag_discovery_safe_mode
)

from lakehouse.config.catalog import SOURCE_CATALOG
from lakehouse.pipelines.bronze import build_bronze_dag

for source_name, datasets in SOURCE_CATALOG.items():
    dag = build_bronze_dag(source=source_name, datasets=datasets)
    globals()[dag.dag_id] = dag
