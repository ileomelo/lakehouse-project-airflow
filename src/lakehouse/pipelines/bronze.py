"""Factory da ingestão Bronze: CSV local para Parquet local e MinIO."""

from __future__ import annotations

import os
from pathlib import Path

import pandas as pd
from airflow.sdk import DAG, get_current_context, task
from minio.error import S3Error

from lakehouse.config.catalog import DatasetConfig
from lakehouse.infrastructure.object_storage import get_minio_client


def build_bronze_dag(
    source: str,
    datasets: tuple[DatasetConfig, ...],
    schedule: str | None = None,
    tags: tuple[str, ...] = (),
) -> DAG:
    """Cria uma DAG Bronze para os datasets de uma fonte."""
    with DAG(
        dag_id=f"{source}_bronze",
        schedule=schedule,
        catchup=False,
        tags=(source, "bronze", *tags),
    ) as dag:

        @task
        def extract(dataset: DatasetConfig) -> dict[str, str]:
            partition_date = get_current_context()["logical_date"].strftime("%Y-%m-%d")
            input_path = Path(f"/opt/airflow/data/source/{source}/{dataset.source_file}")
            staging_path = (
                Path("/opt/airflow/data/bronze")
                / source
                / dataset.name
                / partition_date
                / f"{dataset.name}.parquet"
            )
            staging_path.parent.mkdir(parents=True, exist_ok=True)
            if not input_path.exists():
                raise FileNotFoundError(f"Fonte não encontrada: {input_path}")
            pd.read_csv(input_path).to_parquet(staging_path, index=False)
            return {
                "staging_path": str(staging_path),
                "partition_date": partition_date,
                "name": dataset.name,
            }

        @task
        def load(extract_result: dict[str, str]) -> None:
            local_file = Path(extract_result["staging_path"])
            if not local_file.exists():
                raise FileNotFoundError(f"Arquivo de staging não encontrado: {local_file}")

            object_name = (
                f"bronze/{source}/{extract_result['name']}/"
                f"{extract_result['partition_date']}/{extract_result['name']}.parquet"
            )
            try:
                get_minio_client().fput_object(
                    os.environ["MINIO_BUCKET"], object_name, str(local_file)
                )
            except S3Error:
                raise

        for dataset in datasets:
            extracted = extract.override(task_id=f"extract_{dataset.name}")(dataset)
            load.override(task_id=f"load_{dataset.name}")(extracted)

    return dag

