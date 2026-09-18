import os
from pathlib import Path

import pandas as pd
from airflow.sdk import DAG, get_current_context, task
from minio import Minio
from minio.error import S3Error


def _get_minio_client() -> Minio:
    return Minio(
        os.environ["MINIO_ENDPOINT"].replace("http://", "").replace("https://", ""),
        access_key=os.environ["MINIO_ACCESS_KEY"],
        secret_key=os.environ["MINIO_SECRET_KEY"],
        secure=False,
    )


def build_bronze_dag(
    source: str, datasets: list[dict], schedule=None, tags=None
) -> DAG:
    tags = tags or []

    with DAG(
        dag_id=f"{source}_bronze",
        schedule=schedule,
        catchup=False,
        tags=[source, "bronze", *tags],
    ) as dag:

        @task
        def extract(dataset: dict):
            context = get_current_context()
            partition_date = context["logical_date"].strftime("%Y-%m-%d")

            name = dataset["name"]
            source_file = dataset["source_file"]

            input_path = Path(f"/opt/airflow/data/source/{source}/{source_file}")
            staging_path = (
                Path(f"/opt/airflow/data/bronze/{source}/{name}")
                / partition_date
                / f"{name}.parquet"
            )
            staging_path.parent.mkdir(parents=True, exist_ok=True)

            df = pd.read_csv(input_path)
            df.to_parquet(staging_path)

            return {
                "staging_path": str(staging_path),
                "partition_date": partition_date,
                "name": name,
            }

        @task
        def load(extract_result: dict):
            local_file = Path(extract_result["staging_path"])
            if not local_file.exists():
                raise FileNotFoundError(f"File not found: {local_file}")

            client = _get_minio_client()
            bucket = os.environ["MINIO_BUCKET"]
            object_name = (
                f"bronze/{source}/{extract_result['name']}"
                f"/{extract_result['partition_date']}/{extract_result['name']}.parquet"
            )

            try:
                client.fput_object(bucket, object_name, str(local_file))
            except S3Error as e:
                print(f"Erro ao subir {object_name}: {e}")
                raise

        for dataset in datasets:
            extracted = extract.override(task_id=f"extract_{dataset['name']}")(dataset)
            load.override(task_id=f"load_{dataset['name']}")(extracted) # ty: ignore[invalid-argument-type]

    return dag
