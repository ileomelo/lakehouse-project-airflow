"""Factory da DAG de transformações Silver."""

from __future__ import annotations

from airflow.providers.standard.sensors.external_task import ExternalTaskSensor
from airflow.sdk import DAG, task

from lakehouse.config.partition import current_partition_date
from lakehouse.pipelines.silver import TRANSFORMATIONS
from lakehouse.transformations.silver.common import default_paths


def build_silver_dag(schedule: str | None = "@daily") -> DAG:
    """Cria a Silver após CRM Bronze e ERP Bronze concluírem com sucesso."""
    with DAG(
        dag_id="silver_transformations",
        schedule=schedule,
        catchup=False,
        tags=("silver", "transformations"),
    ) as dag:

        crm_bronze_done = ExternalTaskSensor(
            task_id="wait_for_crm_bronze",
            external_dag_id="crm_bronze",
            allowed_states=["success"],
            failed_states=["failed", "skipped"],
            check_existence=True,
            mode="reschedule",
            poke_interval=60,
            timeout=60 * 60 * 6,
        )
        erp_bronze_done = ExternalTaskSensor(
            task_id="wait_for_erp_bronze",
            external_dag_id="erp_bronze",
            allowed_states=["success"],
            failed_states=["failed", "skipped"],
            check_existence=True,
            mode="reschedule",
            poke_interval=60,
            timeout=60 * 60 * 6,
        )

        @task
        def resolve_partition_date() -> str:
            return current_partition_date()

        @task
        def transform_dataset(dataset: str, run_partition: str) -> int:
            for registered_dataset, source, name, _, transform in TRANSFORMATIONS:
                if registered_dataset == dataset:
                    bronze_path, silver_path = default_paths(
                        source, name, run_partition
                    )
                    return transform(bronze_path, silver_path)
            raise ValueError(f"Dataset Silver não registrado: {dataset}")

        partition_date = resolve_partition_date()
        for dataset, *_ in TRANSFORMATIONS:
            transformation = transform_dataset.override(
                task_id=f"transform_{dataset.replace('.', '_')}"
            )(dataset, partition_date)  # ty: ignore[invalid-argument-type]
            [crm_bronze_done, erp_bronze_done] >> transformation

    return dag
