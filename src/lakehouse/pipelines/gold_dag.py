"""Factory da DAG de transformações Gold."""

from __future__ import annotations

from airflow.providers.standard.sensors.external_task import ExternalTaskSensor
from airflow.sdk import DAG, task

from lakehouse.config.partition import current_partition_date
from lakehouse.transformations.gold.gold import (
    transform_dim_customers,
    transform_dim_products,
    transform_fact_sales,
)


def build_gold_dag(schedule: str | None = "@daily") -> DAG:
    """Cria a Gold após a Silver e mantém a ordem dimensão -> fato."""
    with DAG(
        dag_id="gold_transformations",
        schedule=schedule,
        catchup=False,
        tags=("gold", "transformations"),
    ) as dag:

        silver_done = ExternalTaskSensor(
            task_id="wait_for_silver_transformations",
            external_dag_id="silver_transformations",
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
        def build_customers(run_partition: str) -> int:
            return transform_dim_customers(run_partition)

        @task
        def build_products(run_partition: str) -> int:
            return transform_dim_products(run_partition)

        @task
        def build_sales(run_partition: str) -> int:
            return transform_fact_sales(run_partition)

        partition_date = resolve_partition_date()
        customers = build_customers(partition_date)  # ty: ignore[invalid-argument-type]
        products = build_products(partition_date)  # ty: ignore[invalid-argument-type]
        sales = build_sales(partition_date)  # ty: ignore[invalid-argument-type]
        silver_done >> [customers, products]
        [customers, products] >> sales

    return dag
