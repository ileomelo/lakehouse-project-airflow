from pathlib import Path

import pandas as pd
from airflow.sdk import DAG, task

with DAG(
    dag_id="crm_bronze",
    schedule=None,
) as dag:

    @task
    def extract_cust_info():

        input_path = Path("/opt/airflow/data/source/crm/cust_info.csv")

        staging_path = Path("/opt/airflow/data/bronze/crm/cust_info/cust_info.parquet")

        staging_path.parent.mkdir(
            parents=True,
            exist_ok=True,
        )

        df = pd.read_csv(input_path)

        df.to_parquet(staging_path)

        return str(staging_path)

    extract_cust_info()

    @task
    def load_cust_info(staging_path):

        staging_path = Path(staging_path)

        bronze_path = Path("/opt/airflow/data/bronze/crm/cust_info/cust_info.parquet")

        bronze_path.parent.mkdir(
            parents=True,
            exist_ok=True,
        )

        df = pd.read_parquet(staging_path)

        df.to_parquet(bronze_path)

        print(f"Data loaded to bronze layer at: {bronze_path}")

        staging_path = extract_cust_info()

        load_cust_info(staging_path)
