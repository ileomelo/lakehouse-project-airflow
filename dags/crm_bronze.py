from pathlib import Path

import pandas as pd
from airflow.sdk import DAG, task

with DAG(
    dag_id="crm_bronze",
    schedule=None,
) as dag:

    @task
    def extract_and_load_cust_info():

        input_path = Path("/opt/airflow/data/source/crm/cust_info.csv")

        output_path = Path("/opt/airflow/data/bronze/crm/cust_info/cust_info.parquet")

        output_path.parent.mkdir(
            parents=True,
            exist_ok=True,
        )

        df = pd.read_csv(input_path)

        df.to_parquet(output_path)

        print(f"Bronze criada em: {output_path}")

    extract_and_load_cust_info()
