import os
from pathlib import Path

import pandas as pd
from airflow.sdk import DAG, get_current_context, task
from minio import Minio
from minio.error import S3Error

with DAG(
    dag_id="erp_bronze_cust_az12",
    schedule=None,
) as dag:

    @task
    def extract_cust_az12():

        context = get_current_context()

        logical_date = context["logical_date"]

        partition_date = logical_date.strftime("%Y-%m-%d")

        input_path = Path("/opt/airflow/data/source/erp/CUST_AZ12.csv")

        staging_path = (
            Path("/opt/airflow/data/bronze/erp/cust_az12")
            / partition_date
            / "cust_az12.parquet"
        )

        staging_path.parent.mkdir(
            parents=True,
            exist_ok=True,
        )

        df = pd.read_csv(input_path)

        df.to_parquet(staging_path)

        return {
            "staging_path": str(staging_path),
            "partition_date": partition_date,
        }

    @task
    def load_cust_az12(extract_result):

        staging_path = extract_result["staging_path"]
        partition_date = extract_result["partition_date"]

        print(f"Received from staging_path: {staging_path}")

        local_file = Path(staging_path)
        if not local_file.exists():
            raise FileNotFoundError(f"File not found: {local_file}")

        client = Minio(
            os.environ["MINIO_ENDPOINT"].replace("http://", "").replace("https://", ""),
            access_key=os.environ["MINIO_ACCESS_KEY"],
            secret_key=os.environ["MINIO_SECRET_KEY"],
            secure=False,
        )

        bucket = os.environ["MINIO_BUCKET"]

        object_name = f"bronze/erp/cust_az12/{partition_date}/cust_az12.parquet"

        try:
            client.fput_object(bucket, object_name, str(local_file))
            print(f"File {local_file} uploaded to bucket {bucket} as {object_name}.")
        except S3Error as e:
            print(f"Error occurred while uploading file: {e}")
            raise

    staging_path = extract_cust_az12()

    load_cust_az12(staging_path)
