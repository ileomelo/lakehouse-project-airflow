import os

import duckdb

# docker compose exec airflow-scheduler python /opt/airflow/scripts/transformers/bronze/crm/crm_prd_info.py

bucket = os.environ["MINIO_BUCKET"]
partition_date = "2026-09-18"  # Substitua pela data da partição desejada.
minio_endpoint = (
    os.environ["MINIO_ENDPOINT"].removeprefix("http://").removeprefix("https://")
)

bronze_s3_path = f"s3://{bucket}/bronze/crm/prd_info/{partition_date}/prd_info.parquet"
silver_s3_path = f"s3://{bucket}/silver/crm/prd_info/{partition_date}/prd_info.parquet"


def transform_prd_info(bronze_path: str, silver_path: str) -> None:
    """Transforma o produto Bronze e grava o Parquet resultante na Silver."""
    con = duckdb.connect()

    try:
        # httpfs permite que o DuckDB leia e escreva diretamente no MinIO via S3.
        con.execute("INSTALL httpfs")
        con.execute("LOAD httpfs")
        con.execute(f"SET s3_endpoint = '{minio_endpoint}'")
        con.execute(f"SET s3_access_key_id = '{os.environ['MINIO_ACCESS_KEY']}'")
        con.execute(f"SET s3_secret_access_key = '{os.environ['MINIO_SECRET_KEY']}'")
        con.execute("SET s3_region = 'us-east-1'")
        con.execute("SET s3_use_ssl = false")
        con.execute("SET s3_url_style = 'path'")

        con.execute(f"""
            COPY (
                WITH transformed_products AS (
                    SELECT
                        prd_id,
                        REPLACE(SUBSTRING(prd_key, 1, 5), '-', '_') AS cat_id,
                        SUBSTRING(prd_key, 7) AS prd_key,
                        prd_nm,
                        COALESCE(prd_cost, 0) AS prd_cost,
                        CASE UPPER(TRIM(prd_line))
                            WHEN 'M' THEN 'Mountain'
                            WHEN 'R' THEN 'Road'
                            WHEN 'S' THEN 'Other Sales'
                            WHEN 'T' THEN 'Touring'
                            ELSE 'n/a'
                        END AS prd_line,
                        CAST(prd_start_dt AS DATE) AS prd_start_dt,
                        CAST(
                            LEAD(CAST(prd_start_dt AS DATE)) OVER (
                                PARTITION BY prd_key
                                ORDER BY CAST(prd_start_dt AS DATE)
                            ) - INTERVAL 1 DAY
                            AS DATE
                        ) AS prd_end_dt
                    FROM read_parquet('{bronze_path}')
                )
                SELECT *
                FROM transformed_products
            ) TO '{silver_path}' (FORMAT PARQUET)
        """)
    finally:
        con.close()


transform_prd_info(bronze_s3_path, silver_s3_path)
print(f"Arquivo Silver salvo em: {silver_s3_path}")
