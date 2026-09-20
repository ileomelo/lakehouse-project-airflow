"""Transformação Silver do cadastro de produtos CRM."""

from __future__ import annotations

import os
from pathlib import Path

import duckdb

from lakehouse.infrastructure.object_storage import configure_duckdb_s3


def transform_product_info(bronze_path: str, silver_path: str) -> int:
    """Normaliza chaves, categorias, custos e vigências dos produtos."""
    if not silver_path.startswith("s3://"):
        Path(silver_path).parent.mkdir(parents=True, exist_ok=True)
    con = duckdb.connect()
    try:
        if bronze_path.startswith("s3://") or silver_path.startswith("s3://"):
            configure_duckdb_s3(con)
        con.execute(
            f"""
            COPY (
                WITH transformed AS (
                    SELECT
                        prd_id,
                        replace(substring(prd_key, 1, 5), '-', '_') AS cat_id,
                        substring(prd_key, 7) AS prd_key,
                        prd_nm,
                        coalesce(prd_cost, 0) AS prd_cost,
                        CASE upper(trim(prd_line))
                            WHEN 'M' THEN 'Mountain'
                            WHEN 'R' THEN 'Road'
                            WHEN 'S' THEN 'Other Sales'
                            WHEN 'T' THEN 'Touring'
                            ELSE 'n/a'
                        END AS prd_line,
                        cast(prd_start_dt AS DATE) AS prd_start_dt
                    FROM read_parquet('{bronze_path}')
                )
                SELECT *, cast(lead(prd_start_dt) OVER (
                    PARTITION BY prd_key ORDER BY prd_start_dt
                ) - INTERVAL 1 DAY AS DATE) AS prd_end_dt
                FROM transformed
            ) TO '{silver_path}' (FORMAT PARQUET, OVERWRITE_OR_IGNORE TRUE)
            """
        )
        return con.execute(
            f"SELECT count(*) FROM read_parquet('{silver_path}')"
        ).fetchone()[0]  # ty: ignore[not-subscriptable]
    finally:
        con.close()


def default_paths(partition_date: str | None = None) -> tuple[str, str]:
    """Monta os caminhos da partição local ou do MinIO."""
    date = partition_date or os.environ.get("PARTITION_DATE", "2026-09-20")
    bucket = os.environ.get("MINIO_BUCKET")
    base = f"s3://{bucket}" if bucket else "data"
    return (
        f"{base}/bronze/crm/prd_info/{date}/prd_info.parquet",
        f"{base}/silver/crm/prd_info/{date}/prd_info.parquet",
    )


def main() -> None:
    bronze_path, silver_path = default_paths()
    count = transform_product_info(bronze_path, silver_path)
    print(f"Arquivo Silver salvo em: {silver_path}")
    print(f"Registros persistidos: {count}")


if __name__ == "__main__":
    main()
