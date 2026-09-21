"""Transforma CRM sales details de Bronze para Silver usando DuckDB."""

from __future__ import annotations

from lakehouse.transformations.silver.common import (
    default_paths as build_default_paths,
)
from lakehouse.transformations.silver.common import (
    duckdb_connection,
    parquet_row_count,
)


def transform_sales_details(bronze_path: str, silver_path: str) -> int:
    """Aplica regras de qualidade e grava um Parquet Silver."""
    if bronze_path.startswith("s3://"):
        print(f"Lendo Bronze: {bronze_path}")
    with duckdb_connection(bronze_path, silver_path) as con:
        con.execute(
            f"""
            COPY (
                SELECT
                    sls_ord_num,
                    sls_prd_key,
                    sls_cust_id,
                    CASE WHEN sls_order_dt IS NULL OR sls_order_dt = 0
                              OR length(CAST(sls_order_dt AS VARCHAR)) != 8
                         THEN NULL
                         ELSE try_strptime(CAST(sls_order_dt AS VARCHAR), '%Y%m%d')::DATE
                    END AS sls_order_dt,
                    CASE WHEN sls_ship_dt IS NULL OR sls_ship_dt = 0
                              OR length(CAST(sls_ship_dt AS VARCHAR)) != 8
                         THEN NULL
                         ELSE try_strptime(CAST(sls_ship_dt AS VARCHAR), '%Y%m%d')::DATE
                    END AS sls_ship_dt,
                    CASE WHEN sls_due_dt IS NULL OR sls_due_dt = 0
                              OR length(CAST(sls_due_dt AS VARCHAR)) != 8
                         THEN NULL
                         ELSE try_strptime(CAST(sls_due_dt AS VARCHAR), '%Y%m%d')::DATE
                    END AS sls_due_dt,
                    CASE WHEN sls_sales IS NULL OR sls_sales <= 0
                              OR sls_sales != sls_quantity * abs(sls_price)
                         THEN sls_quantity * abs(sls_price)
                         ELSE sls_sales
                    END AS sls_sales,
                    sls_quantity,
                    CASE WHEN sls_price IS NULL OR sls_price <= 0
                         THEN sls_sales / nullif(sls_quantity, 0)
                         ELSE sls_price
                    END AS sls_price
                FROM read_parquet('{bronze_path}')
            ) TO '{silver_path}' (FORMAT PARQUET, OVERWRITE_OR_IGNORE TRUE)
            """
        )
        return parquet_row_count(con, silver_path)


def default_paths(partition_date: str | None = None) -> tuple[str, str]:
    """Retorna caminhos locais ou S3 conforme a configuração disponível."""
    return build_default_paths("crm", "sales_details", partition_date)


def main() -> None:
    bronze_path, silver_path = default_paths()
    count = transform_sales_details(bronze_path, silver_path)
    print(f"Arquivo Silver salvo em: {silver_path}")
    print(f"Registros persistidos: {count}")


if __name__ == "__main__":
    main()
