"""Transformação Silver da localização de clientes ERP A101."""

from __future__ import annotations

from lakehouse.transformations.silver.common import (
    default_paths as build_default_paths,
)
from lakehouse.transformations.silver.common import (
    duckdb_connection,
    parquet_row_count,
)


def transform_loc_a101(bronze_path: str, silver_path: str) -> int:
    """Normaliza identificadores e códigos de país dos clientes."""
    with duckdb_connection(bronze_path, silver_path) as con:
        con.execute(
            f"""
            COPY (
                SELECT
                    replace(cid, '-', '') AS cid,
                    CASE
                        WHEN trim(cntry) = 'DE' THEN 'Germany'
                        WHEN trim(cntry) IN ('US', 'USA') THEN 'United States'
                        WHEN trim(cntry) = '' OR cntry IS NULL THEN 'n/a'
                        ELSE trim(cntry)
                    END AS cntry
                FROM read_parquet('{bronze_path}')
            ) TO '{silver_path}' (FORMAT PARQUET, OVERWRITE_OR_IGNORE TRUE)
            """
        )
        return parquet_row_count(con, silver_path)


def default_paths(partition_date: str | None = None) -> tuple[str, str]:
    """Monta os caminhos da partição local ou do MinIO."""
    return build_default_paths("erp", "loc_a101", partition_date)


def main() -> None:
    bronze_path, silver_path = default_paths()
    count = transform_loc_a101(bronze_path, silver_path)
    print(f"Arquivo Silver salvo em: {silver_path}")
    print(f"Registros persistidos: {count}")


if __name__ == "__main__":
    main()
