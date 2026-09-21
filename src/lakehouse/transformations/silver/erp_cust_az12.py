"""Transformação Silver do cadastro de clientes ERP AZ12."""

from __future__ import annotations

from lakehouse.transformations.silver.common import (
    default_paths as build_default_paths,
)
from lakehouse.transformations.silver.common import (
    duckdb_connection,
    parquet_row_count,
)


def transform_cust_az12(bronze_path: str, silver_path: str) -> int:
    """Normaliza identificador, data de nascimento e gênero dos clientes."""
    with duckdb_connection(bronze_path, silver_path) as con:
        con.execute(
            f"""
            COPY (
                SELECT
                    CASE
                        WHEN cid LIKE 'NAS%' THEN substring(cid, 4)
                        ELSE cid
                    END AS cid,
                    CASE
                        WHEN try_cast(bdate AS DATE) > current_date()
                            THEN NULL
                        ELSE try_cast(bdate AS DATE)
                    END AS bdate,
                    CASE
                        WHEN upper(trim(gen)) IN ('F', 'FEMALE') THEN 'Female'
                        WHEN upper(trim(gen)) IN ('M', 'MALE') THEN 'Male'
                        ELSE 'n/a'
                    END AS gen
                FROM read_parquet('{bronze_path}')
            ) TO '{silver_path}' (FORMAT PARQUET, OVERWRITE_OR_IGNORE TRUE)
            """
        )
        return parquet_row_count(con, silver_path)


def default_paths(partition_date: str | None = None) -> tuple[str, str]:
    """Monta os caminhos da partição local ou do MinIO."""
    return build_default_paths("erp", "cust_az12", partition_date)


def main() -> None:
    bronze_path, silver_path = default_paths()
    count = transform_cust_az12(bronze_path, silver_path)
    print(f"Arquivo Silver salvo em: {silver_path}")
    print(f"Registros persistidos: {count}")


if __name__ == "__main__":
    main()
