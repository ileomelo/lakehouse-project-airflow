"""Transformação Silver do cadastro de clientes CRM."""

from __future__ import annotations

from lakehouse.transformations.silver.common import (
    default_paths as build_default_paths,
)
from lakehouse.transformations.silver.common import (
    duckdb_connection,
    parquet_row_count,
)


def transform_customer_info(bronze_path: str, silver_path: str) -> int:
    """Limpa clientes, mantém o registro mais recente por ID e grava Parquet."""
    with duckdb_connection(bronze_path, silver_path) as con:
        con.execute(
            f"""
            COPY (
                WITH typed AS (
                    SELECT
                        cast(cst_id AS BIGINT) AS cst_id,
                        cst_key,
                        cst_firstname,
                        cst_lastname,
                        cst_marital_status,
                        cst_gndr,
                        try_cast(cst_create_date AS DATE) AS cst_create_date
                    FROM read_parquet('{bronze_path}')
                    WHERE cst_id IS NOT NULL
                ),
                latest AS (
                    SELECT *
                    FROM typed
                    QUALIFY row_number() OVER (
                        PARTITION BY cst_id
                        ORDER BY cst_create_date DESC NULLS LAST
                    ) = 1
                ),
                cleaned_names AS (
                    SELECT
                        *,
                        nullif(
                            array_to_string(
                                list_transform(
                                    string_split(
                                        regexp_replace(trim(cst_firstname), '\\s+', ' ', 'g'),
                                        ' '
                                    ),
                                    word -> upper(left(word, 1)) || lower(substr(word, 2))
                                ),
                                ' '
                            ),
                            ''
                        ) AS clean_firstname,
                        nullif(
                            array_to_string(
                                list_transform(
                                    string_split(
                                        regexp_replace(trim(cst_lastname), '\\s+', ' ', 'g'),
                                        ' '
                                    ),
                                    word -> upper(left(word, 1)) || lower(substr(word, 2))
                                ),
                                ' '
                            ),
                            ''
                        ) AS clean_lastname
                    FROM latest
                )
                SELECT
                    cst_id,
                    cst_key,
                    coalesce(clean_firstname, 'n/a') AS cst_firstname,
                    coalesce(clean_lastname, 'n/a') AS cst_lastname,
                    CASE cst_marital_status
                        WHEN 'S' THEN 'Single'
                        WHEN 'M' THEN 'Married'
                        ELSE 'Unknown'
                    END AS cst_marital_status,
                    CASE cst_gndr
                        WHEN 'M' THEN 'Male'
                        WHEN 'F' THEN 'Female'
                        ELSE 'Unknown'
                    END AS cst_gndr,
                    cst_create_date,
                    clean_firstname IS NULL OR clean_lastname IS NULL
                        AS dq_missing_name_flag
                FROM cleaned_names
            ) TO '{silver_path}' (FORMAT PARQUET, OVERWRITE_OR_IGNORE TRUE)
            """
        )
        return parquet_row_count(con, silver_path)


def default_paths(partition_date: str | None = None) -> tuple[str, str]:
    """Monta os caminhos da partição local ou do MinIO."""
    return build_default_paths("crm", "cust_info", partition_date)


def main() -> None:
    bronze_path, silver_path = default_paths()
    count = transform_customer_info(bronze_path, silver_path)
    print(f"Arquivo Silver salvo em: {silver_path}")
    print(f"Registros persistidos: {count}")


if __name__ == "__main__":
    main()
