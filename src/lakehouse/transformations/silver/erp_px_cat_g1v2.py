"""Carga Silver pass-through do catálogo de produtos ERP G1V2."""

from __future__ import annotations

from lakehouse.transformations.silver.common import (
    default_paths as build_default_paths,
)
from lakehouse.transformations.silver.common import (
    duckdb_connection,
    parquet_row_count,
)


def load_px_cat_g1v2(bronze_path: str, silver_path: str) -> int:
    """Copia o catálogo do Bronze para o Silver sem alterar seus dados."""
    with duckdb_connection(bronze_path, silver_path) as con:
        con.execute(
            f"""
            COPY (
                SELECT *
                FROM read_parquet('{bronze_path}')
            ) TO '{silver_path}' (FORMAT PARQUET, OVERWRITE_OR_IGNORE TRUE)
            """
        )
        return parquet_row_count(con, silver_path)


def transform_px_cat_g1v2(bronze_path: str, silver_path: str) -> int:
    """Mantém uma API de transformação consistente com as demais tabelas Silver."""
    return load_px_cat_g1v2(bronze_path, silver_path)


def default_paths(partition_date: str | None = None) -> tuple[str, str]:
    """Monta os caminhos da partição local ou do MinIO."""
    return build_default_paths("erp", "px_cat_g1v2", partition_date)


def main() -> None:
    bronze_path, silver_path = default_paths()
    count = load_px_cat_g1v2(bronze_path, silver_path)
    print(f"Arquivo Silver salvo em: {silver_path}")
    print(f"Registros persistidos: {count}")


if __name__ == "__main__":
    main()
