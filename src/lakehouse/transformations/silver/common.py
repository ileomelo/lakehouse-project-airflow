"""Utilitários compartilhados pelas cargas Silver."""

from __future__ import annotations

import os
from collections.abc import Generator
from contextlib import contextmanager
from pathlib import Path

import duckdb

from lakehouse.config.partition import current_partition_date
from lakehouse.infrastructure.object_storage import configure_duckdb_s3


@contextmanager
def duckdb_connection(
    bronze_path: str, silver_path: str
) -> Generator[duckdb.DuckDBPyConnection, None, None]:
    """Abre DuckDB, configura S3 quando necessário e fecha a conexão."""
    if not bronze_path.startswith("s3://") and not Path(bronze_path).exists():
        raise FileNotFoundError(f"Arquivo Bronze não encontrado: {bronze_path}")
    if not silver_path.startswith("s3://") and not Path(silver_path).exists():
        Path(silver_path).parent.mkdir(parents=True, exist_ok=True)

    con = duckdb.connect()
    try:
        if bronze_path.startswith("s3://") or silver_path.startswith("s3://"):
            configure_duckdb_s3(con)
        yield con
    finally:
        con.close()


def parquet_row_count(con: duckdb.DuckDBPyConnection, path: str) -> int:
    """Retorna a quantidade de registros de um Parquet."""
    return con.execute(f"SELECT count(*) FROM read_parquet('{path}')").fetchone()[0]  # ty: ignore[not-subscriptable]


def default_paths(
    source: str, dataset: str, partition_date: str | None = None
) -> tuple[str, str]:
    """Monta os caminhos Bronze/Silver locais ou do MinIO."""
    date = (
        partition_date or os.environ.get("PARTITION_DATE") or current_partition_date()
    )
    bucket = os.environ.get("MINIO_BUCKET")
    base = f"s3://{bucket}" if bucket else "data"
    relative_path = f"{source}/{dataset}/{date}/{dataset}.parquet"
    return (
        f"{base}/bronze/{relative_path}",
        f"{base}/silver/{relative_path}",
    )
