"""Cliente MinIO e utilitários de configuração do DuckDB para S3."""

from __future__ import annotations

import os

import duckdb
from minio import Minio


def get_minio_client() -> Minio:
    endpoint = os.environ["MINIO_ENDPOINT"].removeprefix("http://").removeprefix(
        "https://"
    )
    return Minio(
        endpoint,
        access_key=os.environ["MINIO_ACCESS_KEY"],
        secret_key=os.environ["MINIO_SECRET_KEY"],
        secure=False,
    )


def configure_duckdb_s3(con: duckdb.DuckDBPyConnection) -> None:
    endpoint = os.environ["MINIO_ENDPOINT"].removeprefix("http://").removeprefix(
        "https://"
    )
    con.execute("INSTALL httpfs")
    con.execute("LOAD httpfs")
    con.execute(f"SET s3_endpoint = '{endpoint}'")
    con.execute(f"SET s3_access_key_id = '{os.environ['MINIO_ACCESS_KEY']}'")
    con.execute(f"SET s3_secret_access_key = '{os.environ['MINIO_SECRET_KEY']}'")
    con.execute("SET s3_region = 'us-east-1'")
    con.execute("SET s3_use_ssl = false")
    con.execute("SET s3_url_style = 'path'")

