"""Transformação Silver do cadastro de clientes CRM."""

from __future__ import annotations

import os
from pathlib import Path

import pandas as pd


def transform_customer_info(
    bronze_path: str, silver_path: str, storage_options: dict
) -> int:
    """Limpa clientes, mantém o registro mais recente por ID e grava Parquet."""
    df = pd.read_parquet(bronze_path, storage_options=storage_options)
    df["cst_create_date"] = pd.to_datetime(df["cst_create_date"], errors="coerce")
    df = df.dropna(subset=["cst_id"]).copy()
    df["cst_id"] = df["cst_id"].astype("int64")
    df = df.sort_values("cst_create_date", ascending=False).drop_duplicates(
        subset="cst_id", keep="first"
    )

    for column in ("cst_firstname", "cst_lastname"):
        df[column] = (
            df[column]
            .astype("string")
            .str.strip()
            .str.replace(r"\s+", " ", regex=True)
            .str.title()
            .replace("", pd.NA)
        )
    df["dq_missing_name_flag"] = df["cst_firstname"].isna() | df["cst_lastname"].isna()
    df[["cst_firstname", "cst_lastname"]] = df[
        ["cst_firstname", "cst_lastname"]
    ].fillna("n/a")
    df["cst_gndr"] = df["cst_gndr"].map({"M": "Male", "F": "Female"}).fillna("Unknown")
    df["cst_marital_status"] = (
        df["cst_marital_status"].map({"S": "Single", "M": "Married"}).fillna("Unknown")
    )
    df.to_parquet(silver_path, index=False, storage_options=storage_options)
    return len(df)


def default_paths(partition_date: str | None = None) -> tuple[str, str]:
    """Monta os caminhos da partição local ou do MinIO."""
    date = partition_date or os.environ.get("PARTITION_DATE", "2026-09-20")
    bucket = os.environ.get("MINIO_BUCKET")
    base = f"s3://{bucket}" if bucket else "data"
    return (
        f"{base}/bronze/crm/cust_info/{date}/cust_info.parquet",
        f"{base}/silver/crm/cust_info/{date}/cust_info.parquet",
    )


def main() -> None:
    bronze_path, silver_path = default_paths()
    if not bronze_path.startswith("s3://"):
        Path(silver_path).parent.mkdir(parents=True, exist_ok=True)

    storage_options = {}
    if bronze_path.startswith("s3://"):
        storage_options = {
            "key": os.environ["MINIO_ACCESS_KEY"],
            "secret": os.environ["MINIO_SECRET_KEY"],
            "client_kwargs": {"endpoint_url": os.environ["MINIO_ENDPOINT"]},
        }

    count = transform_customer_info(bronze_path, silver_path, storage_options)
    print(f"Arquivo Silver salvo em: {silver_path}")
    print(f"Registros persistidos: {count}")


if __name__ == "__main__":
    main()
