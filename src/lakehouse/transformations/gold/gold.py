"""Materializa dimensões e fato Gold a partir dos Parquets Silver."""

from __future__ import annotations

import os
from collections.abc import Generator
from contextlib import contextmanager
from pathlib import Path

import duckdb

from lakehouse.config.partition import current_partition_date
from lakehouse.infrastructure.object_storage import configure_duckdb_s3


def gold_path(dataset: str, partition_date: str | None = None) -> str:
    """Retorna o caminho do dataset Gold para uma partição."""
    date = (
        partition_date or os.environ.get("PARTITION_DATE") or current_partition_date()
    )
    bucket = os.environ.get("MINIO_BUCKET")
    base = f"s3://{bucket}" if bucket else "data"
    return f"{base}/gold/{date}/{dataset}.parquet"


def silver_path(source: str, dataset: str, partition_date: str | None = None) -> str:
    """Retorna o caminho do dataset Silver usado pela transformação."""
    date = (
        partition_date or os.environ.get("PARTITION_DATE") or current_partition_date()
    )
    bucket = os.environ.get("MINIO_BUCKET")
    base = f"s3://{bucket}" if bucket else "data"
    return f"{base}/silver/{source}/{dataset}/{date}/{dataset}.parquet"


@contextmanager
def gold_connection() -> Generator[duckdb.DuckDBPyConnection, None, None]:
    """Abre uma conexão DuckDB configurada para os caminhos locais ou S3."""
    con = duckdb.connect()
    try:
        if os.environ.get("MINIO_BUCKET"):
            configure_duckdb_s3(con)
        yield con
    finally:
        con.close()


def _validate_input(path: str) -> None:
    if not path.startswith("s3://") and not Path(path).exists():
        raise FileNotFoundError(f"Arquivo Silver não encontrado: {path}")


def _copy_query(con: duckdb.DuckDBPyConnection, query: str, output_path: str) -> int:
    if not output_path.startswith("s3://"):
        Path(output_path).parent.mkdir(parents=True, exist_ok=True)
    con.execute(
        f"COPY ({query}) TO '{output_path}' (FORMAT PARQUET, OVERWRITE_OR_IGNORE TRUE)"
    )
    return con.execute(
        f"SELECT count(*) FROM read_parquet('{output_path}')"
    ).fetchone()[0]  # ty: ignore[not-subscriptable]


def transform_dim_customers(partition_date: str | None = None) -> int:
    """Cria a dimensão de clientes com a regra de fallback CRM/ERP para gênero."""
    ci = silver_path("crm", "cust_info", partition_date)
    ca = silver_path("erp", "cust_az12", partition_date)
    la = silver_path("erp", "loc_a101", partition_date)
    for path in (ci, ca, la):
        _validate_input(path)
    query = f"""
        SELECT
            row_number() OVER (ORDER BY ci.cst_id) AS customer_key,
            ci.cst_id AS customer_id,
            ci.cst_key AS customer_number,
            ci.cst_firstname AS first_name,
            ci.cst_lastname AS last_name,
            la.cntry AS country,
            ci.cst_marital_status AS marital_status,
            CASE WHEN ci.cst_gndr != 'n/a' THEN ci.cst_gndr
                 ELSE coalesce(ca.gen, 'n/a') END AS gender,
            ca.bdate AS birthdate,
            ci.cst_create_date AS create_date
        FROM read_parquet('{ci}') ci
        LEFT JOIN read_parquet('{ca}') ca ON ci.cst_key = ca.cid
        LEFT JOIN read_parquet('{la}') la ON ci.cst_key = la.cid
    """
    with gold_connection() as con:
        return _copy_query(con, query, gold_path("dim_customers", partition_date))


def transform_dim_products(partition_date: str | None = None) -> int:
    """Cria a dimensão de produtos mantendo apenas o registro vigente."""
    pn = silver_path("crm", "prd_info", partition_date)
    pc = silver_path("erp", "px_cat_g1v2", partition_date)
    for path in (pn, pc):
        _validate_input(path)
    query = f"""
        SELECT
            row_number() OVER (ORDER BY pn.prd_start_dt, pn.prd_key) AS product_key,
            pn.prd_id AS product_id,
            pn.prd_key AS product_number,
            pn.prd_nm AS product_name,
            pn.cat_id AS category_id,
            pc.cat AS category,
            pc.subcat AS subcategory,
            pc.maintenance AS maintenance,
            pn.prd_cost AS cost,
            pn.prd_line AS product_line,
            pn.prd_start_dt AS start_date
        FROM read_parquet('{pn}') pn
        LEFT JOIN read_parquet('{pc}') pc ON pn.cat_id = pc.id
        WHERE pn.prd_end_dt IS NULL
    """
    with gold_connection() as con:
        return _copy_query(con, query, gold_path("dim_products", partition_date))


def transform_fact_sales(partition_date: str | None = None) -> int:
    """Cria o fato de vendas relacionado às dimensões Gold."""
    sd = silver_path("crm", "sales_details", partition_date)
    products = gold_path("dim_products", partition_date)
    customers = gold_path("dim_customers", partition_date)
    for path in (sd, products, customers):
        _validate_input(path)
    query = f"""
        SELECT
            sd.sls_ord_num AS order_number,
            pr.product_key,
            cu.customer_key,
            sd.sls_order_dt AS order_date,
            sd.sls_ship_dt AS shipping_date,
            sd.sls_due_dt AS due_date,
            sd.sls_sales AS sales_amount,
            sd.sls_quantity AS quantity,
            sd.sls_price AS price
        FROM read_parquet('{sd}') sd
        LEFT JOIN read_parquet('{products}') pr
            ON sd.sls_prd_key = pr.product_number
        LEFT JOIN read_parquet('{customers}') cu
            ON sd.sls_cust_id = cu.customer_id
    """
    with gold_connection() as con:
        return _copy_query(con, query, gold_path("fact_sales", partition_date))
