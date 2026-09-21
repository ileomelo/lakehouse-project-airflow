from pathlib import Path

import duckdb

from lakehouse.transformations.silver.crm_sales_details import transform_sales_details


def test_sales_details_transformation(tmp_path: Path) -> None:
    source = Path("data/bronze/crm/sales_details/2026-09-18/sales_details.parquet")
    target = tmp_path / "sales_details.parquet"

    count = transform_sales_details(str(source), str(target))

    assert count == 60398
    invalid = duckdb.query(
        f"""
        SELECT count(*)
        FROM read_parquet('{target}')
        WHERE sls_sales != sls_quantity * abs(sls_price)
        """
    ).fetchone()[0]  # ty: ignore[not-subscriptable]
    assert invalid == 0
