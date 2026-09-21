"""Executa todas as transformações da camada Silver."""

from __future__ import annotations

import argparse
import logging
import time
from collections.abc import Callable

from lakehouse.config.partition import current_partition_date
from lakehouse.transformations.silver.common import default_paths
from lakehouse.transformations.silver.crm_cust_info import transform_customer_info
from lakehouse.transformations.silver.crm_prd_info import transform_product_info
from lakehouse.transformations.silver.crm_sales_details import transform_sales_details
from lakehouse.transformations.silver.erp_cust_az12 import transform_cust_az12
from lakehouse.transformations.silver.erp_loc_a101 import transform_loc_a101
from lakehouse.transformations.silver.erp_px_cat_g1v2 import load_px_cat_g1v2

logger = logging.getLogger("lakehouse.silver")
Transform = Callable[[str, str], int]


TRANSFORMATIONS: tuple[tuple[str, str, str, str, Transform], ...] = (
    (
        "crm.cust_info",
        "crm",
        "cust_info",
        "transform_customer_info",
        transform_customer_info,
    ),
    (
        "crm.prd_info",
        "crm",
        "prd_info",
        "transform_product_info",
        transform_product_info,
    ),
    (
        "crm.sales_details",
        "crm",
        "sales_details",
        "transform_sales_details",
        transform_sales_details,
    ),
    ("erp.cust_az12", "erp", "cust_az12", "transform_cust_az12", transform_cust_az12),
    ("erp.loc_a101", "erp", "loc_a101", "transform_loc_a101", transform_loc_a101),
    ("erp.px_cat_g1v2", "erp", "px_cat_g1v2", "load_px_cat_g1v2", load_px_cat_g1v2),
)


def configure_logging() -> None:
    """Configura logs legíveis para execução no terminal."""
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s | %(levelname)s | %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )


def run_silver(partition_date: str) -> int:
    """Executa todas as transformações Silver e retorna o total de registros."""
    total_records = 0
    logger.info("Iniciando carga Silver | partição=%s", partition_date)

    for index, (dataset, source, name, function_name, transform) in enumerate(
        TRANSFORMATIONS, 1
    ):
        bronze_path, silver_path = default_paths(source, name, partition_date)
        logger.info(
            "[%d/%d] Iniciando %s (%s)",
            index,
            len(TRANSFORMATIONS),
            dataset,
            function_name,
        )
        started_at = time.perf_counter()
        try:
            records = transform(bronze_path, silver_path)
        except Exception:
            logger.exception(
                "[%d/%d] Falha em %s", index, len(TRANSFORMATIONS), dataset
            )
            raise
        elapsed = time.perf_counter() - started_at
        total_records += records
        logger.info(
            "[%d/%d] Concluído %s | registros=%d | duração=%.2fs",
            index,
            len(TRANSFORMATIONS),
            dataset,
            records,
            elapsed,
        )

    logger.info(
        "Carga Silver concluída | datasets=%d | registros=%d",
        len(TRANSFORMATIONS),
        total_records,
    )
    return total_records


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Executa todas as transformações Silver."
    )
    parser.add_argument(
        "--partition-date",
        help="Data da partição no formato YYYY-MM-DD; por padrão, usa a data atual.",
    )
    args = parser.parse_args()
    configure_logging()
    run_silver(args.partition_date or current_partition_date())


if __name__ == "__main__":
    main()
