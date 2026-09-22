"""Executa as transformações da camada Gold."""

from __future__ import annotations

import argparse
import logging

from lakehouse.config.partition import current_partition_date
from lakehouse.transformations.gold.gold import (
    transform_dim_customers,
    transform_dim_products,
    transform_fact_sales,
)

logger = logging.getLogger("lakehouse.gold")


def run_gold(partition_date: str) -> int:
    """Executa dimensões e fato na ordem correta e retorna o total de registros."""
    logger.info("Iniciando carga Gold | partição=%s", partition_date)
    customers = transform_dim_customers(partition_date)
    products = transform_dim_products(partition_date)
    sales = transform_fact_sales(partition_date)
    total = customers + products + sales
    logger.info(
        "Carga Gold concluída | clientes=%d | produtos=%d | vendas=%d | total=%d",
        customers, products, sales, total,
    )
    return total


def main() -> None:
    parser = argparse.ArgumentParser(description="Executa todas as transformações Gold.")
    parser.add_argument("--partition-date", help="Data da partição no formato YYYY-MM-DD.")
    args = parser.parse_args()
    logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(levelname)s | %(message)s")
    run_gold(args.partition_date or current_partition_date())


if __name__ == "__main__":
    main()
