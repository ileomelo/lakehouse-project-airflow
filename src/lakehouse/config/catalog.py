"""Catálogo declarativo das fontes ingeridas pelo lakehouse."""

from dataclasses import dataclass


@dataclass(frozen=True)
class DatasetConfig:
    name: str
    source_file: str


SOURCE_CATALOG: dict[str, tuple[DatasetConfig, ...]] = {
    "crm": (
        DatasetConfig("prd_info", "prd_info.csv"),
        DatasetConfig("cust_info", "cust_info.csv"),
        DatasetConfig("sales_details", "sales_details.csv"),
    ),
    "erp": (
        DatasetConfig("px_cat_g1v2", "PX_CAT_G1V2.csv"),
        DatasetConfig("cust_az12", "CUST_AZ12.csv"),
        DatasetConfig("loc_a101", "LOC_A101.csv"),
    ),
}

