from airflow.sdk import (
    DAG,  # noqa: F401 — garante detecção pelo dag_discovery_safe_mode
)

from scripts.factories.bronze_factory import build_bronze_dag

SOURCES_CONFIG = {
    "crm": [
        {"name": "prd_info", "source_file": "prd_info.csv"},
        {"name": "cust_info", "source_file": "cust_info.csv"},
        {"name": "sales_details", "source_file": "sales_details.csv"},
    ],
    "erp": [
        {"name": "px_cat_g1v2", "source_file": "PX_CAT_G1V2.csv"},
        {"name": "cust_az12", "source_file": "CUST_AZ12.csv"},
        {"name": "loc_a101", "source_file": "LOC_A101.csv"},
    ],
}


for source_name, datasets in SOURCES_CONFIG.items():
    dag = build_bronze_dag(source=source_name, datasets=datasets)
    globals()[dag.dag_id] = dag
