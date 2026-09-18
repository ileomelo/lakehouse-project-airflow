import os

import pandas as pd

# docker compose exec airflow-scheduler python /opt/airflow/scripts/transformers/bronze/crm/crm_cust_info.py

# 1. Configura as opções do S3 para o Pandas / s3fs reconhecerem o MinIO
storage_options = {
    "key": os.environ["MINIO_ACCESS_KEY"],
    "secret": os.environ["MINIO_SECRET_KEY"],
    "client_kwargs": {
        "endpoint_url": os.environ["MINIO_ENDPOINT"]  # Ex: "http://localhost:9000"
    },
}

# 2. Define o caminho do objeto no formato s3://
bucket = os.environ["MINIO_BUCKET"]
partition_date = (
    "2026-09-18"  # Subsitua pela data da partição desejada referente ao arquivo.
)
s3_path = f"s3://{bucket}/bronze/crm/cust_info/{partition_date}/cust_info.parquet"

# ---------------------------------------------------------------------------
# 1. Load
# ---------------------------------------------------------------------------
df = pd.read_parquet(s3_path, storage_options=storage_options)

# ---------------------------------------------------------------------------
# 2. Date handling
# ---------------------------------------------------------------------------
df["cst_create_date"] = pd.to_datetime(df["cst_create_date"], errors="coerce")


# ---------------------------------------------------------------------------
# 3. ID cleaning + deduplication (keep most recent)
# ---------------------------------------------------------------------------
df = df.dropna(subset=["cst_id"])
df["cst_id"] = df["cst_id"].astype("int64")


df = df.sort_values("cst_create_date", ascending=False).drop_duplicates(
    subset="cst_id", keep="first"
)


# ---------------------------------------------------------------------------
# 4. Name cleaning
# ---------------------------------------------------------------------------
for col in ["cst_firstname", "cst_lastname"]:
    df[col] = (
        df[col]
        .str.strip()
        .str.replace(r"\s+", " ", regex=True)  # colapsa espaços internos
        .str.title()
        .replace("", pd.NA)
    )

# Profiling opcional de caracteres estranhos (não altera o DataFrame)
# df[df["cst_firstname"].str.contains(r"[^a-zA-ZÀ-ÿ\s'-]", regex=True, na=False)]

# Flag de nome incompleto (antes do fillna)
df["dq_missing_name_flag"] = df["cst_firstname"].isna() | df["cst_lastname"].isna()

df[["cst_firstname", "cst_lastname"]] = df[["cst_firstname", "cst_lastname"]].fillna(
    "n/a"
)

# ---------------------------------------------------------------------------
# 5. Categorical standardization
# ---------------------------------------------------------------------------
gender_map = {"M": "Male", "F": "Female"}
df["cst_gndr"] = df["cst_gndr"].map(gender_map).fillna("Unknown")


marital_map = {"S": "Single", "M": "Married"}
df["cst_marital_status"] = df["cst_marital_status"].map(marital_map).fillna("Unknown")

# ---------------------------------------------------------------------------
# 6. Persist Silver
# ---------------------------------------------------------------------------
silver_s3_path = (
    f"s3://{bucket}/silver/crm/cust_info/{partition_date}/cust_info.parquet"
)

# O DataFrame final é serializado explicitamente como Parquet antes de ser
# enviado ao MinIO. O s3fs usa as mesmas credenciais da leitura Bronze.
df.to_parquet(silver_s3_path, index=False, storage_options=storage_options)

print(f"Arquivo Silver salvo em: {silver_s3_path}")
print(f"Registros persistidos: {len(df)}")
