import os

import pandas as pd

# Como está dentro do Docker rodar: docker compose exec airflow-scheduler python /opt/airflow/scripts/transformers/crm_cust_info.py

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

print(f"Tipo de cst_create_date: {df['cst_create_date'].dtype}")
print(f"Nulos em cst_create_date: {df['cst_create_date'].isna().sum()}")

# ---------------------------------------------------------------------------
# 3. ID cleaning + deduplication (keep most recent)
# ---------------------------------------------------------------------------
df = df.dropna(subset=["cst_id"])
df["cst_id"] = df["cst_id"].astype("int64")

print(f"Nulos em cst_id: {df['cst_id'].isna().sum()}")
print(f"Duplicados em cst_id (antes): {df['cst_id'].duplicated().sum()}")

df = df.sort_values("cst_create_date", ascending=False).drop_duplicates(
    subset="cst_id", keep="first"
)

print(f"Duplicados em cst_id (depois): {df['cst_id'].duplicated().sum()}")

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

print(f"Valores únicos em cst_gndr: {df['cst_gndr'].unique()}")

marital_map = {"S": "Single", "M": "Married"}
df["cst_marital_status"] = (
    df["cst_marital_status"].map(marital_map).fillna("Unknown")
)
print(f"Valores únicos em cst_marital_status: {df['cst_marital_status'].unique()}")

"""
## 📌 Decisões de Modelagem de Dados

> **Nota sobre Tipos de Dados e Desempenho:**
> Para fins didáticos e facilitar a legibilidade durante a fase de estudos e testes, optei por utilizar representações em caracteres/strings para campos categóricos:
> - **Gênero:** `'M'` (Male), `'F'` (Female)
> - **Estado Civil:** `'S'` (Single), `'M'` (Married)
> 
> **Consideração para Produção / Escala:**
> Em um cenário de produção com grande volume de dados (High Throughput / Large Scale), a boa prática de otimização seria refatorar esses campos para tipos numéricos reduzidos (`TINYINT` / `SMALLINT` ou `ENUM`), associados a constantes no código backend ou tabelas de domínio. Isso reduziria a pegada de memória (I/O) e otimizaria a indexação e busca no banco de dados.
"""