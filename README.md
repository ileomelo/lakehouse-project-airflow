# Lakehouse Project

Pipeline de dados em camadas Bronze/Silver usando Apache Airflow, DuckDB, Parquet e MinIO.

## Estrutura

```text
.
├── dags/                    # Apenas definição/orquestração das DAGs
├── src/lakehouse/
│   ├── config/              # Catálogos e configurações declarativas
│   ├── infrastructure/     # MinIO, S3 e integrações externas
│   ├── pipelines/           # Componentes executados pelo Airflow
│   └── transformations/     # Transformações testáveis por função
├── tests/unit/              # Testes unitários
├── data/source/             # Dados de entrada locais
├── docker-compose.yml
└── pyproject.toml
```

## Execução local

```powershell
& .\\.venv\\Scripts\\python.exe -m lakehouse.transformations.silver.crm_sales_details
pytest
```

Sem `MINIO_BUCKET`, a transformação usa os arquivos locais em `data/`. Com as variáveis
`MINIO_*` configuradas, utiliza os caminhos S3 do MinIO.

## Airflow

```powershell
docker compose up --build
```

As DAGs importam o pacote pelo diretório `src`; a pasta `dags` não contém regra de negócio.
