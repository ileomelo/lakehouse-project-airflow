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
### Pipeline  - SILVER - 

```powershell
docker compose exec airflow-scheduler python -m lakehouse.pipelines.silver
```

O comando da Silver executa todas as transformações em sequência, usando a partição
do dia atual e exibindo no terminal o progresso, a duração e a quantidade de registros
de cada dataset. Para reprocessar outra partição, use `--partition-date YYYY-MM-DD`.

Sem `MINIO_BUCKET`, a transformação usa os arquivos locais em `data/`. Com as variáveis
`MINIO_*` configuradas, utiliza os caminhos S3 do MinIO.

## Airflow

```powershell
docker compose up --build
```


### Para login no Airflow
Procure por -> "Simple auth manager | Password for user 'admin':"
```powershell
docker compose logs airflow-apiserver
```

As DAGs importam o pacote pelo diretório `src`; a pasta `dags` não contém regra de negócio.
