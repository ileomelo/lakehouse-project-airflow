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

No Airflow, a DAG `silver_transformations` cria uma task independente para cada
dataset Silver, usando a mesma partição atual na execução.

Sem `MINIO_BUCKET`, a transformação usa os arquivos locais em `data/`. Com as variáveis
`MINIO_*` configuradas, utiliza os caminhos S3 do MinIO.

### Pipeline - GOLD

```powershell
$env:PYTHONPATH="src"
.venv\Scripts\python.exe -m lakehouse.pipelines.gold --partition-date YYYY-MM-DD
```

A Gold materializa `dim_customers`, `dim_products` e `fact_sales` em
`data/gold/<partição>/` (ou no bucket MinIO configurado). As dimensões são construídas
antes do fato para que as chaves substitutas sejam resolvidas nos relacionamentos.
No Airflow, a DAG `gold_transformations` mantém essa mesma dependência.

As DAGs são agendadas diariamente e sincronizadas pela data lógica da execução:
`crm_bronze` e `erp_bronze` executam primeiro; `silver_transformations` aguarda as
duas concluírem com sucesso; e `gold_transformations` aguarda a Silver. Os sensores
ficam em modo `reschedule`, sem ocupar um worker enquanto aguardam.

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
