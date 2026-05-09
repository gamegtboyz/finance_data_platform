[![CI](https://github.com/gamegtboyz/finance_data_platform/actions/workflows/ci.yml/badge.svg)](https://github.com/gamegtboyz/finance_data_platform/actions/workflows/ci.yml)
[![dbt compile](https://github.com/gamegtboyz/finance_data_platform/actions/workflows/ci.yml/badge.svg?label=dbt-compile)](https://github.com/gamegtboyz/finance_data_platform/actions/workflows/ci.yml)

# Financial Data Platform Project
A production-style batch ETL pipeline for financial stock data, built with Python, PostgreSQL, and AWS (S3 + Redshift). Demonstrates professional data engineering practices: star schema design, cloud-native bulk loading via Redshift COPY, incremental loading, idempotent writes, structured logging, error handling, and a full integration test suite.

## Architecture
Public API (AlphaVantage)  
↓  
Ingestion (fetch_stock_prices(), fetch_company_metadata())  
*rate-limit handling • daily data detection • dual-write (local and S3)*  
↓  
S3 Raw Layer s3://finance-data-platform-raw/{symbols}/{symbols}_YYYY-MM-DD.json  
↓  
Transformation (transform of tabular format)  
*Type casting • calendar attribute derivation • metadata validation*  
↓  
Redshift COPY (via IAM role) ← S3 staging prefix  
↓  
Redshift Serverless Data Warehouse  
├── dim_date (DISTSTYLE ALL, SORTKEY date)  
├── dim_metadata (DISTSTYLE ALL)  
└── stock_prices (DISTSTYLE symbol, SORTKEY date)  
↓  
SQL analytics query  
*query the data from SQL database, then save the results as .csv files*

## Tech Stack
| Layer | Tool |
|---|---|
| Language | Python 3.9+ |
| Local Database | PostgreSQL 15 |
| Cloud Database | Amazon Redshift Serverless |
| Cloud Storage | AWS S3 | 
| AWS SDK | boto 3 |
| DB driver (PostgreSQL) | psycopg2-binary |
| DB driver (Redshift) | redshift-connector |
| DB Toolkit | SQLAlchemy |
| Data processing | pandas, numpy |
| HTTP | requests |
| Config | python-dotenv |
| Containerisation | Docker / Docker Compose |
| Testing | pytest, moto |
---

## Prerequisites
- Python 3.9 or later
- Docker Desktop (for local PostgreSQL mode)
- AlphaVantageAPI (free API key here: https://www.alphavantage.co/support/#api-key)
- AWS account with S3 and Redshift Serverless provisioined (for cloud mode -- see AWS setup below)

## AWS Setup
Follow these steps once before running in Redshift mode.  
**1. S3**  
- Create bucket `finance-data-platform-raw`; enable data versioning
- Prefix conventions: `./{symbol}/{symbol}_YYYY-MM-DD.json` and `./{symbol}/{symbol}_metadata.json`

**2. IAM -- Pipeline user (local → S3)**
- Create policy `FinancePipelineS3Policy` with `s3:GetObject`, `s3:PutObject`, `s3:Listbicket` scoped to our target bucket only.
- Create IAM user `finance-pipeline-local`; attach only the created policy
- Generate access keys; store in `.env` -- never commit via `./.gitignore`

**3. IAM -- Redshift Role (Redshift → S3)**  
- Create role `RedshiftS3ReadRole` with Redshift service trust policy + read-only access to the raw bucket
- Attach the role to your Redshift Serverless namespace

**4. Redshift Serverless**
- Provision a Serverless workgroup (8RPUs recommended); set auto-pause to 15 minutes idling time.
- Open the workgroup's security group inbound rule: port 5439 from your local IP address
- Confirm the pulbic endpoint is enabled

**5. Cost controls**
- Create an AWS Budgets alerts (estimated by $5 per month solely from this project)



## Local Setup
**1. Clone the repository**
```bash
gh repo clone gamegtboyz/finance_data_platform
cd finance_data_platform
```
**2. Create virtual environment**
```bash
# create virtual environment
python -m venv venv

# activate environment
## on macOS / Linux:
source venv/bin/activate

## on Windows Command Prompt
venv\Scripts\activate.bat

## on Windows PowerShell
Set-ExecutionPolicy -Scope CurrentUser -ExecutionPolicy Unrestricted -Force
.\venv\Scripts\Activate.ps1
```

**3. Install Dependencies**
```bash
pip install -r reuqirements.txt
```

**4. Configure Environment**
```bash
# This is just example, use your own settings accordingly
cp .env.example .env
```

5. Start PostgreSQL container:
```bash
docker-compose up -d
```

6. Run the pipeline
```bash
# in case of running entire ETL pipeline
python -m src/pipeline.py

# in case of running without fetching data via API
python -m src/reprocess_pipeline.py
```

With this execution, the `pipeline.py` will:
1. Fetch OHLCV data and company metadata from AlphaVantage API for NVDA, AAPL, MSFT, GOOGL, and AMZN into .json files
2. Dual-write JSON files locally and upload to S3 under`./{symbol}` prefix
3. Transform into tabular format with calendar attributes
4. Create the schema tables -- skipped if already present
5. Load dimension tables, then incremental factual data into fact table
6. In Redshift mode: stage via S3 `COPY` → `DELETE+INSERT` → `VACUUM+ANALYZE`

## Running Tests
This project also includes unit tests for data transformations.
```bash
# All tests (unit + integration)
pytest tests/ -v

# Integration tests only — requires Docker PostgreSQL running
pytest tests/ -v -m integration

# S3 unit tests — no real AWS needed (moto mocks)
pytest tests/test_s3_client.py -v
```

## Stop all AWS spendings
In case you need to pause the spendings made to AWS you could use the following bash command
```bash
# Pause Redshift Serverless from the AWS console (auto-pause handles this automatically)
# To fully remove all resources:
aws s3 rm s3://finance-data-platform-raw --recursive
aws s3api delete-bucket --bucket finance-data-platform-raw

# Also Delete Redshift Serverless workgroup and namespace from the AWS console
# 1. Delete the workgroup (replace name if yours differs)
aws redshift-serverless delete-workgroup \
  --workgroup-name default-workgroup \
  --region us-east-1

# 2. Wait for deletion to complete, then delete the namespace
aws redshift-serverless delete-namespace \
  --namespace-name default-namespace \
  --region us-east-1
```

## Project Structure
```
finance_data_platform/
├── docker-compose.yml              # PostgreSQL container for local mode
├── requirements.txt                # All dependencies
├── .env.example                    # Environment variable template
│
├── sql/
│   ├── sma.sql                     # SMA5 and SMA20 window functions
│   ├── daily_returns.sql           # Daily return calculation
│   ├── volatility.sql              # 21-day rolling volatility (stddev of returns)
│   └── cumulative_return.sql       # Cumulative return from earliest date
│
├── data/
│   ├── raw/{symbol}/               # Raw JSON responses from AlphaVantage
│   └── analytics/                  # CSV output of SQL analytics queries
│
├── src/
│   ├── db_connect.py               # Connection factory — postgres or redshift via DB_ENGINE env flag
│   ├── pipeline.py                 # Main ETL orchestration (fetch → S3 → transform → load)
│   ├── reprocess_pipeline.py       # Reprocess from local/S3 files without API calls
│   ├── analytics.py                # Execute SQL analytics queries; export to CSV
│   │
│   ├── extract/
│   │   └── alphavantage_ingest.py  # API fetch with rate-limit retry, quota detection, dual-write to S3
│   │
│   ├── storage/
│   │   └── s3_client.py            # boto3 wrapper: upload_file, list_objects, download_file
│   │
│   ├── transform/
│   │   └── transform_stock.py      # OHLCV + calendar attribute derivation; metadata parsing
│   │
│   ├── load/
│   │   ├── dimension_loader.py     # dim_date and dim_metadata — Postgres ON CONFLICT / Redshift DELETE+INSERT
│   │   ├── fact_loader.py          # stock_prices — Postgres bulk insert / Redshift S3 COPY + DELETE+INSERT
│   │   └── redshift_copy_loader.py # COPY from S3 into staging tables; VACUUM+ANALYZE post-load
│   │
│   └── modeling/
│       ├── create_dimension_tables.py   # PostgreSQL dimension table DDL
│       ├── create_fact_tables.py        # PostgreSQL fact table DDL
│       ├── create_indexes.py            # PostgreSQL performance indexes
│       └── create_redshift_schema.py    # Redshift DDL — DISTKEY/SORTKEY/ENCODE + staging tables
│
└── tests/
    ├── conftest.py                 # db_cursor fixture — isolated TEMP tables, auto-rollback
    ├── test_transform.py           # Unit tests for transformation logic
    ├── test_loaders.py             # Integration tests for all loaders (dim + fact)
    └── test_s3_client.py           # Unit tests for S3 wrapper using moto (no real AWS)
```

## Key Features
- **Dual-engine pipeline** -- single codebase runs against local PostgreSQL or Redshift Serverless via `DB_ENGINE` env flag.
- **Cloud-native bulk loading** -- Redshift path uses S3 COPY command, not row-by-row `INSERT`
- **Star schema** -- dimensional modeling with Redshift-native `DISTKEY`/`SORTKEY` optimization
- **Idempotent writes** -- Postgres uses `ON CONFLICT DO NOTHING`; Redshift uses `DELETE + INSERT` pattern (no `ON CONFLICT` in Redshift)
- **Incremental loading** -- `get_max_loaded_date()` skips already-loaded dates; safe to re-run daily
- **S3 raw layer** -- all raw API responses persisted to S3 for reprocessing without API quota cost
- **Transaction safety** -- single `conn.commit()` per symbol; rollback on any failure
- **Post-load maintenance** -- `VACUUM SORT ONLY + ANALYZE` run after each Redshift load cycle
- **Mocked S3 tests** -- `test_s3_client.py` uses `moto` -- no real AWS credentials needed in CI



## Key Design Decisions
- **Redshift COPY over INSERT** — row-by-row `INSERT` into Redshift is orders of magnitude slower than `COPY from S3`. All bulk loads in Redshift mode go through the staging-table COPY pattern.

- **DELETE + INSERT for idempotency** — Redshift has no `ON CONFLICT`. The pattern is: `TRUNCATE` staging → `COPY` from S3 → `DELETE` matching keys from target → `INSERT` from staging. Safe to re-run.

- **DISTKEY(symbol) + SORTKEY(date)** — co-locates all rows for a symbol on the same Redshift node, eliminating data shuffling on per-symbol aggregations. SORTKEY enables zone-map pruning on date range queries.

- **DISTSTYLE ALL** on dimension tables — dim_date and dim_metadata are small; broadcasting a full copy to every node makes JOIN operations free (no network transfer).

- **Single transaction per symbol** — dimension and fact loads share one `conn.commit()`, so a partial failure never leaves orphaned dimension rows without corresponding facts.

**`open_price` / `close_price` column names** — `open` and `close` are ANSI SQL reserved words rejected by Redshift's strict parser. Renaming at the transform layer means no quoting is needed anywhere downstream.