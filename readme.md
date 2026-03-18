# Financial Data Platform Project
A production-style batch ETL pipeline for financial stock data, built with Python and PostgreSQL. Demonstrates professional data engineering practices: star schema design, incremental loading, idempotent writes, structured logging, error handling, and a full integration test suite.

## Architecture
Public API (AlphaVantage)  
↓  
Ingestion (fetch_stock_prices(), fetch_company_metadata())  
*rate-limit handling • daily data detection • local JSON persistence*  
↓  
Transformation (transform of tabular format)  
*Type casting • calendar attribute derivation • metadata validation*  
↓  
PostgreSQL Data Warehouse  
├── dim_date (date PK, day, month, year, quarter, day_of_week, week_of_year)  
├── dim_metadata (symbol PK, company_name, and its sector)  
└── stock_prices (symbol + date PK, OHLCV tect tables)  
↓  
SQL analytics query  
*query the data from SQL database, then save the results as .csv files*

## Tech Stack
| Layer | Tool |
|---|---|
| Language | Python 3.9+ |
| Database | PostgreSQL 15 |
| DB driver | psycopg2-binary |
| DB Toolkit | SQLAlchemy |
| Data processing | pandas, numpy |
| HTTP | requests |
| Config | python-dotenv |
| Containerisation | Docker / Docker Compose |
| Testing | pytest |

---
## Prerequisites
- Python 3.9 or later
- Docker Desktop (for PostgreSQL container)
- AlphaVantageAPI (free API key here: https://www.alphavantage.co/support/#api-key)

## Setup
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
cp .env.example .env    # then set ALPHAVANTAGE_API_KEY to yours
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
2. Read the fetched .json data and transform it into desired format.
3. Create the schema tables and indexes.
4. Loaded the transformed data into SQL tables.

## Running Tests
This project also includes unit tests for data transformations.
```bash
# use this one to run everything
pytest tests/ -v

# use this one to only run on the integration tests
# make sure the docker PostgreSQL container is running. Otherwise run docker compose up -d
pytest tests/ -v -m integration
```

## Project Structure
```
finance_data_platform/
├── docker-compose.yml             # Docker container property
├── requirements.txt               # Dependency used in this project
├── .env.example                   # Dummy .env file for git cloners
│
├── sql/
│   └── sma.sql                    # Calculate SMA5 and SMA20
│   └── daily_returns.sql          # Calculate daily returns
│   └── volatility.sql             # Calculate volatility (standard deviation) of the returns from last 21 trading days
│   └── cumulative_return.sql      # Calculate cumulative returns of the stocks from the earliest OHLCV data of stocks
│
├── data/
│   ├── raw/{symbol}/              # Raw JSON responses from AlphaVantage
│   └── analytics/                 # Extracted files for SQL query
│
├── src/
│   ├── db_connect.py              # psycopg2 connection factory (reads from .env)
│   ├── pipeline.py                # Main ETL orchestration (fetch → transform → load)
│   ├── reprocess_pipeline.py      # Load from local files without API calls
│   ├── export_analytics.py        # Save the SQL query into .csv file
│   │
│   ├── extract/
│   │   └── alphavantage_ingest.py # API fetch with rate-limit retry and quota detection
│   │
│   ├── transform/
│   │   └── transform_stock.py     # OHLCV + calendar attribute derivation; metadata parsing
│   │
│   ├── load/
│   │   ├── dimension_loader.py    # Bulk-insert dim_date and dim_metadata (ON CONFLICT DO NOTHING)
│   │   └── fact_loader.py         # Bulk-insert stock_prices; get_max_loaded_date for incremental load
│   │
│   └── modeling/
│       ├── create_dimension_tables.py      # verify and create dimension (date, metadata) tables
│       ├── create_fact_tables.py           # verify and create fact tables
│       └── create_indexes.py               # create performance index on fact tables
│
└── tests/
    ├── conftest.py                # db_cursor fixture — isolated TEMP tables per test
    ├── test_transform.py          # Unit tests for transformation logic
    └── test_loaders.py            # Integration tests for all loaders (dim + fact)
```

## Key Features
- Batch ETL Pipeline: Fetch, transform, and load financial data in daily basis
- Star Schema: Dimensional modeling for analytical queries
- Error Handling: API rate limit handling, database error rollback
- Docker Support: PostgreSQL containerized for reproducibility
- Testing: Unit tests for critical data transformation logic
- Transaction Safety: Atomic operations with rollback on failure

## Key Design Decisions
**Single transactin per symbol** -- dimension and fact loads share one `conn.commit()` so a partial failure never leaves orphaned dimension rows without corresponding facts.

**Idempotent writes** -- all insert statements use `ON CONFLICT DO NOTHING`, making every load safe to re-run.

**Incremental loading** -- `get_max_loaded_date()` prevents re-inserting OHLCV data which are already exists in the database, keeping run efficient after the initial backfill.

**Bulk inserts** -- `execute_values` is implemented in the loading process, reduce processing time of loading row-by-row.

**Test isolation** -- using temporary (TEMP) tables to test the connection, then roll back every test to ensure the isolation of test data from the real production tables.