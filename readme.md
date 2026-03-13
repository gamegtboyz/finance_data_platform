# Financial Data Platform Project
A production-style ETL pipeline for financial stock data, demonstrating data engineering practice including scheme design, error handling, testing, and containerization.

## Architecture
Public API (AlphaVantage)  
↓  
Ingestion (fetch_stock_prices(), fetch_company_metadata())  
↓  
Transformation (transform of tabular format)  
↓  
PostgreSQL Data Warehouse  
├── dim_date (time dimension)  
├── dim_metadata (stock/company dimension)  
└── stock_prices (fact table: OHLCV)

## Prerequisites
- Python 3.9 or later
- PostgreSQL 15 (via Docker or local)
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
cp .env.example .env    # edit ALPHAVANTAGE_API_KEY to yours
```

5. Start PostgreSQL container:
```bash
docker-compose up -d
```

6. Run the pipeline
```bash
python -m src.pipeline
```

## Running Tests
This project also includes unit tests for data transformations.
```bash
pytest tests/ -v
```

## Project Structure
```
src/
├── pipeline.py                 # Main ETL orchestration
├── reprocess_pipeline.py       # Transform and Load without using API requests
├── ingestion/
│   └── alphavantage_ingest.py  # API ingestion logic
├── processing/
│   └── transform_stock.py      # Data transformation
├── loaders/
│   ├── dimension_loader.py     # Load dimension tables
│   └── fact_loader.py          # Load fact table
└── modeling/
    ├── create_dimension_tables.py
    ├── create_fact_tables.py
    └── create_indexes.py

data/
├── raw/
│   └── {symbol}/               # Raw API JSON responses
└── processed/                  # (for future use)

tests/
└── test_transform.py           # Unit tests for transformations
```

## Key Features
- Batch ETL Pipeline: Fetch, transform, and load financial data in daily basis
- Star Schema: Dimensional modeling for analytical queries
- Error Handlind: API rate limit handling, database error rollback
- Docker Support: PostgreSQL containerized for reproducibility
- Testing: Unit tests for critical data transformation logic
- Transaction Safety: Atomic operations with rollback on failure