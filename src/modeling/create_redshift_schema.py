import logging
from db_connect import db_connect

logger = logging.getLogger(__name__)

"""
Key differences of redshift from the PostgreSQL DDL:

No FOREIGN KEY — Redshift supports the syntax but does not enforce FK constraints; they're ignored at runtime. Drop them entirely since they add no value.
No PRIMARY KEY enforcement either — Redshift treats PKs as hints for the query planner, not hard constraints. Keep them as UNIQUE hints via DISTKEY.
DISTSTYLE ALL on dim tables — full copy on every node, makes JOINs to facts free.
DISTKEY(symbol) + SORTKEY(date) on stock_prices — co-locates data per symbol and enables zone-map pruning on date range queries.
No CREATE INDEX — irrelevant in Redshift.

We also ENCODE az64 and lzo as the Redshift's columnar expression encodings. az64 for numeric and dates; lzo for variable-length strings. This optimizes storage and query performance in Redshift's columnar architecture.
"""

# create the query to create the schema
CREATE_FACT_STOCK_PRICES = """
CREATE TABLE IF NOT EXISTS stock_prices (
    symbol      VARCHAR(20)     NOT NULL ENCODE lzo,
    date        DATE            NOT NULL ENCODE az64,
    open_price  NUMERIC(12,4)   ENCODE az64,
    high        NUMERIC(12,4)   ENCODE az64,
    low         NUMERIC(12,4)   ENCODE az64,
    close_price NUMERIC(12,4)   ENCODE az64,
    volume      BIGINT          ENCODE az64,
    PRIMARY KEY (symbol, date)
)
DISTKEY (symbol)
SORTKEY (date);
"""

CREATE_DIM_DATE = """
CREATE TABLE IF NOT EXISTS dim_date (
    date            DATE NOT NULL ENCODE az64,
    day             INT,
    month           INT,
    year            INT,
    quarter         INT,
    day_of_week     INT,
    week_of_year    INT,
    PRIMARY KEY (date)
)
DISTSTYLE ALL
SORTKEY (date);
"""

CREATE_DIM_METADATA = """
CREATE TABLE IF NOT EXISTS dim_metadata (
    symbol          VARCHAR(20)     NOT NULL ENCODE lzo,
    company_name    VARCHAR(255)    ENCODE lzo,
    sector          VARCHAR(100)    ENCODE lzo,
    PRIMARY KEY (symbol)
)
DISTSTYLE ALL;
"""

# create staging table as required by redshift on both fact and dimension tables
CREATE_STAGING_STOCK_PRICES = """
CREATE TABLE IF NOT EXISTS staging_stock_prices (
    symbol      VARCHAR(20)     NOT NULL ENCODE lzo,
    date        DATE            NOT NULL ENCODE az64,
    open_price  NUMERIC(12,4)   ENCODE az64,
    high        NUMERIC(12,4)   ENCODE az64,
    low         NUMERIC(12,4)   ENCODE az64,
    close_price NUMERIC(12,4)   ENCODE az64,
    volume      BIGINT          ENCODE az64
)
DISTSTYLE EVEN;
"""

CREATE_STAGING_DIM_DATE = """
CREATE TABLE IF NOT EXISTS staging_dim_date (
    date            DATE,
    day             INT,
    month           INT,
    year            INT,
    quarter         INT,
    day_of_week     INT,
    week_of_year    INT
)
DISTSTYLE EVEN;
"""

CREATE_STAGING_DIM_METADATA = """
CREATE TABLE IF NOT EXISTS staging_dim_metadata (
    symbol          VARCHAR(20),
    company_name    VARCHAR(255),
    sector          VARCHAR(100)
)
DISTSTYLE EVEN;
"""

def create_redshift_schema():
    conn = db_connect()
    cursor = conn.cursor()

    try:
        logger.info("Creating stock_prices fact table in Redshift...")
        cursor.execute(CREATE_FACT_STOCK_PRICES)

        logger.info("Creating dim_date table in Redshift...")
        cursor.execute(CREATE_DIM_DATE)

        logger.info("Creating dim_metadata table in Redshift...")
        cursor.execute(CREATE_DIM_METADATA)

        # create staging tables for Redshift COPY loading
        logger.info("Creating staging tables for Redshift COPY loading...")
        cursor.execute(CREATE_STAGING_STOCK_PRICES)
        cursor.execute(CREATE_STAGING_DIM_DATE)
        cursor.execute(CREATE_STAGING_DIM_METADATA)

        conn.commit()
        logger.info("Redshift schema created successfully.")
    
    except Exception as e:
        conn.rollback()
        logger.error(f"Failed to create Redshift schema: {e}")
        raise

    finally:
        cursor.close()
        conn.close()

if __name__ == "__main__":
    import logging
    logging.basicConfig(level=logging.INFO, format='%(asctime)s %(levelname)s %(message)s')
    create_redshift_schema()