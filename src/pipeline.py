import logging
import time
import os
import psycopg2
from dotenv import load_dotenv

from ingestion.alphavantage_ingest import fetch_stock_prices, fetch_company_metadata
from processing.transform_stock import transform_stock_prices, transform_company_metadata
from loaders.fact_loader import load_stock_prices
from loaders.dimension_loader import load_dim_dates, load_dim_metadata
from modeling.create_dimension_tables import create_dim_dates, create_dim_metadata
from modeling.create_fact_tables import create_fact_table
from modeling.create_indexes import create_indexes

# config the logging to display info level messages with timestamps
logging.basicConfig(
    level=logging.INFO,
    format = '%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    datefmt = '%Y-%m-%d %H:%M:%S'
)

logger = logging.getLogger(__name__)

load_dotenv()  # load environment variables from .env file

def run(symbols):
    conn = psycopg2.connect(
        host = os.getenv("DB_HOST"),
        port = os.getenv("DB_PORT"),
        dbname = os.getenv("DB_NAME"),
        user = os.getenv("DB_USER"),
        password = os.getenv("DB_PASSWORD")
    )

    cursor = conn.cursor()  # create a cursor object to interact with the database

    for symbol in symbols:
        try:
            logger.info(f"Fetching {symbol}")
            filepath = fetch_stock_prices(symbol)
            time.sleep(12)
            metadata_filepath = fetch_company_metadata(symbol)
            time.sleep(12)

            logger.info("Transforming")
            df = transform_stock_prices(filepath, symbol)
            metadata = transform_company_metadata(metadata_filepath)

            logger.info("Populating dimension tables")
            load_dim_dates(cursor, df)            
            load_dim_metadata(cursor, metadata)
            conn.commit()
            
            logger.info("Loading into fact table")
            load_stock_prices(df)

        except Exception as e:
            conn.rollback()
            logger.error(f"Pipeline failed: {e}")
            raise

# Run the pipeline for a list of stock symbols
if __name__ == "__main__":
    symbols = ["NVDA","AAPL","MSFT","GOOGL","AMZN"]
    create_dim_dates()   # ensure the dim_dates table is created before loading data
    create_dim_metadata()  # ensure the dim_metadata table is created before loading data
    create_fact_table()  # ensure the fact table is created before loading data
    create_indexes()  # create indexes on the fact table for performance
    run(symbols)