import logging
import time
import os
import psycopg2
from dotenv import load_dotenv

from ingestion.alphavantage_ingest import fetch_and_store
from processing.transform_stock import transform
from loaders.postgres_loader import load_to_postgres
from loaders.dimension_loader import load_dim_stocks, load_dim_dates, load_dim_metadata
from modeling.create_dimension_tables import create_dim_stocks, create_dim_dates, create_dim_metadata
from modeling.create_fact_tables import create_fact_table

# config the logging to display info level messages with timestamps
logging.basicConfig(
    level=logging.INFO
)

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
            logging.info(f"Fetching {symbol}")
            filepath = fetch_and_store(symbol)

            logging.info("Transforming")
            df = transform(filepath, symbol)

            logging.info("Populating dimension tables")
            load_dim_stocks(cursor, symbol)
            load_dim_dates(cursor, df)
            load_dim_metadata(cursor, symbol)
            
            logging.info("Loading into fact table")
            load_to_postgres(df)

            time.sleep(12)  # sleep for 12 secs to avoid minute-wise API request limit

        except Exception as e:
            logging.error(f"Pipeline failed: {e}")
            raise

# Run the pipeline for a list of stock symbols
if __name__ == "__main__":
    symbols = ["NVDA","AAPL","MSFT","GOOGL","AMZN"]
    create_dim_stocks()  # ensure the dim_stocks table is created before loading data
    create_dim_dates()   # ensure the dim_dates table is created before loading data
    create_dim_metadata()  # ensure the dim_metadata table is created before loading data
    create_fact_table()  # ensure the fact table is created before loading data
    run(symbols)