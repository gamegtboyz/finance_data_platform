import time
import logging
import time
from ingestion.alphavantage_ingest import fetch_and_store
from processing.transform_stock import transform
from loaders.postgres_loader import load_to_postgres

# config the logging to display info level messages with timestamps
logging.basicConfig(
    level=logging.INFO
)

def run_pipeline(symbols):
    for symbol in symbols:
        try:
            logging.info(f"Fetching {symbol}")
            filepath = fetch_and_store(symbol)

            logging.info("Transforming")
            df = transform(filepath, symbol)

            logging.info("Loading into PostgreSQL")
            load_to_postgres(df)

            time.sleep(12)  # sleep for 12 secs to avoid minute-wise API request limit

        except Exception as e:
            logging.error(f"Pipeline failed: {e}")
            raise

# Run the pipeline for a list of stock symbols
if __name__ == "__main__":
    symbols = ["NVDA","AAPL","MSFT","GOOGL","AMZN"]
    run_pipeline(symbols)