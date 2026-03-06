import logging
from ingestion.extract import fetch_stock_data
from processing.transform import transform_stock_data
from loaders.load import load_to_postgres

# config the logging to display info level messages with timestamps
logging.basicConfig(
    level=logging.INFO,                                 # set the logging level to INFO
    format="%(asctime)s - %(levelname)s - %(message)s"  # specify the format of the log messages to include timestamp, log level, and message content
)

def run_pipeline(symbols):
    for symbol in symbols:
        try:
            logging.info("Starting ETL pipeline for stock symbol: %s", symbol)
            
            logging.info("Extracting data...")
            raw_data = fetch_stock_data(symbol)

            logging.info("Transforming data...")
            df = transform_stock_data(raw_data, symbol)

            logging.info("Loading data into PostgreSQL...")
            load_to_postgres(df)

            logging.info("Pipeline completed successfully.")

        except Exception as e:
            logging.error(f"Pipeline failed: {e}")
            raise

# Run the pipeline for a list of stock symbols
if __name__ == "__main__":
    symbols = ["NVDA","AAPL","MSFT","GOOGL","AMZN"]
    run_pipeline(symbols)