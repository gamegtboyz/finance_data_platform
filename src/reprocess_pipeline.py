import logging
import os
import glob
import psycopg2
from dotenv import load_dotenv

from processing.transform_stock import transform, load_company_metadata
from loaders.postgres_loader import load_to_postgres
from loaders.dimension_loader import load_dim_dates, load_dim_metadata
from modeling.create_dimension_tables import create_dim_dates, create_dim_metadata
from modeling.create_fact_tables import create_fact_table

logging.basicConfig(level=logging.INFO)

load_dotenv()

def get_latest_file(symbol):
    """Return the most recently dated OHLCV JSON file for a symbol."""
    pattern = f"data/raw/{symbol}/{symbol}_*.json"
    files = [f for f in glob.glob(pattern) if "metadata" not in f]
    if not files:
        raise FileNotFoundError(f"No raw data files found for {symbol} at {pattern}")
    return max(files)  # lexicographic max works because filenames are YYYY-MM-DD

def get_metadata_file(symbol):
    """Return the metadata JSON filepath for a symbol."""
    filepath = f"data/raw/{symbol}/{symbol}_metadata.json"
    if not os.path.exists(filepath):
        raise FileNotFoundError(f"No metadata file found for {symbol} at {filepath}")
    return filepath

def reprocess(symbols):
    conn = psycopg2.connect(
        host=os.getenv("DB_HOST"),
        port=os.getenv("DB_PORT"),
        dbname=os.getenv("DB_NAME"),
        user=os.getenv("DB_USER"),
        password=os.getenv("DB_PASSWORD")
    )

    cursor = conn.cursor()

    for symbol in symbols:
        try:
            logging.info(f"Reprocessing {symbol} from local files")

            filepath = get_latest_file(symbol)
            metadata_filepath = get_metadata_file(symbol)
            logging.info(f"Using data file: {filepath}")

            logging.info("Transforming")
            df = transform(filepath, symbol)
            metadata = load_company_metadata(metadata_filepath)

            logging.info("Populating dimension tables")
            load_dim_dates(cursor, df)
            load_dim_metadata(cursor, metadata)
            conn.commit()

            logging.info("Loading into fact table")
            load_to_postgres(df)

            logging.info(f"Reprocess completed for {symbol}")

        except Exception as e:
            conn.rollback()
            logging.error(f"Reprocess failed for {symbol}: {e}")
            raise

    cursor.close()
    conn.close()

if __name__ == "__main__":
    symbols = ["NVDA", "AAPL", "MSFT", "GOOGL", "AMZN"]
    create_dim_dates()
    create_dim_metadata()
    create_fact_table()
    reprocess(symbols)
