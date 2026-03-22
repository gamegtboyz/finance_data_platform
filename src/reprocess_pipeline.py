import logging
import os
import glob
import pathlib

from db_connect import db_connect
from storage.s3_client import s3_download, s3_list_objects
from transform.transform_stock import transform_stock_prices, transform_company_metadata
from load.fact_loader import load_stock_prices
from load.dimension_loader import load_dim_dates, load_dim_metadata
from modeling.create_dimension_tables import create_dim_dates, create_dim_metadata
from modeling.create_fact_tables import create_fact_table

logging.basicConfig(level=logging.INFO)

logger = logging.getLogger(__name__)

def get_latest_file(symbol):
    """Return the most recently dated OHLCV JSON file for a symbol."""
    pattern = str(pathlib.Path(__file__).parent.parent / f"data/raw/{symbol}/{symbol}_*.json")
    files = [f for f in glob.glob(pattern) if "metadata" not in f]

    # download the file from S3 if not found locally, then re-check for the file after download
    if not files:
        logger.info(f"No local files found for {symbol}, attempting to download from S3")
        objects = s3_list_objects(bucket_name=os.getenv('S3_BUCKET_NAME'), prefix=f"{symbol}/{symbol}_")
        symbol_keys = [
            obj['Key'] for obj in objects
            if "metadata" not in obj["Key"]
        ]
        if not symbol_keys:
            raise FileNotFoundError(f"No files found for {symbol} in S3 bucket {os.getenv('S3_BUCKET_NAME')}")
        
        latest_key = max(symbol_keys)
        local_dir = pathlib.Path(__file__).parent.parent / f"data/raw/{symbol}"
        local_dir.mkdir(parents=True, exist_ok=True)
        local_path = str(local_dir / pathlib.Path(latest_key).name)

        s3_download(os.getenv('S3_BUCKET_NAME'), latest_key, local_path)
        files = [f for f in glob.glob(pattern) if "metadata" not in f]

    if not files:
        raise FileNotFoundError(f"No raw data files found for {symbol} at {pattern}")
    return max(files)  # lexicographic max works because filenames are YYYY-MM-DD

def get_metadata_file(symbol):
    """Return the metadata JSON filepath for a symbol."""
    filepath = str(pathlib.Path(__file__).parent.parent / f"data/raw/{symbol}/{symbol}_metadata.json")
    if not os.path.exists(filepath):
        logger.info(f"No local metadata file found for {symbol}, attempting to download from S3")
        s3_download(os.getenv('S3_BUCKET_NAME'), f"{symbol}/{symbol}_metadata.json", filepath)
    if not os.path.exists(filepath):
        raise FileNotFoundError(f"No metadata file found for {symbol} at {filepath}")
    return filepath

def reprocess(symbols):
    conn = db_connect()
    cursor = conn.cursor()

    try:
        for symbol in symbols:
            try:
                logger.info(f"Reprocessing {symbol} from local files")

                filepath = get_latest_file(symbol)
                metadata_filepath = get_metadata_file(symbol)
                logger.info(f"Using data file: {filepath}")

                logger.info("Transforming")
                df = transform_stock_prices(filepath, symbol)
                metadata = transform_company_metadata(metadata_filepath)

                logger.info("Populating dimension tables")
                load_dim_dates(cursor, df)
                load_dim_metadata(cursor, metadata)
                conn.commit()

                logger.info("Loading into fact table")
                load_stock_prices(cursor, df)
                conn.commit()

                logger.info(f"Reprocess completed for {symbol}")

            except Exception as e:
                conn.rollback()
                logger.error(f"Reprocess failed for {symbol}: {e}")
                raise
    finally:
        cursor.close()
        conn.close()

if __name__ == "__main__":
    symbols = ["NVDA", "AAPL", "MSFT", "GOOGL", "AMZN"]
    create_dim_dates()
    create_dim_metadata()
    create_fact_table()
    reprocess(symbols)
