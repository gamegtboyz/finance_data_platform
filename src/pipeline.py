import logging
import time

from db_connect import db_connect
from ingestion.alphavantage_ingest import fetch_stock_prices, fetch_company_metadata
from processing.transform_stock import transform_stock_prices, transform_company_metadata
from loaders.fact_loader import load_stock_prices, get_max_loaded_date
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

def run(symbols):
    conn = db_connect()

    cursor = conn.cursor()  # create a cursor object to interact with the database

    for symbol in symbols:
        try:
            # fetch the data from the API, we use 12 secs sleep after each API call to respect free tier limit (5 calls per minute)
            logger.info(f"Fetching {symbol}")
            filepath = fetch_stock_prices(symbol)
            time.sleep(12)
            metadata_filepath = fetch_company_metadata(symbol)
            time.sleep(12)

            # transform the data and metadata
            logger.info("Transforming")
            df = transform_stock_prices(filepath, symbol)
            metadata = transform_company_metadata(metadata_filepath)

            # add the incremental loading logic here by checking the max date already loaded for the symbol in the fact table
            max_date = get_max_loaded_date(cursor, symbol)
            if max_date is not None:
                df = df[df["date"].dt.date > max_date]  # filter the dataframe to only include rows with date greater than max_date
                logger.info(f"{symbol}: {len(df)} new rows after {max_date}")

            if df.empty:
                logger.info(f"{symbol}: no new data to load, skipping")
                continue

            # load the dimension tables first, as the fact table has foreign key associated to them
            logger.info("Populating dimension tables")
            load_dim_dates(cursor, df)            
            load_dim_metadata(cursor, metadata)

            # load the fact table with transformed data
            logger.info("Loading into fact table")
            load_stock_prices(cursor, df)
            conn.commit()

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