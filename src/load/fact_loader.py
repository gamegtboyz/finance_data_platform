import os
import json
import tempfile
from psycopg2.extras import execute_values
import logging
from datetime import datetime, timezone
from dotenv import load_dotenv

from load.redshift_copy_loader import copy_json_from_s3
from storage.s3_client import s3_upload

load_dotenv()
logger = logging.getLogger(__name__)

# selector function to choose either Postgres or Redshift loader based on environment variable
def load_stock_prices(cursor,df,conn=None):
    engine = os.getenv("DB_ENGINE", "postgres")
    if engine == "redshift":
        _load_stock_prices_redshift(cursor, df)
    elif engine == "postgres":
        _load_stock_prices_postgres(cursor, df)

# open the connection to the PostgreSQL database using credentials from environment variables
def _load_stock_prices_postgres(cursor, df):
    """
    we used to create the connection and cursor inside this function
    but we moed it out to pipeline.py for maintainability purposes
    in case of rollback and error handling across multiple steps
    of the pipeline.
    We want to be able to rollback the entire transaction
    if any step fails, which requires us to use the same connection
    and cursor across all steps in the pipeline.
    """

    # insert data into stock_prices table as a bulk
    # Convert all numpy types to Python native types to avoid psycopg2 adapter errors
    fact_columns = ["symbol", "date", "open", "high", "low", "close", "volume"]
    df_copy = df[fact_columns].copy()  # create a copy to avoid modifying the original DataFrame
    df_copy['date'] = df_copy['date'].dt.to_pydatetime()  # numpy.datetime64 -> datetime
    # Use .values.tolist() to convert numpy types to native Python types
    values = [tuple(row) for row in df_copy[fact_columns].values.tolist()]

    insert_query = """
        INSERT INTO stock_prices (symbol, date, open, high, low, close, volume)
        VALUES %s
        ON CONFLICT (symbol, date) DO NOTHING
    """

    execute_values(cursor, insert_query, values)
    logger.info(f"Loaded {len(values)} new rows into stock_prices")

def _load_stock_prices_redshift(cursor, df):
    """
    Redshift COPY is optimized for bulk loading large datasets, so we:
    1. Serialize the DataFrame to JSONLines format and upload to S3 staging area
    2. Use Redshift COPY command to load from S3 into a staging table
    3. merge from staging table to target fact table with idempotent logic (delete matching keys before insert)
    """

    fact_columns = ["symbol", "date", "open", "high", "low", "close", "volume"]
    df_copy = df[fact_columns].copy()  # create a copy to avoid modifying the original DataFrame
    df_copy['date'] = df_copy['date'].dt.strftime("%Y-%m-%d")

    # 1. Serialize DataFrame to JSONLines and upload to S3 staging prefix
    timestamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    s3_key = f"staging/stock_prices/{timestamp}.jsonl"
    local_tmp = tempfile.NamedTemporaryFile(mode="w", suffix=".jsonl", delete=False)

    values_list = df_copy[fact_columns].values.tolist() # convert numpy to python native types
    columns_list = df_copy[fact_columns].columns.tolist()
    for row in values_list:
        record = dict(zip(columns_list, row))
        local_tmp.write(json.dumps(record) + "\n")
    local_tmp.close() # close the file to flush the buffer and make it available for upload

    s3_upload(local_tmp.name, os.getenv("S3_BUCKET_NAME"), s3_key) # upload the local temp file to S3 staging area
    os.unlink(local_tmp.name) # delete the local temp file after upload

    # 2. COPY into staging table (always start clean for idempotency)
    cursor.execute("TRUNCATE TABLE staging_stock_prices;") # clear staging table before loading new data
    copy_json_from_s3(cursor, "staging_stock_prices", s3_key)

    # 3. DELETE matching rows from target → idempotency
    cursor.execute(
        """
        DELETE FROM stock_prices
        USING staging_stock_prices
        WHERE stock_prices.symbol = staging_stock_prices.symbol
        AND stock_prices.date = staging_stock_prices.date;
        """
    )

    # 4. INSERT from staging into target
    cursor.execute(
        """
        INSERT INTO stock_prices (symbol, date, open, high, low, close, volume)
        SELECT symbol, date, open, high, low, close, volume
        FROM staging_stock_prices;
        """
    )

    logger.info(f"Loaded {len(df_copy)} rows into stock_prices via Redshift COPY")

def get_max_loaded_date(cursor, symbol):
    """
    Check for latest date already loaded for the symbol in the fact table.
    """
    cursor.execute(
        "SELECT MAX(date) FROM stock_prices WHERE symbol = %s;",
        (symbol,)
    )

    # fetchone() returns a tuple, we want the first element which is the max date
    result = cursor.fetchone()
    max_date = result[0]
    logger.info(f"Latest loaded date for {symbol}: {max_date}")
    return max_date