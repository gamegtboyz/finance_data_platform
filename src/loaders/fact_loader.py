from psycopg2.extras import execute_values
import logging

logger = logging.getLogger(__name__)

# open the connection to the PostgreSQL database using credentials from environment variables
def load_stock_prices(cursor, df):
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

def get_max_loaded_date(cursor, symbol):
    """
    Return the latest date already loaded for a symbol, or None if no data is loaded yet.
    """
    cursor.execute(
        "SELECT MAX(date) FROM stock_prices WHERE symbol = %s;",
        (symbol,)
    )

    result = cursor.fetchone()
    max_date = result[0]
    logger.info(f"Latest loaded date for {symbol}: {max_date}")
    return max_date