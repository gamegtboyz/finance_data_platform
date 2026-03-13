import psycopg2
from dotenv import load_dotenv
import os

load_dotenv()

def create_indexes():
    """
    Create performance indexes on the fact table.
    """

    # create the SQL connection
    conn = psycopg2.connect(
        host = os.getenv("DB_HOST"),
        port = os.getenv("DB_PORT"),
        dbname = os.getenv("DB_NAME"),
        user = os.getenv("DB_user"),
        password = os.getenv("DB_PASSWORD")
    )

    # create cursor object to interact with the SQL connection
    cursor = conn.cursor()

    # # index on symbol - frequently used in WHERE clauses
    # cursor.execute(
    #     """
    #     CREATE INDEX IF NOT EXISTS idx_stock_prices_symbol
    #     ON stock_prices (symbol);
    #     """
    # )

    # index on date - frequently used in range queries
    cursor.execute(
        """
        CREATE INDEX IF NOT EXISTS idx_stock_prices_date
        ON stock_prices (date);
        """
    )

    # # composite index on (symbol, date) - covers common queries which use both symbol and date
    # cursor.execute(
    #     """
    #     CREATE INDEX IF NOT EXISTS idx_stock_prices_symbol_date
    #     ON stock_prices (symbol, date);
    #     """
    # )

    conn.commit()
    cursor.close()
    conn.close()
    
    if __name__ == "__main__":
        create_indexes()