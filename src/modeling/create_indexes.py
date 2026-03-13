from db_connect import db_connect

def create_indexes():
    """
    Create performance indexes on the fact table.
    """

    # create the SQL connection
    conn = db_connect()

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