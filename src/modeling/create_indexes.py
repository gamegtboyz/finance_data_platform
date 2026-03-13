from db_connect import db_connect

def create_indexes():
    """
    Create performance indexes on the fact table.
    """

    # create the SQL connection
    conn = db_connect()

    # create cursor object to interact with the SQL connection
    cursor = conn.cursor()

    # idx_stock_prices_symbol (solo symbol) is skipped: the composite PRIMARY KEY
    # (symbol, date) already satisfies queries filtering by symbol alone via index
    # scan on the leading column.

    # index on date — frequently used in range queries
    cursor.execute(
        """
        CREATE INDEX IF NOT EXISTS idx_stock_prices_date
        ON stock_prices (date);
        """
    )

    # idx_stock_prices_symbol_date composite index is skipped: the PRIMARY KEY
    # constraint already creates an equivalent B-tree index on (symbol, date).

    conn.commit()
    cursor.close()
    conn.close()
    
if __name__ == "__main__":
    create_indexes()