from db_connect import db_connect

def create_fact_table():
    # open the SQL connection
    conn = db_connect()

    # create a cursor object to interact with the database
    cursor = conn.cursor()

    cursor.execute(
        """
        CREATE TABLE IF NOT EXISTS stock_prices (
            symbol TEXT,
            date DATE,
            open NUMERIC,
            high NUMERIC,
            low NUMERIC,
            close NUMERIC,
            volume BIGINT,
            PRIMARY KEY (symbol, date),
            FOREIGN KEY (symbol) REFERENCES dim_metadata(symbol),
            FOREIGN KEY (date) REFERENCES dim_date(date)
        );
        """
    )

    conn.commit()
    cursor.close()
    conn.close()

if __name__ == "__main__":
    create_fact_table()