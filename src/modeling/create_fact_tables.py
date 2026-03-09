import psycopg2
from dotenv import load_dotenv
import os

load_dotenv()

def create_fact_table():
    # open the SQL connection
    conn = psycopg2.connect(
        host = os.getenv("DB_HOST"),
        port = os.getenv("DB_PORT"),
        dbname = os.getenv("DB_NAME"),
        user = os.getenv("DB_USER"),
        password = os.getenv("DB_PASSWORD")
    )

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
            FOREIGN KEY (symbol) REFERENCES dim_stocks(symbol),
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