import psycopg2
import os
from dotenv import load_dotenv
from psycopg2.extras import execute_values

load_dotenv()

# open the connection to the PostgreSQL database using credentials from environment variables
def load_stock_prices(df):
    conn = psycopg2.connect(
        host=os.getenv('DB_HOST'),
        port=os.getenv('DB_PORT'),
        dbname=os.getenv('DB_NAME'),
        user=os.getenv('DB_USER'),
        password=os.getenv('DB_PASSWORD')
    )

    cursor = conn.cursor()  # create a cursor object to interact with the database

    # insert data into stock_prices table as a bulk
    # Convert all numpy types to Python native types to avoid psycopg2 adapter errors
    fact_columns = ["symbol", "date", "open", "high", "low", "close", "volume"]
    df['date'] = df['date'].dt.to_pydatetime()  # numpy.datetime64 -> datetime
    # Use .values.tolist() to convert numpy types to native Python types
    values = [tuple(row) for row in df[fact_columns].values.tolist()]

    insert_query = """
        INSERT INTO stock_prices (symbol, date, open, high, low, close, volume)
        VALUES %s
        ON CONFLICT (symbol, date) DO NOTHING
    """

    execute_values(cursor, insert_query, values)

    conn.commit()
    cursor.close()
    conn.close()