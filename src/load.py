import psycopg2
import os
from dotenv import load_dotenv

load_dotenv()

# open the connection to the PostgreSQL database using credentials from environment variables
def load_to_postgres(df):
    conn = psycopg2.connect(
        host=os.getenv('DB_HOST'),
        port=os.getenv('DB_PORT'),
        dbname=os.getenv('DB_NAME'),
        user=os.getenv('DB_USER'),
        password=os.getenv('DB_PASSWORD')
    )

    cursor = conn.cursor()  # create a cursor object to interact with the database

    # Insert data into the stock_prices table
    for _, row in df.iterrows():
        cursor.execute("""
            INSERT INTO stock_prices (symbol, date, open, high, low, close, volume)
            VALUES (%s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (symbol, date) DO NOTHING
        """, (
            row['symbol'],
            row['date'],
            row['open'],
            row['high'],
            row['low'],
            row['close'],
            row['volume']
        ))

    conn.commit()
    cursor.close()
    conn.close()