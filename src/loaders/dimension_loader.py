COMPANY_NAMES = {
    "NVDA": "NVIDIA Corporation",
    "AAPL": "Apple Inc.",
    "MSFT": "Microsoft Corporation",
    "GOOGL": "Alphabet Inc.",
    "AMZN": "Amazon.com Inc."
}

def load_dim_stocks(cursor, symbol):
    cursor.execute(
        """
        INSERT INTO dim_stocks (symbol, company_name)
        VALUES (%s, %s)
        ON CONFLICT (symbol) DO NOTHING;
        """,
        (symbol, COMPANY_NAMES.get(symbol, f"Company name for {symbol}"))
    )

def load_dim_dates(cursor, df):
    for _, row in df.iterrows():
        cursor.execute(
            """
            INSERT INTO dim_date (date, day, month, year, quarter)
            VALUES (%s, %s, %s, %s, %s)
            ON CONFLICT (date) DO NOTHING;
            """,
            (row["date"], row["day"], row["month"], row["year"], row["quarter"])
        )
