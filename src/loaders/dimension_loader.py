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

def load_dim_metadata(cursor, metadata):
    cursor.execute(
        """
        INSERT INTO dim_metadata (symbol, company_name, sector)
        VALUES (%s, %s, %s)
        ON CONFLICT (symbol) DO NOTHING;
        """,
        (metadata["symbol"], metadata["company_name"], metadata["sector"])
    )
