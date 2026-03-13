from psycopg2.extras import execute_values

# this function load the dimension tables in the database row-by-row, we replaced it with the bulk version in a following method
def load_dim_dates_iter(cursor, df):
    for _, row in df.iterrows():
        cursor.execute(
            """
            INSERT INTO dim_date (date, day, month, year, quarter)
            VALUES (%s, %s, %s, %s, %s)
            ON CONFLICT (date) DO NOTHING;
            """,
            (row["date"], row["day"], row["month"], row["year"], row["quarter"])
        )

# add the bulk loading version of load_dim_dates
def load_dim_dates(cursor, df):
    """
    Load the date dimensions in bulk,
    reduce the load time significantly than the iteration row-by-row
    """

    date_cols = ['date', 'day', 'month', 'year', 'quarter', 'day_of_week', 'week_of_year']
    df = df[date_cols].drop_duplicates()
    values = [tuple(row) for row in df.values.tolist()]

    insert_query = """
        INSERT INTO dim_date (date, day, month, year, quarter, day_of_week, week_of_year)
        values %s
        ON CONFLICT (date) DO NOTHING;
    """

    execute_values(cursor, insert_query,values)

def load_dim_metadata(cursor, metadata):
    cursor.execute(
        """
        INSERT INTO dim_metadata (symbol, company_name, sector)
        VALUES (%s, %s, %s)
        ON CONFLICT (symbol) DO NOTHING;
        """,
        (metadata["symbol"], metadata["company_name"], metadata["sector"])
    )
