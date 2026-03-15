from psycopg2.extras import execute_values
import logging

logger = logging.getLogger(__name__)

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
    logger.info(f"Loaded {len(values)} rows into dim_date")

def load_dim_metadata(cursor, metadata):
    cursor.execute(
        """
        INSERT INTO dim_metadata (symbol, company_name, sector)
        VALUES (%s, %s, %s)
        ON CONFLICT (symbol) DO NOTHING;
        """,
        (metadata["symbol"], metadata["company_name"], metadata["sector"])
    )
    logger.info(f"Loaded metadata for {metadata['symbol']} into dim_metadata")
