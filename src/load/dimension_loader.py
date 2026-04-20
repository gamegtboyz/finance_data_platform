from psycopg2.extras import execute_values
import logging
from dotenv import load_dotenv
import os

load_dotenv()
logger = logging.getLogger(__name__)

def load_dim_dates(cursor, df):
    engine = os.getenv("DB_ENGINE", "postgres")
    if engine == "redshift":
        _load_dim_dates_redshift(cursor, df)
    elif engine == "postgres":
        _load_dim_dates_postgres(cursor, df)

def _load_dim_dates_redshift(cursor, df):
    """
    Load the date dimensions in bulk into Redshift database
    """
    date_cols = ['date','day','month','year','quarter','day_of_week','week_of_year']
    df_dates = df[date_cols].drop_duplicates().copy()
    df_dates['date'] = df_dates['date'].dt.strftime("%Y-%m-%d")

    # Inline staging with a VALUES insert - dim_date is tiny (a row per day)
    values = [tuple(row) for row in df_dates.values.tolist()]
    placeholders = ",".join(["%s"] * len(date_cols))
    for row in values:
        cursor.execute(f"""
            DELETE FROM dim_date WHERE date = %s;
        """, (row[0],))
        cursor.execute(f"""
            INSERT INTO dim_date ({", ".join(date_cols)}) VALUES ({placeholders});
            """, row)

    logger.info(f"loaded {len(values)} rows into dim_date on Redshift db")

def _load_dim_dates_postgres(cursor, df):
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
    engine = os.getenv("DB_ENGINE", "postgres")
    if engine == "redshift":
        _load_dim_metadata_redshift(cursor, metadata)
    elif engine == "postgres":
        _load_dim_metadata_postgres(cursor, metadata)

def _load_dim_metadata_redshift(cursor, metadata):
    """
    Load the metadata dimensions into Redshift database, with idempotent logic (delete matching keys before insert)
    """
    cursor.execute("DELETE FROM dim_metadata WHERE symbol = %s;", (metadata["symbol"],))
    cursor.execute(
        """
        INSERT INTO dim_metadata (symbol, company_name, sector)
        VALUES (%s, %s, %s);
        """, (metadata["symbol"], metadata["company_name"], metadata["sector"])
    )

    logger.info(f"loaded metadata for {metadata['symbol']} into dim_metadata on Redshift db")

def _load_dim_metadata_postgres(cursor, metadata):
    cursor.execute(
        """
        INSERT INTO dim_metadata (symbol, company_name, sector)
        VALUES (%s, %s, %s)
        ON CONFLICT (symbol) DO NOTHING;
        """,
        (metadata["symbol"], metadata["company_name"], metadata["sector"])
    )
    logger.info(f"Loaded metadata for {metadata['symbol']} into dim_metadata")
