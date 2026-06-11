import os
import logging
from psycopg2.extras import execute_values
from dotenv import load_dotenv

load_dotenv()
logger = logging.getLogger(__name__)

def load_fundamentals(cursor, df, conn=None):
    engine = os.getenv("DB_ENGINE", "postgres")
    if engine == "redshift":
        _load_fundamentals_redshift(cursor, df)
    elif engine == "postgres":
        _load_fundamentals_postgres(cursor, df)

def _load_fundamentals_postgres(cursor, df):
    cols = ["symbol", "period_end_date", "form_type", "metric", "value", "unit", "filed_date"]
    df_copy = df[cols].copy()
    # convert date to python date object to avoid psycopg2 errors
    df_copy["period_end_date"] = df_copy["period_end_date"].dt.to_pydatetime()
    df_copy["filed_date"] = df_copy["filed_date"].dt.to_pydatetime()
    # convert the DataFrame to a list of tuples for execute_values
    values = [tuple(row) for row in df_copy.values.tolist()]

    insert_query = """
        INSERT INTO fundamentals (symbol, period_end_date, form_type, metric, value, unit, filed_date)
        VALUES %s
        ON CONFLICT (symbol, period_end_date, metric) DO NOTHING;
    """
    execute_values(cursor, insert_query, values)
    logger.info(f"Loaded {len(values)} rows into fundamentals on local PostgreSQL")

def _load_fundamentals_redshift(cursor, df):
    cols = ["symbol", "period_end_date", "form_type", "metric", "value", "unit", "filed_date"]
    df_copy = df[cols].copy()
    # convert date to python date object to avoid psycopg2 errors
    df_copy["period_end_date"] = df_copy["period_end_date"].dt.strftime("%Y-%m-%d")
    df_copy["filed_date"] = df_copy["filed_date"].dt.strftime("%Y-%m-%d")
    # convert the DataFrame to a list of tuples for execute_values
    values = [tuple(row) for row in df_copy.values.tolist()]

    # Redshift does not support ON CONFLICT, so we need to delete existing records for the same (symbol, period_end_date, metric) before inserting new ones
    for row in values:
        cursor.execute(
            "DELETE FROM fundamentals WHERE symbol = %s AND period_end_date = %s AND metric = %s;",
            (row[0], row[1], row[3])
        )

    insert_query = """
        INSERT INTO fundamentals (symbol, period_end_date, form_type, metric, value, unit, filed_date)
        VALUES (%s, %s, %s, %s, %s, %s, %s)
    """
    cursor.executemany(insert_query, values) # Redshift does not support execute_values, so we use executemany instead which is less efficient but necessary for compatibility
    logger.info(f"Loaded {len(values)} rows into fundamentals on Redshift Serverless")