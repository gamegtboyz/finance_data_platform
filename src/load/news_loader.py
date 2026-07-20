import os, json, tempfile, logging
from psycopg2.extras import execute_values
from datetime import datetime, timezone
from dotenv import load_dotenv
from load.redshift_copy_loader import copy_json_from_s3
from storage.s3_client import s3_upload

load_dotenv()
logger = logging.getLogger(__name__)

def load_news_sentiment(cursor, df, conn=None):
    """Load news sentiment data into the database"""
    engine = os.getenv("DB_ENDGINE", "postgres")
    if engine == "redshift":
        _load_news_sentiment_redshift(cursor, df)
    else:
        _load_news_sentiment_postgres(cursor, df)

def _load_news_sentiment_redshift(cursor, df):
    pass

def _load_news_sentiment_postgres(cursor, df):
    pass