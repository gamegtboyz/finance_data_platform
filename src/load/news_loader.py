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
    engine = os.getenv("DB_ENGINE", "postgres")
    if engine == "redshift":
        _load_news_sentiment_redshift(cursor, df)
    else:
        _load_news_sentiment_postgres(cursor, df)

def _load_news_sentiment_postgres(cursor, df):
    """Load the news into local postgres database."""
    # build a list of tuples to load each row of data
    values = [
        (row["symbol"], row["date"].date(), row["headline"], float(row["sentiment_score"]),row["source"], row["url"])
        for _, row in df.iterrows()
    ]

    # execute SQL command to insert the list of tuples into the database
    execute_values(cursor, """
                   INSERT INTO news_sentiment (symbol, date, headline, sentiment_score, source, url)
                   VALUES %s
                   ON CONFLICT (symbol, url) DO NOTHING
                   """, values)
    
    logger.info(f"loaded {len(values)} rows into news_sentiment table in Postgres.")

def _load_news_sentiment_redshift(cursor, df):
    # make a copy of te original DataFrame
    df_copy = df.copy()
    
    # convert the date to the predefined string format to JSON serialization
    df_copy["date"] = df_copy["date"].dt.strftime("%Y-%m-%d")

    # create the timestamp to show the time data is registered into S3 bucket
    timestamp = datetime.now(timezone.utc).strftime("%Y%m%d%H%M%SZ")
    s3_key = f"staging/news_sentiment_{timestamp}.jsonl"
    
    # create the null .jsonl file so we could write the data on it
    tmp = tempfile.NamedTemporaryFile(mode='w', suffix=".jsonl", delete=False)

    # build a list of tuples to load each row of data into the temporary file, then write into the null temp .jsonl file
    for _, row in df_copy.iterrows():
        tmp.write(json.dumps({"symbol": row["symbol"],
                              "date": row["date"],
                              "headline": row["headline"],
                              "sentiment_score": row["sentiment_score"],
                              "source": row["source"],
                              "url": row["url"]}) + "\n")
    
    # after write those .jsonl file, close it
    tmp.close()

    # upload the file into s3 bucket
    s3_upload(tmp.name, os.getenv("S3_BUCKET_NAME"), s3_key)

    # copy the jsonl file from s3 then write into the redshift database
    import os as _os; _os.unlink(tmp.name)
    cursor.execute("TRUNCATE TABLE staging_news_sentiment;")
    copy_json_from_s3(cursor, "staging_news_sentiment", s3_key)

    cursor.execute("""
        DELETE from news_sentiment
        USING staging_news_sentiment
        WHERE news_sentiment.symbol = staging_news_sentiment.symbol
        AND news_sentiment.url = staging_news_sentiment.url;
    """)
    cursor.execute("""
        INSERT INTO news_sentiment (symbol, date, headline, sentiment_score, source, url)
        SELECT symbol, date, headline, sentiment_score, source, url
        FROM staging_news_sentiment;
    """)

    logger.info(f"Loaded {len(df_copy)} news headlines into Redshift Serverless Database")