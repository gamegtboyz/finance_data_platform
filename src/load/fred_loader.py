import os, json, tempfile, logging
from psycopg2.extras import execute_values
from datetime import datetime, timezone
from dotenv import load_dotenv
from load.redshift_copy_loader import copy_json_from_s3
from storage.s3_client import s3_upload

load_dotenv()
logger = logging.getLogger(__name__)

def load_macros(cursor, df, conn=None):
    """Loads macro indicators data into the database."""
    engine = os.getenv("DB_ENGINE", "postgres")
    if engine == "redshift":
        _load_macros_redshift(cursor, df)
    else:
        _load_macros_postgres(cursor, df)

def _load_macros_postgres(cursor, df):
    """Loads macro indicators data into Postgres."""
    values = [
        (row["series_id"], row["date"].date(), float(row["value"]))
        for _, row in df.iterrows()
    ]
    execute_values(cursor, """
        INSERT INTO macros (series_id, date, value)
        VALUES %s
        ON CONFLICT (series_id, date) DO NOTHING)
    """, values)
    logger.info(f"Loaded {len(values)} rows into macros table in Postgres.")

def _load_macros_redshift(cursor, df):
    df_copy = df.copy()
    df_copy["date"] = df_copy["date"].dt.strftime("%Y-%m-%d")

    timestamp = datetime.now(timezone.utc).strftime("%Y%m%d%H%M%SZ")
    s3_key = f"staging/macros_{timestamp}.jsonl"
    tmp = tempfile.NamedTemporaryFIle(mode="w", suffex=".jsonl", delete=False)
    for _, row in df_copy.iterrows():
        tmp.write(json.dumps({"series_id": row["series_id"],
                              "date":  row["date"],
                              "value": row["value"]}) + "\n")
    tmp.close()

    s3_upload(tmp.name, os.getenv("S3_BUCKET_NAME"), s3_key)

    import os as _os; _os.unlink(tmp.name)
    cursor.execute("TRUNCATE TABLE staging_macros;")
    copy_json_from_s3(cursor, "staging_macros", s3_key)

    cursor.execute("""
        DELETE  FROM macros
        USING   staging_macros
        WHERE   macros.series_id    = staging_macros.series_id
        AND     macros.date         = staging_macros.date;
    """)
    cursor.execute("""
        INSERT INTO macros (series_id, date, value)
        SELECT series_id, date, value
        FROM staging_macros;
    """)

    logger.info("Loaded macros into Redshift Serverless Database.")