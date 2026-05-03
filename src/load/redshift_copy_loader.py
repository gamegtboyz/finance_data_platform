import os
import logging
from dotenv import load_dotenv

load_dotenv()
logger = logging.getLogger(__name__)

def copy_json_from_s3(cursor, table_name:str, s3_key:str) -> None:
    """
    Bulk-load a JSONLines file from s3 into a Redshift table using COPY command.
    The file must be JSONLines format with keys exactly matching the table columns.

    the SQL command could be explained as follows:
    - COPY {table_name}: specifies the target Redshift table to load data into.
    - FROM '{s3_url}': specifies the S3 URL of the source file to be loaded.
    - IAM_ROLE '{iam_role}': specifies the IAM role that Redshift will assume to access the S3 bucket.
    - FORMAT AS JSON 'auto': tells Redshift to automatically parse the JSON file and map it to the table columns.
    - TIMEFORMAT 'auto': tells Redshift to automatically parse any time values in the JSON file.
    - TRUNCATECOLUMNS: allows Redshift to truncate any data that exceeds the column width instead of throwing an error.
    - MAXERROR 0: specifies that the COPY command should fail if any errors are encountered during the load process, ensuring data integrity.
    """

    # define the bucket and associated IAM role for Redshift to access the S3 bucket
    bucket = os.getenv("S3_BUCKET_NAME")
    iam_role = os.getenv("REDSHIFT_IAM_ROLE")
    s3_url = f"s3://{bucket}/{s3_key}"

    copy_sql = f"""
        COPY {table_name}
        FROM '{s3_url}'
        IAM_ROLE '{iam_role}'
        FORMAT AS JSON 'auto'
        TIMEFORMAT 'auto'
        TRUNCATECOLUMNS
        MAXERROR 0;
    """

    logger.info(f"Executing COPY into {table_name} from {s3_url}")
    cursor.execute(copy_sql)
    logger.info(f"COPY complete for {table_name}")

def vacuum_analyze(conn, table_name: str) -> None:
    """
    As DELETE mechanism in _load_stock_prices_redshift is a soft delete, we need to VACUUM to reclaim the disk blocks.
    Then ANALYZE to update the table statistics for the query planner.
    It must run outside a transaction -- sets autocommit temporalily.
    """

    original_autocommit = conn.autocommit       # create and store original autocommit setting
    conn.autocommit = True                      # set autocommit to True to run VACUUM and ANALYZE outside of a transaction 
    cursor = conn.cursor()
    try:
        logger.info(f"VACUUM SORT ONLY {table_name}...")
        cursor.execute(f"VACUUM SORT ONLY {table_name};")
        logger.info(f"ANALYZE {table_name}...")
        cursor.execute(f"ANALYZE {table_name};")
    finally:
        cursor.close()
        conn.autocommit = original_autocommit   # restore original autocommit setting