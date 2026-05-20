from datetime import datetime, timedelta
import logging

from airflow import DAG
from airflow.operators.python import PythonOperator

logger = logging.getLogger(__name__)

symbols = ["NVDA","AAPL","MSFT","GOOGL","AMZN"]

# extraction method
def extract_task(**context):
    """Fetch raw data from AlphaVantage API, then upload into S3."""
    import time
    from src.extract.alphavantage_ingest import fetch_stock_prices, fetch_company_metadata
    results = {}
    for symbol in symbols:
        filepath = fetch_stock_prices(symbol)
        time.sleep(12)
        metadata_filepath = fetch_company_metadata(symbol)
        time.sleep(12)
        # append the fetch output to our results as dictionary
        results[symbol] = {
            "prices": filepath,
            "metadata": metadata_filepath
        }
    # push results, indexed by filepath (symbol), to XCom to be accessible by downstream tasks
    context["ti"].xcom_push(key="filepaths", value=results)

# transformation method
def transform_task(**context):
    """Transform raw JSON files into clean DataFrames, then push to XCom."""
    from src.transform.transform_stock import transform_stock_prices, transform_company_metadata
    filepaths = context["ti"].xcom_pull(key="filepaths", task_ids="extract")    # task_id is defined in the DAG below
    transformed = {}
    for symbol, paths in filepaths.items():
        df = transform_stock_prices(paths["prices"], symbol)        # align with extraction results
        metadata = transform_company_metadata(paths["metadata"])
        transformed[symbol] = {
            "df": df.to_json(),
            "metadata": metadata
        }
    context["ti"].xcom_push(key="transformed", value=transformed)

# load method
def load_task(**context):
    """Load transformed data into the Database."""
    import pandas as pd
    from src.db_connect import db_connect
    from src.load.fact_loader import load_stock_prices, get_max_loaded_date
    from src.load.dimension_loader import load_dim_dates, load_dim_metadata
    transformed = context["ti"].xcom_pull(key="transformed", task_ids="transform")
    conn = db_connect()
    cursor = conn.cursor()
    try:
        for symbol, data in transformed.items():
            df = pd.read_json(data["df"])   # align with transformation results (transformed)
            metadata = data["metadata"]
            max_date = get_max_loaded_date(cursor, symbol)
            if max_date is not None:
                df = df[df["date"].dt.date > max_date]
            if df.empty:
                logger.info(f"{symbol}: no new data to load")
                continue
            load_dim_dates(cursor, df)
            load_dim_metadata(cursor, metadata)
            load_stock_prices(cursor, df)
            conn.commit()
    except Exception:
        conn.rollback()
        logger.error("Load task failed", exc_info=True)
        raise
    finally:
        cursor.close()
        conn.close()

def notify_on_failure(context):
    """Called when any task fails. Wire to Slack or email in production."""
    task_instance = context["task_instance"]
    logger.error(
        f"Task FAILED: DAG={task_instance.dag_id}, "
        f"Task={task_instance.task_id}, "
        f"Run={context['run_id']}"
    )

    # area to add SlackWebhookOperator or EmailOperator to send notification on failure

def sla_miss_callback(dag, task_list, blocking_task_list, slas, blocking_tis):
    """Fires when a task exceeds its SLA window. Useful for alerting on performance issues."""
    logger.warning(
        f"SLA MISSED on DAG '{dag.dag_id}'."
        f"MIssed tasks: {[str(t) for t in task_list]}"
    )

    # area to add SlackWebhookOperator or EmailOperator to send notification on SLA miss

default_args = {
    "owner": "airflow",
    "retries": 3,
    "retry_delay": timedelta(minutes=10),
    "on_failure_callback": notify_on_failure
}

# define the DAG
with DAG(
    dag_id="stock_pipeline_dag",
    default_args=default_args,
    start_date=datetime(2026, 5, 1),
    schedule_interval="0 23 * * 1-5",   # every weekday at 11PM, 2 hours after US market close to ensure data availability
    catchup=False,
    sla_miss_callback=sla_miss_callback,
    tags=["finance", "stage3"]
) as dag:
    
    extract = PythonOperator(
        task_id="extract",
        python_callable=extract_task,
        sla=timedelta(hours=1)   # set SLA of 1 hour for extraction task
    )

    transform = PythonOperator(
        task_id="transform",
        python_callable=transform_task,
        sla=timedelta(hours=1, minutes=30)   # set SLA of 1 hour and 30 minutes for transformation task
    )

    load = PythonOperator(
        task_id="load",
        python_callable=load_task,
        sla=timedelta(hours=2)   # set SLA of 2 hours for load task
    )

    # define task dependencies
    extract >> transform >> load