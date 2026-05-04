from datetime import datetime, timedelta
import logging

from airflow import DAG
from airflow.operators.python import PythonOperator

from src.db_connect import db_connect
from src.extract.alphavantage_ingest import fetch_stock_prices, fetch_company_metadata
from src.transform.transform_stock import transform_stock_prices, transform_company_metadata
from src.load.fact_loader import load_stock_prices, get_max_loaded_date
from src.load.dimension_loader import load_dim_dates, load_dim_metadata
from src.load.redshift_copy_loader import vacuum_analyze

logger = logging.getLogger(__name__)

symbols = ["NVDA","AAPL","MSFT","GOOGL","AMZN"]

