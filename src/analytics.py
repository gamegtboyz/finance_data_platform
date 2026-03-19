import pandas as pd
from pathlib import Path
from db_connect import db_connect
import logging
from sqlalchemy import create_engine
from dotenv import load_dotenv
import os

load_dotenv()

logger = logging.getLogger(__name__)

# create SQLAlchemy engine for database connection using connection string
engine = create_engine(
    f"postgresql+psycopg2://{os.getenv('DB_USER')}:{os.getenv('DB_PASSWORD')}@{os.getenv('DB_HOST')}:{os.getenv('DB_PORT')}/{os.getenv('DB_NAME')}"
)

# lookup the queries from /sql
QUERIES = {
    "sma": Path("sql/sma.sql"),
    "daily_returns": Path("sql/daily_returns.sql"),
    "volatility": Path("sql/volatility.sql"),
    "cumulative_return": Path("sql/cumulative_return.sql")
}

# create output directory to save the analytics results
OUTPUT_DIR = Path("data/analytics")
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

# connect to the database and execute each query, saving the results as CSV files
conn = db_connect()

# execute each query and save the results to CSV files
for name, sql_path in QUERIES.items():
    query = sql_path.read_text()
    df = pd.read_sql_query(query, conn)
    output = OUTPUT_DIR / f"{name}.csv"
    df.to_csv(output, index=False)
    logger.info(f"Exported {name} to {output}")

# close the database connection
engine.dispose()