import pandas as pd
from pathlib import Path
from db_connect import db_connect
import logging
from sqlalchemy import create_engine
from dotenv import load_dotenv
import os

load_dotenv()

logger = logging.getLogger(__name__)

engine = create_engine(
    f"postgresql+psycopg2://{os.getenv("DB_USER")}:{os.getenv("DB_PASSWORD")}@{os.getenv("DB_HOST")}:{os.getenv("DB_PORT")}/{os.getenv("DB_NAME")}"
)

QUERIES = {
    "sma": Path("sql/sma.sql"),
    "daily_returns": Path("sql/daily_returns.sql"),
    "volatility": Path("sql/volatility.sql"),
    "cumulative_return": Path("sql/cumulative_return.sql")
}

OUTPUT_DIR = Path("data/analytics")
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

conn = db_connect()

for name, sql_path in QUERIES.items():
    query = sql_path.read_text()
    df = pd.read_sql_query(query, conn)
    output = OUTPUT_DIR / f"{name}.csv"
    df.to_csv(output, index=False)
    logger.info(f"Exported {name} to {output}")

engine.dispose()