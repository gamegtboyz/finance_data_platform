import requests
import os
import json
from datetime import datetime, timezone
import time
import logging
import pathlib
from dotenv import load_dotenv
from storage.s3_client import s3_upload

load_dotenv()   # load environment variables from .env file

FRED_API_KEY = os.getenv('FRED_API_KEY')
logger = logging.getLogger(__name__)
RAW_DATA_DIR = pathlib.Path(os.getenv("RAW_DATA_DIR", str(pathlib.Path(__file__).parent.parent.parent / "data" / "raw")))
SERIES = ["FEDFUNDS", "CPIAUCSL", "T10Y2Y", "UNRATE"]

def fetch_fred_series(series_id: str) -> str:
    """Fetch all observations for a FRED series, write to disk, upload to S3."""
    url = "https://api.stlouisfed.org/fred/series/observations"
    params = {
        "series_id": series_id,
        "api_key": FRED_API_KEY,
        "file_type": "json",
        "observation_start": "2020-01-01"
    }

    for attempt in range(1, 4):
        response = requests.get(url, params=params, timeout=60)
        if response.status_code == 429:
            logger.warning(f"FRED rate limit for {series_id}. Attempt {attempt}/3 — waiting 30s...")
            time.sleep(30)
            continue
        response.raise_for_status()
        data = response.json()
        break
    else:
        raise RuntimeError(f"FRED rate limit persisted for {series_id}")
    
    timestamp = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    filepath = RAW_DATA_DIR / "fred" / f"{series_id}_{timestamp}.json"
    filepath.parent.mkdir(parents=True, exist_ok=True)

    with open(filepath, "w") as f:
        json.dump(data, f)

    try:
        s3_upload(str(filepath), os.getenv("S3_BUCKET_NAME"), f"fred/{filepath.name}")
    except Exception as e:
        logger.warning(f"S3 upload failed for {series_id}: {e}")

    return str(filepath)