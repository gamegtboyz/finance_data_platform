import requests
import os
import json
from datetime import datetime, timezone
import time
import logging
import pathlib
from dotenv import load_dotenv
from storage.s3_client import s3_upload

# load the environment variables from .env files
load_dotenv()

NEWS_API_KEY = os.getenv('NEWS_API_KEY')
logger = logging.getLogger(__name__)
RAW_DATA_DIR = pathlib.Path(os.getenv("RAW_DATA_DIR", str(pathlib.Path(__file__).parent.parent.parent / "data" / "raw")))

def fetch_news_headlines(symbol: str) -> str:
    """Fetch recent news headlines for a symbol, write the raw data to disk, and upload to S3 bucket."""
    url = "https://newsapi.org/v2/everything"

    params = {
        "apiKey": NEWS_API_KEY,
        "q": symbol,
        "language": "en",
        "sortBy": "publishedAt"
    }

    for attempt in range(1,4):
        response = requests.get(url, params=params, timeout=60)
        if response.status_code == 429:
            logger.warning(f"NewsAPI rate limit for {symbol}. Attempt {attempt} / 3 — waiting for 30s...")
            time.sleep(30)
            continue
        response.raise_for_status()
        data = response.json()
        break
    else:
        raise RuntimeError(f"NewAPI rate limit persisted for {symbol}")
    
    timestamp = datetime.now(timezone.utc).strftime("%Y-%m-%d")

    filepath = RAW_DATA_DIR / symbol / f"{symbol}_{timestamp}.json"
    filepath.parent.mkdir(parents=True, exist_ok=True)

    with open(filepath, "w") as f:
        json.dump(data, f)

    # upload the file into S3 bucket
    try:
        s3_upload(str(filepath), os.getenv('S3_BUCKET_NAME'), f"{symbol}/{filepath.name}")
    except Exception as e:
        logger.warning(f"Failed to upload news response to S3 for {symbol}: {str(e)}")

    return str(filepath)