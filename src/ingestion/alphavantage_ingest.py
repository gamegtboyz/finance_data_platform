import requests
import os
import json
from datetime import datetime
import time
import logging
import pathlib
from dotenv import load_dotenv

load_dotenv()   # load environment variables from .env file

API_KEY = os.getenv('ALPHAVANTAGE_API_KEY')

logger = logging.getLogger(__name__)

RAW_DATA_DIR = pathlib.Path(__file__).parent.parent.parent / "data" / "raw"

def fetch_stock_prices(symbol):
    url = "https://www.alphavantage.co/query"

    params = {
        "function": "TIME_SERIES_DAILY",
        "symbol": symbol,
        "apikey": API_KEY,
        "outputsize": "compact"
    }

    response = requests.get(url, params=params, timeout=60)
    response.raise_for_status()
    data = response.json()

    # Daily quota exceeded (free tier: 25 req/day). "Information" key is returned
    # instead of "Note" when the per-day limit is hit — this is distinct from the
    # per-minute rate limit and cannot be resolved by waiting.
    if "Information" in data:
        raise RuntimeError(f"AlphaVantage daily quota exceeded for {symbol}: {data['Information']}")

    # Per-minute rate limit: retry up to 3 times with 60-second backoff.
    if "Note" in data:
        for attempt in range(1, 4):
            logger.warning(f"API rate limit reached for {symbol}. Attempt {attempt}/3 — waiting 60 seconds...")
            time.sleep(60)
            retry_response = requests.get(url, params=params, timeout=60)
            retry_response.raise_for_status()
            retry_data = retry_response.json()
            if "Note" not in retry_data:
                data = retry_data
                break
        else:
            raise RuntimeError(f"API rate limit for {symbol} persisted after 3 retry attempts")

    # Check for other error messages
    if "Error Message" in data:
        logger.error(f"API error for {symbol}: {data['Error Message']}")
        raise RuntimeError(f"AlphaVantage API error for {symbol}: {data['Error Message']}")

    timestamp = datetime.utcnow().strftime("%Y-%m-%d")

    filepath = RAW_DATA_DIR / symbol / f"{symbol}_{timestamp}.json"
    filepath.parent.mkdir(parents=True, exist_ok=True)

    with open(filepath, "w") as f:
        json.dump(data, f)

    return str(filepath)

def fetch_company_metadata(symbol):
    url = "https://www.alphavantage.co/query"

    params = {
        "function": "OVERVIEW",
        "symbol": symbol,
        "apikey": API_KEY
    }

    response = requests.get(url, params=params, timeout=60)
    response.raise_for_status()
    data = response.json()

    if "Information" in data:
        raise RuntimeError(f"AlphaVantage daily quota exceeded for {symbol} metadata: {data['Information']}")

    if "Note" in data:
        for attempt in range(1, 4):
            logger.warning(f"API rate limit reached fetching metadata for {symbol}. Attempt {attempt}/3 — waiting 60 seconds...")
            time.sleep(60)
            retry_response = requests.get(url, params=params, timeout=60)
            retry_response.raise_for_status()
            retry_data = retry_response.json()
            if "Note" not in retry_data:
                data = retry_data
                break
        else:
            raise RuntimeError(f"API rate limit for {symbol} metadata persisted after 3 retry attempts")

    if "Error Message" in data:
        raise RuntimeError(f"AlphaVantage API error for {symbol} metadata: {data['Error Message']}")

    filepath = RAW_DATA_DIR / symbol / f"{symbol}_metadata.json"
    filepath.parent.mkdir(parents=True, exist_ok=True)

    with open(filepath, "w") as f:
        json.dump(data, f)

    return str(filepath)