import requests
import os
import json
from datetime import datetime
import time
import logging
from dotenv import load_dotenv

load_dotenv()   # load environment variables from .env file

API_KEY = os.getenv('ALPHAVANTAGE_API_KEY')

def fetch_and_store(symbol):
    url = "https://www.alphavantage.co/query"

    params = {
        "function": "TIME_SERIES_DAILY",
        "symbol": symbol,
        "apikey": API_KEY,
        "outputsize": "compact"
    }

    response = requests.get(url, params=params)
    data = response.json()

    # add minute-wise rate limit handling
    if "Note" in data:
        logging.warning(f"API rate limit reached for {symbol}. Waiting 60 seconds...")
        time.sleep(60)
        return fetch_and_store(symbol)

    # Daily quota exceeded (free tier: 25 req/day). "Information" key is returned
    # instead of "Note" when the per-day limit is hit — this is distinct from the
    # per-minute rate limit and cannot be resolved by waiting.
    if "Information" in data:
        raise RuntimeError(f"AlphaVantage daily quota exceeded for {symbol}: {data['Information']}")

    # Check for other error messages
    if "Error Message" in data:
        logging.error(f"API error for {symbol}: {data['Error Message']}")
        time.sleep(60)
        return fetch_and_store(symbol)
    
    timestamp = datetime.now(datetime.timezone.utc).strftime("%Y%m%d_%H%M%S")

    # create the new blank json file
    filepath = f"data/raw/{symbol}_{timestamp}.json"

    # write the extracted data to the json file
    with open(filepath, "w") as f:
        json.dump(data,f)
    
    return filepath