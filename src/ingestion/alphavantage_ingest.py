import requests
import os
import json
from datetime import datetime
import time
import logging
from dotenv import load_dotenv

load_dotenv()   # load environment variables from .env file

API_KEY = os.getenv('ALPHAVANTAGE_API_KEY')

def fetch_stock_prices(symbol):
    url = "https://www.alphavantage.co/query"

    params = {
        "function": "TIME_SERIES_DAILY",
        "symbol": symbol,
        "apikey": API_KEY,
        "outputsize": "compact"
    }

    response = requests.get(url, params=params, timeout=60)
    data = response.json()
    
    # Daily quota exceeded (free tier: 25 req/day). "Information" key is returned
    # instead of "Note" when the per-day limit is hit — this is distinct from the
    # per-minute rate limit and cannot be resolved by waiting.
    if "Information" in data:
        raise RuntimeError(f"AlphaVantage daily quota exceeded for {symbol}: {data['Information']}")
    
    # add minute-wise rate limit handling
    if "Note" in data:
        logging.warning(f"API rate limit reached for {symbol}. Waiting 60 seconds...")
        attempt = 0
        while attempt < 3:
            time.sleep(12)
            attempt += 1
            return fetch_stock_prices(symbol)
        raise RuntimeError(f"API rate limit reached for {symbol} after 3 attempts")

    # Check for other error messages
    if "Error Message" in data:
        logging.error(f"API error for {symbol}: {data['Error Message']}")
        raise RuntimeError(f"AlphaVantage API error for {symbol}: {data['Error Message']}")
    
    timestamp = datetime.utcnow().strftime("%Y-%m-%d")

    # create the new blank json file
    filepath = f"data/raw/{symbol}/{symbol}_{timestamp}.json"

    # create the directory if it doesn't exist
    os.makedirs(os.path.dirname(filepath), exist_ok=True)

    # write the extracted data to the json file
    with open(filepath, "w") as f:
        json.dump(data,f)
    
    return filepath

def fetch_company_metadata(symbol):
    url = "https://www.alphavantage.co/query"

    params = {
        "function": "OVERVIEW",
        "symbol": symbol,
        "apikey": API_KEY
    }

    response = requests.get(url, params=params, timeout=60)
    data = response.json()

    if "Information" in data:
        raise RuntimeError(f"AlphaVantage daily quota exceeded for {symbol} metadata: {data['Information']}")

    if "Note" in data:
        logging.warning(f"API rate limit reached fetching metadata for {symbol}. Waiting 60 seconds...")
        time.sleep(60)
        return fetch_company_metadata(symbol)

    if "Error Message" in data:
        raise RuntimeError(f"AlphaVantage API error for {symbol} metadata: {data['Error Message']}")

    filepath = f"data/raw/{symbol}/{symbol}_metadata.json"

    os.makedirs(os.path.dirname(filepath), exist_ok=True)

    with open(filepath, "w") as f:
        json.dump(data, f)
    
    return filepath