import requests
import os
import time
import logging
from dotenv import load_dotenv

load_dotenv()   # load environment variables from .env file

API_KEY = os.getenv('ALPHAVANTAGE_API_KEY')

def fetch_stock_data(symbol):
    url = "https://www.alphavantage.co/query"

    params = {
        "function": "TIME_SERIES_DAILY",
        "symbol": symbol,
        "apikey": API_KEY,
        "outputsize": "compact"
    }

    response = requests.get(url, params=params)
    data = response.json()

    # add rate limit handling
    if "Note" in data:
        logging.warning(f"API rate limit reached for {symbol}. Waiting 60 seconds...")
        time.sleep(60)
        return fetch_stock_data(symbol)
    
    # Check for error messages
    if "Error Message" in data:
        logging.error(f"API error for {symbol}: {data['Error Message']}")
        time.sleep(60)
        return fetch_stock_data(symbol)

    return data