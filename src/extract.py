import requests
import os
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

    return data