import json
import pandas as pd

def transform(filepath, symbol):

    # read the extracted JSON file
    with open (filepath, "r") as f:
        data = json.load(f)

    # get the dictionary values (the daily OHLCV data) from the "Time Series (Daily)" key in the JSON data >> refer to API documentation for other APIs
    time_series = data.get("Time Series (Daily)", {}) # {} means if the key is not found, it will return an empty dictionary instead of throwing an error

    records = []

    for date, values in time_series.items():
        records.append({
            "symbol": symbol,   # pass the argument symbol to the function and include it in the records
            "company_name": "Company name for " + symbol,
            "date": date,       # convert the date string to a datetime object
            "open": float(values["1. open"]),
            "high": float(values["2. high"]),
            "low": float(values["3. low"]),
            "close": float(values["4. close"]),
            "volume": int(values["5. volume"]),
            "day" : pd.to_datetime(date).day,
            "month" : pd.to_datetime(date).month,
            "year" : pd.to_datetime(date).year,
            "quarter" : ((pd.to_datetime(date).month - 1) // 3) + 1 # ensure the quarter is classified correctly
        })

    df = pd.DataFrame(records)
    df["date"] = pd.to_datetime(df["date"])

    return df