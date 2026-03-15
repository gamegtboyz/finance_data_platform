import json
import pandas as pd

def transform_stock_prices(filepath, symbol):

    # read the extracted JSON file
    with open (filepath, "r") as f:
        data = json.load(f)

    # get the dictionary values (the daily OHLCV data) from the "Time Series (Daily)" key in the JSON data >> refer to API documentation for other APIs
    time_series = data.get("Time Series (Daily)", {}) # {} means if the key is not found, it will return an empty dictionary instead of throwing an error

    records = []

    for date, values in time_series.items():
        dt = pd.to_datetime(date)
        records.append({
            "symbol": symbol,   # pass the argument symbol to the function and include it in the records
            "date": dt,       # convert the date string to a datetime object
            "open": float(values["1. open"]),
            "high": float(values["2. high"]),
            "low": float(values["3. low"]),
            "close": float(values["4. close"]),
            "volume": int(values["5. volume"]),
            "day" : dt.day,
            "month" : dt.month,
            "year" : dt.year,
            "quarter" : ((dt.month - 1) // 3) + 1, # ensure the quarter is classified correctly
            "day_of_week" : dt.dayofweek,   # Monday=0, Sunday=6
            "week_of_year" : int(dt.isocalendar().week)   # typecasting to int to avoid pandas Int64 type which is not compatible with psycopg2 when loading into the database
        })

    df = pd.DataFrame(records)

    if df.empty:
        return df

    df["date"] = pd.to_datetime(df["date"])

    return df

def transform_company_metadata(filepath):
    with open(filepath, "r") as f:
        data = json.load(f)

    if "Information" in data or "Note" in data:
        raise ValueError(f"Metadata file contains an API quota/rate limit response, not real data. Re-fetch the metadata file.")

    symbol = data.get("Symbol", "")
    if not symbol:
        raise ValueError(f"Metadata file is missing 'Symbol' field. The file may contain an API error response.")

    metadata = {
        "symbol": symbol,
        "company_name": data.get("Name", ""),
        "sector": data.get("Sector", "")
    }

    return metadata