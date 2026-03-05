import pandas as pd

def transform_stock_data(raw_data, symbol):
    time_series = raw_data.get("Time Series (Daily)", {}) # {} means if the key is not found, it will return an empty dictionary instead of throwing an error

    records = []

    for date, values in time_series.items():
        records.append({
            "symbol": symbol,   # pass the argument symbol to the function and include it in the records
            "date": date,       # convert the date string to a datetime object
            "open": float(values["1. open"]),
            "high": float(values["2. high"]),
            "low": float(values["3. low"]),
            "close": float(values["4. close"]),
            "volume": int(values["5. volume"])
        })

    df = pd.DataFrame(records)
    df["date"] = pd.to_datetime(df["date"])

    return df