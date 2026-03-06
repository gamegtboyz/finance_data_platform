import pandas as pd

def transform_stock_data(raw_data, symbol):
    # Check for API errors from extraction phase first.
    if "Error Message" in raw_data:
        raise ValueError(f"API Error for {symbol}: {raw_data['Error Message']}")
    
    if "Note" in raw_data:
        raise ValueError(f"API Rate Limit for {symbol}: {raw_data['Note']}")
    
    if "Information" in raw_data:
        raise ValueError(f"API Daily Quota Exceeded for {symbol}: {raw_data['Information']}")
    
    # Extract the data from the extracted JSON
    time_series = raw_data.get("Time Series (Daily)", {}) # {} means if the key is not found, it will return an empty dictionary instead of throwing an error

    if not time_series:
        raise ValueError(f"No data received for symbol {symbol}. API may have returned an error or rate limit.")

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
    if "date" not in df.columns:
        raise ValueError(f"'date' column not found in transformed data for {symbol}")
    
    df["date"] = pd.to_datetime(df["date"])

    return df