import json
import pandas as pd

def transform_fred_series(filepath: str, series_id: str) -> pd.DataFrame:
    with open(filepath, "r") as f:
        data = json.load(f)

    observations = data.get("observations", [])
    records = []
    for obs in observations:
        if obs["value"] == ".":
            continue
        dt = pd.to_datetime(obs["date"])
        records.append({
            "series_id": series_id,
            "date": dt,
            "value": float(obs["value"])
        })

        df = pd.DataFrame(records)
        if not df.empty:
            df["date"] = pd.to_datetime(df["date"])
        
        return df