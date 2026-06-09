import json
import pandas as pd

# Only extract these metrics to avoids hundreds of irrelevant XBRL tags)
TARGET_METRICS = {
    "Revenues": "revenue",
    "NetIncomeLoss": "net_income",
    "EarningsPerShareDiluted": "eps_diluted",
    "LongTermDebt": "long_term_debt",
    "Assets": "total_assets"
}

def transform_company_facts(filepath: str, symbol: str) -> pd.DataFrame:
    with open(filepath, 'r') as f:
        data = json.load(f)

    us_gaap = data.get("facts", {}).get("us-gaap", {})
    records = []

    for xbrl_concept, metric_name in TARGET_METRICS.items():
        concept_data = us_gaap.get(xbrl_concept, {})

        # EDGAR stores values grouped by unit (e.g. "USD", "USD/shares")
        for unit_type, filings in concept_data.get("units", {}).items():
            for filing in filings:
                form = filing.get("form", "")
                # only keep annual (10-K) and uarterly (10-Q) filings
                if form not in ("10-K", "10-Q"):
                    continue
                records.append({
                    "symbol": symbol,
                    "period_end_date": pd.to_datetime(filing["end"]),
                    "form_type": form,
                    "metric": metric_name,
                    "value": float(filing["val"]),
                    "unit": unit_type,
                    "filed_date": pd.to_datetime(filing.get("filed"))
                })

    df = pd.DataFrame(records)

    if df.empty:
        return df
    
    # Keep only the most recent filing per (symbol, period_end_date, metric)
    # – companies sometimes amend filings, so deduplicate by taking latest filed_date
    df = (df.sort_values("filed_date", ascending=False).drop_duplicates(subset=["symbol", "period_end_date", "metric"]).reset_index(drop=True))

    return df