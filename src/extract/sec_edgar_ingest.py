import requests
import os
import json
from datetime import datetime, timezone
import time
import logging
import pathlib
from dotenv import load_dotenv
from storage.s3_client import s3_upload

load_dotenv()

logger = logging.getLogger(__name__)

EDGAR_HEADERS = {"User-Agent": os.getenv("EDGAR_USER_AGENT")}

RAW_DATA_DIR = pathlib.Path(os.getenv("RAW_DATA_DIR", str(pathlib.Path(__file__).parent.parent.parent / "data" / "raw")))

METRICS = [
    "Revenues",
    "NetIncomeLoss",
    "EarningsPerShareDiluted",
    "LongTermDebt",
    "Assets"
]

def _get_cik(symbol: str) -> str:
    """Get CIK (Central Index Key) for a given stock symbol."""
    url = "https://www.sec.gov/files/company_tickers.json"
    response = requests.get(url, headers=EDGAR_HEADERS, timeout=30)
    response.raise_for_status()
    tickers = response.json()
    for entry in tickers.values():
        if entry["ticker"].upper() == symbol.upper():
            return str(entry["cik_str"]).zfill(10)
    raise ValueError(f"CIK not found for symbol: {symbol}")

def fetch_company_facts(symbol: str):
    """Fetch all XBRL financial facts for a symbol, save locally and to S3."""
    cik = _get_cik(symbol)
    time.sleep(0.15)

    url = f"https://data.sec.gov/api/xbrl/companyfacts/CIK{cik}.json"
    response = requests.get(url, headers=EDGAR_HEADERS, timeout=60)
    response.raise_for_status()
    data = response.json()

    timestamp = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    filepath = RAW_DATA_DIR / symbol / f"{symbol}_fundamentals_{timestamp}.json"
    filepath.parent.mkdir(parents=True, exist_ok=True)

    with open(filepath, "w") as f:
        json.dump(data, f)

    try:
        s3_upload(str(filepath), os.getenv("S3_BUCKET_NAME"), f"{symbol}/{filepath.name}")
    except Exception as e:
        logger.warning(f"S3 upload failed for {symbol} fundamentals: {e}")

    return str(filepath)