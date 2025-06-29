import time
import json
import requests
import pandas as pd
from pathlib import Path

# Load Tiingo API key from config file
with open("config/credentials.json", "r") as f:
    api_key = json.load(f)["TIINGO_API_KEY"]

# Input/output paths
INPUT_FILE = "data/tiingo/tiingo_all_tickers.txt"
OHLCV_DIR = Path("data/tiingo/ohlcv/")
OHLCV_DIR.mkdir(parents=True, exist_ok=True)

# Load tickers
tickers = [line.strip() for line in open(INPUT_FILE) if line.strip()]

# Set headers for Tiingo
headers = {"Authorization": f"Token {api_key}"}


# Replace '.' with '-' for Tiingo compatibility
def tiingo_safe_ticker(tkr):
    return tkr.replace('.', '-')


# Loop through tickers and fetch OHLCV
for i, tkr in enumerate(tickers):
    try:
        safe_tkr = tiingo_safe_ticker(tkr)
        ohlcv_url = (
            f"https://api.tiingo.com/tiingo/daily/{safe_tkr}/prices"
            f"?startDate=2017-01-01"
        )
        ohlcv_resp = requests.get(ohlcv_url, headers=headers)

        if ohlcv_resp.ok:
            df = pd.DataFrame(ohlcv_resp.json())
            df.to_csv(OHLCV_DIR / f"{safe_tkr}.csv", index=False)
            print(
                f"[{i+1}/{len(tickers)}] OHLCV -> {safe_tkr} | Rows: {len(df)}"
            )
        else:
            print(
                f"[{i+1}] OHLCV Error: {safe_tkr} - "
                f"{ohlcv_resp.status_code}"
            )

        time.sleep(1)

    except Exception as e:
        print(f"[{i+1}] Failed: {safe_tkr} - {e}")
