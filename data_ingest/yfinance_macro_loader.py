import yfinance as yf
from pathlib import Path

macro_tickers = {
    "DXY": "DX-Y.NYB",  # US Dollar Index
    "VIX": "^VIX",  # Volatility Index
    "OIL": "CL=F",  # Crude Oil Futures
    "GOLD": "GC=F",  # Gold Futures
    "10Y": "^TNX",  # 10-Year Treasury Yield
    "5Y": "^FVX",  # 5-Year Treasury Yield
    "3M": "^IRX"  # 3-Month Treasury Yield
}

OUT_DIR = Path("data/yfinance/macro/")
OUT_DIR.mkdir(parents=True, exist_ok=True)

for label, yf_ticker in macro_tickers.items():
    df = yf.download(yf_ticker, start="2016-01-01", progress=False)
    df.to_csv(OUT_DIR / f"{label}.csv")
    print(f"Saved {label} -> {OUT_DIR / f'{label}.csv'}")
