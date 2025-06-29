import pandas as pd
import yfinance as yf
from pathlib import Path

# Config
tickers_dates = {
    "LIN":  ("2017-01-01", "2025-06-22"),
    "VTRS": ("2017-01-01", "2025-06-22"),
    "TKO":  ("2017-01-01", "2025-06-22"),
    "DOC":  ("2017-01-01", "2025-06-22")
}
OUTPUT_DIR = Path("data/yfinance/ohlcv")
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)


def download_and_save(ticker, start, end):
    print(f"Downloading {ticker} from {start} to {end}")
    df = yf.download(
        ticker,
        start=start,
        end=end,
        auto_adjust=False,
        actions=True,
        group_by="ticker"
    )

    if df.empty:
        print(f"No data found for {ticker}")
        return

    # Flatten MultiIndex
    if isinstance(df.columns, pd.MultiIndex):
        df.columns = df.columns.get_level_values(1)

    # Rename to lower-case
    df.rename(columns={
        "Open": "open",
        "High": "high",
        "Low": "low",
        "Close": "close",
        "Adj Close": "adjClose",
        "Volume": "volume"
    }, inplace=True)

    # UTC date
    df.index = (
        df.index.tz_localize("UTC")
        .strftime("%Y-%m-%dT%H:%M:%S.000Z")
    )
    df.index.name = "date"
    df = df.reset_index()

    # Placeholder adjusted OHLCV
    df["adjHigh"] = df["high"]
    df["adjLow"] = df["low"]
    df["adjOpen"] = df["open"]
    df["adjVolume"] = df["volume"]

    # Dividends & splits
    yt = yf.Ticker(ticker)
    dividends = yt.dividends
    splits = yt.splits

    df["divCash"] = df["date"].map(dividends.to_dict()).fillna(0.0)
    df["splitFactor"] = df["date"].map(splits.to_dict()).fillna(1.0)

    # Column order
    df = df[[
        "date", "close", "high", "low", "open", "volume",
        "adjClose", "adjHigh", "adjLow", "adjOpen", "adjVolume",
        "divCash", "splitFactor"
    ]]

    out_file = OUTPUT_DIR / f"{ticker}.csv"
    df.to_csv(out_file, index=False)
    print(f"Saved {ticker} to {out_file}")


# Main loop
for ticker, (start, end) in tickers_dates.items():
    download_and_save(ticker, start, end)
