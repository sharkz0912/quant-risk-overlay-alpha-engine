import pandas as pd
from tqdm import tqdm
from pathlib import Path

# Config
HOLDINGS_FILE = Path("data_ingest/index_holdings_xlc.csv")
TIINGO_DIR = Path("data/tiingo/ohlcv/")
YFINANCE_DIR = Path("data/yfinance/ohlcv/")
OUTPUT_FILE_RAW = Path("data_ingest/xlc_backfilled_raw.csv")
OUTPUT_FILE_SCALED = Path("data_ingest/xlc_backfilled_scaled.csv")

EXCLUDED_TICKERS = {"FOX", "FOXA"}
XLC_START_DATE = pd.to_datetime("2017-01-01")
XLC_END_DATE = pd.to_datetime("2018-06-18")

REAL_XLC_ADJCLOSE_ON_619 = 46.898736712
BACKFILLED_ADJCLOSE_ON_618 = 120.52373226637262

# Load and rebalance weights
df = pd.read_csv(HOLDINGS_FILE, skiprows=1)
df["Symbol"] = df["Symbol"].str.strip()
df["Index Weight"] = (
    df["Index Weight"]
    .str.replace('%', '')
    .astype(float) / 100.0
)

df = df[~df["Symbol"].isin(EXCLUDED_TICKERS)].copy()
df["Rebalanced Weight"] = df["Index Weight"] / df["Index Weight"].sum()
weights = dict(zip(df["Symbol"], df["Rebalanced Weight"]))

# XLC columns
columns = [
    "close", "high", "low", "open", "volume",
    "adjClose", "adjHigh", "adjLow", "adjOpen", "adjVolume",
    "divCash", "splitFactor"
]

# Load stock data
stock_data = {}
for ticker in tqdm(weights.keys(), desc="Loading stock data"):
    file_path = (
        YFINANCE_DIR / f"{ticker}.csv"
        if ticker == "TKO"
        else TIINGO_DIR / f"{ticker}.csv"
    )

    if not file_path.exists():
        print(f"[WARNING] Missing file for {ticker}: {file_path}")
        continue

    df_stock = pd.read_csv(file_path, parse_dates=["date"])
    df_stock["date"] = pd.to_datetime(df_stock["date"]).dt.tz_localize(None)
    df_stock = df_stock[
        df_stock["date"].between(XLC_START_DATE, XLC_END_DATE)
    ].copy()
    df_stock.set_index("date", inplace=True)
    stock_data[ticker] = df_stock

# Master date range
all_dates = pd.date_range(start=XLC_START_DATE, end=XLC_END_DATE, freq="B")

# Compute raw backfill
output_rows = []
for date in tqdm(all_dates, desc="Computing XLC composite"):
    row = {"date": date}
    for col in columns:
        val = 0
        total_weight = 0
        for ticker, weight in weights.items():
            df = stock_data.get(ticker)
            if df is not None and date in df.index and col in df.columns:
                try:
                    value = pd.to_numeric(df.at[date, col], errors='coerce')
                    if pd.notna(value):
                        val += value * weight
                        total_weight += weight
                except Exception as e:
                    print(
                        f"[ERROR] {ticker} on {date} for column '{col}': {e}"
                    )
        row[col] = val if total_weight > 0 else None
    output_rows.append(row)

# Save raw backfilled data
xlc_df = pd.DataFrame(output_rows)
xlc_df = xlc_df.dropna(subset=["close", "open", "high", "low", "volume"])
xlc_df.to_csv(OUTPUT_FILE_RAW, index=False)
print(f"Raw backfilled data saved to {OUTPUT_FILE_RAW}")

# Apply scaling to price columns
scaling_factor = REAL_XLC_ADJCLOSE_ON_619 / BACKFILLED_ADJCLOSE_ON_618
print(f"Scaling factor = {scaling_factor:.6f}")

price_columns = [
    "close", "high", "low", "open",
    "adjClose", "adjHigh", "adjLow", "adjOpen"
]

xlc_df[price_columns] = xlc_df[price_columns].multiply(scaling_factor)
xlc_df[price_columns] = xlc_df[price_columns].round(6)

# Save scaled final data
xlc_df.to_csv(OUTPUT_FILE_SCALED, index=False)
print(f"Scaled backfilled XLC data saved to {OUTPUT_FILE_SCALED}")

# Merge with actual XLC data
XLC_REAL_FILE = Path("data/tiingo/ohlcv/XLC.csv")  # Update path if different
XLC_MERGED_OUTPUT = Path("data/tiingo/ohlcv/XLC.csv")

# Load real XLC data and ensure date parsing
real_xlc_df = pd.read_csv(XLC_REAL_FILE, parse_dates=["date"])
real_xlc_df["date"] = pd.to_datetime(real_xlc_df["date"]).dt.tz_localize(None)

# Load backfilled scaled data
backfilled_df = pd.read_csv(OUTPUT_FILE_SCALED, parse_dates=["date"])
backfilled_df["date"] = pd.to_datetime(
    backfilled_df["date"]
).dt.tz_localize(None)

# Filter real data to exclude any overlap with backfilled data
real_xlc_df = real_xlc_df[real_xlc_df["date"] > XLC_END_DATE]

# Combine backfilled and real data
merged_df = pd.concat([backfilled_df, real_xlc_df], ignore_index=True)

# Save merged dataset
merged_df.to_csv(XLC_MERGED_OUTPUT, index=False)
print(f"Full XLC dataset saved to {XLC_MERGED_OUTPUT}")
