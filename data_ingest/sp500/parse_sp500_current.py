import pandas as pd
from pathlib import Path

INPUT_FILE = Path("data/sp500/sp500_current.txt")
OUTPUT_TABLE_FILE = Path("data/sp500/sp500_current_constituents.csv")
OUTPUT_TICKERS_FILE = Path("data/sp500/sp500_constituents_list.txt")

# === Load table ===
df = pd.read_csv(INPUT_FILE, sep="\t", engine="python")
df["Symbol"] = df["Symbol"].str.strip()
df = df.dropna(subset=["Symbol"])

# === Save full table as CSV ===
OUTPUT_TABLE_FILE.parent.mkdir(parents=True, exist_ok=True)
df.to_csv(OUTPUT_TABLE_FILE, index=False)

# === Save ticker list only ===
tickers = df["Symbol"].tolist()
with open(OUTPUT_TICKERS_FILE, "w") as f:
    for tkr in tickers:
        f.write(f"{tkr}\n")

print(df.head())
print(f"Saved full table -> {OUTPUT_TABLE_FILE}")
print(f"Saved {len(tickers)} tickers to -> {OUTPUT_TICKERS_FILE}")
