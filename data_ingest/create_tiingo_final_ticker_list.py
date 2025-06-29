from pathlib import Path

INPUT_FILE = Path("data/sp500/sp500_all_unique_post2017_tickers.txt")
OUTPUT_FILE = Path("data/tiingo/tiingo_all_tickers.txt")
sector_etfs = ["XLC", "XLY", "XLP", "XLE", "XLF", "XLV",
               "XLI", "XLB", "XLRE", "XLK", "XLU"]

# Read and merge
tickers = INPUT_FILE.read_text().splitlines()
all_tickers = sorted(set(tickers + sector_etfs))

# Write to output
OUTPUT_FILE.parent.mkdir(parents=True, exist_ok=True)
with open(OUTPUT_FILE, "w") as f:
    for t in all_tickers:
        f.write(f"{t}\n")

print(f"Saved {len(all_tickers)} tickers to {OUTPUT_FILE}")
