import pandas as pd
from pathlib import Path

# Input files
CHANGES_FILE = Path("data/sp500/sp500_changes_parsed.csv")
CURRENT_FILE = Path("data/sp500/sp500_current_constituents.csv")

# Output
ALL_TICKERS_FILE = Path("data/sp500/sp500_all_unique_post2017_tickers.txt")

# Load datasets
changes_df = pd.read_csv(CHANGES_FILE, parse_dates=["Date"])
current_df = pd.read_csv(CURRENT_FILE)

# Filter changes to only those after Jan 1, 2016
filtered_changes = changes_df[changes_df["Date"] >= "2017-01-01"]

# Collect relevant tickers
added = filtered_changes["AddTicker"].dropna().unique().tolist()
removed = filtered_changes["RemoveTicker"].dropna().unique().tolist()
current = current_df["Symbol"].dropna().unique().tolist()

# Combine and deduplicate
tickers = sorted(set(added + removed + current))

# Save to file
ALL_TICKERS_FILE.parent.mkdir(parents=True, exist_ok=True)
with open(ALL_TICKERS_FILE, "w") as f:
    for ticker in tickers:
        f.write(f"{ticker}\n")

print(
    f"Collected {len(tickers)} unique tickers "
    "(post-2017 changes only + current)."
)
print(f"Saved -> {ALL_TICKERS_FILE}")
