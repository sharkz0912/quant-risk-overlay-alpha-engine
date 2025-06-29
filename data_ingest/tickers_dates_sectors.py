import pandas as pd
from pathlib import Path

# Paths
CUR_CONSTIT = Path("data/sp500/sp500_current_constituents.csv")
VALID_RESULTS = Path("data_ingest/validation_results.csv")
OUT_FILE = Path("data_ingest/final_tickers_dates_sectors.csv")

# Sector ETF dictionary
ETF_MAP = {
    "Communication Services": "XLC",
    "Consumer Discretionary": "XLY",
    "Consumer Staples": "XLP",
    "Energy": "XLE",
    "Financials": "XLF",
    "Health Care": "XLV",
    "Industrials": "XLI",
    "Information Technology": "XLK",
    "Materials": "XLB",
    "Real Estate": "XLRE",
    "Utilities": "XLU"
}

# Load current S&P-500 constituents & keep sector info
cur = (
    pd.read_csv(CUR_CONSTIT, usecols=["Symbol", "GICS Sector"])
    .rename(columns={"Symbol": "Ticker", "GICS Sector": "Sector"})
)

# Load validation results
base = pd.read_csv(VALID_RESULTS)

# Attach sector & map to ETF (left join)
final = (
    base.merge(cur, on="Ticker", how="left")
    .assign(
        SectorETF=lambda d: d["Sector"]
        .map(ETF_MAP)
        .fillna("")
    )
)

blanks = final.loc[final["SectorETF"] == "", "Ticker"]
print(
    f"{len(blanks)} tickers have no SectorETF "
    "(no longer in current S&P 500)."
)

# Save final results
final[["Ticker", "StartDate", "EndDate", "SectorETF"]].to_csv(
    OUT_FILE,
    index=False
)
print(f"File written: {OUT_FILE}")

# Show blanks
blanks = final.loc[final["SectorETF"] == "", "Ticker"]

print(
    f"{len(blanks)} tickers have no SectorETF "
    "(no longer in current S&P 500)."
)

# List tickers
if len(blanks):
    print("Tickers missing SectorETF:")
    for t in sorted(blanks):
        print("  •", t)
else:
    print("Great! No blanks")
