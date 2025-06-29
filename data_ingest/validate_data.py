import pandas as pd
from pathlib import Path
from datetime import timedelta
import pandas_market_calendars as mcal

# Config
CHANGES_FILE = Path("data/sp500/sp500_changes_parsed.csv")
CURRENT_FILE = Path("data/sp500/sp500_current_constituents.csv")
TICKERS_FILE = Path("data/sp500/sp500_all_unique_post2017_tickers.txt")
IGNORE_FILE = Path("data/tiingo/tiingo_ignore_tickers.txt")
TIINGO_DIR = Path("data/tiingo/ohlcv/")
OVERRIDE_FILE = Path("data/tiingo/tiingo_new_date_ranges.csv")
YFINANCE_DIR = Path("data/yfinance/ohlcv/")
START_DATE_DEFAULT = pd.to_datetime("2017-01-01")
TODAY = pd.to_datetime("today").normalize()

YFINANCE_TICKERS = {"VTRS", "LIN", "TKO"}

# Load Tickers
changes_df = pd.read_csv(CHANGES_FILE, parse_dates=["Date"])
current_df = pd.read_csv(CURRENT_FILE)
current_tickers = set(current_df["Symbol"].str.strip())

with open(IGNORE_FILE, "r") as f:
    IGNORED_TICKERS = {line.strip() for line in f if line.strip()}

with open(TICKERS_FILE, "r") as f:
    TICKERS = [
        line.strip()
        for line in f
        if line.strip() and line.strip() not in IGNORED_TICKERS
    ]

# Select only Tiingo and YFinance tickers
TICKERS = sorted(set(TICKERS).union(YFINANCE_TICKERS))

# Override Date Ranges
override_df = pd.read_csv(
    OVERRIDE_FILE,
    header=None,
    names=["Ticker", "Start", "End"]
)
override_df["Start"] = pd.to_datetime(override_df["Start"])
override_df["End"] = pd.to_datetime(override_df["End"]) - timedelta(days=1)
OVERRIDE_MAP = {
    row["Ticker"].strip(): (row["Start"], row["End"])
    for _, row in override_df.iterrows()
}


# Get file path based on source
def get_file_path(ticker: str):
    if ticker in YFINANCE_TICKERS:
        return YFINANCE_DIR / f"{ticker}.csv"
    return TIINGO_DIR / f"{ticker.replace('.', '-')}.csv"


# Get date range
def get_date_range(ticker: str):
    if ticker in OVERRIDE_MAP:
        return OVERRIDE_MAP[ticker]

    adds = changes_df[changes_df["AddTicker"] == ticker]
    removes = changes_df[changes_df["RemoveTicker"] == ticker]
    default_start = START_DATE_DEFAULT

    if ticker in current_tickers:
        row = current_df[current_df["Symbol"].str.strip() == ticker]
        if not row.empty and "Date added" in row.columns:
            date_added_val = row.iloc[0]["Date added"]
            added_date = pd.to_datetime(date_added_val, errors="coerce")
            if pd.notna(added_date) and added_date > START_DATE_DEFAULT:
                default_start = added_date

    start_date = (
        max(default_start, adds["Date"].min())
        if not adds.empty else default_start
    )
    end_date = (
        TODAY if ticker in current_tickers
        else removes["Date"].max() - timedelta(days=1)
        if not removes.empty else TODAY
    )
    return start_date, end_date


# Validation logic
def is_valid_data(df, start, end, ticker):
    if "Date" in df.columns and "date" not in df.columns:
        df = df.rename(columns={"Date": "date"})
    if "date" not in df.columns:
        return False, "Missing 'date' column"

    df["date"] = pd.to_datetime(df["date"], errors="coerce")
    df = df.dropna(subset=["date"]).copy()
    if df["date"].dt.tz is not None:
        df["date"] = df["date"].dt.tz_localize(None)
    df["date"] = df["date"].dt.normalize()

    present_dates = df["date"].dt.date.unique()
    present_dates_set = set(present_dates)

    nyse = mcal.get_calendar("NYSE")
    trading_days = nyse.valid_days(start_date=start, end_date=end).date
    expected_dates_set = set(trading_days)

    coverage_ratio = (
        len(present_dates_set & expected_dates_set) / len(expected_dates_set)
    )
    start_ok = trading_days[0] in present_dates_set
    end_ok = trading_days[-1] in present_dates_set

    print(
        f"[DEBUG] {ticker} | Start OK: {start_ok}, End OK: {end_ok}, "
        f"Coverage: {coverage_ratio:.2%}"
    )

    if not start_ok or not end_ok:
        return False, "Missing bounds"
    if coverage_ratio < 0.95:
        return False, "Insufficient coverage"
    return True, "Complete"


# Validation execution
results = {"valid": [], "invalid": [], "errors": [], "missing_file": []}

for ticker in TICKERS:
    file_path = get_file_path(ticker)
    if not file_path.exists():
        print(f"[MISSING FILE] {ticker} – {file_path.name}")
        results["missing_file"].append((ticker, "Missing file"))
        continue

    try:
        df = pd.read_csv(file_path)

        if "Date" in df.columns and "date" not in df.columns:
            df = df.rename(columns={"Date": "date"})

        if df.empty or "date" not in df.columns:
            print(f"[ERROR] {ticker} CSV malformed or empty.")
            results["errors"].append((ticker, "Malformed or empty CSV"))
            continue

        start, end = get_date_range(ticker)
        is_valid, reason = is_valid_data(df, start, end, ticker)

        if is_valid:
            print(f"{ticker} data is complete and valid.")
            results["valid"].append(ticker)
        else:
            print(f"{ticker} data is incomplete – {reason}.")
            results["invalid"].append((ticker, reason))

    except Exception as e:
        print(f"[ERROR] {ticker} – {e}")
        results["errors"].append((ticker, str(e)))

# Save summary
output_rows = []

for ticker in results["valid"]:
    start, end = get_date_range(ticker)
    output_rows.append({
        "Ticker": ticker,
        "Status": "Valid",
        "Reason": "Complete",
        "StartDate": start.date(),
        "EndDate": end.date(),
        "MissingBound": ""
    })

for ticker, reason in results["invalid"]:
    start, end = get_date_range(ticker)
    output_rows.append({
        "Ticker": ticker,
        "Status": "Invalid",
        "Reason": reason,
        "StartDate": start.date(),
        "EndDate": end.date(),
        "MissingBound": ""
    })

for ticker, error in results["errors"]:
    output_rows.append({
        "Ticker": ticker,
        "Status": "Error",
        "Reason": error,
        "StartDate": None,
        "EndDate": None,
        "MissingBound": ""
    })

for ticker, msg in results["missing_file"]:
    output_rows.append({
        "Ticker": ticker,
        "Status": "Missing File",
        "Reason": msg,
        "StartDate": None,
        "EndDate": None,
        "MissingBound": ""
    })

results_df = pd.DataFrame(output_rows)
results_df.to_csv("data_ingest/validation_results.csv", index=False)
print("\nSaved all results to 'validation_results.csv'")
