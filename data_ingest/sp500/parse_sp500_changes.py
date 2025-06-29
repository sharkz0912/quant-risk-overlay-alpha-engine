import re
import pandas as pd
from pathlib import Path

INPUT_FILE = Path("data/sp500/sp500_changes.txt")
OUTPUT_FILE = Path("data/sp500/sp500_changes_parsed.csv")

# Load the S&P 500 changes history text file
with open(INPUT_FILE, "r", encoding="utf-8") as f:
    history_text = f.read()

# Parse the S&P 500 changes history from Wikipedia
rows = []
for line in history_text.strip().splitlines()[2:]:
    clean = re.sub(r' {2,}', '\t', line)
    parts = clean.split('\t')
    parts += [''] * (6 - len(parts))
    date, add_tkr, add_sec, rem_tkr, rem_sec, reason = parts[:6]
    rows.append({
        "Date": date.strip(),
        "AddTicker": add_tkr.strip(),
        "AddSecurity": add_sec.strip(),
        "RemoveTicker": rem_tkr.strip(),
        "RemoveSecurity": rem_sec.strip(),
        "Reason": re.sub(r'\[\d+]', '', reason).strip()
    })

df = pd.DataFrame(rows)
df["Date"] = pd.to_datetime(df["Date"], errors="coerce")
print(df)

# Save as CSV
df.to_csv(OUTPUT_FILE, index=False)
print(f"Saved {len(df):,} rows -> {OUTPUT_FILE}")
