import pandas as pd
import yfinance as yf
import os
import time

# === CONFIG ===
OUTPUT_FILE = "india_stocks_4gb.csv"
target_size_gb = 3.0
start_date = "2000-01-01"
end_date = "2025-01-01"
interval = "1d"
TICKER_FILE = "/content/20MICRONS.txt"   # your txt file with tickers (without .NS)

# === LOAD TICKERS ===
def load_tickers(file_path):
    with open(file_path, "r") as f:
        tickers = [line.strip() + ".NS" for line in f if line.strip()]
    return tickers

# Load from file (and add .NS)
tickers = load_tickers(TICKER_FILE)
tickers = tickers * 10

print(f"✅ Loaded {len(tickers)} tickers (with .NS added) from {TICKER_FILE}")

# === REMOVE OLD FILE IF EXISTS ===
if os.path.exists(OUTPUT_FILE):
    os.remove(OUTPUT_FILE)

# === DOWNLOAD LOOP ===
for idx, ticker in enumerate(tickers, 1):
    try:
        df = yf.download(
            ticker, start=start_date, end=end_date,
            interval=interval, progress=False
        )
        if df.empty:
            continue

        df["Ticker"] = ticker
        df.to_csv(OUTPUT_FILE, mode="a", header=not os.path.exists(OUTPUT_FILE))

        # Check file size
        size_gb = os.path.getsize(OUTPUT_FILE) / (1024**3)
        print(f"[{idx}/{len(tickers)}] {ticker} added → {size_gb:.2f} GB")

        if size_gb >= target_size_gb:
            print("✅ Reached ~4GB dataset. Stopping download.")
            break

    except Exception as e:
        print(f"⚠️ Error with {ticker}: {e}")

print("\n🎉 Download complete.")
