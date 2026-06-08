#!/usr/bin/env python3
"""
Collect SOXL and SOXX 5-minute candle data
Runs every 5 minutes via cron-job.org
Appends to single CSV file (market_data.csv)
No emails - pure data collection
"""

import os
import base64
import json
import urllib.request
import csv
from datetime import datetime
from pathlib import Path
import pytz

# ============================================================
# CONFIGURATION
# ============================================================

ALPACA_API_KEY = os.environ.get("ALPACA_API_KEY", "")
ALPACA_SECRET_KEY = os.environ.get("ALPACA_SECRET_KEY", "")
ALPACA_DATA_URL = "https://data.alpaca.markets"

# Data file
DATA_DIR = Path("data/ttp")
DATA_FILE = DATA_DIR / "market_data.csv"

# Eastern Time
ET = pytz.timezone('US/Eastern')

# Logging hours (8 AM - 8 PM ET)
LOG_START_HOUR = 8
LOG_END_HOUR = 20

# ============================================================
# HELPERS
# ============================================================

def get_alpaca_headers():
    auth = base64.b64encode(f"{ALPACA_API_KEY}:{ALPACA_SECRET_KEY}".encode()).decode()
    return {"Authorization": f"Basic {auth}", "Accept": "application/json"}

def get_eastern_now():
    return datetime.now(ET)

def fetch_bars(symbol, limit=15, timeframe="5Min"):
    """Fetch last N 5-minute bars for a symbol"""
    url = f"{ALPACA_DATA_URL}/v2/stocks/{symbol}/bars?timeframe={timeframe}&limit={limit}"
    req = urllib.request.Request(url, headers=get_alpaca_headers())
    try:
        with urllib.request.urlopen(req, timeout=30) as response:
            data = json.loads(response.read().decode())
            return data.get("bars", [])
    except Exception as e:
        print(f"Error fetching {symbol}: {e}")
        return None

def calculate_rolling_metrics(bars):
    """Calculate 5-period rolling averages and changes"""
    if not bars or len(bars) < 10:
        return None, None, None, None
    
    current_5 = bars[-5:]
    prev_5 = bars[-10:-5]
    
    if len(current_5) < 5 or len(prev_5) < 5:
        return None, None, None, None
    
    current_vol = sum(float(b.get("v", 0)) for b in current_5) / 5
    prev_vol = sum(float(b.get("v", 0)) for b in prev_5) / 5
    vol_change = ((current_vol - prev_vol) / prev_vol * 100) if prev_vol > 0 else 0
    
    current_range = sum(float(b.get("h", 0)) - float(b.get("l", 0)) for b in current_5) / 5
    prev_range = sum(float(b.get("h", 0)) - float(b.get("l", 0)) for b in prev_5) / 5
    range_change = ((current_range - prev_range) / prev_range * 100) if prev_range > 0 else 0
    
    return current_vol, current_range, vol_change, range_change

def extract_candle_data(bars, symbol_prefix):
    """Extract latest candle data with rolling metrics"""
    if not bars or len(bars) < 10:
        return None
    
    latest = bars[-1]
    current_vol, current_range, vol_change, range_change = calculate_rolling_metrics(bars)
    
    close = float(latest.get("c", 0))
    volume = float(latest.get("v", 0))
    high = float(latest.get("h", 0))
    low = float(latest.get("l", 0))
    open_price = float(latest.get("o", 0))
    
    candle_range = high - low
    candle_body = close - open_price
    
    # Convert timestamp to ET
    utc_str = latest.get("t", "")
    try:
        utc_time = datetime.strptime(utc_str, "%Y-%m-%dT%H:%M:%SZ")
        utc_time = pytz.UTC.localize(utc_time)
        et_time = utc_time.astimezone(ET)
        timestamp = et_time.strftime("%Y-%m-%d %H:%M:%S ET")
    except:
        timestamp = utc_str
    
    return {
        "timestamp": timestamp,
        f"{symbol_prefix}_price": round(close, 2),
        f"{symbol_prefix}_volume": int(volume),
        f"{symbol_prefix}_candle_range": round(candle_range, 2),
        f"{symbol_prefix}_candle_body": round(candle_body, 2),
        f"{symbol_prefix}_vol_5_avg": int(current_vol) if current_vol else 0,
        f"{symbol_prefix}_range_5_avg": round(current_range, 2) if current_range else 0,
        f"{symbol_prefix}_vol_change_pct": round(vol_change, 1) if vol_change else 0,
        f"{symbol_prefix}_range_change_pct": round(range_change, 1) if range_change else 0,
    }

def is_within_logging_hours(timestamp_et):
    """Check if timestamp is between 8 AM and 8 PM ET"""
    try:
        dt = datetime.strptime(timestamp_et.split(" ET")[0], "%Y-%m-%d %H:%M:%S")
        dt = ET.localize(dt)
        hour = dt.hour
        return LOG_START_HOUR <= hour < LOG_END_HOUR
    except:
        return True

def row_exists(timestamp, existing_df=None):
    """Check if row with this timestamp already exists"""
    if not DATA_FILE.exists():
        return False
    
    import pandas as pd
    try:
        df = pd.read_csv(DATA_FILE)
        return timestamp in df["timestamp"].values
    except:
        return False

def save_to_csv(row):
    """Append row to CSV if not duplicate and within hours"""
    if not is_within_logging_hours(row["timestamp"]):
        print(f"Skipping {row['timestamp']} - outside logging hours")
        return False
    
    if row_exists(row["timestamp"]):
        print(f"Skipping {row['timestamp']} - already exists")
        return False
    
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    file_exists = DATA_FILE.exists()
    
    with open(DATA_FILE, "a", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=row.keys())
        if not file_exists:
            writer.writeheader()
        writer.writerow(row)
    
    print(f"Saved: {row['timestamp']}")
    return True

# ============================================================
# MAIN
# ============================================================

def main():
    print("=" * 50)
    print("Data Collector - SOXL + SOXX")
    print("=" * 50)
    
    if not ALPACA_API_KEY or not ALPACA_SECRET_KEY:
        print("❌ Alpaca API keys not found")
        return
    
    now = get_eastern_now()
    print(f"Run time: {now.strftime('%Y-%m-%d %H:%M:%S ET')}")
    
    # Fetch SOXL data
    print("\n📊 Fetching SOXL...")
    soxl_bars = fetch_bars("SOXL", limit=15, timeframe="5Min")
    if not soxl_bars:
        print("❌ Failed to fetch SOXL")
        return
    print(f"   Fetched {len(soxl_bars)} candles")
    
    # Fetch SOXX data
    print("\n📊 Fetching SOXX...")
    soxx_bars = fetch_bars("SOXX", limit=15, timeframe="5Min")
    if not soxx_bars:
        print("❌ Failed to fetch SOXX")
        return
    print(f"   Fetched {len(soxx_bars)} candles")
    
    # Extract latest candle data
    soxl_data = extract_candle_data(soxl_bars, "soxl")
    soxx_data = extract_candle_data(soxx_bars, "soxx")
    
    if not soxl_data or not soxx_data:
        print("❌ Failed to extract data")
        return
    
    # Ensure timestamps match
    if soxl_data["timestamp"] != soxx_data["timestamp"]:
        # Use the earlier timestamp
        ts_soxl = soxl_data["timestamp"]
        ts_soxx = soxx_data["timestamp"]
        print(f"⚠️ Timestamp mismatch: SOXL={ts_soxl}, SOXX={ts_soxx}")
        # Use SOXL timestamp (arbitrary choice)
        soxx_data["timestamp"] = ts_soxl
    
    # Merge data
    merged_row = {**soxl_data, **soxx_data}
    
    # Save to CSV
    if save_to_csv(merged_row):
        print(f"\n✅ Data saved to {DATA_FILE}")
    else:
        print(f"\n⚠️ Data not saved (duplicate or outside hours)")
    
    print(f"   SOXL: ${soxl_data['soxl_price']} | Vol: {soxl_data['soxl_volume']}")
    print(f"   SOXX: ${soxx_data['soxx_price']} | Vol: {soxx_data['soxx_volume']}")

if __name__ == "__main__":
    main()
