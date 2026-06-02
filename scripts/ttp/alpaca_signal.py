#!/usr/bin/env python3
"""
Alpaca SOXL Signal Generator with Logging
Fetches SOXL data, detects Green Day, logs volume and candle size
"""

import os
import smtplib
import base64
import json
import urllib.request
import csv
from email.message import EmailMessage
from email.utils import formataddr
from datetime import datetime
from pathlib import Path

# ============================================================
# READ FROM ENVIRONMENT VARIABLES (GitHub Secrets)
# ============================================================

ALPACA_API_KEY = os.environ.get("ALPACA_API_KEY", "")
ALPACA_SECRET_KEY = os.environ.get("ALPACA_SECRET_KEY", "")
EMAIL_USERNAME = os.environ.get("EMAIL_USERNAME", "")
EMAIL_PASSWORD = os.environ.get("EMAIL_PASSWORD", "")

EMAIL_RECIPIENTS = ["mrpink970@gmail.com"]
GREEN_DAY_THRESHOLD_PERCENT = 0.5

ALPACA_DATA_URL = "https://data.alpaca.markets"

# Log file path
LOG_FILE = Path("data/ttp/soxl_candles.csv")

# ============================================================
# NO EDITS NEEDED BELOW THIS LINE
# ============================================================

def get_alpaca_headers():
    auth = base64.b64encode(f"{ALPACA_API_KEY}:{ALPACA_SECRET_KEY}".encode()).decode()
    return {"Authorization": f"Basic {auth}", "Accept": "application/json"}

def fetch_soxl_bars(limit=20, timeframe="5Min"):
    url = f"{ALPACA_DATA_URL}/v2/stocks/SOXL/bars?timeframe={timeframe}&limit={limit}"
    req = urllib.request.Request(url, headers=get_alpaca_headers())
    try:
        with urllib.request.urlopen(req, timeout=30) as response:
            data = json.loads(response.read().decode())
            return data.get("bars", [])
    except Exception as e:
        print(f"Error fetching SOXL bars: {e}")
        return None

def fetch_soxl_latest():
    url = f"{ALPACA_DATA_URL}/v2/stocks/SOXL/trades/latest"
    req = urllib.request.Request(url, headers=get_alpaca_headers())
    try:
        with urllib.request.urlopen(req, timeout=30) as response:
            data = json.loads(response.read().decode())
            trade = data.get("trade", {})
            return {"price": float(trade.get("p", 0)), "volume": int(trade.get("s", 0)), "timestamp": trade.get("t", "")}
    except Exception as e:
        print(f"Error fetching latest SOXL trade: {e}")
        return None

def send_email(subject, body):
    if not EMAIL_PASSWORD:
        print("❌ Email password not set")
        return False
    msg = EmailMessage()
    msg.set_content(body)
    msg["Subject"] = subject
    msg["From"] = f"Alpaca Signal <{EMAIL_USERNAME}>"
    msg["To"] = ", ".join(EMAIL_RECIPIENTS)
    try:
        with smtplib.SMTP_SSL("smtp.gmail.com", 465) as smtp:
            smtp.login(EMAIL_USERNAME, EMAIL_PASSWORD)
            smtp.send_message(msg)
        print("✅ Email sent")
        return True
    except Exception as e:
        print(f"❌ Email failed: {e}")
        return False

def log_candles(bars):
    """Log candle data to CSV with volume and range trends"""
    if not bars or len(bars) < 10:
        print("⚠️ Not enough bars to log trends")
        return
    
    # Create directory if needed
    Path("data/ttp").mkdir(parents=True, exist_ok=True)
    
    # Calculate rolling metrics
    for i in range(5, len(bars)):
        current_5 = bars[i-4:i+1]  # last 5 candles
        prev_5 = bars[i-9:i-4]     # previous 5 candles
        
        if len(current_5) < 5 or len(prev_5) < 5:
            continue
        
        current_vol = sum(float(c.get("v", 0)) for c in current_5) / 5
        prev_vol = sum(float(c.get("v", 0)) for c in prev_5) / 5
        vol_change = ((current_vol - prev_vol) / prev_vol * 100) if prev_vol > 0 else 0
        
        current_range = sum(float(c.get("h", 0)) - float(c.get("l", 0)) for c in current_5) / 5
        prev_range = sum(float(c.get("h", 0)) - float(c.get("l", 0)) for c in prev_5) / 5
        range_change = ((current_range - prev_range) / prev_range * 100) if prev_range > 0 else 0
        
        latest = bars[i]
        timestamp = latest.get("t", "")
        close = float(latest.get("c", 0))
        volume = float(latest.get("v", 0))
        high = float(latest.get("h", 0))
        low = float(latest.get("l", 0))
        candle_range = high - low
        candle_body = close - float(latest.get("o", 0))
        
        row = {
            "timestamp": timestamp,
            "price": round(close, 2),
            "volume": int(volume),
            "candle_range": round(candle_range, 2),
            "candle_body": round(candle_body, 2),
            "vol_5_avg": int(current_vol),
            "range_5_avg": round(current_range, 2),
            "vol_change_pct": round(vol_change, 1),
            "range_change_pct": round(range_change, 1)
        }
        
        # Write to CSV
        file_exists = LOG_FILE.exists()
        with open(LOG_FILE, "a", newline="") as f:
            writer = csv.DictWriter(f, fieldnames=row.keys())
            if not file_exists:
                writer.writeheader()
            writer.writerow(row)
    
    print(f"✅ Logged {len(bars)-5} candles to {LOG_FILE}")

def check_green_day(bars):
    if not bars or len(bars) < 2:
        return False, None, None
    for i in range(1, len(bars)):
        prev_close = float(bars[i-1].get("c", 0))
        curr_close = float(bars[i].get("c", 0))
        if prev_close > 0:
            return_pct = (curr_close - prev_close) / prev_close * 100
            if return_pct >= GREEN_DAY_THRESHOLD_PERCENT:
                return True, round(return_pct, 2), {"time": bars[i].get("t", ""), "price": curr_close, "return_pct": round(return_pct, 2)}
    return False, None, None

def main():
    print("=" * 50)
    print("Alpaca SOXL Signal Generator with Logging")
    print("=" * 50)
    
    if not ALPACA_API_KEY or not ALPACA_SECRET_KEY:
        print("❌ Alpaca API keys not found")
        return
    
    # Fetch bars
    print("\n📈 Fetching 20 bars (5-min)...")
    bars = fetch_soxl_bars(limit=20, timeframe="5Min")
    
    if not bars:
        print("❌ No bar data")
        send_email("⚠️ ALPACA DATA ERROR", "Could not fetch SOXL bar data.")
        return
    
    print(f"   Fetched {len(bars)} bars")
    
    # Log candle data with trends
    log_candles(bars)
    
    # Check for Green Day
    is_green_day, return_pct, best_candle = check_green_day(bars)
    
    # Get latest price
    latest = fetch_soxl_latest()
    latest_price = latest["price"] if latest else 0
    
    now = datetime.now().strftime("%Y-%m-%d %I:%M %p ET")
    separator = "=" * 50
    
    if is_green_day and best_candle:
        subject = f"🟢 GREEN DAY - SOXL +{return_pct}% - {now}"
        body = f"""{separator}
  GREEN DAY SIGNAL DETECTED
{separator}

🟢 Green Day Confirmed
   Time: {best_candle['time']}
   Price: ${best_candle['price']:.2f}
   Move: +{best_candle['return_pct']}%

📈 Latest SOXL: ${latest_price:.2f}

✅ Action: Ready to buy on pullback

📊 Data logged to: soxl_candles.csv

⚠️ Rules: $140 DD | $90/trade | $60/day | 2% stop | 6% target
{separator}"""
    else:
        subject = f"🔴 RED DAY - {now}"
        body = f"""{separator}
  MARKET ANALYSIS - NO SIGNAL
{separator}

🔴 No Green Day detected
   Threshold: {GREEN_DAY_THRESHOLD_PERCENT}%

📈 Latest SOXL: ${latest_price:.2f}

📊 Data logged to: soxl_candles.csv

⏳ Status: Wait for Green Day confirmation
{separator}"""
    
    send_email(subject, body)
    print("\n✅ Complete")

if __name__ == "__main__":
    main()
