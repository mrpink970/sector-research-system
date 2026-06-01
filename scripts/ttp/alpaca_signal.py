#!/usr/bin/env python3
"""
Alpaca SOXL Signal Generator
Fetches SOXL data from Alpaca API using CORRECT endpoint for individual accounts
"""

import os
import smtplib
import base64
import json
import urllib.request
from email.message import EmailMessage
from email.utils import formataddr
from datetime import datetime

# ============================================================
# READ FROM ENVIRONMENT VARIABLES (GitHub Secrets)
# ============================================================

ALPACA_API_KEY = os.environ.get("ALPACA_API_KEY", "")
ALPACA_SECRET_KEY = os.environ.get("ALPACA_SECRET_KEY", "")
EMAIL_USERNAME = os.environ.get("EMAIL_USERNAME", "")
EMAIL_PASSWORD = os.environ.get("EMAIL_PASSWORD", "")

EMAIL_RECIPIENTS = ["mrpink970@gmail.com"]
GREEN_DAY_THRESHOLD_PERCENT = 0.5

# CRITICAL FIX: Use data.alpaca.markets, NOT paper-api.alpaca.markets
# The paper-api endpoint is for Broker API users only
ALPACA_DATA_URL = "https://data.alpaca.markets"

# ============================================================
# NO EDITS NEEDED BELOW THIS LINE
# ============================================================

def get_alpaca_headers():
    """Return HTTP headers for Alpaca API authentication"""
    auth = base64.b64encode(f"{ALPACA_API_KEY}:{ALPACA_SECRET_KEY}".encode()).decode()
    return {
        "Authorization": f"Basic {auth}",
        "Accept": "application/json"
    }

def fetch_soxl_bars(limit=10, timeframe="5Min"):
    """Fetch SOXL bars from Alpaca data endpoint"""
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
    """Fetch latest SOXL trade from Alpaca data endpoint"""
    url = f"{ALPACA_DATA_URL}/v2/stocks/SOXL/trades/latest"
    
    req = urllib.request.Request(url, headers=get_alpaca_headers())
    
    try:
        with urllib.request.urlopen(req, timeout=30) as response:
            data = json.loads(response.read().decode())
            trade = data.get("trade", {})
            return {
                "price": float(trade.get("p", 0)),
                "volume": int(trade.get("s", 0)),
                "timestamp": trade.get("t", "")
            }
    except Exception as e:
        print(f"Error fetching latest SOXL trade: {e}")
        return None

def send_email(subject, body):
    """Send email alert"""
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

def check_green_day(bars):
    """Check if any 5-min candle meets Green Day threshold"""
    if not bars or len(bars) < 2:
        return False, None, None
    
    for i in range(1, len(bars)):
        prev_close = float(bars[i-1].get("c", 0))
        curr_close = float(bars[i].get("c", 0))
        
        if prev_close > 0:
            return_pct = (curr_close - prev_close) / prev_close * 100
            if return_pct >= GREEN_DAY_THRESHOLD_PERCENT:
                return True, round(return_pct, 2), {
                    "time": bars[i].get("t", ""),
                    "price": curr_close,
                    "return_pct": round(return_pct, 2)
                }
    
    return False, None, None

def main():
    print("=" * 50)
    print("Alpaca SOXL Signal Generator")
    print("=" * 50)
    
    if not ALPACA_API_KEY or not ALPACA_SECRET_KEY:
        print("❌ Alpaca API keys not found")
        return
    
    print(f"\n📊 Using endpoint: {ALPACA_DATA_URL}")
    
    # Fetch latest price
    print("\n📊 Fetching latest SOXL...")
    latest = fetch_soxl_latest()
    if latest and latest["price"] > 0:
        print(f"   SOXL: ${latest['price']:.2f}")
    else:
        print("   ⚠️ Could not fetch latest price")
    
    # Fetch bars
    print("\n📈 Fetching 5-min bars...")
    bars = fetch_soxl_bars(limit=10, timeframe="5Min")
    
    if not bars:
        print("❌ No bar data")
        send_email("⚠️ ALPACA DATA ERROR", "Could not fetch SOXL bar data from Alpaca.")
        return
    
    print(f"   Fetched {len(bars)} bars")
    
    # Check for Green Day
    is_green_day, return_pct, best_candle = check_green_day(bars)
    
    now = datetime.now().strftime("%Y-%m-%d %I:%M %p ET")
    separator = "=" * 50
    
    if is_green_day and best_candle:
        subject = f"🟢 GREEN DAY - SOXL +{return_pct}% - {now}"
        body = f"""
{separator}
  GREEN DAY SIGNAL DETECTED
{separator}

🟢 Green Day Confirmed
   Time: {best_candle['time']}
   Price: ${best_candle['price']:.2f}
   Move: +{best_candle['return_pct']}%

📈 Latest SOXL: ${latest['price']:.2f}

✅ Action: Ready to buy on pullback

⚠️ Rules: $140 DD | $90/trade | $60/day | 2% stop | 6% target
{separator}
"""
    else:
        subject = f"🔴 RED DAY - {now}"
        body = f"""
{separator}
  MARKET ANALYSIS - NO SIGNAL
{separator}

🔴 No Green Day detected
   Threshold: {GREEN_DAY_THRESHOLD_PERCENT}%

📈 Latest SOXL: ${latest['price']:.2f}

⏳ Status: Wait for Green Day confirmation
{separator}
"""
    
    send_email(subject, body)
    print("\n✅ Complete")

if __name__ == "__main__":
    main()
