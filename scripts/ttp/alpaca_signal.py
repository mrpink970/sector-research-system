#!/usr/bin/env python3
"""
Alpaca SOXL Signal Generator with Logging
Fetches SOXL data, detects Green Day, logs volume and candle size
Uses Eastern Time for all timestamps
Only logs candles between 8:00 AM and 8:00 PM ET
ADDED: Volume filter, trend confirmation, session-based rules
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
import pytz

# ============================================================
# READ FROM ENVIRONMENT VARIABLES (GitHub Secrets)
# ============================================================

ALPACA_API_KEY = os.environ.get("ALPACA_API_KEY", "")
ALPACA_SECRET_KEY = os.environ.get("ALPACA_SECRET_KEY", "")
EMAIL_USERNAME = os.environ.get("EMAIL_USERNAME", "")
EMAIL_PASSWORD = os.environ.get("EMAIL_PASSWORD", "")

EMAIL_RECIPIENTS = ["mrpink970@gmail.com"]

# Signal thresholds
GREEN_DAY_THRESHOLD_PERCENT = 0.5
STRONG_GREEN_DAY_THRESHOLD_PERCENT = 1.0
VOLUME_RATIO_THRESHOLD = 0.8
PREMARKET_VOLUME_RATIO_THRESHOLD = 0.5

ALPACA_DATA_URL = "https://data.alpaca.markets"

# Log file path
LOG_FILE = Path("data/ttp/soxl_candles.csv")

# Eastern Time Zone
ET = pytz.timezone('US/Eastern')

# Logging hours: 8:00 AM to 8:00 PM ET
LOG_START_HOUR = 8
LOG_END_HOUR = 20

# ============================================================
# NO EDITS NEEDED BELOW THIS LINE
# ============================================================

def get_eastern_now():
    """Return current datetime in Eastern Time"""
    return datetime.now(ET)

def get_alpaca_headers():
    auth = base64.b64encode(f"{ALPACA_API_KEY}:{ALPACA_SECRET_KEY}".encode()).decode()
    return {"Authorization": f"Basic {auth}", "Accept": "application/json"}

def fetch_soxl_bars(limit=50, timeframe="5Min"):
    """Fetch SOXL bars from Alpaca API"""
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
    """Fetch latest SOXL trade"""
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

def fetch_qqq_bars(limit=10, timeframe="5Min"):
    """Fetch QQQ bars for trend confirmation"""
    url = f"{ALPACA_DATA_URL}/v2/stocks/QQQ/bars?timeframe={timeframe}&limit={limit}"
    req = urllib.request.Request(url, headers=get_alpaca_headers())
    try:
        with urllib.request.urlopen(req, timeout=30) as response:
            data = json.loads(response.read().decode())
            return data.get("bars", [])
    except Exception as e:
        print(f"Error fetching QQQ bars: {e}")
        return None

def calculate_ma20(bars):
    """Calculate 20-period moving average from bars"""
    if not bars or len(bars) < 20:
        return None
    closes = [float(bar.get("c", 0)) for bar in bars[-20:]]
    return sum(closes) / len(closes)

def calculate_volume_ratio(bars, current_volume):
    """Calculate volume ratio vs 20-period average"""
    if not bars or len(bars) < 20:
        return 1.0
    volumes = [float(bar.get("v", 0)) for bar in bars[-20:] if float(bar.get("v", 0)) > 0]
    if not volumes:
        return 1.0
    avg_volume = sum(volumes) / len(volumes)
    return current_volume / avg_volume if avg_volume > 0 else 1.0

def analyze_qqq_trend(bars):
    """Determine QQQ trend direction"""
    if not bars or len(bars) < 5:
        return "UNKNOWN"
    closes = [float(bar.get("c", 0)) for bar in bars]
    start = closes[0]
    end = closes[-1]
    change_pct = ((end - start) / start) * 100 if start > 0 else 0
    if change_pct > 0.3:
        return "UP"
    elif change_pct < -0.3:
        return "DOWN"
    else:
        return "FLAT"

def get_session(current_time_et):
    """Determine trading session based on Eastern Time"""
    hour = current_time_et.hour
    minute = current_time_et.minute
    
    if hour < 9 or (hour == 9 and minute < 30):
        return "premarket"
    elif 9 <= hour < 16:
        return "regular"
    else:
        return "afterhours"

def is_tradeable_signal(session, volume_ratio, price_above_ma20, qqq_trend, candle_strength):
    """Determine if a Green Day signal is tradeable"""
    
    if session == "premarket":
        # Pre-market: alert only, not tradeable
        # Require strong candle and decent volume for alert
        if candle_strength >= 0.8 and volume_ratio >= PREMARKET_VOLUME_RATIO_THRESHOLD:
            return "alert", "PRELIMINARY GREEN DAY - Monitor Only"
        else:
            return "ignore", "Weak pre-market movement - ignoring"
    
    elif session == "regular":
        # Regular hours: tradeable with conditions
        if volume_ratio < VOLUME_RATIO_THRESHOLD:
            return "no_trade", f"GREEN DAY but low volume ({volume_ratio:.1f}x) - wait"
        if not price_above_ma20:
            return "no_trade", "GREEN DAY but price below MA20 - wait"
        if qqq_trend == "DOWN":
            return "no_trade", "GREEN DAY but QQQ trending down - wait"
        
        if candle_strength >= GREEN_DAY_THRESHOLD_PERCENT:
            return "trade", "CONFIRMED GREEN DAY - Ready to trade"
        else:
            return "no_trade", "Weak movement - waiting for confirmation"
    
    else:  # afterhours
        return "alert", "After-hours movement - monitor for tomorrow"

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

def is_within_logging_hours(timestamp_utc):
    """Check if timestamp is between 8:00 AM and 8:00 PM ET"""
    try:
        utc_time = datetime.strptime(timestamp_utc, "%Y-%m-%dT%H:%M:%SZ")
        utc_time = pytz.UTC.localize(utc_time)
        et_time = utc_time.astimezone(ET)
        hour = et_time.hour
        return LOG_START_HOUR <= hour < LOG_END_HOUR
    except:
        return True

def log_candles(bars):
    """Log candle data to CSV with volume and range trends (8am-8pm ET only)"""
    if not bars or len(bars) < 10:
        print("⚠️ Not enough bars to log trends")
        return
    
    Path("data/ttp").mkdir(parents=True, exist_ok=True)
    logged_count = 0
    
    for i in range(5, len(bars)):
        current_5 = bars[i-4:i+1]
        prev_5 = bars[i-9:i-4]
        
        if len(current_5) < 5 or len(prev_5) < 5:
            continue
        
        latest = bars[i]
        
        if not is_within_logging_hours(latest.get("t", "")):
            continue
        
        current_vol = sum(float(c.get("v", 0)) for c in current_5) / 5
        prev_vol = sum(float(c.get("v", 0)) for c in prev_5) / 5
        vol_change = ((current_vol - prev_vol) / prev_vol * 100) if prev_vol > 0 else 0
        
        current_range = sum(float(c.get("h", 0)) - float(c.get("l", 0)) for c in current_5) / 5
        prev_range = sum(float(c.get("h", 0)) - float(c.get("l", 0)) for c in prev_5) / 5
        range_change = ((current_range - prev_range) / prev_range * 100) if prev_range > 0 else 0
        
        utc_str = latest.get("t", "")
        try:
            utc_time = datetime.strptime(utc_str, "%Y-%m-%dT%H:%M:%SZ")
            utc_time = pytz.UTC.localize(utc_time)
            et_time = utc_time.astimezone(ET)
            timestamp = et_time.strftime("%Y-%m-%d %H:%M:%S ET")
        except:
            timestamp = utc_str
        
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
        
        file_exists = LOG_FILE.exists()
        with open(LOG_FILE, "a", newline="") as f:
            writer = csv.DictWriter(f, fieldnames=row.keys())
            if not file_exists:
                writer.writeheader()
            writer.writerow(row)
        logged_count += 1
    
    print(f"✅ Logged {logged_count} candles to {LOG_FILE}")

def check_green_day(bars, current_price, volume_ratio, price_above_ma20, qqq_trend, session):
    """Check for Green Day with session-based rules"""
    if not bars or len(bars) < 2:
        return False, None, None, None, None
    
    best_return = 0
    best_candle = None
    
    for i in range(1, len(bars)):
        prev_close = float(bars[i-1].get("c", 0))
        curr_close = float(bars[i].get("c", 0))
        if prev_close > 0:
            return_pct = (curr_close - prev_close) / prev_close * 100
            if return_pct > best_return:
                best_return = return_pct
                best_candle = {
                    "time": bars[i].get("t", ""),
                    "price": curr_close,
                    "return_pct": round(return_pct, 2)
                }
    
    # Determine if this is a valid signal
    is_green_day = best_return >= GREEN_DAY_THRESHOLD_PERCENT
    
    if not is_green_day:
        return False, None, None, None, None
    
    # Check if tradeable
    decision, message = is_tradeable_signal(
        session, volume_ratio, price_above_ma20, qqq_trend, best_return
    )
    
    return True, best_candle, decision, message, best_return

def main():
    print("=" * 50)
    print("Alpaca SOXL Signal Generator (Enhanced)")
    print("=" * 50)
    
    if not ALPACA_API_KEY or not ALPACA_SECRET_KEY:
        print("❌ Alpaca API keys not found")
        return
    
    current_time = get_eastern_now()
    session = get_session(current_time)
    
    # Fetch bars
    print(f"\n📈 Fetching 50 bars (5-min) at {current_time.strftime('%I:%M %p ET')}...")
    bars = fetch_soxl_bars(limit=50, timeframe="5Min")
    
    if not bars:
        print("❌ No bar data")
        send_email("⚠️ ALPACA DATA ERROR", "Could not fetch SOXL bar data.")
        return
    
    print(f"   Fetched {len(bars)} bars")
    
    # Fetch QQQ bars for trend
    qqq_bars = fetch_qqq_bars(limit=10, timeframe="5Min")
    qqq_trend = analyze_qqq_trend(qqq_bars) if qqq_bars else "UNKNOWN"
    
    # Calculate indicators
    ma20 = calculate_ma20(bars)
    current_price = float(bars[-1].get("c", 0)) if bars else 0
    current_volume = float(bars[-1].get("v", 0)) if bars else 0
    volume_ratio = calculate_volume_ratio(bars, current_volume)
    price_above_ma20 = current_price > ma20 if ma20 else False
    
    # Log candle data
    log_candles(bars)
    
    # Check for Green Day
    is_green, best_candle, decision, message, return_pct = check_green_day(
        bars, current_price, volume_ratio, price_above_ma20, qqq_trend, session
    )
    
    # Get latest trade price
    latest = fetch_soxl_latest()
    latest_price = latest["price"] if latest else current_price
    
    # Get current time for email
    now = current_time.strftime("%Y-%m-%d %I:%M %p ET")
    separator = "=" * 50
    
    # Build email based on signal status
    if is_green and best_candle:
        if decision == "trade":
            subject = f"🟢 {message} - SOXL +{return_pct}% - {now}"
            body = f"""{separator}
  {message}
{separator}

🟢 Green Day Confirmed
   Candle Time (UTC): {best_candle['time']}
   Candle Close: ${best_candle['price']:.2f}
   Candle Move: +{best_candle['return_pct']}%

📊 Market Conditions:
   Session: {session.upper()}
   Volume Ratio: {volume_ratio:.1f}x {'✅' if volume_ratio >= 0.8 else '⚠️'}
   Price vs MA20: {'Above ✅' if price_above_ma20 else 'Below ⚠️'}
   QQQ Trend: {qqq_trend}

📈 Current SOXL: ${latest_price:.2f}

✅ ACTION: Ready to trade
   Entry: On pullback to support
   Stop: 2% trailing stop
   Target: $15-30 profit
   Max shares: 7 (2K account)

⚠️ Rules: $140 DD | $90/trade | $60/day | No trades before 9:45 AM ET
{separator}"""
        
        elif decision == "alert":
            subject = f"🟡 {message} - SOXL +{return_pct}% - {now}"
            body = f"""{separator}
  {message}
{separator}

🟡 Green Day Detected - Session: {session.upper()}

📊 Signal Details:
   Candle Time (UTC): {best_candle['time']}
   Candle Close: ${best_candle['price']:.2f}
   Candle Move: +{best_candle['return_pct']}%
   Volume Ratio: {volume_ratio:.1f}x

📈 Current SOXL: ${latest_price:.2f}

⏳ ACTION: Monitor Only - Do Not Trade Yet
   Wait for regular market session (9:30 AM - 4:00 PM ET)
   Wait for volume confirmation (≥ 0.8x)
   No trades before 9:45 AM ET

⚠️ This is a preliminary alert, not a trade signal.
{separator}"""
        
        else:
            subject = f"🔴 {message} - {now}"
            body = f"""{separator}
  {message}
{separator}

🔴 Green Day detected but conditions not met for trading

📊 Signal Details:
   Candle Move: +{best_candle['return_pct']}%
   Volume Ratio: {volume_ratio:.1f}x
   Price vs MA20: {'Above' if price_above_ma20 else 'Below'}
   QQQ Trend: {qqq_trend}

📈 Current SOXL: ${latest_price:.2f}

⏳ ACTION: Wait for better conditions
   Need volume ≥ 0.8x
   Need price above MA20
   Need QQQ not in downtrend
{separator}"""
    
    else:
        subject = f"🔴 No Signal - {now}"
        body = f"""{separator}
  MARKET ANALYSIS - NO GREEN DAY
{separator}

🔴 No Green Day detected in the last 50 candles
   Threshold: {GREEN_DAY_THRESHOLD_PERCENT}%

📊 Current Conditions:
   Session: {session.upper()}
   Volume Ratio: {volume_ratio:.1f}x
   Price vs MA20: {'Above' if price_above_ma20 else 'Below'}
   QQQ Trend: {qqq_trend}

📈 Current SOXL: ${latest_price:.2f}

⏳ ACTION: Wait for Green Day confirmation
{separator}"""
    
    send_email(subject, body)
    print(f"\n📊 Summary:")
    print(f"   Session: {session}")
    print(f"   Volume Ratio: {volume_ratio:.1f}x")
    print(f"   Price vs MA20: {'Above' if price_above_ma20 else 'Below'}")
    print(f"   QQQ Trend: {qqq_trend}")
    print(f"   Signal: {'GREEN DAY' if is_green else 'NO SIGNAL'}")
    if is_green and best_candle:
        print(f"   Decision: {decision}")
        print(f"   Message: {message}")
    print("\n✅ Complete")

if __name__ == "__main__":
    main()
