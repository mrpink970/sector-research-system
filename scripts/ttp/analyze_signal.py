#!/usr/bin/env python3
"""
Analyze collected data and send hourly signal email
Runs every hour via cron-job.org
Reads from market_data.csv, sends ONE email per hour
"""

import os
import smtplib
import pandas as pd
from email.message import EmailMessage
from email.utils import formataddr
from datetime import datetime, timedelta
from pathlib import Path
import pytz

# ============================================================
# CONFIGURATION
# ============================================================

EMAIL_USERNAME = os.environ.get("EMAIL_USERNAME", "")
EMAIL_PASSWORD = os.environ.get("EMAIL_PASSWORD", "")
EMAIL_RECIPIENTS = ["mrpink970@gmail.com"]

DATA_FILE = Path("data/ttp/market_data.csv")

# Signal thresholds
GREEN_DAY_THRESHOLD = 0.5  # 0.5% move in 5-min candle
VOLUME_THRESHOLD = 0.8     # Volume ratio > 0.8x
STRONG_VOLUME_THRESHOLD = 1.5

# Confidence scoring
CONFIDENCE_HIGH = 70
CONFIDENCE_MEDIUM = 50

# Account rules (5K Day Trade Flex Beginner)
ACCOUNT_STARTING_BALANCE = 5000
PROFIT_TARGET = 300
MAX_DRAWDOWN = 200      # $4,800 floor
DAILY_PAUSE = 100
MIN_TRADES = 10
CONSISTENCY_RULE_PCT = 50  # 50% of profit target = $150 max per trade
PROFIT_DAYS_REQUIRED = 3
PROFIT_DAY_THRESHOLD = 25   # $25 in a day

# Eastern Time
ET = pytz.timezone('US/Eastern')

# ============================================================
# SIGNAL CALCULATION
# ============================================================

def get_eastern_now():
    return datetime.now(ET)

def load_recent_data(hours=2):
    """Load last N hours of data from CSV"""
    if not DATA_FILE.exists():
        return None
    
    df = pd.read_csv(DATA_FILE)
    if df.empty:
        return None
    
    # Parse timestamps
    df['datetime'] = pd.to_datetime(df['timestamp'].str.split(' ET').str[0])
    
    # Filter last N hours
    cutoff = get_eastern_now() - timedelta(hours=hours)
    df = df[df['datetime'] >= cutoff]
    
    return df

def calculate_green_day_score(df):
    """Calculate Green Day confidence score (0-100) based on recent candles"""
    if df is None or len(df) < 3:
        return 0, "Insufficient data"
    
    score = 0
    reasons = []
    
    # Check for Green Day candles (≥0.5% move)
    soxl_returns = df['soxl_price'].pct_change() * 100
    green_candles = soxl_returns[soxl_returns >= GREEN_DAY_THRESHOLD]
    
    if len(green_candles) > 0:
        score += 30
        reasons.append(f"{len(green_candles)} Green Day candles")
    else:
        reasons.append("No Green Day candles")
    
    # Check volume
    latest_vol_ratio = df.iloc[-1].get('soxl_vol_5_avg', 0)
    if latest_vol_ratio > 0:
        vol_ratio = df.iloc[-1]['soxl_volume'] / latest_vol_ratio if latest_vol_ratio > 0 else 0
        if vol_ratio >= VOLUME_THRESHOLD:
            score += 25
            reasons.append(f"Volume OK ({vol_ratio:.1f}x)")
        else:
            reasons.append(f"Low volume ({vol_ratio:.1f}x)")
    
    # Check trend (price vs 30-min average)
    if len(df) >= 6:
        ma30 = df['soxl_price'].tail(6).mean()
        current_price = df.iloc[-1]['soxl_price']
        if current_price > ma30:
            score += 25
            reasons.append("Price above 30-min MA")
        else:
            reasons.append("Price below 30-min MA")
    
    # Check momentum (last 3 candles)
    recent_returns = soxl_returns.tail(3)
    if len(recent_returns) >= 3:
        if recent_returns.sum() > 0:
            score += 20
            reasons.append("Positive momentum")
        else:
            reasons.append("Negative momentum")
    
    # Determine action
    if score >= CONFIDENCE_HIGH:
        action = "BUY"
    elif score >= CONFIDENCE_MEDIUM:
        action = "WATCH"
    else:
        action = "AVOID"
    
    return score, action, reasons

def get_latest_prices(df):
    """Get latest SOXL and SOXX prices"""
    if df is None or df.empty:
        return 0, 0
    latest = df.iloc[-1]
    return latest.get('soxl_price', 0), latest.get('soxx_price', 0)

def get_volume_ratio(df):
    """Get latest volume ratio"""
    if df is None or df.empty:
        return 0
    latest = df.iloc[-1]
    vol = latest.get('soxl_volume', 0)
    avg_vol = latest.get('soxl_vol_5_avg', 1)
    return vol / avg_vol if avg_vol > 0 else 0

# ============================================================
# EMAIL
# ============================================================

def send_email(subject, body):
    if not EMAIL_PASSWORD:
        print("❌ Email password not set")
        return False
    
    msg = EmailMessage()
    msg.set_content(body)
    msg["Subject"] = subject
    msg["From"] = formataddr(("TTP Signal Engine", EMAIL_USERNAME))
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

# ============================================================
# MAIN
# ============================================================

def main():
    print("=" * 50)
    print("Signal Analyzer - Hourly")
    print("=" * 50)
    
    now = get_eastern_now()
    print(f"Analysis time: {now.strftime('%Y-%m-%d %H:%M:%S ET')}")
    
    # Load data
    df = load_recent_data(hours=2)
    if df is None or df.empty:
        print("❌ No data available")
        send_email("⚠️ TTP DATA ERROR", "No market data available for analysis.")
        return
    
    print(f"📊 Loaded {len(df)} candles")
    
    # Calculate signals
    score, action, reasons = calculate_green_day_score(df)
    soxl_price, soxx_price = get_latest_prices(df)
    volume_ratio = get_volume_ratio(df)
    
    # Determine signal emoji
    if action == "BUY":
        emoji = "🟢"
    elif action == "WATCH":
        emoji = "🟡"
    else:
        emoji = "🔴"
    
    # Build email
    subject = f"{emoji} TTP SIGNAL - {action} - {now.strftime('%Y-%m-%d %H:%M %p ET')}"
    
    body = f"""
{'=' * 50}
  TTP SIGNAL ENGINE - HOURLY UPDATE
{'=' * 50}

{emoji} SIGNAL: {action}
   Confidence Score: {score}/100

📊 MARKET DATA:
   SOXL: ${soxl_price:.2f}
   SOXX: ${soxx_price:.2f}
   Volume Ratio: {volume_ratio:.1f}x

📈 SIGNAL COMPONENTS:
   {' | '.join(reasons)}

{'=' * 50}
📋 ACCOUNT RULES (5K Day Trade Flex):

   Profit Target: ${PROFIT_TARGET} (6%)
   Max Drawdown: ${MAX_DRAWDOWN} ($4,800 floor)
   Daily Pause: ${DAILY_PAUSE}
   Min Trades: {MIN_TRADES}
   Max per Trade: ${int(PROFIT_TARGET * CONSISTENCY_RULE_PCT / 100)} (50% of target)
   Profit Days: {PROFIT_DAYS_REQUIRED} days with ${PROFIT_DAY_THRESHOLD}+

{'=' * 50}
✅ ACTION RECOMMENDATION:

"""
    if action == "BUY":
        body += """
   ✅ Ready to consider a long position

   Entry: On pullback to support
   Stop: $25 trailing stop (enter in ticks)
   Target: $45 profit
   Shares: 15 (adjust based on price)

   ⚠️ No trades before 9:45 AM ET
   ⚠️ Be flat by 4:00 PM ET
"""
    elif action == "WATCH":
        body += """
   🟡 Monitor only - wait for confirmation

   Conditions needed for BUY:
   - Stronger volume (≥ 0.8x)
   - Price above 30-min MA
   - Positive momentum

   ⚠️ Do not enter yet
"""
    else:
        body += """
   🔴 No trade - conditions not met

   Stay in cash. Wait for next hourly update.

   ⚠️ Do not force trades
"""

    body += f"""
{'=' * 50}
⚠️ REMINDERS:
   ❌ Max drawdown: $4,800 floor
   ❌ Daily pause: ${DAILY_PAUSE} loss
   ❌ Max profit per trade: ${int(PROFIT_TARGET * CONSISTENCY_RULE_PCT / 100)}
   ✅ Min trades to pass: {MIN_TRADES}
   ✅ Profit days needed: {PROFIT_DAYS_REQUIRED}

{'=' * 50}
"""
    
    send_email(subject, body)
    print(f"\n📊 Summary:")
    print(f"   Score: {score}/100")
    print(f"   Action: {action}")
    print(f"   SOXL: ${soxl_price}")
    print(f"   Volume ratio: {volume_ratio:.1f}x")
    print("\n✅ Complete")

if __name__ == "__main__":
    main()
