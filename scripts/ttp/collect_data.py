#!/usr/bin/env python3
"""
Trade The Pool - SOXX Data Collector
Collects SOXX data at 15-minute intervals
INCLUDES PRE-MARKET DATA (prepost=True)
INCLUDES DAY RETURN from market open
"""

import yfinance as yf
import pandas as pd
from pathlib import Path
from datetime import datetime, time

DATA_DIR = Path("data/ttp")
LOG_PATH = DATA_DIR / "price_log.csv"

# Market open time ET
MARKET_OPEN = time(9, 30)

def get_day_open_price(ticker):
    """Get today's opening price at 9:30 AM ET using 1-minute data"""
    try:
        # Get 1-minute data from today starting at 9:30 AM
        data = ticker.history(period="1d", interval="1m", start="2026-05-26 09:30:00", prepost=False)
        if data is not None and not data.empty:
            # First candle of the day (9:30 AM)
            return round(float(data['Open'].iloc[0]), 2)
        return None
    except Exception as e:
        print(f"Error getting day open: {e}")
        return None

def get_soxx_data():
    """Get current SOXX data including pre-market and technical indicators"""
    ticker = yf.Ticker("SOXX")
    
    # Get current price with PRE-MARKET data enabled
    current = ticker.history(period="2d", interval="1m", prepost=True)
    if current.empty:
        return None
    
    latest = current.iloc[-1]
    price = round(latest['Close'], 2)
    
    # Get day open price
    day_open = get_day_open_price(ticker)
    
    # Calculate day return (from market open to now)
    day_return_pct = 0
    if day_open and day_open > 0:
        day_return_pct = round((price - day_open) / day_open * 100, 2)
    
    # Get historical data for indicators (15-min candles, including pre-market)
    hist = ticker.history(period="3d", interval="15m", prepost=True)
    if hist.empty:
        return None
    
    # Remove timezone for comparison
    hist.index = hist.index.tz_localize(None)
    now = datetime.now()
    
    # Calculate 20-period MA (20 * 15min = 5 hours)
    ma20 = round(hist['Close'].rolling(window=20).mean().iloc[-1], 2) if len(hist) >= 20 else price
    
    # Calculate 1-hour return (4 x 15min candles) using pre-market data if available
    if len(hist) >= 5:
        hour_ago = hist['Close'].iloc[-5]
        return_1h = round((price - hour_ago) / hour_ago * 100, 2)
    else:
        return_1h = 0
    
    # Calculate RSI (14-period on 15-min candles)
    delta = hist['Close'].diff()
    gain = (delta.where(delta > 0, 0)).rolling(window=14).mean()
    loss = (-delta.where(delta < 0, 0)).rolling(window=14).mean()
    rs = gain / loss
    rsi = round(100 - (100 / (1 + rs.iloc[-1])), 1) if len(hist) >= 14 and not pd.isna(rs.iloc[-1]) else 50
    
    # Determine session based on time
    hour = now.hour
    minute = now.minute
    
    if hour < 9 or (hour == 9 and minute < 30):
        session = "premarket"
    elif hour < 12:
        session = "morning"
    elif hour < 16:
        session = "afternoon"
    else:
        session = "late"
    
    return {
        'timestamp': now.strftime("%Y-%m-%d %H:%M:%S"),
        'session': session,
        'price': price,
        'day_open': day_open,
        'day_return_pct': day_return_pct,
        'ma20': ma20,
        'above_ma20': price > ma20,
        'return_1h_pct': return_1h,
        'rsi': rsi
    }

def main():
    print("=" * 50)
    print("SOXX Data Collector (with Pre-Market Data & Day Return)")
    print("=" * 50)
    
    data = get_soxx_data()
    if not data:
        print("❌ Failed to fetch SOXX data")
        return
    
    print(f"Time: {data['timestamp']}")
    print(f"Session: {data['session']}")
    print(f"Price: ${data['price']}")
    print(f"Day Open: ${data['day_open']}")
    print(f"Day Return: {data['day_return_pct']}%")
    print(f"MA20: ${data['ma20']} (Above: {data['above_ma20']})")
    print(f"1h Return: {data['return_1h_pct']}%")
    print(f"RSI: {data['rsi']}")
    
    # Save to CSV
    new_row = pd.DataFrame([data])
    
    if LOG_PATH.exists():
        existing = pd.read_csv(LOG_PATH)
        # Keep last 4 days of data (~96 rows)
        updated = pd.concat([existing, new_row], ignore_index=True).tail(100)
    else:
        DATA_DIR.mkdir(parents=True, exist_ok=True)
        updated = new_row
    
    updated.to_csv(LOG_PATH, index=False)
    print(f"\n✅ Data saved to {LOG_PATH}")
    print(f"Total records: {len(updated)}")

if __name__ == "__main__":
    main()
