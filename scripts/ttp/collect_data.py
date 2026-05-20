#!/usr/bin/env python3
"""
Trade The Pool - SOXX Data Collector
Collects SOXX data at 5-minute intervals for fast breakout detection
"""

import yfinance as yf
import pandas as pd
from pathlib import Path
from datetime import datetime
import time

DATA_DIR = Path("data/ttp")
LOG_PATH = DATA_DIR / "price_log.csv"

def get_soxx_data():
    """Get current SOXX data including technical indicators"""
    ticker = yf.Ticker("SOXX")
    
    # Get current price and volume
    current = ticker.history(period="1d", interval="1m")
    if current.empty:
        return None
    
    latest = current.iloc[-1]
    price = round(latest['Close'], 2)
    volume = int(latest['Volume'])
    
    # Get historical data for indicators
    hist = ticker.history(period="2d", interval="5m")
    if hist.empty:
        return None
    
    # Calculate 20-period MA (20 * 5min = 1.67 hours, ~100 minutes)
    ma20 = round(hist['Close'].rolling(window=20).mean().iloc[-1], 2)
    
    # Calculate 1-hour return (12 x 5min candles)
    if len(hist) >= 13:
        hour_ago = hist['Close'].iloc[-13]
        return_1h = round((price - hour_ago) / hour_ago * 100, 2)
    else:
        return_1h = 0
    
    # Calculate volume ratio (compare to 20-period average)
    avg_volume = hist['Volume'].rolling(window=20).mean().iloc[-1]
    volume_ratio = round(volume / avg_volume, 2) if avg_volume > 0 else 1.0
    
    # Calculate RSI (14-period on 5-min candles = 70 minutes)
    delta = hist['Close'].diff()
    gain = (delta.where(delta > 0, 0)).rolling(window=14).mean()
    loss = (-delta.where(delta < 0, 0)).rolling(window=14).mean()
    rs = gain / loss
    rsi = round(100 - (100 / (1 + rs.iloc[-1])), 1) if not pd.isna(rs.iloc[-1]) else 50
    
    # Determine session
    now = datetime.now()
    hour = now.hour
    minute = now.minute
    
    if hour < 12:
        session = "morning"
    elif hour < 15:
        session = "afternoon"
    else:
        session = "late"
    
    return {
        'timestamp': now.strftime("%Y-%m-%d %H:%M:%S"),
        'session': session,
        'price': price,
        'ma20': ma20,
        'above_ma20': price > ma20,
        'return_1h_pct': return_1h,
        'volume_ratio': volume_ratio,
        'rsi': rsi,
        'volume': volume
    }

def main():
    print("=" * 50)
    print("SOXX Data Collector (5-min intervals)")
    print("=" * 50)
    
    data = get_soxx_data()
    if not data:
        print("❌ Failed to fetch SOXX data")
        return
    
    print(f"Time: {data['timestamp']}")
    print(f"Session: {data['session']}")
    print(f"Price: ${data['price']}")
    print(f"MA20: ${data['ma20']} (Above: {data['above_ma20']})")
    print(f"1h Return: {data['return_1h_pct']}%")
    print(f"Volume Ratio: {data['volume_ratio']}")
    print(f"RSI: {data['rsi']}")
    
    # Save to CSV
    new_row = pd.DataFrame([data])
    
    if LOG_PATH.exists():
        existing = pd.read_csv(LOG_PATH)
        # Keep last 24 hours of 5-min data (288 rows)
        updated = pd.concat([existing, new_row], ignore_index=True).tail(300)
    else:
        DATA_DIR.mkdir(parents=True, exist_ok=True)
        updated = new_row
    
    updated.to_csv(LOG_PATH, index=False)
    print(f"\n✅ Data saved to {LOG_PATH}")
    print(f"Total records: {len(updated)}")

if __name__ == "__main__":
    main()
