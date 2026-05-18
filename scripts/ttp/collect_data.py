#!/usr/bin/env python3
"""
Trade The Pool - SOXX Data Collector
Fetches SOXX data at scheduled times (10 AM, 1 PM, 3:30 PM ET)
"""

import yfinance as yf
import pandas as pd
from datetime import datetime
from pathlib import Path
import sys

# Paths
DATA_DIR = Path("data/ttp")
DATA_DIR.mkdir(parents=True, exist_ok=True)

TICKER = "SOXX"
HISTORY_DAYS = 30


def fetch_current_data():
    """Fetch current SOXX price and indicators"""
    ticker = yf.Ticker(TICKER)
    
    # Get intraday data for today (1-min intervals)
    today_data = ticker.history(period="1d", interval="1m")
    
    # Get daily data for indicators
    hist = ticker.history(period=f"{HISTORY_DAYS}d")
    
    current_price = today_data['Close'].iloc[-1] if not today_data.empty else 0
    current_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    
    # Calculate 1-hour return
    if len(today_data) >= 60:
        price_1h_ago = today_data['Close'].iloc[-60]
        return_1h = ((current_price - price_1h_ago) / price_1h_ago) * 100
    else:
        return_1h = 0
    
    # Calculate moving averages (daily)
    ma20 = hist['Close'].rolling(20).mean().iloc[-1] if len(hist) >= 20 else current_price
    ma50 = hist['Close'].rolling(50).mean().iloc[-1] if len(hist) >= 50 else current_price
    
    # Calculate volume
    avg_volume = hist['Volume'].rolling(20).mean().iloc[-1] if len(hist) >= 20 else 0
    current_volume = today_data['Volume'].sum() if not today_data.empty else 0
    volume_ratio = current_volume / avg_volume if avg_volume > 0 else 1
    
    # Calculate RSI (daily)
    delta = hist['Close'].diff()
    gain = delta.where(delta > 0, 0).rolling(window=14).mean()
    loss = (-delta.where(delta < 0, 0)).rolling(window=14).mean()
    rs = gain / loss
    rsi = 100 - (100 / (1 + rs)).iloc[-1] if len(rs) > 0 else 50
    
    # Determine session
    hour = datetime.now().hour
    if hour < 12:
        session = "morning"
    elif hour < 15:
        session = "midday"
    else:
        session = "afternoon"
    
    return {
        'timestamp': current_time,
        'session': session,
        'price': round(current_price, 2),
        'return_1h_pct': round(return_1h, 2),
        'ma20': round(ma20, 2),
        'ma50': round(ma50, 2),
        'above_ma20': current_price > ma20,
        'volume_ratio': round(volume_ratio, 2),
        'rsi': round(rsi, 1),
        'session_complete': session
    }


def save_data(data: dict):
    """Append to data log"""
    log_path = DATA_DIR / "price_log.csv"
    
    new_row = pd.DataFrame([{
        'timestamp': data['timestamp'],
        'session': data['session'],
        'price': data['price'],
        'return_1h_pct': data['return_1h_pct'],
        'ma20': data['ma20'],
        'ma50': data['ma50'],
        'above_ma20': data['above_ma20'],
        'volume_ratio': data['volume_ratio'],
        'rsi': data['rsi']
    }])
    
    if log_path.exists():
        existing = pd.read_csv(log_path)
        updated = pd.concat([existing, new_row], ignore_index=True)
    else:
        updated = new_row
    
    updated.to_csv(log_path, index=False)
    print(f"✅ Saved {data['session']} data: ${data['price']:.2f}")


def main():
    print("=" * 50)
    print("TTP SOXX Data Collector")
    print("=" * 50)
    
    data = fetch_current_data()
    print(f"Time: {data['timestamp']}")
    print(f"Session: {data['session']}")
    print(f"SOXX Price: ${data['price']:.2f}")
    print(f"1h Return: {data['return_1h_pct']:.2f}%")
    print(f"Above MA20: {data['above_ma20']}")
    print(f"Volume Ratio: {data['volume_ratio']:.2f}")
    print(f"RSI: {data['rsi']:.1f}")
    
    save_data(data)
    
    print("\n✅ Complete")


if __name__ == "__main__":
    main()
