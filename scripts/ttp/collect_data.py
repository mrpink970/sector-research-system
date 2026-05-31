#!/usr/bin/env python3
"""
Trade The Pool - SOXX + QQQ Data Collector
Collects SOXX and QQQ data at 15-minute intervals
SIMPLIFIED: Removed pre-market volume due to Yahoo API limits
Uses 5-minute intervals for current data to avoid rate limiting
"""

import yfinance as yf
import pandas as pd
import numpy as np
from pathlib import Path
from datetime import datetime, time, timedelta

DATA_DIR = Path("data/ttp")
LOG_PATH = DATA_DIR / "price_log.csv"

MARKET_OPEN_HOUR = 9
MARKET_OPEN_MINUTE = 30

REQUIRED_COLUMNS = [
    'timestamp', 'session',
    'soxx_price', 'soxx_day_open', 'soxx_day_return_pct', 'soxx_ma20', 'soxx_above_ma20',
    'soxx_return_1h_pct', 'soxx_rsi', 'soxx_volume', 'soxx_avg_volume_20', 'soxx_volume_ratio',
    'qqq_price', 'qqq_day_open', 'qqq_day_return_pct', 'qqq_ma20', 'qqq_above_ma20',
    'qqq_return_1h_pct', 'qqq_rsi'
]


def migrate_existing_csv():
    """Add missing columns to existing price_log.csv without deleting data"""
    if not LOG_PATH.exists():
        return
    
    df = pd.read_csv(LOG_PATH)
    original_columns = set(df.columns)
    required_columns = set(REQUIRED_COLUMNS)
    
    missing_columns = required_columns - original_columns
    
    if missing_columns:
        print(f"📋 Migrating: Adding {len(missing_columns)} columns")
        for col in missing_columns:
            df[col] = None
        df.to_csv(LOG_PATH, index=False)
        print(f"✅ Added: {', '.join(sorted(missing_columns))}")
    else:
        print(f"✅ CSV has all required columns")


def get_day_open_price(ticker_symbol):
    """Get today's opening price at 9:30 AM ET"""
    try:
        ticker = yf.Ticker(ticker_symbol)
        # Use 5m interval to avoid 1m rate limits
        data = ticker.history(period="2d", interval="5m", prepost=False)
        
        if data is None or data.empty:
            return None
        
        data.index = pd.to_datetime(data.index).tz_localize(None)
        today = datetime.now().date()
        
        for idx, row in data.iterrows():
            if idx.date() == today and idx.hour >= MARKET_OPEN_HOUR:
                return round(float(row['Open']), 2)
        
        yesterday = today - timedelta(days=1)
        yesterday_data = data[data.index.date == yesterday]
        if not yesterday_data.empty:
            return round(float(yesterday_data['Close'].iloc[-1]), 2)
        
        return None
        
    except Exception as e:
        print(f"   Error getting day open for {ticker_symbol}: {e}")
        return None


def get_ticker_data(ticker_symbol):
    """Get current data for a single ticker"""
    try:
        ticker = yf.Ticker(ticker_symbol)
        
        # Get current price using 5m interval (bypasses 1m rate limit)
        current = ticker.history(period="1d", interval="5m", prepost=True)
        if current.empty:
            # Fallback to 15m if 5m fails
            current = ticker.history(period="1d", interval="15m", prepost=True)
            if current.empty:
                return None
        
        latest = current.iloc[-1]
        price = round(latest['Close'], 2)
        current_volume = int(latest['Volume']) if not pd.isna(latest['Volume']) else 0
        
        # Get day open price
        day_open = get_day_open_price(ticker_symbol)
        
        # Calculate day return
        day_return_pct = 0
        if day_open and day_open > 0:
            day_return_pct = round((price - day_open) / day_open * 100, 2)
        
        # Get historical data for indicators (15m interval, last 5 days)
        hist = ticker.history(period="5d", interval="15m", prepost=True)
        if hist.empty or len(hist) < 5:
            # Fallback to without pre-market
            hist = ticker.history(period="5d", interval="15m", prepost=False)
        
        if hist.empty:
            return None
        
        hist.index = pd.to_datetime(hist.index).tz_localize(None)
        
        # Calculate MA20
        ma20 = round(hist['Close'].rolling(window=20).mean().iloc[-1], 2) if len(hist) >= 20 else price
        
        # Calculate average volume (last 10 periods to avoid zeros)
        volume_series = hist['Volume'][hist['Volume'] > 0]
        if len(volume_series) >= 10:
            avg_volume = int(volume_series.rolling(window=10).mean().iloc[-1])
        elif len(hist) >= 10:
            avg_volume = int(hist['Volume'].rolling(window=10).mean().iloc[-1])
        else:
            avg_volume = current_volume if current_volume > 0 else 1
        
        volume_ratio = round(current_volume / avg_volume, 1) if avg_volume > 0 else 1.0
        
        # Calculate 1-hour return (4 x 15min candles)
        if len(hist) >= 5:
            hour_ago = hist['Close'].iloc[-5]
            return_1h = round((price - hour_ago) / hour_ago * 100, 2)
        else:
            return_1h = 0
        
        # Calculate RSI
        delta = hist['Close'].diff()
        gain = (delta.where(delta > 0, 0)).rolling(window=14).mean()
        loss = (-delta.where(delta < 0, 0)).rolling(window=14).mean()
        rs = gain / loss
        rsi = round(100 - (100 / (1 + rs.iloc[-1])), 1) if len(hist) >= 14 and not pd.isna(rs.iloc[-1]) else 50
        
        above_ma20 = price > ma20
        
        return {
            'price': price,
            'day_open': day_open,
            'day_return_pct': day_return_pct,
            'ma20': ma20,
            'above_ma20': above_ma20,
            'return_1h_pct': return_1h,
            'rsi': rsi,
            'volume': current_volume,
            'avg_volume_20': avg_volume,
            'volume_ratio': volume_ratio
        }
    except Exception as e:
        print(f"   Error fetching {ticker_symbol}: {e}")
        return None


def get_soxx_qqq_data():
    """Get current data for both SOXX and QQQ"""
    soxx_data = get_ticker_data("SOXX")
    qqq_data = get_ticker_data("QQQ")
    
    if not soxx_data or not qqq_data:
        return None
    
    now = datetime.now()
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
        'soxx_price': soxx_data['price'],
        'soxx_day_open': soxx_data['day_open'],
        'soxx_day_return_pct': soxx_data['day_return_pct'],
        'soxx_ma20': soxx_data['ma20'],
        'soxx_above_ma20': soxx_data['above_ma20'],
        'soxx_return_1h_pct': soxx_data['return_1h_pct'],
        'soxx_rsi': soxx_data['rsi'],
        'soxx_volume': soxx_data['volume'],
        'soxx_avg_volume_20': soxx_data['avg_volume_20'],
        'soxx_volume_ratio': soxx_data['volume_ratio'],
        'qqq_price': qqq_data['price'],
        'qqq_day_open': qqq_data['day_open'],
        'qqq_day_return_pct': qqq_data['day_return_pct'],
        'qqq_ma20': qqq_data['ma20'],
        'qqq_above_ma20': qqq_data['above_ma20'],
        'qqq_return_1h_pct': qqq_data['return_1h_pct'],
        'qqq_rsi': qqq_data['rsi']
    }


def main():
    print("=" * 50)
    print("SOXX + QQQ Data Collector (Simplified)")
    print("=" * 50)
    
    migrate_existing_csv()
    
    data = get_soxx_qqq_data()
    if not data:
        print("❌ Failed to fetch data")
        return
    
    print(f"\n📊 Data collected at {data['timestamp']}")
    print(f"   Session: {data['session']}")
    print(f"   SOXX: ${data['soxx_price']} | Day: {data['soxx_day_return_pct']}% | RSI: {data['soxx_rsi']}")
    print(f"   SOXX Volume: {data['soxx_volume']:,} | Ratio: {data['soxx_volume_ratio']}x")
    print(f"   QQQ:  ${data['qqq_price']} | Day: {data['qqq_day_return_pct']}% | RSI: {data['qqq_rsi']}")
    
    new_row = pd.DataFrame([data])
    
    if LOG_PATH.exists():
        existing = pd.read_csv(LOG_PATH)
        updated = pd.concat([existing, new_row], ignore_index=True).tail(500)
    else:
        DATA_DIR.mkdir(parents=True, exist_ok=True)
        updated = new_row
    
    updated.to_csv(LOG_PATH, index=False)
    print(f"\n✅ Data saved to {LOG_PATH}")


if __name__ == "__main__":
    main()
