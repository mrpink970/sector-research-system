#!/usr/bin/env python3

import pandas as pd
import yaml
import yfinance as yf
from pathlib import Path
from datetime import datetime

OUTPUT_FILE = "data/market_data.csv"
LOOKBACK_PERIOD = "2y"


def load_sector_map():
    with open("config/sector_map.yaml", "r") as f:
        return yaml.safe_load(f)


def get_all_tickers(config):
    tickers = set()

    for s in config["sectors"]:
        if s.get("signal_etf"):
            tickers.add(s["signal_etf"])
        if s.get("bull_etf"):
            tickers.add(s["bull_etf"])
        if s.get("bear_etf"):
            if s["bear_etf"] != "":
                tickers.add(s["bear_etf"])
        if s.get("benchmark"):
            tickers.add(s["benchmark"])

    return sorted(tickers)


def fetch_history(ticker):
    """Fetch historical data for a single ticker"""
    try:
        # Remove progress and threads parameters - they cause issues in newer yfinance
        df = yf.download(
            ticker,
            period=LOOKBACK_PERIOD,
            interval="1d",
            auto_adjust=False,
        )

        if df.empty:
            print(f"   No data returned for {ticker}")
            return None

        # Handle MultiIndex columns if present
        if hasattr(df.columns, "nlevels") and df.columns.nlevels > 1:
            df.columns = df.columns.get_level_values(0)

        df = df.reset_index()

        out = pd.DataFrame({
            "date": pd.to_datetime(df["Date"]).dt.strftime("%Y-%m-%d"),
            "ticker": ticker,
            "open": df["Open"].round(4),
            "high": df["High"].round(4),
            "low": df["Low"].round(4),
            "close": df["Close"].round(4),
            "volume": df["Volume"].astype(int),
        })

        return out
    except Exception as e:
        print(f"   Error fetching {ticker}: {e}")
        return None


def main():
    print("=" * 50)
    print("Collecting Market Data for Sector System")
    print("=" * 50)
    
    config = load_sector_map()
    tickers = get_all_tickers(config)
    
    print(f"Tickers to fetch: {len(tickers)}")
    print(f"Tickers: {tickers}")
    print()
    
    all_data = []

    for ticker in tickers:
        print(f"Fetching {ticker}...")
        df = fetch_history(ticker)

        if df is None or df.empty:
            print(f"   ❌ No data for {ticker}")
            continue
        
        print(f"   ✅ {len(df)} rows (${df['close'].iloc[-1]:.2f})")
        all_data.append(df)

    if not all_data:
        print("❌ No market data collected")
        return

    final = pd.concat(all_data, ignore_index=True)
    final = final.sort_values(["ticker", "date"]).reset_index(drop=True)

    # Ensure directory exists
    Path("data").mkdir(exist_ok=True)
    
    final.to_csv(OUTPUT_FILE, index=False)
    
    print()
    print("=" * 50)
    print(f"✅ Wrote {len(final)} rows to {OUTPUT_FILE}")
    print(f"   Date range: {final['date'].min()} to {final['date'].max()}")
    print(f"   Unique tickers: {final['ticker'].nunique()}")
    print("=" * 50)


if __name__ == "__main__":
    main()
