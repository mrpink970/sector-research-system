#!/usr/bin/env python3
"""
Stock Universe Validator
Checks every ticker in stock_universe.csv to verify:
- Ticker exists and has valid data
- Current price is available
- Trading volume > 0 (active)

Outputs:
- invalid_tickers.csv: List of tickers that failed validation
- valid_tickers.csv: List of tickers that passed
- summary_report.txt: Summary of findings
"""

import time
from pathlib import Path
from datetime import datetime

import pandas as pd
import yfinance as yf


def validate_ticker(ticker: str) -> dict:
    """
    Check if a ticker is valid and actively trading.
    Returns dict with status and details.
    """
    try:
        # Fetch ticker info
        stock = yf.Ticker(ticker)
        
        # Try to get current price from history
        hist = stock.history(period="5d")
        
        if hist.empty:
            return {
                "ticker": ticker,
                "status": "NO_DATA",
                "reason": "No historical data found",
                "current_price": None,
                "volume": None,
            }
        
        # Get latest data
        last_close = hist['Close'].iloc[-1]
        last_volume = hist['Volume'].iloc[-1]
        last_date = hist.index[-1].strftime("%Y-%m-%d")
        
        # Check if data is recent
        days_old = (datetime.now() - hist.index[-1]).days
        
        # Get additional info
        info = stock.info
        long_name = info.get('longName', 'N/A')
        market_cap = info.get('marketCap', 'N/A')
        
        # Determine status
        if days_old > 5:
            status = "STALE_DATA"
            reason = f"No data since {last_date} ({days_old} days ago)"
        elif last_volume == 0 or pd.isna(last_volume):
            status = "NO_VOLUME"
            reason = "Zero trading volume"
        elif last_close <= 0 or pd.isna(last_close):
            status = "INVALID_PRICE"
            reason = f"Invalid price: {last_close}"
        else:
            status = "VALID"
            reason = "OK"
        
        return {
            "ticker": ticker,
            "status": status,
            "reason": reason,
            "current_price": round(last_close, 2) if last_close else None,
            "volume": int(last_volume) if last_volume else None,
            "last_date": last_date,
            "long_name": long_name,
            "market_cap": market_cap,
        }
        
    except Exception as e:
        return {
            "ticker": ticker,
            "status": "ERROR",
            "reason": str(e)[:100],
            "current_price": None,
            "volume": None,
            "last_date": None,
            "long_name": "N/A",
            "market_cap": "N/A",
        }


def main():
    root = Path(".")
    universe_path = root / "data" / "stocks" / "stock_universe.csv"
    
    if not universe_path.exists():
        print(f"Error: {universe_path} not found")
        return
    
    # Load universe
    df = pd.read_csv(universe_path)
    tickers = df["ticker"].dropna().astype(str).str.upper().tolist()
    
    print("=" * 80)
    print("Stock Universe Validator")
    print(f"Checking {len(tickers)} tickers...")
    print("=" * 80)
    
    results = []
    valid_count = 0
    invalid_count = 0
    
    for i, ticker in enumerate(tickers):
        print(f"  Checking {i+1}/{len(tickers)}: {ticker}...", end=" ", flush=True)
        
        result = validate_ticker(ticker)
        results.append(result)
        
        if result["status"] == "VALID":
            valid_count += 1
            print(f"✅ VALID - ${result['current_price']}")
        else:
            invalid_count += 1
            print(f"❌ {result['status']} - {result['reason'][:50]}")
        
        # Be nice to Yahoo Finance
        time.sleep(0.2)
    
    # Convert to DataFrame
    results_df = pd.DataFrame(results)
    
    # Save all results
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    all_results_path = root / "data" / "stocks" / f"universe_validation_{timestamp}.csv"
    results_df.to_csv(all_results_path, index=False)
    print(f"\n✅ Full results saved to: {all_results_path}")
    
    # Save invalid tickers
    invalid_df = results_df[results_df["status"] != "VALID"]
    invalid_path = root / "data" / "stocks" / "invalid_tickers.csv"
    invalid_df.to_csv(invalid_path, index=False)
    print(f"✅ Invalid tickers saved to: {invalid_path}")
    
    # Save valid tickers
    valid_df = results_df[results_df["status"] == "VALID"]
    valid_path = root / "data" / "stocks" / "valid_tickers.csv"
    valid_df.to_csv(valid_path, index=False)
    print(f"✅ Valid tickers saved to: {valid_path}")
    
    # Summary report
    print("\n" + "=" * 80)
    print("SUMMARY REPORT")
    print("=" * 80)
    print(f"Total tickers checked: {len(tickers)}")
    print(f"✅ Valid: {valid_count}")
    print(f"❌ Invalid: {invalid_count}")
    print(f"  - NO_DATA: {len(results_df[results_df['status'] == 'NO_DATA'])}")
    print(f"  - STALE_DATA: {len(results_df[results_df['status'] == 'STALE_DATA'])}")
    print(f"  - NO_VOLUME: {len(results_df[results_df['status'] == 'NO_VOLUME'])}")
    print(f"  - INVALID_PRICE: {len(results_df[results_df['status'] == 'INVALID_PRICE'])}")
    print(f"  - ERROR: {len(results_df[results_df['status'] == 'ERROR'])}")
    
    # Show invalid tickers
    if invalid_count > 0:
        print("\n❌ INVALID TICKERS TO REMOVE:")
        for _, row in invalid_df.iterrows():
            print(f"  {row['ticker']} - {row['status']}: {row['reason'][:60]}")
    
    # Create a cleaned universe CSV
    cleaned_df = df[df["ticker"].isin(valid_df["ticker"])]
    cleaned_path = root / "data" / "stocks" / "stock_universe_cleaned.csv"
    cleaned_df.to_csv(cleaned_path, index=False)
    print(f"\n✅ Cleaned universe saved to: {cleaned_path}")
    print(f"   (Replace stock_universe.csv with this file after review)")


if __name__ == "__main__":
    main()
