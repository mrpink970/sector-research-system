from __future__ import annotations

#!/usr/bin/env python3
"""
Quantum Computing System - MINIMAL WORKING VERSION
"""

from datetime import datetime, timedelta
from pathlib import Path
import pandas as pd
import yfinance as yf

# Config
TICKERS = ['IONQ', 'QBTS', 'RGTI', 'QUBT', 'XNDU', 'INFQ', 'HQ']
START_BALANCE = 5000
MIN_SCORE = 2.0
TRAILING_STOP = 0.25

def fetch_prices():
    start = (datetime.now() - timedelta(days=60)).strftime("%Y-%m-%d")
    data = yf.download(TICKERS, start=start, progress=False)
    if data.empty:
        return pd.DataFrame()
    return data['Close']

def compute_score(prices):
    """Simple momentum score based on 1d, 3d, 5d returns"""
    ret_1d = prices.pct_change() * 100
    ret_3d = prices.pct_change(3) * 100
    ret_5d = prices.pct_change(5) * 100
    
    score = ret_1d * 0.30 + ret_3d * 0.25 + ret_5d * 0.20
    
    # Bonus for all positive
    mask = (ret_1d > 0) & (ret_3d > 0) & (ret_5d > 0)
    score += mask * 5 * 0.15
    
    # Penalty for all negative
    mask = (ret_1d < 0) & (ret_3d < 0) & (ret_5d < 0)
    score -= mask * 5 * 0.15
    
    return score.round(2)

def main():
    print("=" * 60)
    print("QUANTUM SYSTEM - MINIMAL")
    print("=" * 60)
    
    # Fetch data
    print("Fetching prices...")
    df = fetch_prices()
    if df.empty:
        print("No data")
        return
    
    print(f"Data: {df.index[0].date()} to {df.index[-1].date()}")
    
    # Compute scores
    scores = compute_score(df)
    
    # Get latest scores
    latest_scores = scores.iloc[-1].to_dict()
    sorted_scores = sorted(latest_scores.items(), key=lambda x: x[1], reverse=True)
    
    print("\n📊 LATEST SCORES:")
    for ticker, score in sorted_scores:
        print(f"   {ticker}: {score:.1f}")
    
    # Check if we should enter
    best_ticker, best_score = sorted_scores[0]
    print(f"\n🎯 Best: {best_ticker} with score {best_score:.1f}")
    
    if best_score >= MIN_SCORE:
        print(f"✅ WOULD ENTER {best_ticker}")
        latest_price = df[best_ticker].iloc[-1]
        shares = int(START_BALANCE / latest_price)
        print(f"   Entry: ${latest_price:.2f}, Shares: {shares}")
    else:
        print(f"❌ No entry (score {best_score:.1f} < {MIN_SCORE})")
    
    # Save results
    data_dir = Path("data/quantum")
    data_dir.mkdir(parents=True, exist_ok=True)
    
    scores_df = scores.reset_index()
    scores_df.to_csv(data_dir / "quantum_scores.csv", index=False)
    print(f"\n✅ Scores saved to data/quantum/quantum_scores.csv")

if __name__ == "__main__":
    main()
