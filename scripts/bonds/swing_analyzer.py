#!/usr/bin/env python3
"""
Baby Bond Swing Analyzer
Analyzes 1-year price history for all tickers in the universe to identify swing candidates.
Run once to build a curated watchlist.
"""

import yfinance as yf
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from pathlib import Path
import time


def load_universe(file_path: Path) -> pd.DataFrame:
    """Load the baby bond universe CSV"""
    if not file_path.exists():
        print(f"Error: {file_path} not found")
        return pd.DataFrame()
    
    df = pd.read_csv(file_path)
    print(f"Loaded {len(df)} bonds from universe")
    return df


def find_swings(prices: pd.Series) -> dict:
    """
    Identify swing highs and lows in a price series.
    Returns swing metrics.
    """
    if len(prices) < 50:
        return {"num_swings": 0, "avg_swing_pct": 0, "swing_highs": [], "swing_lows": []}
    
    # Find peaks and troughs using a 5-day lookback window
    # A peak is a point where price is higher than 5 days before and after
    # A trough is a point where price is lower than 5 days before and after
    lookback = 5
    peaks = []
    troughs = []
    
    for i in range(lookback, len(prices) - lookback):
        window = prices.iloc[i-lookback:i+lookback+1]
        if prices.iloc[i] == window.max():
            peaks.append((prices.index[i], prices.iloc[i]))
        if prices.iloc[i] == window.min():
            troughs.append((prices.index[i], prices.iloc[i]))
    
    # Merge consecutive peaks/troughs (keep the highest/lowest in a cluster)
    merged_peaks = []
    merged_troughs = []
    
    for i, (date, price) in enumerate(peaks):
        if i == 0 or (date - merged_peaks[-1][0]).days > 10:
            merged_peaks.append((date, price))
        elif price > merged_peaks[-1][1]:
            merged_peaks[-1] = (date, price)
    
    for i, (date, price) in enumerate(troughs):
        if i == 0 or (date - merged_troughs[-1][0]).days > 10:
            merged_troughs.append((date, price))
        elif price < merged_troughs[-1][1]:
            merged_troughs[-1] = (date, price)
    
    # Calculate swing sizes (from trough to next peak)
    swings = []
    for i in range(min(len(merged_troughs), len(merged_peaks))):
        trough = merged_troughs[i]
        # Find the next peak after this trough
        next_peaks = [p for p in merged_peaks if p[0] > trough[0]]
        if next_peaks:
            peak = next_peaks[0]
            swing_pct = ((peak[1] - trough[1]) / trough[1]) * 100
            if swing_pct > 0:
                swings.append(swing_pct)
    
    # Also calculate swings from peak to next trough
    for i in range(min(len(merged_peaks), len(merged_troughs))):
        peak = merged_peaks[i]
        next_troughs = [t for t in merged_troughs if t[0] > peak[0]]
        if next_troughs:
            trough = next_troughs[0]
            swing_pct = ((trough[1] - peak[1]) / peak[1]) * 100
            if swing_pct < 0:
                swings.append(abs(swing_pct))
    
    avg_swing = np.mean(swings) if swings else 0
    num_swings = len(swings)
    
    return {
        "num_swings": num_swings,
        "avg_swing_pct": round(avg_swing, 1),
        "swing_highs": merged_peaks,
        "swing_lows": merged_troughs,
    }


def fetch_and_analyze(ticker: str, row: dict) -> dict:
    """Fetch 1-year data and analyze swings"""
    try:
        # Fetch 1 year of daily data
        hist = yf.download(ticker, period="1y", interval="1d", progress=False, auto_adjust=False)
        
        if hist.empty or len(hist) < 50:
            return None
        
        # Handle multi-index columns
        if hasattr(hist.columns, "nlevels") and hist.columns.nlevels > 1:
            hist.columns = hist.columns.get_level_values(0)
        
        # Get close prices
        close = hist['Close']
        
        # Calculate 52-week range
        high_52w = hist['High'].max()
        low_52w = hist['Low'].min()
        current_price = close.iloc[-1]
        
        # Calculate range width
        range_width = ((high_52w - low_52w) / low_52w) * 100 if low_52w > 0 else 0
        
        # Calculate position in range
        if high_52w > low_52w:
            position = ((current_price - low_52w) / (high_52w - low_52w)) * 100
        else:
            position = 50
        
        # Find swings
        swing_data = find_swings(close)
        
        # Calculate current yield for bonds/preferreds
        current_yield = None
        type_val = row.get('type', '')
        coupon = row.get('coupon', 0)
        par = row.get('par_value', 25)
        
        if type_val in ['baby_bond', 'preferred', 'corporate'] and coupon > 0 and current_price > 0:
            current_yield = round((coupon * par) / current_price, 1)
        
        # For BDCs, get dividend yield from yfinance if available
        if type_val == 'bdc':
            info = yf.Ticker(ticker).info
            dividend_yield = info.get('dividendYield', None)
            if dividend_yield:
                current_yield = round(dividend_yield * 100, 1)
        
        # Volume
        avg_volume = hist['Volume'].mean()
        
        return {
            'ticker': ticker,
            'type': type_val,
            'description': row.get('description', ''),
            'coupon': row.get('coupon', 0),
            'current_price': round(current_price, 2),
            'low_52w': round(low_52w, 2),
            'high_52w': round(high_52w, 2),
            'range_width_pct': round(range_width, 1),
            'position_pct': round(position, 1),
            'num_swings': swing_data['num_swings'],
            'avg_swing_pct': swing_data['avg_swing_pct'],
            'current_yield_pct': current_yield,
            'avg_volume': int(avg_volume),
            'is_swing_candidate': swing_data['num_swings'] >= 3 and swing_data['avg_swing_pct'] >= 15,
        }
        
    except Exception as e:
        print(f"  Error analyzing {ticker}: {e}")
        return None


def main():
    repo_root = Path(".")
    universe_file = repo_root / "data" / "bonds" / "baby_bond_universe.csv"
    output_file = repo_root / "data" / "bonds" / "swing_analysis.csv"
    
    print("=" * 80)
    print("Baby Bond Swing Analyzer")
    print("Analyzing 1-year price history to identify swing candidates")
    print("=" * 80)
    
    # Load universe
    universe = load_universe(universe_file)
    if universe.empty:
        return
    
    # Filter to active bonds only
    active = universe[universe['status'] == 'active']
    print(f"Active bonds to analyze: {len(active)}")
    print("-" * 80)
    
    # Analyze each ticker
    results = []
    failed = []
    
    for idx, row in active.iterrows():
        ticker = row['ticker']
        print(f"Analyzing {ticker}...", end=" ", flush=True)
        
        result = fetch_and_analyze(ticker, row)
        if result:
            results.append(result)
            print(f"OK - Swings: {result['num_swings']}, Avg Swing: {result['avg_swing_pct']}%")
        else:
            failed.append(ticker)
            print("FAILED")
        
        # Be nice to Yahoo Finance
        time.sleep(0.5)
    
    # Create DataFrame
    if results:
        df = pd.DataFrame(results)
        
        # Sort by swing quality
        df = df.sort_values(['num_swings', 'avg_swing_pct'], ascending=[False, False])
        
        # Save full results
        df.to_csv(output_file, index=False)
        print(f"\n✅ Full analysis saved to: {output_file}")
        
        # Filter for swing candidates
        swing_candidates = df[df['is_swing_candidate'] == True].copy()
        swing_candidates = swing_candidates.sort_values(['num_swings', 'avg_swing_pct'], ascending=[False, False])
        
        print("\n" + "=" * 80)
        print(f"SWING CANDIDATES (≥3 swings, avg swing ≥15%): {len(swing_candidates)}")
        print("=" * 80)
        
        if not swing_candidates.empty:
            # Show key columns
            display_cols = ['ticker', 'type', 'current_price', 'low_52w', 'high_52w', 
                           'range_width_pct', 'position_pct', 'num_swings', 'avg_swing_pct', 
                           'current_yield_pct', 'avg_volume']
            print(swing_candidates[display_cols].head(30).to_string(index=False))
        
        # Also show high yield candidates (yield > 7%)
        high_yield = df[df['current_yield_pct'] > 7].copy()
        high_yield = high_yield.sort_values('current_yield_pct', ascending=False)
        
        print("\n" + "=" * 80)
        print(f"HIGH YIELD CANDIDATES (Yield > 7%): {len(high_yield)}")
        print("=" * 80)
        
        if not high_yield.empty:
            print(high_yield[['ticker', 'type', 'current_price', 'current_yield_pct', 
                              'num_swings', 'avg_swing_pct', 'position_pct']].head(20).to_string(index=False))
        
        # Create a curated watchlist
        watchlist = swing_candidates.copy()
        # Add high yield candidates that didn't make swing list
        high_yield_extra = high_yield[~high_yield['ticker'].isin(swing_candidates['ticker'])].head(10)
        watchlist = pd.concat([watchlist, high_yield_extra])
        watchlist = watchlist.drop_duplicates(subset=['ticker'])
        
        watchlist_file = repo_root / "data" / "bonds" / "swing_watchlist.csv"
        watchlist.to_csv(watchlist_file, index=False)
        print(f"\n✅ Curated watchlist saved to: {watchlist_file}")
        print(f"   Watchlist size: {len(watchlist)} tickers")
        
        print("\n" + "=" * 80)
        print("STATISTICS")
        print("=" * 80)
        print(f"Total bonds in universe: {len(universe)}")
        print(f"Active bonds: {len(active)}")
        print(f"Successfully analyzed: {len(results)}")
        print(f"Failed: {len(failed)}")
        if failed:
            print(f"Failed tickers (first 20): {', '.join(failed[:20])}")
            if len(failed) > 20:
                print(f"... and {len(failed) - 20} more")
        
    else:
        print("\n❌ No data fetched successfully.")


if __name__ == "__main__":
    main()
