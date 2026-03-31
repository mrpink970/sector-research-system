#!/usr/bin/env python3
"""
Weekly Stock Candidate Scraper (Finviz Version)
Fetches stocks from a Finviz screener URL, scores them, and outputs candidates.
"""

from __future__ import annotations

from datetime import datetime
from pathlib import Path
from typing import Dict, List, Optional, Set

import pandas as pd
import requests
import yfinance as yf


# ============================================================
# CONFIGURATION
# ============================================================

FINVIZ_URL = "https://finviz.com/screener.ashx?v=111&f=sh_curvol_o300%2Csh_price_o3%2Csh_relvol_o1.5%2Cta_highlow20d_nh%2Cta_rsi_ob60%2Cta_sma50_pa&ft=3"

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.5",
}


# ============================================================
# FINVIZ SCRAPER
# ============================================================

def fetch_finviz_tickers(url: str) -> List[str]:
    """Fetch tickers from Finviz screener page."""
    try:
        print(f"Fetching Finviz screener...")
        response = requests.get(url, headers=HEADERS, timeout=30)
        
        if response.status_code != 200:
            print(f"  Failed: HTTP {response.status_code}")
            return []
        
        tables = pd.read_html(response.text)
        if not tables:
            print("  No tables found")
            return []
        
        df = tables[0]
        
        # Find ticker column
        ticker_col = None
        for col in df.columns:
            if "ticker" in str(col).lower() or "symbol" in str(col).lower():
                ticker_col = col
                break
        
        if ticker_col is None:
            ticker_col = df.columns[0]
        
        tickers = df[ticker_col].dropna().tolist()
        tickers = [str(t).strip().upper() for t in tickers]
        tickers = [t for t in tickers if t and t.isalpha() and len(t) <= 5]
        
        print(f"  Found {len(tickers)} tickers")
        return tickers
        
    except Exception as e:
        print(f"  Error: {e}")
        return []


# ============================================================
# PRICE DATA & SCORING
# ============================================================

def fetch_price_data(ticker: str, period: str = "1y") -> Optional[pd.DataFrame]:
    """Fetch historical price data for a ticker"""
    try:
        df = yf.download(ticker, period=period, interval="1d", progress=False, auto_adjust=False)
        
        if df.empty:
            return None
        
        # Handle multi-index columns
        if hasattr(df.columns, "nlevels") and df.columns.nlevels > 1:
            df.columns = df.columns.get_level_values(0)
        
        df = df.reset_index()
        
        # Standardize column names
        df.columns = [col.lower().replace(' ', '_') for col in df.columns]
        
        # Map to expected names
        result = pd.DataFrame()
        
        if 'date' in df.columns:
            result['date'] = df['date']
        elif 'datetime' in df.columns:
            result['date'] = df['datetime']
        else:
            # First column is likely date
            result['date'] = df.iloc[:, 0]
        
        # Map price columns
        for price_col in ['open', 'high', 'low', 'close', 'volume']:
            if price_col in df.columns:
                result[price_col] = df[price_col]
            elif price_col.upper() in df.columns:
                result[price_col] = df[price_col.upper()]
        
        # Handle adj_close if needed
        if 'adj_close' in df.columns:
            result['adj_close'] = df['adj_close']
        elif 'adj close' in df.columns:
            result['adj_close'] = df['adj close']
        else:
            result['adj_close'] = result['close']
        
        # Ensure all required columns exist
        required = ['date', 'open', 'high', 'low', 'close', 'volume']
        missing = [c for c in required if c not in result.columns]
        if missing:
            print(f"  Missing columns {missing} for {ticker}")
            return None
        
        # Convert date
        result['date'] = pd.to_datetime(result['date']).dt.strftime('%Y-%m-%d')
        
        # Convert to numeric
        for col in ['open', 'high', 'low', 'close', 'volume']:
            result[col] = pd.to_numeric(result[col], errors='coerce')
        
        return result
        
    except Exception as e:
        print(f"  Error fetching {ticker}: {e}")
        return None


def calculate_breakout_score(df: pd.DataFrame) -> Dict:
    """Calculate breakout score for a stock"""
    if df.empty or len(df) < 50:
        return {"breakout_score": 0, "breakout_signal": "Insufficient Data"}
    
    close = df["close"]
    volume = df["volume"]
    high = df["high"]
    
    current_close = close.iloc[-1]
    
    # ATR compression
    tr = (df["high"] - df["low"]).rolling(14).mean()
    atr_14 = tr.iloc[-1]
    atr_50 = tr.rolling(50).mean().iloc[-1] if len(df) > 50 else atr_14
    compression_ratio = atr_14 / atr_50 if atr_50 > 0 else 1.0
    
    # Volume ratio
    avg_vol_5 = volume.rolling(5).mean().iloc[-1]
    avg_vol_20 = volume.rolling(20).mean().iloc[-1]
    vol_ratio = avg_vol_5 / avg_vol_20 if avg_vol_20 > 0 else 1.0
    
    # Proximity to highs
    high_20 = high.rolling(20).max().iloc[-1]
    high_50 = high.rolling(50).max().iloc[-1]
    proximity_20 = (current_close / high_20) - 1 if high_20 > 0 else 0
    proximity_50 = (current_close / high_50) - 1 if high_50 > 0 else 0
    
    # Moving averages
    ma_20 = close.rolling(20).mean().iloc[-1]
    ma_50 = close.rolling(50).mean().iloc[-1]
    
    # Extension
    extension_50ma = (current_close - ma_50) / ma_50 if ma_50 > 0 else 1.0
    extension_20ma = (current_close - ma_20) / ma_20 if ma_20 > 0 else 1.0
    
    # Calculate score (max 10)
    score = 0
    
    if compression_ratio < 0.85:
        score += 2
    elif compression_ratio < 0.95:
        score += 1
    
    if vol_ratio >= 1.3:
        score += 2
    elif vol_ratio >= 1.1:
        score += 1
    
    if proximity_20 >= -0.02:
        score += 2
    elif proximity_50 >= -0.02:
        score += 1
    
    if extension_50ma <= 0.10 and extension_20ma <= 0.05:
        score += 2
    elif extension_50ma <= 0.15 and extension_20ma <= 0.10:
        score += 1
    
    if current_close > ma_50 and ma_20 > ma_50:
        score += 1
    
    if score >= 6:
        signal = "Strong Breakout Candidate"
    elif score >= 4:
        signal = "Breakout Watch"
    elif score >= 2:
        signal = "Weak Setup"
    else:
        signal = "No Setup"
    
    return {
        "breakout_score": score,
        "breakout_signal": signal,
        "current_price": round(float(current_close), 2),
        "ma_20": round(float(ma_20), 2),
        "ma_50": round(float(ma_50), 2),
        "compression_ratio": round(float(compression_ratio), 3),
        "vol_ratio": round(float(vol_ratio), 2),
        "proximity_20d": round(float(proximity_20 * 100), 1),
        "extension_50ma": round(float(extension_50ma * 100), 1),
    }


def load_existing_universe() -> Set[str]:
    """Load existing tickers from your stock universe"""
    universe_path = Path("data/stocks/stock_universe.csv")
    if not universe_path.exists():
        return set()
    
    df = pd.read_csv(universe_path)
    return set(df["ticker"].str.upper().tolist())


# ============================================================
# MAIN
# ============================================================

def main():
    print("Weekly Stock Candidate Scraper (Finviz Version)")
    print("=" * 50)
    
    # Load existing universe
    existing_tickers = load_existing_universe()
    print(f"Existing universe: {len(existing_tickers)} stocks")
    
    # Fetch tickers from Finviz
    print("\nFetching tickers from Finviz...")
    finviz_tickers = fetch_finviz_tickers(FINVIZ_URL)
    
    if not finviz_tickers:
        print("\n⚠️ No tickers found from Finviz.")
        print("   Using fallback static list.")
        
        fallback_tickers = [
            "RDDT", "ARM", "CRWD", "DDOG", "MDB", "ZS", "NET", "SNOW", "PANW",
            "FTNT", "TEAM", "NOW", "WDAY", "HUBS", "INTU", "ADSK", "ANET", "CDNS",
            "SNPS", "MRVL", "ON", "MPWR", "MCHP", "ADI", "LRCX", "KLAC", "AMAT"
        ]
        finviz_tickers = [t for t in fallback_tickers if t not in existing_tickers]
        print(f"  Using {len(finviz_tickers)} fallback candidates")
    
    # Filter out existing ones
    new_candidates = [t for t in finviz_tickers if t not in existing_tickers]
    print(f"\nNew candidates to score: {len(new_candidates)}")
    
    if not new_candidates:
        print("\n⚠️ No new candidates found.")
        output_path = Path("data/stocks/weekly_candidates.csv")
        output_path.parent.mkdir(parents=True, exist_ok=True)
        pd.DataFrame(columns=["ticker", "breakout_score", "breakout_signal", "current_price", 
                               "ma_20", "ma_50", "compression_ratio", "vol_ratio", 
                               "proximity_20d", "extension_50ma", "date"]).to_csv(output_path, index=False)
        print("Created empty weekly_candidates.csv")
        return
    
    # Score each candidate
    scored_results = []
    for i, ticker in enumerate(new_candidates):
        if i % 10 == 0:
            print(f"  Scoring {i}/{len(new_candidates)}...")
        
        df = fetch_price_data(ticker)
        if df is None or df.empty:
            continue
        
        scores = calculate_breakout_score(df)
        
        if scores["breakout_score"] >= 2:
            scored_results.append({
                "ticker": ticker,
                "breakout_score": scores["breakout_score"],
                "breakout_signal": scores["breakout_signal"],
                "current_price": scores["current_price"],
                "ma_20": scores["ma_20"],
                "ma_50": scores["ma_50"],
                "compression_ratio": scores["compression_ratio"],
                "vol_ratio": scores["vol_ratio"],
                "proximity_20d": scores["proximity_20d"],
                "extension_50ma": scores["extension_50ma"],
                "date": datetime.now().strftime("%Y-%m-%d"),
            })
    
    # Sort and save
    scored_results.sort(key=lambda x: x["breakout_score"], reverse=True)
    
    output_path = Path("data/stocks/weekly_candidates.csv")
    output_path.parent.mkdir(parents=True, exist_ok=True)
    
    if scored_results:
        df = pd.DataFrame(scored_results)
        df.to_csv(output_path, index=False)
        print(f"\n✅ Saved {len(scored_results)} candidates")
        print("\n🏆 Top 10:")
        for i, row in enumerate(df.head(10).to_dict('records')):
            print(f"  {i+1}. {row['ticker']} | Score: {row['breakout_score']} | {row['breakout_signal']} | ${row['current_price']:.2f}")
    else:
        print("\n⚠️ No candidates with score >= 2 found.")
        pd.DataFrame(columns=["ticker", "breakout_score", "breakout_signal", "current_price", 
                               "ma_20", "ma_50", "compression_ratio", "vol_ratio", 
                               "proximity_20d", "extension_50ma", "date"]).to_csv(output_path, index=False)
    
    print("\nDone.")


if __name__ == "__main__":
    main()
