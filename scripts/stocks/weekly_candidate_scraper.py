#!/usr/bin/env python3
"""
Weekly Stock Candidate Scraper
Runs on Saturdays, finds potential breakout candidates from free sources,
scores them using your breakout logic, and outputs a CSV for manual review.

No API keys required. Uses yfinance for price data and pandas for scoring.
"""

from __future__ import annotations

from datetime import datetime
from pathlib import Path
from typing import Dict, List, Optional, Set

import pandas as pd
import yfinance as yf


def get_candidate_tickers() -> List[str]:
    """Get a list of candidate tickers to score"""
    # List of actively traded stocks across sectors
    # This covers most high-probability candidates
    tickers = [
        # Mega cap
        "AAPL", "MSFT", "GOOGL", "AMZN", "NVDA", "META", "TSLA", "BRK-B", "UNH", "JNJ",
        "V", "PG", "JPM", "MA", "HD", "DIS", "BAC", "ADBE", "CRM", "NFLX",
        "PFE", "INTC", "CMCSA", "VZ", "T", "ABT", "NKE", "WFC", "KO", "MRK",
        "PEP", "TMO", "AVGO", "COST", "MCD", "CSCO", "ABBV", "DHR", "ACN", "LIN",
        "AMD", "IBM", "GE", "CVX", "WMT", "QCOM", "TXN", "AMGN", "NEE", "LOW",
        # Growth/emerging
        "PLTR", "SNOW", "UBER", "SHOP", "RBLX", "COIN", "SOFI", "NIO", "RIVN",
        "LCID", "FSLR", "ENPH", "SEDG", "RUN", "PLUG", "FCEL", "QS", "CHPT",
        "IONQ", "RKLB", "ASTS", "SPCE", "AI", "BBAI", "PATH", "DOCU", "OKTA",
        "TWLO", "FIVN", "ESTC", "GTLB", "DUOL", "APP", "TTD", "ROKU", "PINS",
        "SNAP", "U", "TTWO", "EA", "RNG", "ZM", "PTON", "CVNA", "CAR", "ABNB",
        "EXPE", "BKNG", "DASH", "ETSY", "CHWY", "BYND", "CELH", "ELF", "HIMS",
        "TDOC", "RXRX", "EXAS", "BEAM", "CRSP", "EDIT", "NTLA", "PACB", "ILMN",
        "GH", "VRTX", "MRNA", "BNTX", "NVAX", "ALNY", "REGN", "INCY", "ICLR",
        "IQV", "MEDP", "AXSM", "SAVA", "ADMA", "ARDX", "SRPT", "FATE", "NKLA",
        "WKHS", "MVST", "ENVX", "STEM", "FLNC", "AMPX", "CHRS", "KURA", "DNA",
        "TWST", "TXG", "WAL", "ZION", "CMA", "FITB", "KEY", "HBAN", "ALLY",
        "RKT", "UWMC", "OPEN", "RDFN", "STWD", "BXMT", "ABR", "NLY", "AGNC",
        "TWO", "ARR", "PMT", "DX", "CLSK", "MARA", "RIOT", "BITF", "HUT",
        "BTBT", "CAN", "CIFR", "IREN", "WULF", "GREE", "HIVE", "ARBK", "GLBE",
        "MELI", "SE", "JD", "BILI", "PDD", "TME", "NTES", "BIDU", "CSIQ", "JKS",
        "MAXN", "SPWR", "FSLY", "AKAM", "PANW", "FTNT", "CYBR", "TEAM", "NOW",
        "WDAY", "HUBS", "CRM", "ORCL", "ADBE", "INTU", "PYPL", "SQ"
    ]
    return tickers


def fetch_price_data(ticker: str, period: str = "1y") -> Optional[pd.DataFrame]:
    """Fetch historical price data for a ticker"""
    try:
        df = yf.download(ticker, period=period, interval="1d", progress=False, auto_adjust=False)
        if df.empty:
            return None
        if hasattr(df.columns, "nlevels") and df.columns.nlevels > 1:
            df.columns = df.columns.get_level_values(0)
        df = df.reset_index()
        df.columns = ["date", "open", "high", "low", "close", "volume"]
        return df
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


def main():
    print("Weekly Stock Candidate Scraper")
    print("=" * 50)
    
    # Load existing universe
    existing_tickers = load_existing_universe()
    print(f"Existing universe: {len(existing_tickers)} stocks")
    
    # Get candidate tickers
    print("Fetching candidate tickers...")
    candidates = get_candidate_tickers()
    print(f"Total candidates to check: {len(candidates)}")
    
    # Filter out existing ones
    new_candidates = [t for t in candidates if t not in existing_tickers]
    print(f"New candidates to score: {len(new_candidates)}")
    
    # Score each new candidate
    scored_results = []
    for i, ticker in enumerate(new_candidates):
        if i % 50 == 0:
            print(f"  Scoring {i}/{len(new_candidates)}...")
        
        df = fetch_price_data(ticker)
        if df is None or df.empty:
            continue
        
        scores = calculate_breakout_score(df)
        if scores["breakout_score"] >= 4:
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
    
    # Sort by score
    scored_results.sort(key=lambda x: x["breakout_score"], reverse=True)
    
    # Save to CSV
    output_path = Path("data/stocks/weekly_candidates.csv")
    output_path.parent.mkdir(parents=True, exist_ok=True)
    
    if scored_results:
        df = pd.DataFrame(scored_results)
        df.to_csv(output_path, index=False)
        print(f"\n✅ Saved {len(scored_results)} candidates to {output_path}")
        print("\n🏆 Top 10 Candidates:")
        for i, row in enumerate(df.head(10).to_dict('records')):
            print(f"  {i+1}. {row['ticker']} | Score: {row['breakout_score']} | {row['breakout_signal']} | ${row['current_price']:.2f}")
    else:
        print("\n⚠️ No new breakout candidates found this week.")
        pd.DataFrame(columns=["ticker", "breakout_score", "breakout_signal", "current_price", 
                               "ma_20", "ma_50", "compression_ratio", "vol_ratio", 
                               "proximity_20d", "extension_50ma", "date"]).to_csv(output_path, index=False)
    
    print("\nDone.")


if __name__ == "__main__":
    main()
