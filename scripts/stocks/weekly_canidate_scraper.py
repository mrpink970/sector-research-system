#!/usr/bin/env python3
"""
Weekly Stock Candidate Scraper
Runs on Saturdays, finds potential breakout candidates from free sources,
scores them using your breakout logic, and outputs a CSV for manual review.

No API keys required. Uses yfinance for price data and pandas for scoring.
"""

from __future__ import annotations

from datetime import datetime, timedelta
from pathlib import Path
from typing import Dict, List, Optional, Set

import pandas as pd
import yfinance as yf


def get_top_gainers() -> List[str]:
    """Get top gainers from Yahoo Finance using yfinance screener"""
    try:
        # Yahoo Finance does not have a direct screener API, but we can get tickers from ETFs
        # This is a fallback using major ETFs to capture market movers
        
        # Alternative: use web scraping (more fragile) or use a pre-defined list of liquid stocks
        # For now, we'll use a combination of major indices and ETFs
        
        # Major index components (free, via yfinance)
        spy = yf.Ticker("SPY")
        try:
            # Get components from S&P 500 (requires yfinance >= 0.2.0)
            sp500_tickers = yf.Ticker("^GSPC").components
            if sp500_tickers:
                return list(sp500_tickers)[:200]  # Limit to 200 for performance
        except:
            pass
            
        # Fallback: Use a pre-defined list of liquid stocks
        # These are the most actively traded stocks across exchanges
        liquid_stocks = [
            "AAPL", "MSFT", "GOOGL", "AMZN", "NVDA", "META", "TSLA", "BRK-B", "UNH", "JNJ",
            "V", "PG", "JPM", "MA", "HD", "DIS", "BAC", "ADBE", "CRM", "NFLX",
            "PFE", "INTC", "CMCSA", "VZ", "T", "ABT", "NKE", "WFC", "KO", "MRK",
            "PEP", "TMO", "AVGO", "COST", "MCD", "CSCO", "ABBV", "DHR", "ACN", "LIN",
            "AMD", "IBM", "GE", "CVX", "WMT", "QCOM", "TXN", "AMGN", "NEE", "LOW",
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
        return liquid_stocks
    except Exception as e:
        print(f"Error getting top gainers: {e}")
        return []


def get_etf_components(etf: str) -> List[str]:
    """Get ETF components using yfinance"""
    try:
        ticker = yf.Ticker(etf)
        holdings = ticker.get_holdings()
        if holdings is not None and not holdings.empty:
            return holdings.index.tolist()
    except:
        pass
    return []


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
        print(f"Error fetching {ticker}: {e}")
        return None


def calculate_breakout_score(df: pd.DataFrame) -> Dict[str, float]:
    """Calculate breakout score for a stock based on available data"""
    if df.empty or len(df) < 50:
        return {"breakout_score": 0, "reason": "Insufficient data"}
    
    close = df["close"]
    volume = df["volume"]
    high = df["high"]
    
    # Get most recent values
    current_close = close.iloc[-1]
    
    # Calculate metrics
    atr_14 = (df["high"] - df["low"]).rolling(14).mean().iloc[-1]
    atr_50 = (df["high"] - df["low"]).rolling(50).mean().iloc[-1] if len(df) > 50 else atr_14
    compression_ratio = atr_14 / atr_50 if atr_50 > 0 else 1.0
    
    # Volume metrics
    avg_vol_5 = volume.rolling(5).mean().iloc[-1]
    avg_vol_20 = volume.rolling(20).mean().iloc[-1]
    vol_ratio = avg_vol_5 / avg_vol_20 if avg_vol_20 > 0 else 1.0
    
    # Price proximity to highs
    high_20 = high.rolling(20).max().iloc[-1]
    high_50 = high.rolling(50).max().iloc[-1]
    proximity_20 = (current_close / high_20) - 1 if high_20 > 0 else 0
    proximity_50 = (current_close / high_50) - 1 if high_50 > 0 else 0
    
    # Moving averages
    ma_20 = close.rolling(20).mean().iloc[-1]
    ma_50 = close.rolling(50).mean().iloc[-1]
    
    # Extension from MA
    extension_50ma = (current_close - ma_50) / ma_50 if ma_50 > 0 else 1.0
    extension_20ma = (current_close - ma_20) / ma_20 if ma_20 > 0 else 1.0
    
    # Calculate breakout score (simplified version of your system)
    score = 0
    
    # Volatility compression
    if compression_ratio < 0.85:
        score += 2
    elif compression_ratio < 0.95:
        score += 1
    
    # Volume expansion
    if vol_ratio >= 1.3:
        score += 2
    elif vol_ratio >= 1.1:
        score += 1
    
    # Price proximity
    if proximity_20 >= -0.02:
        score += 2
    elif proximity_50 >= -0.02:
        score += 1
    
    # Low extension (not extended)
    if extension_50ma <= 0.10 and extension_20ma <= 0.05:
        score += 2
    elif extension_50ma <= 0.15 and extension_20ma <= 0.10:
        score += 1
    
    # Price structure (above MAs)
    if current_close > ma_50 and ma_20 > ma_50:
        score += 1
    
    # Determine signal
    if score >= 6:
        signal = "Strong Breakout Candidate"
    elif score >= 4:
        signal = "Breakout Watch"
    else:
        signal = "No Setup"
    
    return {
        "breakout_score": score,
        "breakout_signal": signal,
        "compression_ratio": round(compression_ratio, 3),
        "vol_ratio": round(vol_ratio, 2),
        "proximity_to_20d_high": round(proximity_20 * 100, 1),
        "proximity_to_50d_high": round(proximity_50 * 100, 1),
        "extension_50ma": round(extension_50ma * 100, 1),
        "extension_20ma": round(extension_20ma * 100, 1),
        "close": current_close,
        "ma_20": round(ma_20, 2),
        "ma_50": round(ma_50, 2),
    }


def load_existing_universe() -> Set[str]:
    """Load existing tickers from your stock universe to avoid duplicates"""
    universe_path = Path("data/stocks/stock_universe.csv")
    if not universe_path.exists():
        return set()
    
    df = pd.read_csv(universe_path)
    return set(df["ticker"].str.upper().tolist())


def main():
    print("Weekly Stock Candidate Scraper")
    print("=" * 50)
    
    # Load existing universe to avoid duplicates
    existing_tickers = load_existing_universe()
    print(f"Existing universe: {len(existing_tickers)} stocks")
    
    # Get candidate tickers from various sources
    print("Fetching candidate tickers...")
    candidates = get_top_gainers()
    print(f"Found {len(candidates)} candidate tickers")
    
    # Filter out existing ones
    new_candidates = [t for t in candidates if t not in existing_tickers]
    print(f"New candidates to score: {len(new_candidates)}")
    
    # Score each new candidate
    scored_results = []
    for i, ticker in enumerate(new_candidates):
        if i % 20 == 0:
            print(f"Scoring {i}/{len(new_candidates)}...")
        
        df = fetch_price_data(ticker)
        if df is None or df.empty:
            continue
        
        scores = calculate_breakout_score(df)
        if scores["breakout_score"] >= 4:  # Only keep Watch and Strong candidates
            scored_results.append({
                "ticker": ticker,
                "breakout_score": scores["breakout_score"],
                "breakout_signal": scores["breakout_signal"],
                "current_price": scores["close"],
                "ma_20": scores["ma_20"],
                "ma_50": scores["ma_50"],
                "compression_ratio": scores["compression_ratio"],
                "vol_ratio": scores["vol_ratio"],
                "proximity_20d": scores["proximity_to_20d_high"],
                "extension_50ma": scores["extension_50ma"],
                "date": datetime.now().strftime("%Y-%m-%d"),
            })
    
    # Sort by score
    scored_results.sort(key=lambda x: x["breakout_score"], reverse=True)
    
    # Save to CSV
    output_path = Path("data/stocks/weekly_candidates.csv")
    if scored_results:
        df = pd.DataFrame(scored_results)
        df.to_csv(output_path, index=False)
        print(f"\nSaved {len(scored_results)} candidates to {output_path}")
        print("\nTop 10 Candidates:")
        for i, row in enumerate(df.head(10).to_dict('records')):
            print(f"  {i+1}. {row['ticker']} | Score: {row['breakout_score']} | {row['breakout_signal']} | ${row['current_price']:.2f}")
    else:
        print("\nNo new breakout candidates found this week.")
        # Create empty file with headers
        pd.DataFrame(columns=["ticker", "breakout_score", "breakout_signal", "current_price", 
                               "ma_20", "ma_50", "compression_ratio", "vol_ratio", 
                               "proximity_20d", "extension_50ma", "date"]).to_csv(output_path, index=False)
    
    print("\nDone.")


if __name__ == "__main__":
    main()
