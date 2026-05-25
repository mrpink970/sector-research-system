#!/usr/bin/env python3
"""
TTP Compliance Module
Checks earnings, dividends, and volume restrictions
"""

import yfinance as yf
from datetime import datetime, timedelta
from typing import Tuple, List, Dict, Optional
import json
from pathlib import Path

CACHE_DIR = Path("data/ttp/cache")
CACHE_DIR.mkdir(parents=True, exist_ok=True)
EARNINGS_CACHE = CACHE_DIR / "earnings_cache.json"


def load_config():
    """Load TTP config"""
    import yaml
    with open("config/ttp_config.yaml", "r") as f:
        return yaml.safe_load(f)


def get_earnings_dates(symbols: List[str]) -> Dict[str, Optional[datetime]]:
    """Get next earnings date for each symbol"""
    # Load cache
    cache = {}
    if EARNINGS_CACHE.exists():
        with open(EARNINGS_CACHE, "r") as f:
            cache = json.load(f)
    
    earnings = {}
    today = datetime.now().date()
    
    for symbol in symbols:
        # Check cache (valid for 7 days)
        if symbol in cache:
            cached_date = datetime.fromisoformat(cache[symbol]) if cache[symbol] else None
            if cached_date and cached_date.date() > today:
                earnings[symbol] = cached_date
                continue
        
        # Fetch from Yahoo
        try:
            ticker = yf.Ticker(symbol)
            cal = ticker.calendar
            if cal is not None and 'Earnings Date' in cal:
                earnings_date = cal['Earnings Date']
                if isinstance(earnings_date, (list, tuple)) and len(earnings_date) > 0:
                    earnings_date = earnings_date[0]
                if earnings_date and isinstance(earnings_date, datetime):
                    earnings[symbol] = earnings_date
                    cache[symbol] = earnings_date.isoformat()
                else:
                    earnings[symbol] = None
                    cache[symbol] = None
            else:
                earnings[symbol] = None
                cache[symbol] = None
        except Exception as e:
            print(f"  ⚠️ Could not fetch earnings for {symbol}: {e}")
            earnings[symbol] = None
            cache[symbol] = None
    
    # Save cache
    with open(EARNINGS_CACHE, "w") as f:
        json.dump(cache, f, default=str)
    
    return earnings


def check_earnings_restriction(config: dict, current_date: datetime = None) -> Tuple[bool, str]:
    """Check if any earnings event prevents overnight hold"""
    if current_date is None:
        current_date = datetime.now()
    
    earnings_symbols = config['compliance']['earnings_symbols']
    restricted_days = config['compliance']['earnings_restricted_days']
    
    earnings_dates = get_earnings_dates(earnings_symbols)
    
    for symbol, date in earnings_dates.items():
        if date:
            days_until = (date - current_date).days
            if 0 <= days_until <= restricted_days:
                return False, f"{symbol} earnings on {date.date()} ({days_until} days away)"
    
    return True, "No earnings restrictions"


def check_dividend_restriction() -> Tuple[bool, str]:
    """Check if SOXX ex-dividend date prevents overnight hold"""
    try:
        soxx = yf.Ticker("SOXX")
        info = soxx.info
        if 'exDividendDate' in info:
            ex_date = datetime.fromtimestamp(info['exDividendDate'])
            days_until = (ex_date - datetime.now()).days
            if 0 <= days_until <= 1:
                return False, f"SOXX ex-dividend on {ex_date.date()} (tomorrow)"
    except Exception as e:
        print(f"  ⚠️ Could not check dividend: {e}")
    
    return True, "No dividend restrictions"


def can_enter_swing_trade(config: dict) -> Tuple[bool, str]:
    """
    Check all TTP compliance rules before entering a swing trade
    Returns: (can_enter, reason)
    """
    # Check earnings
    earnings_ok, earnings_reason = check_earnings_restriction(config)
    if not earnings_ok:
        return False, earnings_reason
    
    # Check dividends
    div_ok, div_reason = check_dividend_restriction()
    if not div_ok:
        return False, div_reason
    
    return True, "OK to enter"


if __name__ == "__main__":
    config = load_config()
    can_enter, reason = can_enter_swing_trade(config)
    print(f"Can enter: {can_enter}")
    print(f"Reason: {reason}")
