#!/usr/bin/env python3
"""
Trade The Pool - Trade Manager
Tracks trade progress and updates profit toward target
"""

import pandas as pd
from pathlib import Path
from datetime import datetime
import sys

DATA_DIR = Path("data/ttp")
TRADES_PATH = DATA_DIR / "trades.csv"
PROGRESS_PATH = DATA_DIR / "progress.csv"


def load_trades():
    """Load existing trades"""
    if TRADES_PATH.exists():
        df = pd.read_csv(TRADES_PATH)
        return df.to_dict('records') if not df.empty else []
    return []


def save_trade(trade: dict):
    """Save a completed trade"""
    trades = load_trades()
    trades.append(trade)
    df = pd.DataFrame(trades)
    df.to_csv(TRADES_PATH, index=False)
    update_progress()
    print(f"✅ Trade saved: {trade['profit']}")


def update_progress():
    """Update progress toward $300 target"""
    trades = load_trades()
    completed = [t for t in trades if t.get('status') == 'completed']
    
    total_profit = sum(t.get('profit', 0) for t in completed)
    trades_completed = len(completed)
    
    progress = {
        'timestamp': datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        'total_profit': total_profit,
        'trades_completed': trades_completed,
        'remaining_to_target': max(0, 300 - total_profit),
        'percent_complete': (total_profit / 300) * 100
    }
    
    df = pd.DataFrame([progress])
    df.to_csv(PROGRESS_PATH, index=False)
    print(f"📊 Progress: ${total_profit:.2f} / $300 ({progress['percent_complete']:.0f}%)")


def close_trade(ticker: str, exit_price: float, exit_date: str = None):
    """Close an open trade and record profit"""
    trades = load_trades()
    
    # Find open trade
    for trade in trades:
        if trade.get('status') == 'open' and trade.get('ticker') == ticker:
            trade['exit_price'] = exit_price
            trade['exit_date'] = exit_date or datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            profit = (exit_price - trade['entry_price']) * trade['shares']
            trade['profit'] = round(profit, 2)
            trade['status'] = 'completed'
            break
    
    df = pd.DataFrame(trades)
    df.to_csv(TRADES_PATH, index=False)
    update_progress()
    print(f"🔴 Trade closed: {ticker} @ ${exit_price:.2f}, Profit: ${profit:.2f}")


def add_open_trade(ticker: str, entry_price: float, shares: int, stop_price: float, target_price: float):
    """Record a new open trade"""
    trades = load_trades()
    
    new_trade = {
        'ticker': ticker,
        'entry_date': datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        'entry_price': entry_price,
        'shares': shares,
        'stop_price': stop_price,
        'target_price': target_price,
        'status': 'open'
    }
    trades.append(new_trade)
    
    df = pd.DataFrame(trades)
    df.to_csv(TRADES_PATH, index=False)
    print(f"🟢 Trade opened: {shares} shares {ticker} @ ${entry_price:.2f}")


if __name__ == "__main__":
    # This is a utility module, not meant to be run directly
    print("Trade Manager Module - Import this into your main script")
    print("Functions: add_open_trade(), close_trade(), update_progress()")
