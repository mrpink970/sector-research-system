#!/usr/bin/env python3
"""
Trade The Pool - Trade Manager
Tracks trade progress and updates profit toward target
Includes TTP evaluation status checking
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
    print(f"✅ Trade saved: ${trade.get('profit', 0):.2f}")


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
        'percent_complete': (total_profit / 300) * 100 if total_profit > 0 else 0
    }
    
    df = pd.DataFrame([progress])
    df.to_csv(PROGRESS_PATH, index=False)
    print(f"📊 Progress: ${total_profit:.2f} / $300 ({progress['percent_complete']:.0f}%)")


def close_trade(ticker: str, exit_price: float, exit_date: str = None, exit_reason: str = ""):
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
            trade['exit_reason'] = exit_reason
            break
    
    df = pd.DataFrame(trades)
    df.to_csv(TRADES_PATH, index=False)
    update_progress()
    print(f"🔴 Trade closed: {ticker} @ ${exit_price:.2f}, Profit: ${profit:.2f}, Reason: {exit_reason}")


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


def check_ready_for_review() -> dict:
    """Check if evaluation is ready for TTP review"""
    trades = load_trades()
    completed = [t for t in trades if t.get('status') == 'completed']
    
    total_profit = sum(t.get('profit', 0) for t in completed)
    trades_completed = len(completed)
    
    profit_target = 300
    min_trades = 5
    
    ready = total_profit >= profit_target and trades_completed >= min_trades
    
    return {
        'ready_for_review': ready,
        'total_profit': round(total_profit, 2),
        'profit_target': profit_target,
        'trades_completed': trades_completed,
        'min_trades_required': min_trades,
        'profit_remaining': round(max(0, profit_target - total_profit), 2),
        'trades_needed': max(0, min_trades - trades_completed)
    }


def print_review_status():
    """Print TTP review status"""
    status = check_ready_for_review()
    
    print("\n" + "=" * 50)
    print("TTP EVALUATION STATUS")
    print("=" * 50)
    print(f"  Profit: ${status['total_profit']:.2f} / ${status['profit_target']}")
    print(f"  Trades: {status['trades_completed']} / {status['min_trades_required']}")
    
    if status['ready_for_review']:
        print("\n  ✅ READY FOR TTP REVIEW!")
        print("  Close all positions and request evaluation completion")
    else:
        if status['profit_remaining'] > 0:
            print(f"\n  ⏳ Need ${status['profit_remaining']:.2f} more profit")
        if status['trades_needed'] > 0:
            print(f"  ⏳ Need {status['trades_needed']} more trades")
    
    print("=" * 50)


def get_performance_summary() -> dict:
    """Get overall performance metrics"""
    trades = load_trades()
    completed = [t for t in trades if t.get('status') == 'completed']
    
    if not completed:
        return {
            'total_trades': 0,
            'winning_trades': 0,
            'losing_trades': 0,
            'win_rate': 0,
            'total_profit': 0,
            'avg_winner': 0,
            'avg_loser': 0,
            'largest_winner': 0,
            'largest_loser': 0
        }
    
    winners = [t for t in completed if t.get('profit', 0) > 0]
    losers = [t for t in completed if t.get('profit', 0) <= 0]
    
    win_rate = len(winners) / len(completed) * 100 if completed else 0
    total_profit = sum(t.get('profit', 0) for t in completed)
    
    return {
        'total_trades': len(completed),
        'winning_trades': len(winners),
        'losing_trades': len(losers),
        'win_rate': round(win_rate, 1),
        'total_profit': round(total_profit, 2),
        'avg_winner': round(sum(t.get('profit', 0) for t in winners) / len(winners), 2) if winners else 0,
        'avg_loser': round(sum(abs(t.get('profit', 0)) for t in losers) / len(losers), 2) if losers else 0,
        'largest_winner': round(max(t.get('profit', 0) for t in winners), 2) if winners else 0,
        'largest_loser': round(min(t.get('profit', 0) for t in losers), 2) if losers else 0
    }


if __name__ == "__main__":
    # Run status check when executed directly
    print_review_status()
    print("\n" + "=" * 50)
    print("PERFORMANCE SUMMARY")
    print("=" * 50)
    perf = get_performance_summary()
    for k, v in perf.items():
        print(f"  {k}: {v}")
