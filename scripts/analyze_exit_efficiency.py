#!/usr/bin/env python3
"""
Exit Efficiency Analysis for Stock System
Analyzes trade logs to identify exit improvements
"""

import pandas as pd
import numpy as np
from pathlib import Path

def load_trade_log(path: Path, system_name: str):
    """Load trade log for a system"""
    if not path.exists():
        print(f"⚠️ No trade log found for {system_name}: {path}")
        return pd.DataFrame()
    
    df = pd.read_csv(path)
    if df.empty:
        print(f"⚠️ Empty trade log for {system_name}")
        return pd.DataFrame()
    
    df['system'] = system_name
    return df

def analyze_exits(df: pd.DataFrame, system_name: str):
    """Analyze exit patterns for a system"""
    if df.empty:
        return {}
    
    winners = df[df['gross_pnl'] > 0]
    losers = df[df['gross_pnl'] < 0]
    
    # Hold time analysis
    avg_hold_winners = winners['duration_days'].mean() if not winners.empty else 0
    avg_hold_losers = losers['duration_days'].mean() if not losers.empty else 0
    
    # Exit reason breakdown
    exit_reasons = df['exit_reason'].value_counts()
    
    # Calculate potential givebacks (for trailing stop exits, how much more could have been captured)
    givebacks = []
    for _, row in df.iterrows():
        if row['exit_reason'] == 'trailing_stop' and row['gross_pnl'] > 0:
            # Estimate potential giveback if stopped earlier/later
            actual_return = row['return_pct']
            givebacks.append({
                'ticker': row['ticker'],
                'entry_date': row['entry_date'],
                'exit_date': row['exit_date'],
                'actual_return': actual_return,
                'exit_reason': row['exit_reason']
            })
    
    return {
        'system': system_name,
        'total_trades': len(df),
        'winners': len(winners),
        'losers': len(losers),
        'win_rate': len(winners) / len(df) * 100 if len(df) > 0 else 0,
        'avg_win_pct': winners['return_pct'].mean() if not winners.empty else 0,
        'avg_loss_pct': abs(losers['return_pct'].mean()) if not losers.empty else 0,
        'avg_hold_winners': avg_hold_winners,
        'avg_hold_losers': avg_hold_losers,
        'exit_reasons': exit_reasons,
        'giveback_candidates': givebacks
    }

def main():
    data_dir = Path("data/stocks")
    
    print("=" * 70)
    print("EXIT EFFICIENCY ANALYSIS - STOCK SYSTEM")
    print("=" * 70)
    
    # Load trade logs
    trend_log = load_trade_log(data_dir / "trend_trade_log.csv", "trend")
    breakout_log = load_trade_log(data_dir / "breakout_trade_log.csv", "breakout")
    
    # Combine if both exist
    if trend_log.empty and breakout_log.empty:
        print("\n❌ No trade logs found. Run the stock system first.")
        return
    
    all_trades = pd.concat([trend_log, breakout_log], ignore_index=True)
    
    # Analyze combined
    total_trades = len(all_trades)
    if total_trades == 0:
        print("\n❌ No trades found in the logs.")
        return
    
    winners = all_trades[all_trades['gross_pnl'] > 0]
    losers = all_trades[all_trades['gross_pnl'] < 0]
    
    win_rate = len(winners) / total_trades * 100
    avg_win = winners['return_pct'].mean() if not winners.empty else 0
    avg_loss = abs(losers['return_pct'].mean()) if not losers.empty else 0
    profit_factor = abs(winners['gross_pnl'].sum() / losers['gross_pnl'].sum()) if not losers.empty else 999
    
    print(f"\n📊 OVERALL STATISTICS")
    print(f"   Total Trades: {total_trades}")
    print(f"   Win Rate: {win_rate:.1f}%")
    print(f"   Avg Win: +{avg_win:.1f}%")
    print(f"   Avg Loss: -{avg_loss:.1f}%")
    print(f"   Profit Factor: {profit_factor:.2f}")
    print(f"   Risk/Reward Ratio: {avg_win/avg_loss:.2f}" if avg_loss > 0 else "")
    
    # Exit reason analysis
    print(f"\n📋 EXIT REASONS")
    exit_reasons = all_trades['exit_reason'].value_counts()
    for reason, count in exit_reasons.items():
        pct = count / total_trades * 100
        print(f"   {reason}: {count} ({pct:.0f}%)")
    
    # Hold time analysis
    print(f"\n⏱️ HOLD TIMES")
    avg_hold_winners = winners['duration_days'].mean() if not winners.empty else 0
    avg_hold_losers = losers['duration_days'].mean() if not losers.empty else 0
    print(f"   Avg hold (winners): {avg_hold_winners:.1f} days")
    print(f"   Avg hold (losers): {avg_hold_losers:.1f} days")
    
    # Holdings under 5 days analysis
    short_holds = all_trades[all_trades['duration_days'] <= 5]
    short_pct = len(short_holds) / total_trades * 100
    print(f"\n📆 SHORT HOLDINGS (≤ 5 days)")
    print(f"   {len(short_holds)} trades ({short_pct:.0f}%)")
    
    # Potential improvement: wider stops
    print(f"\n💡 POTENTIAL IMPROVEMENTS")
    
    # Check if losers would have become winners with wider stops
    if not losers.empty:
        loser_losses = losers['return_pct'].abs()
        avg_loss_pct = loser_losses.mean()
        print(f"\n   1. Current avg loss: -{avg_loss_pct:.1f}%")
        print(f"      → Consider wider initial stop (currently 10%)")
    
    # Check if short holds are cutting winners early
    short_winners = winners[winners['duration_days'] <= 5]
    if not short_winners.empty:
        short_win_pct = len(short_winners) / len(winners) * 100
        print(f"\n   2. {len(short_winners)} winners ({short_win_pct:.0f}%) held ≤5 days")
        print(f"      → Consider longer min hold period")
    
    # Check 2-day confirmation impact
    print(f"\n   3. Current 2-day confirmation may delay entry")
    print(f"      → Test removing confirmation for faster entry")
    
    # Save summary
    summary = pd.DataFrame([{
        'total_trades': total_trades,
        'win_rate_pct': round(win_rate, 1),
        'avg_win_pct': round(avg_win, 1),
        'avg_loss_pct': round(avg_loss, 1),
        'profit_factor': round(profit_factor, 2),
        'avg_hold_winners_days': round(avg_hold_winners, 1),
        'avg_hold_losers_days': round(avg_hold_losers, 1),
        'exit_reasons': str(exit_reasons.to_dict())
    }])
    
    summary.to_csv(data_dir / "exit_efficiency_summary.csv", index=False)
    print(f"\n✅ Analysis saved to: {data_dir}/exit_efficiency_summary.csv")
    
    # Print recent trades
    print(f"\n📜 RECENT TRADES (last 10)")
    recent = all_trades.sort_values('exit_date', ascending=False).head(10)
    for _, trade in recent.iterrows():
        pnl_symbol = "+" if trade['gross_pnl'] >= 0 else ""
        print(f"   {trade['exit_date'][:10]} | {trade['ticker']:5} | {trade['return_pct']:+6.1f}% | {pnl_symbol}${abs(trade['gross_pnl']):8.2f} | {trade['exit_reason']}")

if __name__ == "__main__":
    main()
