#!/usr/bin/env python3
"""
Bear Market Backtest - Test inverse 3x ETF strategy on 2022 data
Rules: Shorter holds, tighter stops, stronger confirmation
"""

import yfinance as yf
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
from datetime import datetime

print("=" * 60)
print("BEAR MARKET BACKTEST - 2022 ONLY")
print("Testing SQQQ/SOXS with tight rules")
print("=" * 60)

# ============================================================
# DOWNLOAD 2022 DATA
# ============================================================
print("\n📥 Downloading 2022 data...")

tickers = ['QQQ', 'SQQQ', 'SOXS']
data = yf.download(tickers, start="2022-01-01", end="2022-12-31", group_by='ticker')

# Extract close prices
prices = {}
for ticker in tickers:
    if ticker in data.columns:
        prices[ticker] = data[ticker]['Close'].dropna()
    else:
        prices[ticker] = data[ticker]['Close']

df = pd.DataFrame(prices)
df = df.dropna()
print(f"✅ Data: {df.index[0].date()} to {df.index[-1].date()} ({len(df)} days)")

# ============================================================
# BEAR MARKET PARAMETERS (tighter than bull)
# ============================================================
BEAR_PARAMS = {
    'trailing_stop': 0.09,      # 9% stop (tighter than 12%)
    'min_score': 3.0,           # Stronger signal needed (was 2.0)
    'confirmation_days': 3,     # 3 days confirmation (was 2)
    'max_hold_days': 10,        # Max 10 days in position
    'position_size': 1.0,       # 100% of cash
}

# Calculate indicators for regime
df['QQQ_MA20'] = df['QQQ'].rolling(20).mean()
df['QQQ_MA50'] = df['QQQ'].rolling(50).mean()
df['QQQ_MA20_slope'] = df['QQQ_MA20'].diff() > 0

# Calculate daily returns for scoring
df['SQQQ_ret'] = df['SQQQ'].pct_change() * 100
df['SOXS_ret'] = df['SOXS'].pct_change() * 100

# 3-day and 5-day returns
df['SQQQ_ret_3d'] = df['SQQQ'].pct_change(3) * 100
df['SQQQ_ret_5d'] = df['SQQQ'].pct_change(5) * 100
df['SOXS_ret_3d'] = df['SOXS'].pct_change(3) * 100
df['SOXS_ret_5d'] = df['SOXS'].pct_change(5) * 100

# ============================================================
# SCORING FUNCTION
# ============================================================
def calculate_score(ret_1d, ret_3d, ret_5d):
    """Calculate score based on weighted returns"""
    weights = {'1d': 0.30, '3d': 0.25, '5d': 0.20, 'trend': 0.15, 'vol': 0.10}
    
    ret_1d = ret_1d if pd.notna(ret_1d) else 0
    ret_3d = ret_3d if pd.notna(ret_3d) else 0
    ret_5d = ret_5d if pd.notna(ret_5d) else 0
    
    score = (ret_1d * weights['1d'] + 
             ret_3d * weights['3d'] + 
             ret_5d * weights['5d'])
    
    # Trend strength (all returns positive = bullish, negative = bearish)
    if ret_1d > 0 and ret_3d > 0 and ret_5d > 0:
        score += 5 * weights['trend']
    elif ret_1d < 0 and ret_3d < 0 and ret_5d < 0:
        score -= 5 * weights['trend']
    
    return round(score, 4)

# ============================================================
# REGIME DETECTION
# ============================================================
def get_regime(row):
    """Return BEAR if QQQ is in confirmed downtrend"""
    if row['QQQ'] < row['QQQ_MA50'] and not row['QQQ_MA20_slope']:
        return "BEAR"
    return "CASH"

# ============================================================
# RUN BACKTEST
# ============================================================
print("\n🔄 Running backtest with bear parameters...")

start_cash = 10000
cash = start_cash
position = None
trade_log = []
equity_curve = []
regime_log = []
days_in_position = 0

for i in range(50, len(df) - 1):
    current = df.iloc[i]
    next_day = df.iloc[i + 1]
    date = df.index[i]
    
    regime = get_regime(current)
    regime_log.append(regime)
    
    # ===== EXIT CHECKS =====
    exit_signal = None
    
    if position:
        days_in_position += 1
        ticker = position['ticker']
        current_price = current[ticker]
        
        # For inverse ETFs, track lowest price
        if current_price < position.get('lowest_price', current_price):
            position['lowest_price'] = current_price
        trailing_stop = position['lowest_price'] * (1 + BEAR_PARAMS['trailing_stop'])
        
        # Check trailing stop
        if current_price >= trailing_stop:
            exit_signal = "trailing_stop"
            exit_price = min(trailing_stop, next_day[ticker])
        
        # Check max hold days
        elif days_in_position >= BEAR_PARAMS['max_hold_days']:
            exit_signal = "max_hold_days"
            exit_price = next_day[ticker]
        
        # Check regime change to CASH
        elif regime == "CASH":
            exit_signal = "regime_cash"
            exit_price = next_day[ticker]
        
        # Check score drop below threshold
        else:
            ret_1d = current[f'{ticker}_ret']
            ret_3d = current[f'{ticker}_ret_3d']
            ret_5d = current[f'{ticker}_ret_5d']
            current_score = calculate_score(ret_1d, ret_3d, ret_5d)
            
            if abs(current_score) < BEAR_PARAMS['min_score']:
                exit_signal = f"score_{current_score:.1f}"
                exit_price = next_day[ticker]
    
    # Execute exit
    if exit_signal and position:
        pl = (exit_price - position['entry']) * position['shares']
        ret_pct = ((exit_price / position['entry']) - 1) * 100
        trade_log.append({
            'entry_date': position['date'].strftime('%Y-%m-%d'),
            'exit_date': date.strftime('%Y-%m-%d'),
            'ticker': ticker,
            'return_pct': round(ret_pct, 2),
            'pl': round(pl, 2),
            'exit_reason': exit_signal,
            'days_held': days_in_position
        })
        cash += pl
        position = None
        days_in_position = 0
    
    # ===== ENTRY CHECKS =====
    if not position and regime == "BEAR":
        # Score both inverse ETFs
        scores = {}
        for etf in ['SQQQ', 'SOXS']:
            ret_1d = current[f'{etf}_ret']
            ret_3d = current[f'{etf}_ret_3d']
            ret_5d = current[f'{etf}_ret_5d']
            scores[etf] = calculate_score(ret_1d, ret_3d, ret_5d)
        
        best = max(scores, key=scores.get)
        best_score = scores[best]
        
        if best_score >= BEAR_PARAMS['min_score']:
            # Check confirmation days
            if i >= BEAR_PARAMS['confirmation_days']:
                confirm_scores = []
                for c in range(1, BEAR_PARAMS['confirmation_days'] + 1):
                    prev = df.iloc[i - c]
                    prev_ret_1d = prev[f'{best}_ret']
                    prev_ret_3d = prev[f'{best}_ret_3d']
                    prev_ret_5d = prev[f'{best}_ret_5d']
                    confirm_scores.append(calculate_score(prev_ret_1d, prev_ret_3d, prev_ret_5d))
                
                if all(s >= BEAR_PARAMS['min_score'] for s in confirm_scores):
                    entry_price = next_day[best]
                    shares = int(cash / entry_price)
                    if shares > 0:
                        position = {
                            'ticker': best,
                            'entry': entry_price,
                            'shares': shares,
                            'date': date,
                            'lowest_price': entry_price
                        }
                        print(f"  📈 ENTRY: {best} @ ${entry_price:.2f} ({shares} shares)")

    # Track equity
    if position:
        current_price = current[position['ticker']]
        equity = cash + (position['shares'] * current_price)
    else:
        equity = cash
    equity_curve.append(equity)

# ============================================================
# RESULTS
# ============================================================
print("\n" + "=" * 60)
print("BACKTEST RESULTS")
print("=" * 60)

final_equity = equity_curve[-1]
total_return = final_equity - start_cash
total_return_pct = (total_return / start_cash) * 100

print(f"\n📈 PERFORMANCE:")
print(f"   Starting Balance: ${start_cash:,.2f}")
print(f"   Final Balance:    ${final_equity:,.2f}")
print(f"   Total Return:     {total_return_pct:+.1f}% (${total_return:+,.2f})")

# Trade stats
if trade_log:
    winners = [t for t in trade_log if t['pl'] > 0]
    losers = [t for t in trade_log if t['pl'] < 0]
    win_rate = len(winners) / len(trade_log) * 100
    
    print(f"\n📊 TRADES:")
    print(f"   Total Trades:     {len(trade_log)}")
    print(f"   Winning Trades:   {len(winners)}")
    print(f"   Losing Trades:    {len(losers)}")
    print(f"   Win Rate:         {win_rate:.1f}%")
    
    if winners:
        avg_win = sum(t['return_pct'] for t in winners) / len(winners)
        print(f"   Avg Win:          {avg_win:+.1f}%")
    if losers:
        avg_loss = sum(t['return_pct'] for t in losers) / len(losers)
        print(f"   Avg Loss:         {avg_loss:+.1f}%")
    
    print(f"\n📜 RECENT TRADES:")
    for t in trade_log[-5:]:
        pl_symbol = "+" if t['pl'] >= 0 else ""
        print(f"   {t['entry_date']} → {t['exit_date']} | {t['ticker']} | {t['return_pct']:+.1f}% | {pl_symbol}${t['pl']:.0f} | {t['exit_reason']} ({t['days_held']} days)")
    
    # Save trades to CSV
    trades_df = pd.DataFrame(trade_log)
    trades_df.to_csv('bear_trades.csv', index=False)
    print(f"\n✅ Trade log saved to bear_trades.csv")
else:
    print("\n   No trades executed")

# Compare to sitting in cash
print(f"\n💰 COMPARE TO CASH:")
print(f"   Bear Strategy:    ${final_equity:,.2f} ({total_return_pct:+.1f}%)")
print(f"   Sitting in Cash:  ${start_cash:,.2f} (0%)")
print(f"   Difference:       ${final_equity - start_cash:+,.2f}")

# Cash percentage
cash_days = regime_log.count("CASH")
bear_days = regime_log.count("BEAR")
total_days = len(regime_log)
print(f"\n📊 REGIME BREAKDOWN:")
print(f"   Bear days:  {bear_days} ({bear_days/total_days*100:.1f}%)")
print(f"   Cash days:  {cash_days} ({cash_days/total_days*100:.1f}%)")

# Plot
fig, (ax1, ax2) = plt.subplots(2, 1, figsize=(12, 8))

# Equity curve
ax1.plot(df.index[50:len(equity_curve)+50], equity_curve, color='red', linewidth=1)
ax1.axhline(y=start_cash, color='gray', linestyle='--', alpha=0.5)
ax1.set_title('Bear Strategy Equity Curve (2022 Only)')
ax1.set_ylabel('Portfolio Value ($)')
ax1.grid(True, alpha=0.3)

# Drawdown
equity_series = pd.Series(equity_curve)
running_max = equity_series.cummax()
drawdown = (equity_series - running_max) / running_max * 100
max_dd = drawdown.min()

ax2.fill_between(df.index[50:len(drawdown)+50], 0, drawdown, color='red', alpha=0.3)
ax2.set_title(f'Drawdown (Max: {max_dd:.1f}%)')
ax2.set_ylabel('Drawdown (%)')
ax2.grid(True, alpha=0.3)

plt.tight_layout()
plt.savefig('bear_backtest_results.png', dpi=150, bbox_inches='tight')
print(f"\n✅ Chart saved to bear_backtest_results.png")

print("\n✅ Backtest complete!")
