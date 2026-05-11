#!/usr/bin/env python3
"""
Quantum Computing System - Pure play quantum stocks + 2026 IPOs
Single position, 25% trailing stop, momentum scoring
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta
from pathlib import Path
from typing import Dict, List, Optional, Tuple

import pandas as pd
import yaml
import yfinance as yf


@dataclass
class Position:
    ticker: str
    entry_date: str
    entry_price: float
    shares: int
    highest_price: float
    stop_pct: float
    trailing_stop: float
    entry_score: float


def load_config() -> dict:
    config_path = Path("config/quantum_parameters.yaml")
    with open(config_path, "r", encoding="utf-8") as f:
        return yaml.safe_load(f)


def fetch_market_data(tickers: List[str]) -> pd.DataFrame:
    """Fetch daily OHLC data for all tickers plus QQQ"""
    all_tickers = list(set(tickers + ["QQQ"]))
    start_date = (datetime.now() - timedelta(days=365)).strftime("%Y-%m-%d")
    data = yf.download(all_tickers, start=start_date, end=datetime.now().strftime("%Y-%m-%d"), group_by='ticker', progress=False)
    
    prices = {}
    for ticker in all_tickers:
        if ticker in data.columns:
            prices[ticker] = data[ticker]['Close'].dropna()
        else:
            prices[ticker] = data[ticker]['Close']
    
    df = pd.DataFrame(prices)
    return df.dropna()


def calculate_score(ret_1d: float, ret_3d: float, ret_5d: float, available_days: int = 30) -> float:
    """Calculate momentum score with adaptive weights"""
    weights = {'1d': 0.30, '3d': 0.25, '5d': 0.20, 'trend': 0.15, 'vol': 0.10}
    
    # Adjust for limited data
    if available_days < 5:
        weights['1d'] = 0.70
        weights['trend'] = 0.30
        weights['3d'] = 0
        weights['5d'] = 0
        weights['vol'] = 0
    elif available_days < 10:
        weights['1d'] = 0.45
        weights['3d'] = 0.35
        weights['trend'] = 0.20
        weights['5d'] = 0
        weights['vol'] = 0
    
    ret_1d = ret_1d if pd.notna(ret_1d) else 0
    ret_3d = ret_3d if pd.notna(ret_3d) else 0
    ret_5d = ret_5d if pd.notna(ret_5d) else 0
    
    score = (ret_1d * weights['1d'] + ret_3d * weights['3d'] + ret_5d * weights['5d'])
    
    if available_days >= 5:
        if ret_1d > 0 and ret_3d > 0 and ret_5d > 0:
            score += 5 * weights['trend']
        elif ret_1d < 0 and ret_3d < 0 and ret_5d < 0:
            score -= 5 * weights['trend']
    
    if available_days >= 3:
        volatility = abs(ret_1d - ret_3d) if pd.notna(ret_1d - ret_3d) else 0
        score += max(0, 10 - volatility) * weights['vol']
    
    return round(score, 4)


def get_regime(df: pd.DataFrame, idx: int) -> str:
    """Determine market regime based on QQQ"""
    if len(df) < 50:
        return "BULL"  # Assume bull if not enough data
    
    qqq = df['QQQ'].iloc[idx]
    qqq_ma50 = df['QQQ'].rolling(50).mean().iloc[idx]
    qqq_ma20 = df['QQQ'].rolling(20).mean().iloc[idx]
    ma20_slope = qqq_ma20 > df['QQQ'].rolling(20).mean().shift(1).iloc[idx]
    
    if qqq > qqq_ma50 and ma20_slope:
        return "BULL"
    return "CASH"


def main():
    print("=" * 60)
    print("QUANTUM COMPUTING SYSTEM")
    print("=" * 60)
    
    config = load_config()
    universe = config['universe']
    tickers = [item['ticker'] for item in universe]
    start_balance = config['starting_balance']
    position_limit = config['position_limit']
    min_score = config['min_score']
    trailing_stop_pct = config['trailing_stop']
    
    print(f"Universe: {', '.join(tickers)}")
    print(f"Start balance: ${start_balance:,.2f}")
    print(f"Position limit: {position_limit}")
    print(f"Trailing stop: {trailing_stop_pct * 100:.0f}%")
    
    # Fetch data
    print("\n📥 Fetching market data...")
    df = fetch_market_data(tickers)
    print(f"✅ Data: {df.index[0].date()} to {df.index[-1].date()} ({len(df)} days)")
    
    # Print data availability
    print("\n📊 Data availability:")
    for ticker in tickers + ["QQQ"]:
        available = len(df[ticker].dropna())
        pct = (available / len(df)) * 100 if len(df) > 0 else 0
        print(f"   {ticker}: {available} days ({pct:.0f}%)")
    
    # Calculate returns
    for ticker in tickers:
        df[f'{ticker}_ret_1d'] = df[ticker].pct_change() * 100
        df[f'{ticker}_ret_3d'] = df[ticker].pct_change(3) * 100
        df[f'{ticker}_ret_5d'] = df[ticker].pct_change(5) * 100
    
    # Initialize
    cash = start_balance
    positions: List[Position] = []
    trade_log = []
    score_history = {}
    daily_scores = []  # Store daily scores and prices for dashboard
    
    # Determine minimum index
    min_idx = max(20, len(df) - 200)
    
    # Main loop
    for i in range(min_idx, len(df) - 1):
        date = df.index[i]
        next_date = df.index[i + 1]
        
        regime = get_regime(df, i)
        
        # === EXITS ===
        survivors = []
        for pos in positions:
            ticker = pos.ticker
            current_price = df[ticker].iloc[i]
            
            # Update highest price
            if current_price > pos.highest_price:
                pos.highest_price = current_price
            
            # Calculate trailing stop
            trailing_stop = pos.highest_price * (1 - trailing_stop_pct)
            
            # Check stop
            low_price = df[ticker].iloc[i]
            if low_price <= trailing_stop:
                exit_price = min(trailing_stop, df[ticker].iloc[i + 1])
                gross_pl = (exit_price - pos.entry_price) * pos.shares
                ret_pct = ((exit_price / pos.entry_price) - 1) * 100
                
                trade_log.append({
                    'ticker': ticker,
                    'entry_date': pos.entry_date,
                    'exit_date': next_date.strftime("%Y-%m-%d"),
                    'entry_price': pos.entry_price,
                    'exit_price': exit_price,
                    'shares': pos.shares,
                    'return_pct': round(ret_pct, 2),
                    'gross_pl': round(gross_pl, 2),
                    'exit_reason': 'trailing_stop'
                })
                cash += gross_pl
                continue
            
            # Check regime
            if regime == "CASH":
                exit_price = df[ticker].iloc[i + 1]
                gross_pl = (exit_price - pos.entry_price) * pos.shares
                ret_pct = ((exit_price / pos.entry_price) - 1) * 100
                
                trade_log.append({
                    'ticker': ticker,
                    'entry_date': pos.entry_date,
                    'exit_date': next_date.strftime("%Y-%m-%d"),
                    'entry_price': pos.entry_price,
                    'exit_price': exit_price,
                    'shares': pos.shares,
                    'return_pct': round(ret_pct, 2),
                    'gross_pl': round(gross_pl, 2),
                    'exit_reason': 'regime_cash'
                })
                cash += gross_pl
                continue
            
            # Check score
            ticker_data = df[ticker].dropna()
            available_days = len(ticker_data)
            ret_1d = df[f'{ticker}_ret_1d'].iloc[i]
            ret_3d = df[f'{ticker}_ret_3d'].iloc[i] if available_days >= 3 else 0
            ret_5d = df[f'{ticker}_ret_5d'].iloc[i] if available_days >= 5 else 0
            current_score = calculate_score(ret_1d, ret_3d, ret_5d, available_days)
            
            if current_score < min_score:
                exit_price = df[ticker].iloc[i + 1]
                gross_pl = (exit_price - pos.entry_price) * pos.shares
                ret_pct = ((exit_price / pos.entry_price) - 1) * 100
                
                trade_log.append({
                    'ticker': ticker,
                    'entry_date': pos.entry_date,
                    'exit_date': next_date.strftime("%Y-%m-%d"),
                    'entry_price': pos.entry_price,
                    'exit_price': exit_price,
                    'shares': pos.shares,
                    'return_pct': round(ret_pct, 2),
                    'gross_pl': round(gross_pl, 2),
                    'exit_reason': f'score_{current_score:.1f}'
                })
                cash += gross_pl
                continue
            
            pos.trailing_stop = trailing_stop
            survivors.append(pos)
        
        positions = survivors
        
        # === ENTRIES ===
        if regime == "BULL" and len(positions) < position_limit:
            # Score all assets
            scores = {}
            for item in universe:
                ticker = item['ticker']
                ticker_data = df[ticker].dropna()
                available_days = len(ticker_data)
                
                if available_days < 10:
                    scores[ticker] = -999
                    continue
                
                ret_1d = df[f'{ticker}_ret_1d'].iloc[i]
                ret_3d = df[f'{ticker}_ret_3d'].iloc[i] if available_days >= 3 else 0
                ret_5d = df[f'{ticker}_ret_5d'].iloc[i] if available_days >= 5 else 0
                raw_score = calculate_score(ret_1d, ret_3d, ret_5d, available_days)
                
                prev = score_history.get(ticker, raw_score)
                smoothed = prev * 0.80 + raw_score * 0.20
                score_history[ticker] = smoothed
                scores[ticker] = smoothed
            
            # Store daily scores and prices for dashboard
            current_prices = {ticker: round(df[ticker].iloc[i], 2) for ticker in tickers}
            daily_scores.append({
                'date': date.strftime("%Y-%m-%d"),
                'scores': str(scores),
                'prices': str(current_prices)
            })
            
            # Sort and pick best
            sorted_scores = sorted(scores.items(), key=lambda x: x[1], reverse=True)
            open_tickers = [p.ticker for p in positions]
            
            for ticker, score in sorted_scores:
                if ticker in open_tickers:
                    continue
                if score >= min_score:
                    entry_price = df[ticker].iloc[i + 1]
                    if entry_price > 0:
                        shares = int(cash / entry_price)
                        if shares > 0:
                            positions.append(Position(
                                ticker=ticker,
                                entry_date=next_date.strftime("%Y-%m-%d"),
                                entry_price=entry_price,
                                shares=shares,
                                highest_price=entry_price,
                                stop_pct=trailing_stop_pct,
                                trailing_stop=entry_price * (1 - trailing_stop_pct),
                                entry_score=score
                            ))
                            print(f"  📈 ENTRY: {ticker} @ ${entry_price:.2f} ({shares} shares, score: {score:.1f})")
                            cash -= entry_price * shares
                            break
    
    # Save results
    data_dir = Path("data/quantum")
    data_dir.mkdir(parents=True, exist_ok=True)
    
    # Save daily scores with prices
    if daily_scores:
        scores_df = pd.DataFrame(daily_scores)
        scores_df.to_csv(data_dir / "quantum_scores.csv", index=False)
        print(f"✅ Saved {len(daily_scores)} days of scores to quantum_scores.csv")
    
    # Positions
    if positions:
        pos_df = pd.DataFrame([{
            'ticker': p.ticker,
            'entry_date': p.entry_date,
            'entry_price': p.entry_price,
            'shares': p.shares,
            'highest_price': p.highest_price,
            'trailing_stop': p.trailing_stop,
            'entry_score': p.entry_score
        } for p in positions])
        pos_df.to_csv(data_dir / "positions.csv", index=False)
        print(f"✅ Saved {len(positions)} open positions")
    
    # Trade log
    if trade_log:
        trade_df = pd.DataFrame(trade_log)
        trade_df.to_csv(data_dir / "trade_log.csv", index=False)
        print(f"✅ Saved {len(trade_log)} closed trades")
    
    # Performance
    if trade_log:
        total_pl = sum(t['gross_pl'] for t in trade_log)
        final_balance = start_balance + total_pl
        total_return = (final_balance / start_balance - 1) * 100
        
        winners = [t for t in trade_log if t['gross_pl'] > 0]
        losers = [t for t in trade_log if t['gross_pl'] < 0]
        win_rate = len(winners) / len(trade_log) * 100 if trade_log else 0
        avg_win = sum(t['return_pct'] for t in winners) / len(winners) if winners else 0
        avg_loss = sum(t['return_pct'] for t in losers) / len(losers) if losers else 0
        
        perf_df = pd.DataFrame([{
            'total_trades': len(trade_log),
            'win_rate_pct': round(win_rate, 2),
            'avg_win_pct': round(avg_win, 2),
            'avg_loss_pct': round(avg_loss, 2),
            'total_return_pct': round(total_return, 2),
            'net_profit_dollars': round(total_pl, 2),
            'final_balance': round(final_balance, 2)
        }])
        perf_df.to_csv(data_dir / "performance.csv", index=False)
        
        print(f"\n📊 QUANTUM SYSTEM RESULTS:")
        print(f"   Trades: {len(trade_log)}")
        print(f"   Win Rate: {win_rate:.1f}%")
        print(f"   Avg Win: +{avg_win:.1f}%")
        print(f"   Avg Loss: {avg_loss:.1f}%")
        print(f"   Total Return: {total_return:+.1f}%")
        print(f"   Final Balance: ${final_balance:,.2f}")
    else:
        print("\n📊 No trades executed")
    
    # Show current scores
    if daily_scores and len(daily_scores) > 0:
        latest = daily_scores[-1]['scores']
        try:
            latest_dict = eval(latest)
            print(f"\n📈 Latest Scores:")
            for ticker, score in sorted(latest_dict.items(), key=lambda x: x[1], reverse=True):
                if score > -999:
                    print(f"   {ticker}: {score:.1f}")
        except:
            pass
    
    print(f"\n✅ Quantum System complete. Data saved to data/quantum/")
    print(f"   Positions: {len(positions)} open")
    print(f"   Trades: {len(trade_log)} closed")


if __name__ == "__main__":
    main()
