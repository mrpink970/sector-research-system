#!/usr/bin/env python3
"""
Quantum Computing System - DEBUG VERSION
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
    all_tickers = list(set(tickers + ["QQQ"]))
    start_date = (datetime.now() - timedelta(days=60)).strftime("%Y-%m-%d")
    print(f"  Fetching data from {start_date} to {datetime.now().strftime('%Y-%m-%d')}")
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
    weights = {'1d': 0.30, '3d': 0.25, '5d': 0.20, 'trend': 0.15, 'vol': 0.10}
    
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
    if len(df) < 50:
        return "BULL"
    
    qqq = df['QQQ'].iloc[idx]
    qqq_ma50 = df['QQQ'].rolling(50).mean().iloc[idx]
    qqq_ma20 = df['QQQ'].rolling(20).mean().iloc[idx]
    ma20_slope = qqq_ma20 > df['QQQ'].rolling(20).mean().shift(1).iloc[idx]
    
    if qqq > qqq_ma50 and ma20_slope:
        return "BULL"
    return "CASH"


def main():
    print("=" * 60)
    print("QUANTUM COMPUTING SYSTEM (DEBUG)")
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
    print(f"Min score: {min_score}")
    print(f"Trailing stop: {trailing_stop_pct * 100:.0f}%")
    
    # Fetch data
    print("\n📥 Fetching market data...")
    df = fetch_market_data(tickers)
    print(f"✅ Data: {df.index[0].date()} to {df.index[-1].date()} ({len(df)} days)")
    
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
    daily_scores = []
    
    min_idx = max(20, len(df) - 200)
    print(f"\n🔄 Processing days {min_idx} to {len(df)-1}")
    
    for i in range(min_idx, len(df) - 1):
        date = df.index[i]
        next_date = df.index[i + 1]
        regime = get_regime(df, i)
        
        print(f"\n  📅 Date: {date.date()}, Regime: {regime}, Positions: {len(positions)}")
        
        # === EXITS ===
        survivors = []
        for pos in positions:
            ticker = pos.ticker
            current_price = df[ticker].iloc[i]
            
            if current_price > pos.highest_price:
                pos.highest_price = current_price
            
            trailing_stop = pos.highest_price * (1 - trailing_stop_pct)
            low_price = df[ticker].iloc[i]
            
            if low_price <= trailing_stop:
                print(f"    EXIT: {ticker} - trailing stop")
                exit_price = min(trailing_stop, df[ticker].iloc[i + 1])
                gross_pl = (exit_price - pos.entry_price) * pos.shares
                ret_pct = ((exit_price / pos.entry_price) - 1) * 100
                trade_log.append({
                    'ticker': ticker, 'entry_date': pos.entry_date, 'exit_date': next_date.strftime("%Y-%m-%d"),
                    'entry_price': pos.entry_price, 'exit_price': exit_price, 'shares': pos.shares,
                    'return_pct': round(ret_pct, 2), 'gross_pl': round(gross_pl, 2), 'exit_reason': 'trailing_stop'
                })
                cash += gross_pl
                continue
            
            if regime == "CASH":
                print(f"    EXIT: {ticker} - regime cash")
                exit_price = df[ticker].iloc[i + 1]
                gross_pl = (exit_price - pos.entry_price) * pos.shares
                ret_pct = ((exit_price / pos.entry_price) - 1) * 100
                trade_log.append({
                    'ticker': ticker, 'entry_date': pos.entry_date, 'exit_date': next_date.strftime("%Y-%m-%d"),
                    'entry_price': pos.entry_price, 'exit_price': exit_price, 'shares': pos.shares,
                    'return_pct': round(ret_pct, 2), 'gross_pl': round(gross_pl, 2), 'exit_reason': 'regime_cash'
                })
                cash += gross_pl
                continue
            
            available_days = len(df[ticker].dropna())
            ret_1d = df[f'{ticker}_ret_1d'].iloc[i]
            ret_3d = df[f'{ticker}_ret_3d'].iloc[i] if available_days >= 3 else 0
            ret_5d = df[f'{ticker}_ret_5d'].iloc[i] if available_days >= 5 else 0
            current_score = calculate_score(ret_1d, ret_3d, ret_5d, available_days)
            
            if current_score < min_score:
                print(f"    EXIT: {ticker} - score {current_score:.1f} < {min_score}")
                exit_price = df[ticker].iloc[i + 1]
                gross_pl = (exit_price - pos.entry_price) * pos.shares
                ret_pct = ((exit_price / pos.entry_price) - 1) * 100
                trade_log.append({
                    'ticker': ticker, 'entry_date': pos.entry_date, 'exit_date': next_date.strftime("%Y-%m-%d"),
                    'entry_price': pos.entry_price, 'exit_price': exit_price, 'shares': pos.shares,
                    'return_pct': round(ret_pct, 2), 'gross_pl': round(gross_pl, 2), 'exit_reason': f'score_{current_score:.1f}'
                })
                cash += gross_pl
                continue
            
            pos.trailing_stop = trailing_stop
            survivors.append(pos)
        
        positions = survivors
        
        # === ENTRIES ===
        if len(positions) < position_limit:
            scores = {}
            for item in universe:
                ticker = item['ticker']
                available_days = len(df[ticker].dropna())
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
            
            # Print top scores
            sorted_scores = sorted(scores.items(), key=lambda x: x[1], reverse=True)
            print(f"    Top scores: {sorted_scores[:3]}")
            
            # Check if any score meets threshold
            for ticker, score in sorted_scores:
                if score >= min_score:
                    entry_price = df[ticker].iloc[i + 1]
                    if entry_price > 0:
                        shares = int(cash / entry_price)
                        if shares > 0:
                            positions.append(Position(
                                ticker=ticker, entry_date=next_date.strftime("%Y-%m-%d"), entry_price=entry_price,
                                shares=shares, highest_price=entry_price, stop_pct=trailing_stop_pct,
                                trailing_stop=entry_price * (1 - trailing_stop_pct), entry_score=score
                            ))
                            print(f"    ✅ ENTRY: {ticker} @ ${entry_price:.2f} ({shares} shares, score: {score:.1f})")
                            cash -= entry_price * shares
                            break
            
            # Store daily scores
            current_prices = {ticker: round(float(df[ticker].iloc[i]), 2) for ticker in tickers}
            clean_scores = {ticker: float(score) for ticker, score in scores.items()}
            daily_scores.append({
                'date': date.strftime("%Y-%m-%d"),
                'scores': str(clean_scores),
                'prices': str(current_prices)
            })
    
    # Save results (same as before)
    data_dir = Path("data/quantum")
    data_dir.mkdir(parents=True, exist_ok=True)
    
    if daily_scores:
        scores_df = pd.DataFrame(daily_scores)
        scores_df.to_csv(data_dir / "quantum_scores.csv", index=False)
        print(f"\n✅ Saved {len(daily_scores)} days of scores")
    
    if positions:
        pos_df = pd.DataFrame([{
            'ticker': p.ticker, 'entry_date': p.entry_date, 'entry_price': p.entry_price,
            'shares': p.shares, 'highest_price': p.highest_price, 'trailing_stop': p.trailing_stop, 'entry_score': p.entry_score
        } for p in positions])
        pos_df.to_csv(data_dir / "positions.csv", index=False)
    
    if trade_log:
        trade_df = pd.DataFrame(trade_log)
        trade_df.to_csv(data_dir / "trade_log.csv", index=False)
    
    # Summary
    if trade_log:
        total_pl = sum(t['gross_pl'] for t in trade_log)
        final_balance = start_balance + total_pl
        print(f"\n📊 Final Balance: ${final_balance:.2f}")
    
    print(f"\n✅ Quantum System complete. Positions: {len(positions)} open")


if __name__ == "__main__":
    main()
