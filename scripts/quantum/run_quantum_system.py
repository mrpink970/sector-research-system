#!/usr/bin/env python3
"""
Quantum Computing System - WITH PRICES
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta
from pathlib import Path
from typing import Dict, List

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
    trailing_stop: float
    entry_score: float


def load_config() -> dict:
    with open("config/quantum_parameters.yaml", "r") as f:
        return yaml.safe_load(f)


def fetch_prices(tickers: List[str]) -> pd.DataFrame:
    """Fetch closing prices"""
    start = (datetime.now() - timedelta(days=90)).strftime("%Y-%m-%d")
    data = yf.download(tickers, start=start, progress=False)
    if data.empty:
        return pd.DataFrame()
    return data['Close']


def calculate_score(ret_1d: float, ret_3d: float, ret_5d: float) -> float:
    """Momentum score"""
    score = (ret_1d * 0.30) + (ret_3d * 0.25) + (ret_5d * 0.20)
    
    if ret_1d > 0 and ret_3d > 0 and ret_5d > 0:
        score += 5 * 0.15
    elif ret_1d < 0 and ret_3d < 0 and ret_5d < 0:
        score -= 5 * 0.15
    
    vol = abs(ret_1d - ret_3d)
    score += max(0, 10 - vol) * 0.10
    
    return round(score, 2)


def get_qqq_regime() -> str:
    """Simple QQQ regime check"""
    qqq = yf.Ticker("QQQ")
    hist = qqq.history(period="3mo")
    if len(hist) < 50:
        return "BULL"
    
    current = hist['Close'].iloc[-1]
    ma50 = hist['Close'].rolling(50).mean().iloc[-1]
    
    return "BULL" if current > ma50 else "CASH"


def main():
    print("=" * 60)
    print("QUANTUM COMPUTING SYSTEM")
    print("=" * 60)
    
    config = load_config()
    tickers = [item['ticker'] for item in config['universe']]
    start_balance = config['starting_balance']
    trailing_stop_pct = config['trailing_stop']
    min_score = config['min_score']
    
    print(f"Tickers: {tickers}")
    print(f"Start: ${start_balance:,.2f}")
    print(f"Stop: {trailing_stop_pct * 100:.0f}%")
    print(f"Min score: {min_score}")
    
    # Fetch data
    print("\n📥 Fetching data...")
    df = fetch_prices(tickers)
    
    if df.empty:
        print("❌ No data")
        return
    
    print(f"✅ Data: {df.index[0].date()} to {df.index[-1].date()}")
    
    # Calculate returns
    ret_1d = df.pct_change() * 100
    ret_3d = df.pct_change(3) * 100
    ret_5d = df.pct_change(5) * 100
    
    # Calculate scores for each day
    scores = pd.DataFrame(index=df.index)
    for ticker in tickers:
        scores[ticker] = [
            calculate_score(
                ret_1d[ticker].iloc[i] if pd.notna(ret_1d[ticker].iloc[i]) else 0,
                ret_3d[ticker].iloc[i] if pd.notna(ret_3d[ticker].iloc[i]) else 0,
                ret_5d[ticker].iloc[i] if pd.notna(ret_5d[ticker].iloc[i]) else 0
            )
            for i in range(len(df))
        ]
    
    # Save scores and prices together
    scores_with_prices = []
    for i in range(len(df)):
        date = df.index[i]
        price_dict = {ticker: round(df[ticker].iloc[i], 2) for ticker in tickers}
        score_dict = {ticker: round(scores[ticker].iloc[i], 2) for ticker in tickers}
        scores_with_prices.append({
            'date': date.strftime("%Y-%m-%d"),
            'scores': str(score_dict),
            'prices': str(price_dict)
        })
    
    scores_df = pd.DataFrame(scores_with_prices)
    scores_df.to_csv("data/quantum/quantum_scores.csv", index=False)
    print(f"✅ Saved scores and prices")
    
    # Run simulation
    cash = start_balance
    position = None
    trades = []
    regime = get_qqq_regime()
    
    print(f"\n📊 Regime: {regime}")
    print(f"🔄 Running simulation...\n")
    
    for i in range(5, len(df) - 1):
        date = df.index[i]
        next_day = df.index[i + 1]
        
        # Exit logic
        if position:
            ticker = position['ticker']
            current = df[ticker].iloc[i]
            high = max(position['highest_price'], current)
            stop = high * (1 - trailing_stop_pct)
            
            if current <= stop:
                exit_price = stop
                pl = (exit_price - position['entry_price']) * position['shares']
                ret = (exit_price / position['entry_price'] - 1) * 100
                trades.append({
                    'ticker': ticker,
                    'entry_date': position['entry_date'],
                    'exit_date': next_day.strftime("%Y-%m-%d"),
                    'entry_price': position['entry_price'],
                    'exit_price': exit_price,
                    'shares': position['shares'],
                    'return_pct': round(ret, 2),
                    'gross_pl': round(pl, 2),
                    'exit_reason': 'trailing_stop'
                })
                cash += pl
                print(f"  EXIT: {ticker} @ ${exit_price:.2f} ({ret:+.1f}%)")
                position = None
            else:
                position['highest_price'] = high
        
        # Entry logic
        if not position and regime == "BULL":
            today_scores = {ticker: scores[ticker].iloc[i] for ticker in tickers}
            best = max(today_scores, key=today_scores.get)
            best_score = today_scores[best]
            
            if best_score >= min_score:
                entry_price = df[best].iloc[i + 1]
                shares = int(cash / entry_price)
                if shares > 0:
                    position = {
                        'ticker': best,
                        'entry_date': next_day.strftime("%Y-%m-%d"),
                        'entry_price': entry_price,
                        'shares': shares,
                        'highest_price': entry_price,
                        'entry_score': best_score
                    }
                    print(f"  ENTRY: {best} @ ${entry_price:.2f} ({shares} shares, score: {best_score:.1f})")
                    cash -= entry_price * shares
    
    # Save results
    data_dir = Path("data/quantum")
    data_dir.mkdir(parents=True, exist_ok=True)
    
    if trades:
        trade_df = pd.DataFrame(trades)
        trade_df.to_csv(data_dir / "trade_log.csv", index=False)
        total_pl = sum(t['gross_pl'] for t in trades)
        final = start_balance + total_pl
        ret_pct = (final / start_balance - 1) * 100
        
        print(f"\n📊 RESULTS:")
        print(f"   Trades: {len(trades)}")
        print(f"   Return: {ret_pct:+.1f}%")
        print(f"   Final: ${final:,.2f}")
        
        perf_df = pd.DataFrame([{
            'total_trades': len(trades),
            'win_rate_pct': 0,
            'total_return_pct': round(ret_pct, 2),
            'net_profit_dollars': round(total_pl, 2),
            'final_balance': round(final, 2)
        }])
        perf_df.to_csv(data_dir / "performance.csv", index=False)
    
    if position:
        pos_df = pd.DataFrame([{
            'ticker': position['ticker'],
            'entry_date': position['entry_date'],
            'entry_price': position['entry_price'],
            'shares': position['shares'],
            'highest_price': position['highest_price'],
            'trailing_stop': position['highest_price'] * (1 - trailing_stop_pct),
            'entry_score': position['entry_score']
        }])
        pos_df.to_csv(data_dir / "positions.csv", index=False)
    
    print(f"\n✅ Complete")


if __name__ == "__main__":
    main()
