#!/usr/bin/env python3
from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Optional, Tuple

import pandas as pd
import yaml


@dataclass
class Position:
    system: str  # "trend" or "breakout"
    ticker: str
    entry_date: str
    entry_price: float
    shares: int
    highest_price: float
    stop_pct: float
    trailing_stop: float
    entry_score: int
    entry_signal: str


def load_yaml(path: Path) -> dict:
    with path.open("r", encoding="utf-8") as f:
        return yaml.safe_load(f)


def load_price_data(scores_df: pd.DataFrame) -> Dict[Tuple[str, str], dict]:
    """
    Build a price lookup dictionary from the scores CSV.
    Keys: (date, ticker)
    Values: {open, high, low, close}
    """
    price_map = {}
    for _, row in scores_df.iterrows():
        key = (row["date"], row["ticker"])
        price_map[key] = {
            "open": float(row["open"]),
            "high": float(row["high"]),
            "low": float(row["low"]),
            "close": float(row["close"]),
        }
    return price_map


def load_trend_candidates(date: str, scores_df: pd.DataFrame, min_score: int = 6) -> List[dict]:
    """Load trend candidates (signal = Strong Bullish, total_score >= min_score)"""
    day_df = scores_df[scores_df["date"] == date].copy()
    candidates = day_df[
        (day_df["signal"] == "Strong Bullish") &
        (day_df["total_score"] >= min_score)
    ]
    
    results = []
    for _, row in candidates.iterrows():
        results.append({
            "ticker": row["ticker"],
            "score": int(row["total_score"]),
            "signal": row["signal"],
            "close": float(row["close"]),
        })
    
    # Sort by score descending, then ticker alphabetically
    results.sort(key=lambda x: (-x["score"], x["ticker"]))
    return results


def load_breakout_candidates(date: str, scores_df: pd.DataFrame, min_score: int = 6) -> List[dict]:
    """Load breakout candidates with tiebreaker: score → rs_acceleration → volume → proximity → ticker"""
    day_df = scores_df[scores_df["date"] == date].copy()
    candidates = day_df[
        (day_df["breakout_signal"] == "Strong Breakout Candidate") &
        (day_df["breakout_total_score"] >= min_score)
    ]
    
    results = []
    for _, row in candidates.iterrows():
        results.append({
            "ticker": row["ticker"],
            "score": int(row["breakout_total_score"]),
            "signal": row["breakout_signal"],
            "close": float(row["close"]),
            "rs_acceleration_score": int(row.get("breakout_rs_acceleration_score", 0)),
            "volume_score": int(row.get("breakout_volume_score", 0)),
            "proximity_score": int(row.get("breakout_proximity_score", 0)),
        })
    
    # Sort by: score desc, then rs_acceleration desc, then volume desc, then proximity desc, then ticker asc
    results.sort(key=lambda x: (
        -x["score"],
        -x["rs_acceleration_score"],
        -x["volume_score"],
        -x["proximity_score"],
        x["ticker"]
    ))
    return results


def get_stop_levels_trend(gain_pct: float) -> float:
    """Stepped trailing stops for trend system"""
    if gain_pct >= 0.40:
        return 0.08
    elif gain_pct >= 0.20:
        return 0.10
    elif gain_pct >= 0.10:
        return 0.12
    else:
        return 0.15


def get_stop_levels_breakout(gain_pct: float) -> float:
    """Stepped trailing stops for breakout system (tighter)"""
    if gain_pct >= 0.40:
        return 0.06
    elif gain_pct >= 0.20:
        return 0.07
    elif gain_pct >= 0.10:
        return 0.08
    else:
        return 0.10


def get_initial_stop(system: str) -> float:
    """Initial stop percentage at entry"""
    if system == "trend":
        return 0.15
    else:  # breakout
        return 0.10


def requires_confirmation(system: str, scores_df: pd.DataFrame, ticker: str, signal_date: str, required_days: int) -> bool:
    """Check if stock has been a candidate for required_days consecutive days"""
    if required_days <= 1:
        return True
    
    # Get historical scores for this ticker
    ticker_df = scores_df[scores_df["ticker"] == ticker].sort_values("date")
    ticker_df = ticker_df[ticker_df["date"] <= signal_date].tail(required_days)
    
    if len(ticker_df) < required_days:
        return False
    
    if system == "trend":
        return all(ticker_df["signal"] == "Strong Bullish")
    else:  # breakout
        return all(ticker_df["breakout_signal"] == "Strong Breakout Candidate")


def close_position(
    position: Position,
    exit_date: str,
    exit_price: float,
    exit_reason: str,
) -> dict:
    gross_pnl = (exit_price - position.entry_price) * position.shares
    return_pct = ((exit_price - position.entry_price) / position.entry_price) * 100.0
    duration_days = (pd.to_datetime(exit_date) - pd.to_datetime(position.entry_date)).days
    
    return {
        "system": position.system,
        "ticker": position.ticker,
        "entry_date": position.entry_date,
        "entry_price": round(position.entry_price, 4),
        "exit_date": exit_date,
        "exit_price": round(exit_price, 4),
        "shares": position.shares,
        "entry_score": position.entry_score,
        "entry_signal": position.entry_signal,
        "exit_reason": exit_reason,
        "gross_pnl": round(gross_pnl, 2),
        "return_pct": round(return_pct, 2),
        "duration_days": duration_days,
    }


def update_performance(balance: float, trade_log: pd.DataFrame, system_name: str) -> pd.DataFrame:
    """Update performance metrics for a system"""
    if trade_log.empty:
        return pd.DataFrame([{
            "system": system_name,
            "balance": balance,
            "total_trades": 0,
            "win_rate": 0.0,
            "total_return_pct": 0.0,
            "max_drawdown_pct": 0.0,
            "avg_win_pct": 0.0,
            "avg_loss_pct": 0.0,
            "profit_factor": 0.0,
        }])
    
    returns = trade_log["return_pct"].astype(float)
    winners = returns[returns > 0]
    losers = returns[returns < 0]
    
    total_trades = len(trade_log)
    win_rate = len(winners) / total_trades if total_trades > 0 else 0.0
    
    # Calculate drawdown from running balance
    trade_log = trade_log.sort_values("exit_date")
    trade_log["cumulative_return"] = trade_log["return_pct"].cumsum()
    trade_log["peak"] = trade_log["cumulative_return"].cummax()
    trade_log["drawdown"] = trade_log["cumulative_return"] - trade_log["peak"]
    max_drawdown = trade_log["drawdown"].min() if not trade_log.empty else 0.0
    
    gross_profit = trade_log[trade_log["gross_pnl"] > 0]["gross_pnl"].sum() if not trade_log[trade_log["gross_pnl"] > 0].empty else 0.0
    gross_loss = abs(trade_log[trade_log["gross_pnl"] < 0]["gross_pnl"].sum()) if not trade_log[trade_log["gross_pnl"] < 0].empty else 0.0
    profit_factor = gross_profit / gross_loss if gross_loss > 0 else 0.0
    
    return pd.DataFrame([{
        "system": system_name,
        "balance": round(balance, 2),
        "total_trades": total_trades,
        "win_rate": round(win_rate * 100, 2),
        "total_return_pct": round(float(returns.sum()), 2),
        "max_drawdown_pct": round(max_drawdown, 2),
        "avg_win_pct": round(winners.mean(), 2) if not winners.empty else 0.0,
        "avg_loss_pct": round(abs(losers.mean()), 2) if not losers.empty else 0.0,
        "profit_factor": round(profit_factor, 2),
    }])


def main():
    root = Path(".")
    data_dir = root / "data" / "stocks"
    
    # Load scores (now includes open, high, low, close)
    scores = pd.read_csv(data_dir / "stock_scores_history.csv")
    scores["date"] = pd.to_datetime(scores["date"]).dt.strftime("%Y-%m-%d")
    
    # Build price lookup from the CSV
    price_map = load_price_data(scores)
    
    # Get unique dates from scores
    all_dates = sorted(scores["date"].unique())
    
    # Handle first day with only one date
    if len(all_dates) < 2:
        print("Only one day of history available. Skipping trading until tomorrow.")
        
        # Create empty output files so workflow doesn't fail
        pd.DataFrame().to_csv(data_dir / "trend_trade_log.csv", index=False)
        pd.DataFrame().to_csv(data_dir / "breakout_trade_log.csv", index=False)
        pd.DataFrame().to_csv(data_dir / "trend_open_positions.csv", index=False)
        pd.DataFrame().to_csv(data_dir / "breakout_open_positions.csv", index=False)
        
        perf = pd.DataFrame([{
            "system": "trend",
            "balance": 1000.0,
            "total_trades": 0,
            "win_rate": 0.0,
            "total_return_pct": 0.0,
            "max_drawdown_pct": 0.0,
            "avg_win_pct": 0.0,
            "avg_loss_pct": 0.0,
            "profit_factor": 0.0,
        }, {
            "system": "breakout",
            "balance": 1000.0,
            "total_trades": 0,
            "win_rate": 0.0,
            "total_return_pct": 0.0,
            "max_drawdown_pct": 0.0,
            "avg_win_pct": 0.0,
            "avg_loss_pct": 0.0,
            "profit_factor": 0.0,
        }])
        perf.to_csv(data_dir / "stock_performance.csv", index=False)
        print("Empty files created. Exiting gracefully.")
        return
    
    # Configuration
    trend_confirmation_days = 2
    breakout_confirmation_days = 1
    trend_min_score = 6
    breakout_min_score = 6
    
    # Initialize systems
    trend_balance = 1000.0
    breakout_balance = 1000.0
    
    trend_positions: List[Position] = []
    breakout_positions: List[Position] = []
    
    trend_trades: List[dict] = []
    breakout_trades: List[dict] = []
    
    print(f"Starting parallel paper trading")
    print(f"Trend system: ${trend_balance:.2f} | Breakout system: ${breakout_balance:.2f}")
    print(f"Dates range: {all_dates[0]} to {all_dates[-1]}")
    print("-" * 60)
    
    # Process each trading day (use yesterday's signal for today's entry)
    for i in range(1, len(all_dates)):
        signal_date = all_dates[i - 1]
        trade_date = all_dates[i]
        
        print(f"\nProcessing {trade_date} (signal from {signal_date})")
        
        # Load candidates for this signal date
        trend_candidates = load_trend_candidates(signal_date, scores, trend_min_score)
        breakout_candidates = load_breakout_candidates(signal_date, scores, breakout_min_score)
        
        print(f"  Trend candidates ({len(trend_candidates)}): {[c['ticker'] for c in trend_candidates]}")
        print(f"  Breakout candidates ({len(breakout_candidates)}): {[c['ticker'] for c in breakout_candidates]}")
        
        # =========================================================
        # TREND SYSTEM - Exits first, then entries
        # =========================================================
        survivors = []
        for pos in trend_positions:
            # Get price from CSV instead of yfinance
            bar = price_map.get((trade_date, pos.ticker))
            if not bar:
                print(f"  [TREND] No price data for {pos.ticker} on {trade_date}, holding")
                survivors.append(pos)
                continue
            
            # Update highest price and trailing stop
            current_high = max(pos.highest_price, bar["high"])
            current_gain = (current_high - pos.entry_price) / pos.entry_price
            new_stop_pct = get_stop_levels_trend(current_gain)
            new_trailing_stop = current_high * (1 - new_stop_pct)
            
            # Check if stop hit
            if bar["low"] <= new_trailing_stop:
                closed = close_position(pos, trade_date, bar["open"], "trailing_stop")
                trend_trades.append(closed)
                trend_balance += closed["gross_pnl"]
                print(f"  [TREND] EXIT {pos.ticker} @ ${bar['open']:.2f} PnL: ${closed['gross_pnl']:.2f} ({closed['return_pct']}%)")
                continue
            
            # Update position
            pos.highest_price = current_high
            pos.stop_pct = new_stop_pct
            pos.trailing_stop = new_trailing_stop
            survivors.append(pos)
        
        trend_positions = survivors
        
        # Trend system entries
        if len(trend_positions) == 0 and trend_candidates:
            best = trend_candidates[0]
            print(f"  [TREND] Best candidate: {best['ticker']} (score {best['score']})")
            
            # Check 2-day confirmation
            confirmed = requires_confirmation("trend", scores, best["ticker"], signal_date, trend_confirmation_days)
            print(f"  [TREND] Confirmation for {best['ticker']}: {confirmed}")
            
            if confirmed:
                bar = price_map.get((trade_date, best["ticker"]))
                if bar:
                    shares = int(trend_balance / bar["open"])
                    if shares > 0:
                        initial_stop = get_initial_stop("trend")
                        trend_positions.append(Position(
                            system="trend",
                            ticker=best["ticker"],
                            entry_date=trade_date,
                            entry_price=bar["open"],
                            shares=shares,
                            highest_price=bar["high"],
                            stop_pct=initial_stop,
                            trailing_stop=bar["high"] * (1 - initial_stop),
                            entry_score=best["score"],
                            entry_signal=best["signal"],
                        ))
                        print(f"  [TREND] ENTRY {best['ticker']} @ ${bar['open']:.2f} shares:{shares}")
                else:
                    print(f"  [TREND] No price data for {best['ticker']} on {trade_date}")
            else:
                print(f"  [TREND] {best['ticker']} failed confirmation")
        else:
            if len(trend_positions) > 0:
                print(f"  [TREND] Position exists, skipping entry")
            elif not trend_candidates:
                print(f"  [TREND] No candidates")
        
        # =========================================================
        # BREAKOUT SYSTEM - Exits first, then entries
        # =========================================================
        survivors = []
        for pos in breakout_positions:
            # Get price from CSV instead of yfinance
            bar = price_map.get((trade_date, pos.ticker))
            if not bar:
                print(f"  [BREAKOUT] No price data for {pos.ticker} on {trade_date}, holding")
                survivors.append(pos)
                continue
            
            # Update highest price and trailing stop
            current_high = max(pos.highest_price, bar["high"])
            current_gain = (current_high - pos.entry_price) / pos.entry_price
            new_stop_pct = get_stop_levels_breakout(current_gain)
            new_trailing_stop = current_high * (1 - new_stop_pct)
            
            # Check if stop hit
            if bar["low"] <= new_trailing_stop:
                closed = close_position(pos, trade_date, bar["open"], "trailing_stop")
                breakout_trades.append(closed)
                breakout_balance += closed["gross_pnl"]
                print(f"  [BREAKOUT] EXIT {pos.ticker} @ ${bar['open']:.2f} PnL: ${closed['gross_pnl']:.2f} ({closed['return_pct']}%)")
                continue
            
            # Update position
            pos.highest_price = current_high
            pos.stop_pct = new_stop_pct
            pos.trailing_stop = new_trailing_stop
            survivors.append(pos)
        
        breakout_positions = survivors
        
        # Breakout system entries
        if len(breakout_positions) == 0 and breakout_candidates:
            best = breakout_candidates[0]
            print(f"  [BREAKOUT] Best candidate: {best['ticker']} (score {best['score']})")
            
            # Check 1-day confirmation
            confirmed = requires_confirmation("breakout", scores, best["ticker"], signal_date, breakout_confirmation_days)
            print(f"  [BREAKOUT] Confirmation for {best['ticker']}: {confirmed}")
            
            if confirmed:
                bar = price_map.get((trade_date, best["ticker"]))
                if bar:
                    shares = int(breakout_balance / bar["open"])
                    if shares > 0:
                        initial_stop = get_initial_stop("breakout")
                        breakout_positions.append(Position(
                            system="breakout",
                            ticker=best["ticker"],
                            entry_date=trade_date,
                            entry_price=bar["open"],
                            shares=shares,
                            highest_price=bar["high"],
                            stop_pct=initial_stop,
                            trailing_stop=bar["high"] * (1 - initial_stop),
                            entry_score=best["score"],
                            entry_signal=best["signal"],
                        ))
                        print(f"  [BREAKOUT] ENTRY {best['ticker']} @ ${bar['open']:.2f} shares:{shares}")
                else:
                    print(f"  [BREAKOUT] No price data for {best['ticker']} on {trade_date}")
            else:
                print(f"  [BREAKOUT] {best['ticker']} failed confirmation")
        else:
            if len(breakout_positions) > 0:
                print(f"  [BREAKOUT] Position exists, skipping entry")
            elif not breakout_candidates:
                print(f"  [BREAKOUT] No candidates")
    
    # =========================================================
    # Save outputs
    # =========================================================
    
    # Trade logs
    trend_trade_df = pd.DataFrame(trend_trades)
    breakout_trade_df = pd.DataFrame(breakout_trades)
    
    trend_trade_df.to_csv(data_dir / "trend_trade_log.csv", index=False)
    breakout_trade_df.to_csv(data_dir / "breakout_trade_log.csv", index=False)
    
    # Open positions
    trend_open_df = pd.DataFrame([{
        "system": p.system,
        "ticker": p.ticker,
        "entry_date": p.entry_date,
        "entry_price": p.entry_price,
        "shares": p.shares,
        "highest_price": p.highest_price,
        "stop_pct": p.stop_pct,
        "trailing_stop": p.trailing_stop,
        "entry_score": p.entry_score,
        "entry_signal": p.entry_signal,
    } for p in trend_positions])
    
    breakout_open_df = pd.DataFrame([{
        "system": p.system,
        "ticker": p.ticker,
        "entry_date": p.entry_date,
        "entry_price": p.entry_price,
        "shares": p.shares,
        "highest_price": p.highest_price,
        "stop_pct": p.stop_pct,
        "trailing_stop": p.trailing_stop,
        "entry_score": p.entry_score,
        "entry_signal": p.entry_signal,
    } for p in breakout_positions])
    
    trend_open_df.to_csv(data_dir / "trend_open_positions.csv", index=False)
    breakout_open_df.to_csv(data_dir / "breakout_open_positions.csv", index=False)
    
    # Performance
    trend_perf = update_performance(trend_balance, trend_trade_df, "trend")
    breakout_perf = update_performance(breakout_balance, breakout_trade_df, "breakout")
    
    combined_perf = pd.concat([trend_perf, breakout_perf], ignore_index=True)
    combined_perf.to_csv(data_dir / "stock_performance.csv", index=False)
    
    # Summary
    print("\n" + "=" * 60)
    print("FINAL SUMMARY")
    print("=" * 60)
    print(f"Trend System:    ${trend_balance:.2f}  | Trades: {len(trend_trades)}  | Win Rate: {trend_perf['win_rate'].iloc[0]}%")
    print(f"Breakout System: ${breakout_balance:.2f}  | Trades: {len(breakout_trades)}  | Win Rate: {breakout_perf['win_rate'].iloc[0]}%")
    print("=" * 60)
    print(f"Outputs saved to {data_dir}/")
    print(f"  - trend_trade_log.csv")
    print(f"  - breakout_trade_log.csv")
    print(f"  - trend_open_positions.csv")
    print(f"  - breakout_open_positions.csv")
    print(f"  - stock_performance.csv")


if __name__ == "__main__":
    main()
