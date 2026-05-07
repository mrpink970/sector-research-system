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


@dataclass
class PendingEntry:
    system: str
    ticker: str
    scheduled_date: str
    estimated_price: float
    score: int
    signal: str
    shares: int


def load_yaml(path: Path) -> dict:
    with path.open("r", encoding="utf-8") as f:
        return yaml.safe_load(f)


def load_price_data(scores_df: pd.DataFrame) -> Dict[Tuple[str, str], dict]:
    """Build a price lookup dictionary from the scores CSV."""
    price_map = {}
    for _, row in scores_df.iterrows():
        key = (row["date"], row["ticker"])
        open_val = row.get("open")
        high_val = row.get("high")
        low_val = row.get("low")
        close_val = row.get("close")
        
        if pd.isna(open_val) or pd.isna(high_val) or pd.isna(low_val) or pd.isna(close_val):
            continue
            
        price_map[key] = {
            "open": float(open_val),
            "high": float(high_val),
            "low": float(low_val),
            "close": float(close_val),
        }
    return price_map


def load_trend_candidates(date: str, scores_df: pd.DataFrame, min_score: int = 6) -> List[dict]:
    """Load trend candidates with priority sorting."""
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
            "rs_acceleration": float(row.get("rs_acceleration", -999)),
            "trend_score": int(row.get("trend_score", 0)),
            "volume_score": int(row.get("volume_score", 0)),
            "relative_strength_score": int(row.get("relative_strength_score", 0)),
            "proximity_20": float(row.get("proximity_20", -999)),
        })
    
    # Sort by priority
    results.sort(key=lambda x: (
        -x["score"],
        -x["rs_acceleration"],
        -x["trend_score"],
        -x["volume_score"],
        -x["relative_strength_score"],
        -x["proximity_20"],
        x["ticker"]
    ))
    return results


def load_breakout_candidates(date: str, scores_df: pd.DataFrame, min_score: int = 6) -> List[dict]:
    """Load breakout candidates with priority sorting."""
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
            "compression_score": int(row.get("breakout_compression_score", 0)),
            "rs_acceleration_score": int(row.get("breakout_rs_acceleration_score", 0)),
            "proximity_score": int(row.get("breakout_proximity_score", 0)),
            "volume_score": int(row.get("breakout_volume_score", 0)),
            "extension_score": int(row.get("breakout_extension_score", 0)),
        })
    
    # Sort by priority
    results.sort(key=lambda x: (
        -x["score"],
        -x["compression_score"],
        -x["rs_acceleration_score"],
        -x["proximity_score"],
        -x["volume_score"],
        -x["extension_score"],
        x["ticker"]
    ))
    return results


def get_stop_levels_trend(gain_pct: float) -> float:
    """Stepped trailing stops for trend system - tighter as profits increase"""
    if gain_pct >= 0.50:      # 50%+ profit
        return 0.05
    elif gain_pct >= 0.30:    # 30-50% profit
        return 0.07
    elif gain_pct >= 0.20:    # 20-30% profit
        return 0.08
    elif gain_pct >= 0.10:    # 10-20% profit
        return 0.09
    else:                      # 0-10% profit
        return 0.10


def get_stop_levels_breakout(gain_pct: float) -> float:
    """Stepped trailing stops for breakout system - even tighter"""
    if gain_pct >= 0.40:      # 40%+ profit
        return 0.04
    elif gain_pct >= 0.20:    # 20-40% profit
        return 0.05
    elif gain_pct >= 0.10:    # 10-20% profit
        return 0.06
    else:                      # 0-10% profit
        return 0.08


def get_initial_stop(system: str) -> float:
    """Initial stop percentage at entry (10% for both)"""
    return 0.10  # 10% stop for both systems


def requires_confirmation(system: str, scores_df: pd.DataFrame, ticker: str, signal_date: str, required_days: int) -> bool:
    """
    Check if stock has been a candidate for required_days consecutive days.
    REQUIRES 2 DAYS CONFIRMATION as requested.
    """
    if required_days <= 1:
        return True
    
    # Get data including today and previous days
    ticker_df = scores_df[
        (scores_df["ticker"] == ticker) & 
        (scores_df["date"] <= signal_date)  # Include today
    ].sort_values("date").tail(required_days)
    
    if len(ticker_df) < required_days:
        return False
    
    if system == "trend":
        # Check if all required days have "Strong Bullish" signal
        return all(ticker_df["signal"] == "Strong Bullish")
    else:
        # Check if all required days have "Strong Breakout Candidate" signal
        return all(ticker_df["breakout_signal"] == "Strong Breakout Candidate")


def is_already_held_or_pending(ticker: str, positions: List[Position], pending_entries: List[PendingEntry], system: str) -> bool:
    """Check if a ticker is already in open positions or pending entries."""
    if any(p.ticker == ticker for p in positions):
        return True
    if any(p.ticker == ticker and p.system == system for p in pending_entries):
        return True
    return False


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


def load_pending_entries(pending_path: Path) -> List[PendingEntry]:
    """Load pending entries from previous run"""
    if not pending_path.exists():
        return []
    
    try:
        df = pd.read_csv(pending_path)
        pending = []
        for _, row in df.iterrows():
            pending.append(PendingEntry(
                system=row["system"],
                ticker=row["ticker"],
                scheduled_date=row["scheduled_date"],
                estimated_price=float(row["estimated_price"]),
                score=int(row["score"]),
                signal=row["signal"],
                shares=int(row["shares"]),
            ))
        return pending
    except Exception as e:
        print(f"Error loading pending entries: {e}")
        return []


def save_pending_entries(pending: List[PendingEntry], pending_path: Path) -> None:
    """Save pending entries for next run"""
    if not pending:
        if pending_path.exists():
            pending_path.unlink()
        return
    
    df = pd.DataFrame([{
        "system": p.system,
        "ticker": p.ticker,
        "scheduled_date": p.scheduled_date,
        "estimated_price": p.estimated_price,
        "score": p.score,
        "signal": p.signal,
        "shares": p.shares,
    } for p in pending])
    df.to_csv(pending_path, index=False)


def get_todays_score(ticker: str, date: str, scores_df: pd.DataFrame, system: str) -> Optional[int]:
    """Get today's score for a ticker."""
    day_data = scores_df[(scores_df["date"] == date) & (scores_df["ticker"] == ticker)]
    if day_data.empty:
        return None
    
    if system == "trend":
        return int(day_data["total_score"].iloc[0])
    else:
        return int(day_data["breakout_total_score"].iloc[0])


def calculate_position_size(balance: float, price: float) -> int:
    """Calculate position size - use 25% of available balance per trade"""
    position_value = balance * 0.25  # 25% of current balance
    shares = int(position_value / price)
    return max(1, shares)  # Minimum 1 share


def main():
    root = Path(".")
    data_dir = root / "data" / "stocks"
    pending_path = data_dir / "pending_entries.csv"
    
    # Load scores
    scores = pd.read_csv(data_dir / "stock_scores_history.csv")
    scores["date"] = pd.to_datetime(scores["date"]).dt.strftime("%Y-%m-%d")
    
    # Build price lookup
    price_map = load_price_data(scores)
    
    # Get unique dates from scores
    all_dates = sorted(scores["date"].unique())
    
    if len(all_dates) < 3:
        print("Not enough dates in history. Need at least 3 days.")
        return
    
    # Configuration - 2 DAY CONFIRMATION as requested
    trend_confirmation_days = 2
    breakout_confirmation_days = 2
    trend_min_score = 6
    breakout_min_score = 6
    min_hold_days = 3
    
    # Position limits - 2 POSITIONS EACH as requested
    MAX_TREND_POSITIONS = 2
    MAX_BREAKOUT_POSITIONS = 2
    
    # Initialize systems with $1000 each
    trend_balance = 1000.0
    breakout_balance = 1000.0
    
    trend_positions: List[Position] = []
    breakout_positions: List[Position] = []
    
    trend_trades: List[dict] = []
    breakout_trades: List[dict] = []
    
    # Load pending entries from previous run
    pending_entries = load_pending_entries(pending_path)
    
    print("=" * 80)
    print("TRADING SYSTEM STARTING")
    print(f"Dates range: {all_dates[0]} to {all_dates[-1]}")
    print(f"Trend: {trend_confirmation_days}-day confirmation, max {MAX_TREND_POSITIONS} positions")
    print(f"Breakout: {breakout_confirmation_days}-day confirmation, max {MAX_BREAKOUT_POSITIONS} positions")
    print("=" * 80)
    
    # Process each trading day
    for i in range(len(all_dates) - 2):  # Need 2 days ahead for scheduling
        signal_date = all_dates[i]      # Today's close data for scoring
        trade_date = all_dates[i + 1]   # Tomorrow's open for entry
        next_date = all_dates[i + 2]    # Day after for scheduling
        
        print(f"\n📅 [{i+1}/{len(all_dates)-2}] Signal: {signal_date} → Execute: {trade_date}")
        
        # =========================================================
        # STEP 1: Execute pending entries scheduled for TODAY
        # =========================================================
        remaining_pending = []
        for pending in pending_entries:
            if pending.scheduled_date == trade_date:
                bar = price_map.get((trade_date, pending.ticker))
                if bar:
                    actual_price = bar["open"]
                    initial_stop = get_initial_stop(pending.system)
                    
                    if pending.system == "trend":
                        trend_positions.append(Position(
                            system="trend",
                            ticker=pending.ticker,
                            entry_date=trade_date,
                            entry_price=actual_price,
                            shares=pending.shares,
                            highest_price=bar["high"],
                            stop_pct=initial_stop,
                            trailing_stop=bar["high"] * (1 - initial_stop),
                            entry_score=pending.score,
                            entry_signal=pending.signal,
                        ))
                        print(f"  ✅ TREND ENTRY: {pending.ticker} @ ${actual_price:.2f} ({pending.shares} shares)")
                    else:
                        breakout_positions.append(Position(
                            system="breakout",
                            ticker=pending.ticker,
                            entry_date=trade_date,
                            entry_price=actual_price,
                            shares=pending.shares,
                            highest_price=bar["high"],
                            stop_pct=initial_stop,
                            trailing_stop=bar["high"] * (1 - initial_stop),
                            entry_score=pending.score,
                            entry_signal=pending.signal,
                        ))
                        print(f"  ✅ BREAKOUT ENTRY: {pending.ticker} @ ${actual_price:.2f} ({pending.shares} shares)")
                else:
                    print(f"  ⚠️ No price data for {pending.ticker} on {trade_date}, keeping pending")
                    remaining_pending.append(pending)
            else:
                remaining_pending.append(pending)
        
        pending_entries = remaining_pending
        
        # =========================================================
        # STEP 2: EXITS - Trend System
        # =========================================================
        survivors = []
        for pos in trend_positions:
            bar = price_map.get((trade_date, pos.ticker))
            if not bar:
                survivors.append(pos)
                continue
            
            # Update highest price
            current_high = max(pos.highest_price, bar["high"])
            current_gain = (current_high - pos.entry_price) / pos.entry_price
            
            # Get updated stop level based on gain
            new_stop_pct = get_stop_levels_trend(current_gain)
            new_trailing_stop = current_high * (1 - new_stop_pct)
            
            # Check trailing stop
            if bar["low"] <= new_trailing_stop:
                closed = close_position(pos, trade_date, bar["open"], "trailing_stop")
                trend_trades.append(closed)
                trend_balance += closed["gross_pnl"]
                print(f"  🔴 TREND EXIT: {pos.ticker} @ ${bar['open']:.2f} (stop) PnL: ${closed['gross_pnl']:.2f} ({closed['return_pct']}%)")
                continue
            
            # Check signal loss after min hold days
            days_held = (pd.to_datetime(trade_date) - pd.to_datetime(pos.entry_date)).days
            if days_held >= min_hold_days:
                current_score = get_todays_score(pos.ticker, signal_date, scores, "trend")
                if current_score is not None and current_score < trend_min_score:
                    closed = close_position(pos, trade_date, bar["open"], "signal_loss")
                    trend_trades.append(closed)
                    trend_balance += closed["gross_pnl"]
                    print(f"  🔴 TREND EXIT: {pos.ticker} @ ${bar['open']:.2f} (signal loss) PnL: ${closed['gross_pnl']:.2f} ({closed['return_pct']}%)")
                    continue
            
            # Update position
            pos.highest_price = current_high
            pos.stop_pct = new_stop_pct
            pos.trailing_stop = new_trailing_stop
            survivors.append(pos)
        
        trend_positions = survivors
        
        # =========================================================
        # STEP 3: EXITS - Breakout System  
        # =========================================================
        survivors = []
        for pos in breakout_positions:
            bar = price_map.get((trade_date, pos.ticker))
            if not bar:
                survivors.append(pos)
                continue
            
            # Update highest price
            current_high = max(pos.highest_price, bar["high"])
            current_gain = (current_high - pos.entry_price) / pos.entry_price
            
            # Get updated stop level based on gain
            new_stop_pct = get_stop_levels_breakout(current_gain)
            new_trailing_stop = current_high * (1 - new_stop_pct)
            
            # Check trailing stop
            if bar["low"] <= new_trailing_stop:
                closed = close_position(pos, trade_date, bar["open"], "trailing_stop")
                breakout_trades.append(closed)
                breakout_balance += closed["gross_pnl"]
                print(f"  🔴 BREAKOUT EXIT: {pos.ticker} @ ${bar['open']:.2f} (stop) PnL: ${closed['gross_pnl']:.2f} ({closed['return_pct']}%)")
                continue
            
            # Check signal loss after min hold days
            days_held = (pd.to_datetime(trade_date) - pd.to_datetime(pos.entry_date)).days
            if days_held >= min_hold_days:
                current_score = get_todays_score(pos.ticker, signal_date, scores, "breakout")
                if current_score is not None and current_score < breakout_min_score:
                    closed = close_position(pos, trade_date, bar["open"], "signal_loss")
                    breakout_trades.append(closed)
                    breakout_balance += closed["gross_pnl"]
                    print(f"  🔴 BREAKOUT EXIT: {pos.ticker} @ ${bar['open']:.2f} (signal loss) PnL: ${closed['gross_pnl']:.2f} ({closed['return_pct']}%)")
                    continue
            
            # Update position
            pos.highest_price = current_high
            pos.stop_pct = new_stop_pct
            pos.trailing_stop = new_trailing_stop
            survivors.append(pos)
        
        breakout_positions = survivors
        
        # =========================================================
        # STEP 4: SCHEDULE New Entries
        # =========================================================
        
        # Get today's candidates
        trend_candidates = load_trend_candidates(signal_date, scores, trend_min_score)
        breakout_candidates = load_breakout_candidates(signal_date, scores, breakout_min_score)
        
        # TREND - Schedule if we have room
        open_slots = MAX_TREND_POSITIONS - len(trend_positions)
        if open_slots > 0 and trend_candidates:
            print(f"  📊 TREND: {open_slots} open slots, {len(trend_candidates)} candidates")
            
            for candidate in trend_candidates[:open_slots]:  # Take top N candidates
                if not is_already_held_or_pending(candidate["ticker"], trend_positions, pending_entries, "trend"):
                    # 2-DAY CONFIRMATION as requested
                    confirmed = requires_confirmation("trend", scores, candidate["ticker"], signal_date, trend_confirmation_days)
                    
                    if confirmed:
                        shares = calculate_position_size(trend_balance, candidate["close"])
                        
                        if shares > 0:
                            pending_entries.append(PendingEntry(
                                system="trend",
                                ticker=candidate["ticker"],
                                scheduled_date=next_date,  # Schedule for day after tomorrow
                                estimated_price=candidate["close"],
                                score=candidate["score"],
                                signal=candidate["signal"],
                                shares=shares,
                            ))
                            print(f"  📝 TREND SCHEDULED: {candidate['ticker']} (score {candidate['score']}) for {next_date} @ est ${candidate['close']:.2f} ({shares} shares)")
                    else:
                        print(f"  ❌ TREND: {candidate['ticker']} failed {trend_confirmation_days}-day confirmation")
                else:
                    print(f"  ⏭️ TREND: {candidate['ticker']} already held or pending")
        
        # BREAKOUT - Schedule if we have room
        open_slots = MAX_BREAKOUT_POSITIONS - len(breakout_positions)
        if open_slots > 0 and breakout_candidates:
            print(f"  📊 BREAKOUT: {open_slots} open slots, {len(breakout_candidates)} candidates")
            
            for candidate in breakout_candidates[:open_slots]:  # Take top N candidates
                if not is_already_held_or_pending(candidate["ticker"], breakout_positions, pending_entries, "breakout"):
                    # 2-DAY CONFIRMATION as requested
                    confirmed = requires_confirmation("breakout", scores, candidate["ticker"], signal_date, breakout_confirmation_days)
                    
                    if confirmed:
                        shares = calculate_position_size(breakout_balance, candidate["close"])
                        
                        if shares > 0:
                            pending_entries.append(PendingEntry(
                                system="breakout",
                                ticker=candidate["ticker"],
                                scheduled_date=next_date,  # Schedule for day after tomorrow
                                estimated_price=candidate["close"],
                                score=candidate["score"],
                                signal=candidate["signal"],
                                shares=shares,
                            ))
                            print(f"  📝 BREAKOUT SCHEDULED: {candidate['ticker']} (score {candidate['score']}) for {next_date} @ est ${candidate['close']:.2f} ({shares} shares)")
                    else:
                        print(f"  ❌ BREAKOUT: {candidate['ticker']} failed {breakout_confirmation_days}-day confirmation")
                else:
                    print(f"  ⏭️ BREAKOUT: {candidate['ticker']} already held or pending")
    
    # Save pending entries for next run
    save_pending_entries(pending_entries, pending_path)
    
    # =========================================================
    # SAVE OUTPUTS
    # =========================================================
    
    print("\n" + "=" * 80)
    print("FINAL RESULTS")
    print("=" * 80)
    
    # Trade logs
    trend_trade_df = pd.DataFrame(trend_trades)
    breakout_trade_df = pd.DataFrame(breakout_trades)
    
    if not trend_trade_df.empty:
        trend_trade_df.to_csv(data_dir / "trend_trade_log.csv", index=False)
        print(f"TREND: {len(trend_trade_df)} trades completed")
    else:
        print("TREND: No trades completed")
    
    if not breakout_trade_df.empty:
        breakout_trade_df.to_csv(data_dir / "breakout_trade_log.csv", index=False)
        print(f"BREAKOUT: {len(breakout_trade_df)} trades completed")
    else:
        print("BREAKOUT: No trades completed")
    
    # Open positions
    if trend_positions:
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
        trend_open_df.to_csv(data_dir / "trend_open_positions.csv", index=False)
        print(f"TREND: {len(trend_positions)} positions still open")
    
    if breakout_positions:
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
        breakout_open_df.to_csv(data_dir / "breakout_open_positions.csv", index=False)
        print(f"BREAKOUT: {len(breakout_positions)} positions still open")
    
    # Performance
    trend_perf = update_performance(trend_balance, trend_trade_df, "trend")
    breakout_perf = update_performance(breakout_balance, breakout_trade_df, "breakout")
    
    combined_perf = pd.concat([trend_perf, breakout_perf], ignore_index=True)
    combined_perf.to_csv(data_dir / "stock_performance.csv", index=False)
    
    # Summary
    print("\n" + "=" * 80)
    print("PERFORMANCE SUMMARY")
    print("=" * 80)
    
    trend_return = ((trend_balance - 1000) / 1000) * 100
    breakout_return = ((breakout_balance - 1000) / 1000) * 100
    
    print(f"TREND SYSTEM:")
    print(f"  Starting Balance: $1,000.00")
    print(f"  Current Balance:  ${trend_balance:.2f}")
    print(f"  Total Return:     ${trend_balance - 1000:.2f} ({trend_return:+.1f}%)")
    print(f"  Total Trades:     {len(trend_trades)}")
    if not trend_trade_df.empty:
        win_rate = (trend_trade_df[trend_trade_df['gross_pnl'] > 0].shape[0] / len(trend_trade_df)) * 100
        print(f"  Win Rate:         {win_rate:.1f}%")
    
    print(f"\nBREAKOUT SYSTEM:")
    print(f"  Starting Balance: $1,000.00")
    print(f"  Current Balance:  ${breakout_balance:.2f}")
    print(f"  Total Return:     ${breakout_balance - 1000:.2f} ({breakout_return:+.1f}%)")
    print(f"  Total Trades:     {len(breakout_trades)}")
    if not breakout_trade_df.empty:
        win_rate = (breakout_trade_df[breakout_trade_df['gross_pnl'] > 0].shape[0] / len(breakout_trade_df)) * 100
        print(f"  Win Rate:         {win_rate:.1f}%")
    
    # Pending entries for next run
    if pending_entries:
        print(f"\n📋 PENDING ENTRIES ({len(pending_entries)}):")
        for p in pending_entries:
            print(f"  {p.system.upper()}: {p.ticker} (score {p.score}) scheduled for {p.scheduled_date}")
    
    print("\n" + "=" * 80)
    print("NEXT STEPS:")
    print("1. Run this script again tomorrow to process pending entries")
    print("2. Check the CSV files in data/stocks/ for details")
    print("=" * 80)


if __name__ == "__main__":
    main()
