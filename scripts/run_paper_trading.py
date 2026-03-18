#!/usr/bin/env python3
from __future__ import annotations

from dataclasses import dataclass, asdict
from pathlib import Path
from typing import Dict, List, Optional, Tuple
import math

import pandas as pd
import yaml


SIGNAL_BULL = {"Bull", "Strong Bull"}
SIGNAL_BEAR = {"Bear", "Strong Bear"}


@dataclass
class Position:
    sector: str
    ticker: str
    side: str  # bull or bear
    entry_date: str
    entry_price: float
    shares: int
    highest_price: float
    stop_pct: float
    trailing_stop: float
    entry_signal: str
    entry_strength: float


def load_yaml(path: str | Path) -> dict:
    with open(path, "r", encoding="utf-8") as f:
        return yaml.safe_load(f)


def normalize_signal(signal: str) -> str:
    signal = str(signal).strip()
    mapping = {
        "Strong Bullish": "Strong Bull",
        "Bullish": "Bull",
        "Neutral": "Neutral",
        "Bearish": "Bear",
        "Strong Bearish": "Strong Bear",
        "Strong Bull": "Strong Bull",
        "Bull": "Bull",
        "Bear": "Bear",
        "Strong Bear": "Strong Bear",
    }
    return mapping.get(signal, signal)


def signal_direction(signal: str) -> str:
    signal = normalize_signal(signal)
    if signal in SIGNAL_BULL:
        return "bull"
    if signal in SIGNAL_BEAR:
        return "bear"
    return "neutral"


def instrument_perspective_signal(raw_signal: str, side: str) -> str:
    raw_signal = normalize_signal(raw_signal)
    if side == "bull":
        return raw_signal

    reverse_map = {
        "Strong Bull": "Strong Bear",
        "Bull": "Bear",
        "Neutral": "Neutral",
        "Bear": "Bull",
        "Strong Bear": "Strong Bull",
    }
    return reverse_map[raw_signal]


def leverage_for_ticker(ticker: str) -> int:
    t = ticker.upper()
    three_x = {
        "SOXL","SOXS","TECL","TECS","FAS","FAZ","ERX","ERY",
        "LABU","LABD","DUSL","WANT","TQQQ","SQQQ","DRV"
    }
    two_x = {"CURE","RXD"}
    if t in three_x:
        return 3
    if t in two_x:
        return 2
    return 1


def stop_pct_for_ticker(ticker: str, params: dict) -> float:
    lev = leverage_for_ticker(ticker)
    stops = params["stops"]
    if lev == 3:
        return float(stops["leverage_3x_pct"]) / 100.0
    if lev == 2:
        return float(stops["leverage_2x_pct"]) / 100.0
    return float(stops["leverage_1x_pct"]) / 100.0


def mapping_for_signal(sector_row: dict, raw_signal: str, skip_placeholder_mappings: bool) -> Tuple[Optional[str], Optional[str], str]:
    """
    Returns (ticker, side, reason_if_none)
    """
    direction = signal_direction(raw_signal)
    notes = str(sector_row.get("notes", "") or "")
    if direction == "neutral":
        return None, None, "neutral"

    if direction == "bull":
        ticker = str(sector_row.get("bull_etf", "") or "").strip()
        if not ticker:
            return None, None, "missing bull mapping"
        return ticker, "bull", ""

    # bear direction
    ticker = str(sector_row.get("bear_etf", "") or "").strip()
    if not ticker:
        return None, None, "missing bear mapping"

    if skip_placeholder_mappings and "placeholder" in notes.lower():
        return None, None, "placeholder bear mapping disabled"

    return ticker, "bear", ""


def current_signal_confirmed(history: pd.DataFrame, sector: str, current_date: str, required_closes: int = 2) -> bool:
    subset = history[(history["sector"] == sector) & (history["date"] <= current_date)].sort_values("date")
    if len(subset) < required_closes:
        return False

    tail = subset.tail(required_closes)
    directions = tail["signal_state"].apply(lambda s: signal_direction(s)).tolist()
    if "neutral" in directions:
        return False

    return len(set(directions)) == 1


def latest_sector_state_on_date(scores: pd.DataFrame, date: str) -> pd.DataFrame:
    day = scores[scores["date"] == date].copy()
    if day.empty:
        return day
    return day.sort_values(["rank", "sector"]).reset_index(drop=True)


def build_candidate_table(scores: pd.DataFrame, sector_map: pd.DataFrame, date: str, params: dict) -> pd.DataFrame:
    skip_placeholder = bool(params["logging"].get("skip_placeholder_mappings", True))
    required = int(params["confirmation"]["required_consecutive_closes"])

    day = latest_sector_state_on_date(scores, date)
    if day.empty:
        return day

    merged = day.merge(sector_map, on="sector", how="left", suffixes=("", "_map"))
    rows = []

    for _, row in merged.iterrows():
        raw_signal = normalize_signal(row["signal_state"])
        confirmed = current_signal_confirmed(scores, row["sector"], date, required)
        ticker, side, skip_reason = mapping_for_signal(row, raw_signal, skip_placeholder)
        tradable_strength = 0.0
        if side == "bull":
            tradable_strength = float(row["total_score"])
        elif side == "bear":
            tradable_strength = float(-row["total_score"])

        rows.append({
            "date": row["date"],
            "sector": row["sector"],
            "signal_etf": row["signal_etf"],
            "raw_signal": raw_signal,
            "confirmed": confirmed,
            "mapped_ticker": ticker,
            "side": side,
            "skip_reason": skip_reason,
            "total_score": float(row["total_score"]),
            "raw_rank": int(row["rank"]),
            "score_change": float(row.get("score_change", 0)),
            "tradable_strength": tradable_strength,
            "instrument_signal": instrument_perspective_signal(raw_signal, side) if side else "Neutral",
        })

    candidates = pd.DataFrame(rows)

    eligible = candidates[
        candidates["confirmed"]
        & candidates["mapped_ticker"].notna()
        & candidates["side"].notna()
        & (candidates["tradable_strength"] > 0)
    ].copy()

    if eligible.empty:
        candidates["tradable_rank"] = pd.NA
        return candidates

    eligible = eligible.sort_values(
        ["tradable_strength", "sector"], ascending=[False, True]
    ).reset_index(drop=True)
    eligible["tradable_rank"] = range(1, len(eligible) + 1)

    out = candidates.merge(
        eligible[["sector", "tradable_rank"]],
        on="sector",
        how="left",
    )
    return out.sort_values(["tradable_rank", "raw_rank", "sector"], na_position="last").reset_index(drop=True)


def load_price_table(market_data: pd.DataFrame) -> Dict[Tuple[str, str], dict]:
    price_map: Dict[Tuple[str, str], dict] = {}
    for _, row in market_data.iterrows():
        key = (row["date"], row["ticker"])
        price_map[key] = {
            "open": float(row["open"]),
            "high": float(row["high"]),
            "low": float(row["low"]),
            "close": float(row["close"]),
        }
    return price_map


def performance_row(trade_log: pd.DataFrame) -> pd.DataFrame:
    if trade_log.empty:
        return pd.DataFrame([{
            "total_trades": 0,
            "win_rate_pct": 0.0,
            "loss_rate_pct": 0.0,
            "average_gain_pct": 0.0,
            "average_loss_pct": 0.0,
            "largest_gain_pct": 0.0,
            "largest_loss_pct": 0.0,
            "total_return_pct": 0.0,
            "max_drawdown_pct": 0.0,
            "expectancy_per_trade_pct": 0.0,
            "gross_profit_dollars": 0.0,
            "gross_loss_dollars": 0.0,
            "net_profit_dollars": 0.0,
        }])

    returns = trade_log["return_pct"].astype(float)
    winners = returns[returns > 0]
    losers = returns[returns < 0]

    total_trades = len(trade_log)
    win_rate = len(winners) / total_trades if total_trades else 0.0
    loss_rate = len(losers) / total_trades if total_trades else 0.0
    avg_gain = float(winners.mean()) if not winners.empty else 0.0
    avg_loss = float(abs(losers.mean())) if not losers.empty else 0.0
    expectancy = (win_rate * avg_gain) - (loss_rate * avg_loss)

    trade_log = trade_log.copy()
    trade_log["equity_curve_pct"] = trade_log["return_pct"].cumsum()
    trade_log["equity_peak_pct"] = trade_log["equity_curve_pct"].cummax()
    trade_log["drawdown_pct"] = trade_log["equity_curve_pct"] - trade_log["equity_peak_pct"]
    max_drawdown = float(trade_log["drawdown_pct"].min()) if not trade_log.empty else 0.0

    gross_profit = float(trade_log.loc[trade_log["gross_pnl_dollars"] > 0, "gross_pnl_dollars"].sum())
    gross_loss = float(trade_log.loc[trade_log["gross_pnl_dollars"] < 0, "gross_pnl_dollars"].sum())
    net_profit = float(trade_log["gross_pnl_dollars"].sum())

    return pd.DataFrame([{
        "total_trades": total_trades,
        "win_rate_pct": round(win_rate * 100, 4),
        "loss_rate_pct": round(loss_rate * 100, 4),
        "average_gain_pct": round(avg_gain, 4),
        "average_loss_pct": round(avg_loss, 4),
        "largest_gain_pct": round(float(returns.max()) if not returns.empty else 0.0, 4),
        "largest_loss_pct": round(float(returns.min()) if not returns.empty else 0.0, 4),
        "total_return_pct": round(float(returns.sum()), 4),
        "max_drawdown_pct": round(max_drawdown, 4),
        "expectancy_per_trade_pct": round(expectancy, 4),
        "gross_profit_dollars": round(gross_profit, 2),
        "gross_loss_dollars": round(gross_loss, 2),
        "net_profit_dollars": round(net_profit, 2),
    }])


def close_position(position: Position, exit_date: str, exit_price: float, exit_signal: str, exit_type: str) -> dict:
    gross_pnl = (exit_price - position.entry_price) * position.shares
    return_pct = ((exit_price - position.entry_price) / position.entry_price) * 100.0
    duration_days = (pd.to_datetime(exit_date) - pd.to_datetime(position.entry_date)).days

    return {
        "sector": position.sector,
        "ticker": position.ticker,
        "side": position.side,
        "entry_date": position.entry_date,
        "entry_price": round(position.entry_price, 4),
        "exit_date": exit_date,
        "exit_price": round(exit_price, 4),
        "shares": position.shares,
        "entry_signal": position.entry_signal,
        "exit_signal": exit_signal,
        "gross_pnl_dollars": round(gross_pnl, 2),
        "return_pct": round(return_pct, 4),
        "trade_duration_days": int(duration_days),
        "exit_type": exit_type,
    }


def main():
    root = Path(".")
    params = load_yaml(root / "config" / "paper_trading_parameters.yaml")
    scores = pd.read_csv(root / "data" / "sector_scores.csv")
    market = pd.read_csv(root / "data" / "market_data.csv")
    sector_map_yaml = load_yaml(root / "config" / "sector_map.yaml")["sectors"]
    sector_map = pd.DataFrame(sector_map_yaml)

    scores["date"] = pd.to_datetime(scores["date"]).dt.strftime("%Y-%m-%d")
    market["date"] = pd.to_datetime(market["date"]).dt.strftime("%Y-%m-%d")

    # Build candidate tables once per date.
    all_dates = sorted(set(market["date"]).intersection(set(scores["date"])))
    if len(all_dates) < 2:
        raise SystemExit("Need at least 2 aligned market/signal dates to replay paper trades.")

    price_map = load_price_table(market)
    max_positions = int(params["positions"]["max_concurrent_positions"])
    shares_per_trade = int(params["positions"]["shares_per_trade"])
    required_closes = int(params["confirmation"]["required_consecutive_closes"])
    min_advantage = float(params["replacement"]["min_strength_advantage_points"])

    candidates_by_date = {d: build_candidate_table(scores, sector_map, d, params) for d in sorted(set(scores["date"]))}

    active_positions: List[Position] = []
    trade_log: List[dict] = []
    pending_entries: List[dict] = []
    pending_forced_exits: Dict[str, str] = {}  # ticker -> exit type
    pending_replacements: Dict[str, dict] = {}  # old ticker -> new candidate dict

    for i in range(1, len(all_dates)):
        current_date = all_dates[i]
        previous_date = all_dates[i - 1]

        # 1) Execute any exits at today's open
        survivors: List[Position] = []
        for position in active_positions:
            open_bar = price_map.get((current_date, position.ticker))
            if open_bar is None:
                survivors.append(position)
                continue

            if position.ticker in pending_forced_exits:
                prev_candidates = candidates_by_date.get(previous_date)
                exit_signal = ""
                if prev_candidates is not None:
                    row = prev_candidates[prev_candidates["sector"] == position.sector]
                    if not row.empty:
                        exit_signal = row.iloc[0]["raw_signal"]
                trade_log.append(
                    close_position(position, current_date, float(open_bar["open"]), exit_signal, pending_forced_exits[position.ticker])
                )
                continue

            if position.ticker in pending_replacements:
                exit_signal = pending_replacements[position.ticker]["raw_signal"]
                trade_log.append(
                    close_position(position, current_date, float(open_bar["open"]), exit_signal, "Replacement")
                )
                continue

            survivors.append(position)

        active_positions = survivors

        # 2) Execute replacement entries and regular pending entries at today's open
        consumed_tickers = {p.ticker for p in active_positions}
        execution_queue = list(pending_replacements.values()) + pending_entries
        for entry in execution_queue:
            if len(active_positions) >= max_positions:
                break
            ticker = entry["mapped_ticker"]
            if ticker in consumed_tickers:
                continue
            bar = price_map.get((current_date, ticker))
            if bar is None:
                continue
            stop_pct = stop_pct_for_ticker(ticker, params)
            entry_price = float(bar["open"])
            highest_price = max(entry_price, float(bar["high"]))
            trailing_stop = entry_price * (1.0 - stop_pct)
            active_positions.append(Position(
                sector=entry["sector"],
                ticker=ticker,
                side=entry["side"],
                entry_date=current_date,
                entry_price=entry_price,
                shares=shares_per_trade,
                highest_price=highest_price,
                stop_pct=stop_pct,
                trailing_stop=trailing_stop,
                entry_signal=entry["raw_signal"],
                entry_strength=float(entry["tradable_strength"]),
            ))
            consumed_tickers.add(ticker)

        pending_entries = []
        pending_forced_exits = {}
        pending_replacements = {}

        # 3) Intraday protective stop checks and close-based position updates
        updated_positions: List[Position] = []
        for position in active_positions:
            bar = price_map.get((current_date, position.ticker))
            if bar is None:
                updated_positions.append(position)
                continue

            # Stop uses prior trailing_stop only; avoids assuming intraday high occurred before low.
            if float(bar["low"]) <= position.trailing_stop:
                exit_signal = ""
                current_candidates = candidates_by_date.get(current_date)
                if current_candidates is not None:
                    row = current_candidates[current_candidates["sector"] == position.sector]
                    if not row.empty:
                        exit_signal = row.iloc[0]["raw_signal"]
                trade_log.append(
                    close_position(position, current_date, position.trailing_stop, exit_signal, "Stop")
                )
                continue

            new_high = max(position.highest_price, float(bar["high"]))
            position.highest_price = new_high
            position.trailing_stop = round(new_high * (1.0 - position.stop_pct), 6)
            updated_positions.append(position)

        active_positions = updated_positions

        # 4) Build next-day decisions from today's close
        today_candidates = candidates_by_date.get(current_date, pd.DataFrame())
        if today_candidates.empty:
            continue

        # Managed exits and replacement exits.
        tradable = today_candidates[
            today_candidates["confirmed"]
            & today_candidates["mapped_ticker"].notna()
            & today_candidates["side"].notna()
            & (today_candidates["tradable_strength"] > 0)
        ].copy()

        tradable = tradable.sort_values(["tradable_rank", "sector"], na_position="last")
        top_tradable = tradable.head(max_positions)

        # Determine exits for active positions
        active_after_decisions: List[Position] = []
        occupied_tickers = set()
        for position in active_positions:
            row = today_candidates[today_candidates["sector"] == position.sector]
            row = row.iloc[0] if not row.empty else None

            if row is None:
                active_after_decisions.append(position)
                occupied_tickers.add(position.ticker)
                continue

            raw_signal = row["raw_signal"]
            inst_signal = instrument_perspective_signal(raw_signal, position.side)

            # Managed exit: held instrument turns hostile for 2 confirmed closes.
            # For bull positions, hostile = raw sector bear confirmed.
            # For bear positions, hostile = raw sector bull confirmed.
            raw_confirmed = bool(row["confirmed"])
            hostile = (
                (position.side == "bull" and signal_direction(raw_signal) == "bear" and raw_confirmed)
                or
                (position.side == "bear" and signal_direction(raw_signal) == "bull" and raw_confirmed)
            )
            if hostile:
                pending_forced_exits[position.ticker] = "Managed"
                continue

            # Replacement logic
            candidate_rank = row["tradable_rank"] if not pd.isna(row["tradable_rank"]) else math.inf
            is_strong_for_position = inst_signal == "Strong Bull"
            best_candidate = tradable.iloc[0] if not tradable.empty else None

            if (
                not is_strong_for_position
                and candidate_rank > max_positions
                and best_candidate is not None
                and best_candidate["mapped_ticker"] != position.ticker
                and float(best_candidate["tradable_strength"]) >= float(row["tradable_strength"]) + min_advantage
                and bool(best_candidate["confirmed"])
                and len(active_positions) <= max_positions
            ):
                pending_replacements[position.ticker] = best_candidate.to_dict()
                continue

            active_after_decisions.append(position)
            occupied_tickers.add(position.ticker)

        active_positions = active_after_decisions

        # New entries if slots open.
        open_slots = max_positions - len(active_positions)
        if open_slots > 0 and not tradable.empty:
            candidate_rows = []
            existing_tickers = {p.ticker for p in active_positions}
            replacement_targets = {v["mapped_ticker"] for v in pending_replacements.values()}

            for _, cand in tradable.iterrows():
                if len(candidate_rows) >= open_slots:
                    break
                if cand["mapped_ticker"] in existing_tickers or cand["mapped_ticker"] in replacement_targets:
                    continue
                candidate_rows.append(cand.to_dict())

            pending_entries = candidate_rows

    # Final active positions snapshot at latest available close
    latest_date = all_dates[-1]
    latest_positions_rows = []
    for position in active_positions:
        bar = price_map.get((latest_date, position.ticker))
        current_close = float(bar["close"]) if bar else position.entry_price
        unrealized_pnl = (current_close - position.entry_price) * position.shares
        unrealized_return = ((current_close - position.entry_price) / position.entry_price) * 100.0
        latest_positions_rows.append({
            "as_of_date": latest_date,
            "sector": position.sector,
            "ticker": position.ticker,
            "side": position.side,
            "entry_date": position.entry_date,
            "entry_price": round(position.entry_price, 4),
            "shares": position.shares,
            "entry_signal": position.entry_signal,
            "highest_price": round(position.highest_price, 4),
            "trailing_stop": round(position.trailing_stop, 4),
            "current_close": round(current_close, 4),
            "unrealized_pnl_dollars": round(unrealized_pnl, 2),
            "unrealized_return_pct": round(unrealized_return, 4),
        })

    positions_df = pd.DataFrame(latest_positions_rows)
    trade_log_df = pd.DataFrame(trade_log, columns=[
        "sector","ticker","side","entry_date","entry_price","exit_date","exit_price","shares",
        "entry_signal","exit_signal","gross_pnl_dollars","return_pct","trade_duration_days","exit_type"
    ])
    performance_df = performance_row(trade_log_df)

    data_dir = root / "data"
    positions_df.to_csv(data_dir / "paper_positions.csv", index=False)
    trade_log_df.to_csv(data_dir / "paper_trade_log.csv", index=False)
    performance_df.to_csv(data_dir / "paper_performance.csv", index=False)

    print(f"Wrote {len(positions_df)} active positions to data/paper_positions.csv")
    print(f"Wrote {len(trade_log_df)} closed trades to data/paper_trade_log.csv")
    print("Wrote performance summary to data/paper_performance.csv")


if __name__ == "__main__":
    main()
