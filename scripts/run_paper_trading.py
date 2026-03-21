#!/usr/bin/env python3
from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Dict, List, Optional, Tuple

import pandas as pd
import yaml
import yfinance as yf


@dataclass
class Position:
    sector: str
    ticker: str
    direction: str
    entry_date: str
    entry_price: float
    shares: int
    highest_price: float
    hard_stop: float              # Hard stop loss price
    stop_pct: float               # Trailing stop percentage
    trailing_stop: float          # Current trailing stop price
    entry_signal: str
    entry_strength: float


class MarketRegimeFilter:
    """Simple market filter using SPY moving average"""
    
    def __init__(self, ma_period: int = 50):
        self.ma_period = ma_period
        self.spy_data = None
        self.spy_ma = None
    
    def load_data(self, start_date: str, end_date: str) -> bool:
        """Load SPY data for the period"""
        try:
            spy = yf.download('SPY', start=start_date, end=end_date, progress=False)
            if spy.empty:
                return False
            self.spy_data = spy['Close']
            self.spy_ma = self.spy_data.rolling(self.ma_period).mean()
            return True
        except Exception as e:
            print(f"Warning: Could not load SPY data: {e}")
            return False
    
    def is_favorable(self, date: str) -> bool:
        """Return True if market conditions are favorable for entry"""
        if self.spy_data is None or self.spy_ma is None:
            return True  # Default to allow if no data
        
        try:
            spy_value = self.spy_data.asof(pd.to_datetime(date))
            ma_value = self.spy_ma.asof(pd.to_datetime(date))
            
            if pd.isna(spy_value) or pd.isna(ma_value):
                return True
            
            return spy_value > ma_value
        except:
            return True


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


def is_bullish_signal(signal: str) -> bool:
    s = normalize_signal(signal)
    return s in {"Bull", "Strong Bull"}


def leverage_for_ticker(ticker: str) -> int:
    t = str(ticker).upper()
    three_x = {
        "SOXL", "SOXS", "TECL", "TECS", "FAS", "FAZ", "ERX", "ERY",
        "LABU", "LABD", "DUSL", "WANT", "TQQQ", "SQQQ", "DRV",
        "UTSL", "SDP", "UYM", "SMN", "UGE", "SZK", "DRN", "SIJ", "SCC"
    }
    two_x = {"CURE", "RXD"}
    if t in three_x:
        return 3
    if t in two_x:
        return 2
    return 1


def get_hard_stop_pct(ticker: str, params: dict) -> float:
    """Get hard stop percentage for a ticker based on leverage"""
    lev = leverage_for_ticker(ticker)
    hard_stops = params["stops"]["hard_stop"]
    
    if lev == 3:
        return hard_stops["leverage_3x_pct"] / 100.0
    elif lev == 2:
        return hard_stops["leverage_2x_pct"] / 100.0
    else:
        return hard_stops["leverage_1x_pct"] / 100.0


def get_trailing_stop_pct(ticker: str, gain_pct: float, params: dict) -> float:
    """
    Get trailing stop percentage based on current gain.
    Uses the stepped logic from exp03 but with tighter initial stops.
    """
    lev = leverage_for_ticker(ticker)
    
    # Use stepped logic for trailing (tightens as profit grows)
    if lev == 3:
        if gain_pct >= 0.40:
            return 0.10
        elif gain_pct >= 0.20:
            return 0.12
        elif gain_pct >= 0.10:
            return 0.14
        else:
            return 0.18
    
    if lev == 2:
        if gain_pct >= 0.40:
            return 0.08
        elif gain_pct >= 0.20:
            return 0.09
        elif gain_pct >= 0.10:
            return 0.11
        else:
            return 0.14
    
    # 1x
    if gain_pct >= 0.40:
        return 0.06
    elif gain_pct >= 0.20:
        return 0.07
    elif gain_pct >= 0.10:
        return 0.09
    else:
        return 0.10


def calculate_position_size(
    account_value: float,
    entry_price: float,
    hard_stop_pct: float,
    risk_per_trade_pct: float,
    signal_multiplier: float = 1.0
) -> int:
    """
    Calculate position size based on account risk.
    
    Args:
        account_value: Current account value
        entry_price: Entry price per share
        hard_stop_pct: Stop loss percentage (decimal, e.g., 0.09)
        risk_per_trade_pct: Risk per trade as decimal (e.g., 0.02)
        signal_multiplier: 1.0 for Bull, 1.5 for Strong Bull
    
    Returns:
        Number of shares to buy
    """
    risk_dollars = account_value * risk_per_trade_pct * signal_multiplier
    stop_distance = entry_price * hard_stop_pct
    
    if stop_distance <= 0:
        return 0
    
    shares = int(risk_dollars / stop_distance)
    return max(1, shares)  # At least 1 share


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


def signal_confirmed_for_entry(
    scores: pd.DataFrame,
    sector: str,
    signal_date: str,
    required_closes: int,
) -> bool:
    subset = scores[(scores["sector"] == sector) & (scores["date"] <= signal_date)].sort_values("date")
    if len(subset) < required_closes:
        return False

    tail = subset.tail(required_closes)

    directions = tail["direction"].astype(str).tolist()
    etfs = tail["selected_etf"].fillna("").astype(str).tolist()

    if any(d != "long" for d in directions):
        return False
    if any(e.strip() == "" for e in etfs):
        return False

    return len(set(directions)) == 1 and len(set(etfs)) == 1


def latest_scores_for_date(scores: pd.DataFrame, date: str) -> pd.DataFrame:
    day = scores[scores["date"] == date].copy()
    if day.empty:
        return day

    day["strength"] = day["total_score"].astype(float).abs()
    day = day.sort_values(["strength", "sector"], ascending=[False, True]).reset_index(drop=True)
    return day


def close_position(position: Position, exit_date: str, exit_price: float, exit_signal: str, exit_type: str) -> dict:
    gross_pnl = (exit_price - position.entry_price) * position.shares
    return_pct = ((exit_price - position.entry_price) / position.entry_price) * 100.0
    duration_days = (pd.to_datetime(exit_date) - pd.to_datetime(position.entry_date)).days

    return {
        "sector": position.sector,
        "ticker": position.ticker,
        "direction": position.direction,
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

    tl = trade_log.copy()
    tl["equity_curve_pct"] = tl["return_pct"].cumsum()
    tl["equity_peak_pct"] = tl["equity_curve_pct"].cummax()
    tl["drawdown_pct"] = tl["equity_curve_pct"] - tl["equity_peak_pct"]
    max_drawdown = float(tl["drawdown_pct"].min()) if not tl.empty else 0.0

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


def main():
    root = Path(".")
    data_dir = root / "data"

    params = load_yaml(root / "config" / "paper_trading_parameters.yaml")
    scores = pd.read_csv(data_dir / "sector_scores.csv")
    market = pd.read_csv(data_dir / "market_data.csv")

    scores["date"] = pd.to_datetime(scores["date"]).dt.strftime("%Y-%m-%d")
    market["date"] = pd.to_datetime(market["date"]).dt.strftime("%Y-%m-%d")
    scores["selected_etf"] = scores["selected_etf"].fillna("").astype(str)
    scores["direction"] = scores["direction"].fillna("none").astype(str)
    signal_col = "signal_state" if "signal_state" in scores.columns else "signal"

    all_dates = sorted(set(market["date"]).intersection(set(scores["date"])))
    if len(all_dates) < 2:
        raise SystemExit("Need at least 2 aligned market/signal dates to replay paper trades.")

    price_map = load_price_table(market)
    
    # Load parameters
    max_positions = int(params["positions"]["max_concurrent_positions"])
    required_closes = int(params["confirmation"]["required_consecutive_closes"])
    
    # Account and risk parameters
    account_value = float(params["account"]["initial_balance"])
    risk_per_trade_pct = params["account"]["risk_per_trade_pct"] / 100.0
    strong_bull_multiplier = params["entry"]["strong_bull_multiplier"]
    min_score_threshold = params["entry"]["min_score_threshold"]
    excluded_sectors = params["entry"].get("excluded_sectors", [])
    
    # Market filter
    market_filter_enabled = params["market_filter"]["enabled"]
    market_filter = None
    if market_filter_enabled:
        market_filter = MarketRegimeFilter(ma_period=params["market_filter"]["ma_period"])
        market_filter.load_data(all_dates[0], all_dates[-1])

    active_positions: List[Position] = []
    trade_log: List[dict] = []

    # yesterday's signal drives today's open
    for i in range(1, len(all_dates)):
        signal_date = all_dates[i - 1]
        trade_date = all_dates[i]

        signal_day = latest_scores_for_date(scores, signal_date)
        signal_by_sector = {row["sector"]: row for _, row in signal_day.iterrows()}

        # 1) exits
        survivors: List[Position] = []
        for position in active_positions:
            bar = price_map.get((trade_date, position.ticker))
            if not bar:
                survivors.append(position)
                continue

            # Check hard stop first
            if bar["low"] <= position.hard_stop:
                trade_log.append(
                    close_position(
                        position=position,
                        exit_date=trade_date,
                        exit_price=bar["open"],
                        exit_signal="Stop",
                        exit_type="hard_stop",
                    )
                )
                # Update account value after exit
                account_value += (bar["open"] * position.shares)
                continue

            # Update trailing stop based on current gain
            current_gain_pct = (position.highest_price - position.entry_price) / position.entry_price
            position.stop_pct = get_trailing_stop_pct(position.ticker, current_gain_pct, params)
            position.trailing_stop = position.highest_price * (1 - position.stop_pct)

            # Check trailing stop
            if bar["low"] <= position.trailing_stop:
                trade_log.append(
                    close_position(
                        position=position,
                        exit_date=trade_date,
                        exit_price=bar["open"],
                        exit_signal="Stop",
                        exit_type="trailing_stop",
                    )
                )
                account_value += (bar["open"] * position.shares)
                continue

            # Check signal-based exits
            signal_row = signal_by_sector.get(position.sector)
            if signal_row is None:
                survivors.append(position)
                continue

            raw_signal = normalize_signal(signal_row[signal_col])
            row_ticker = str(signal_row["selected_etf"]).strip()
            row_direction = str(signal_row["direction"]).strip().lower()

            exit_type: Optional[str] = None

            if not is_bullish_signal(raw_signal):
                exit_type = "signal_change"
            elif row_direction != "long":
                exit_type = "direction_change"
            elif row_ticker != "" and row_ticker != position.ticker:
                exit_type = "ticker_changed"

            if exit_type:
                trade_log.append(
                    close_position(
                        position=position,
                        exit_date=trade_date,
                        exit_price=bar["open"],
                        exit_signal=raw_signal,
                        exit_type=exit_type,
                    )
                )
                account_value += (bar["open"] * position.shares)
            else:
                new_high = max(position.highest_price, bar["high"])
                position.highest_price = new_high
                survivors.append(position)

        active_positions = survivors

        # 2) entries (long-only)
        # Market filter check
        if market_filter_enabled and market_filter:
            if not market_filter.is_favorable(trade_date):
                # Skip entries for this day
                continue

        candidates = []
        for _, row in signal_day.iterrows():
            sector = row["sector"]
            direction = str(row["direction"]).strip().lower()
            ticker = str(row["selected_etf"]).strip()
            total_score = float(row["total_score"])
            normalized_signal = normalize_signal(row[signal_col])

            # Filter by minimum score threshold
            if total_score < min_score_threshold:
                continue

            # Filter out excluded sectors
            if sector in excluded_sectors:
                continue

            if direction != "long":
                continue
            if not is_bullish_signal(normalized_signal):
                continue
            if ticker == "":
                continue
            if any(p.sector == sector for p in active_positions):
                continue
            if not signal_confirmed_for_entry(scores, sector, signal_date, required_closes):
                continue

            candidates.append({
                "sector": sector,
                "ticker": ticker,
                "direction": direction,
                "signal": normalized_signal,
                "strength": float(abs(total_score)),
            })

        candidates = sorted(candidates, key=lambda x: (-x["strength"], x["sector"]))

        for candidate in candidates:
            if len(active_positions) >= max_positions:
                break

            bar = price_map.get((trade_date, candidate["ticker"]))
            if not bar:
                continue

            entry_price = float(bar["open"])
            
            # Calculate position size based on risk
            hard_stop_pct = get_hard_stop_pct(candidate["ticker"], params)
            signal_multiplier = strong_bull_multiplier if candidate["signal"] == "Strong Bull" else 1.0
            
            shares = calculate_position_size(
                account_value=account_value,
                entry_price=entry_price,
                hard_stop_pct=hard_stop_pct,
                risk_per_trade_pct=risk_per_trade_pct,
                signal_multiplier=signal_multiplier
            )
            
            if shares <= 0:
                continue
            
            # Calculate stop prices
            hard_stop = entry_price * (1 - hard_stop_pct)
            
            # Initial trailing stop (use base stop from legacy settings)
            base_stop_pct = 0.18  # Default for 3x, will be updated dynamically
            if leverage_for_ticker(candidate["ticker"]) == 1:
                base_stop_pct = 0.10
            elif leverage_for_ticker(candidate["ticker"]) == 2:
                base_stop_pct = 0.14
            else:
                base_stop_pct = 0.18
            
            trailing_stop = float(bar["high"]) * (1 - base_stop_pct)
            
            # Deduct position cost from account (simulate capital allocation)
            position_cost = entry_price * shares
            if position_cost > account_value:
                # Not enough capital for full position, reduce shares
                shares = int(account_value / entry_price)
                if shares <= 0:
                    continue
                position_cost = entry_price * shares
            
            account_value -= position_cost

            active_positions.append(
                Position(
                    sector=candidate["sector"],
                    ticker=candidate["ticker"],
                    direction="long",
                    entry_date=trade_date,
                    entry_price=entry_price,
                    shares=shares,
                    highest_price=float(bar["high"]),
                    hard_stop=hard_stop,
                    stop_pct=base_stop_pct,
                    trailing_stop=trailing_stop,
                    entry_signal=candidate["signal"],
                    entry_strength=float(candidate["strength"]),
                )
            )

    # Add remaining open positions to trade log at last price
    if active_positions and all_dates:
        last_date = all_dates[-1]
        for position in active_positions:
            bar = price_map.get((last_date, position.ticker))
            if bar:
                trade_log.append(
                    close_position(
                        position=position,
                        exit_date=last_date,
                        exit_price=bar["close"],
                        exit_signal="End",
                        exit_type="end_of_period",
                    )
                )

    positions_df = pd.DataFrame([
        {
            "sector": p.sector,
            "ticker": p.ticker,
            "direction": p.direction,
            "entry_date": p.entry_date,
            "entry_price": round(p.entry_price, 4),
            "shares": p.shares,
            "highest_price": round(p.highest_price, 4),
            "hard_stop": round(p.hard_stop, 4),
            "stop_pct": round(p.stop_pct, 4),
            "trailing_stop": round(p.trailing_stop, 4),
            "entry_signal": p.entry_signal,
            "entry_strength": round(p.entry_strength, 4),
        }
        for p in active_positions
    ])
    trade_log_df = pd.DataFrame(trade_log)
    perf_df = performance_row(trade_log_df)

    positions_df.to_csv(data_dir / "paper_positions.csv", index=False)
    trade_log_df.to_csv(data_dir / "paper_trade_log.csv", index=False)
    perf_df.to_csv(data_dir / "paper_performance.csv", index=False)

    print(f"Paper trading complete.")
    print(f"  Final account value: ${account_value:,.2f}")
    print(f"  Open positions: {len(active_positions)}")
    print(f"  Closed trades: {len(trade_log)}")
    print(f"  Net profit: ${perf_df['net_profit_dollars'].iloc[0]:,.2f}")
    print(f"  Total return: {perf_df['total_return_pct'].iloc[0]}%")


if __name__ == "__main__":
    main()
