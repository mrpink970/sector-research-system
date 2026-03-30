#!/usr/bin/env python3
from **future** import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Dict, List, Optional, Set, Tuple

import pandas as pd
import yaml

@dataclass
class Position:
sector: str
ticker: str
direction: str
entry_date: str
entry_price: float
shares: int
highest_price: float
stop_pct: float
trailing_stop: float
entry_signal: str
entry_strength: float

def load_yaml(path: str | Path) -> dict:
with open(path, “r”, encoding=“utf-8”) as f:
return yaml.safe_load(f)

def normalize_signal(signal: str) -> str:
signal = str(signal).strip()
mapping = {
“Strong Bullish”: “Strong Bull”,
“Bullish”: “Bull”,
“Neutral”: “Neutral”,
“Bearish”: “Bear”,
“Strong Bearish”: “Strong Bear”,
“Strong Bull”: “Strong Bull”,
“Bull”: “Bull”,
“Bear”: “Bear”,
“Strong Bear”: “Strong Bear”,
}
return mapping.get(signal, signal)

def is_bullish_signal(signal: str) -> bool:
return normalize_signal(signal) in {“Bull”, “Strong Bull”}

def leverage_for_ticker(ticker: str) -> int:
t = str(ticker).upper()
three_x = {
“SOXL”, “SOXS”, “TECL”, “TECS”, “FAS”, “FAZ”, “ERX”, “ERY”,
“LABU”, “LABD”, “DUSL”, “WANT”, “TQQQ”, “SQQQ”, “DRV”,
“UTSL”, “SDP”, “UYM”, “SMN”, “UGE”, “SZK”, “DRN”, “SIJ”, “SCC”
}
two_x = {“CURE”, “RXD”}
if t in three_x:
return 3
if t in two_x:
return 2
return 1

def base_stop_pct_for_ticker(ticker: str, params: dict) -> float:
lev = leverage_for_ticker(ticker)
stops = params[“stops”]
if lev == 3:
return float(stops[“leverage_3x_pct”]) / 100.0
if lev == 2:
return float(stops[“leverage_2x_pct”]) / 100.0
return float(stops[“leverage_1x_pct”]) / 100.0

def stepped_stop_pct_for_ticker(ticker: str, gain_pct: float, params: dict) -> float:
“””
EXP03+: stop distance tightens in steps as profit grows.
gain_pct is decimal (0.10 = +10%, 0.20 = +20%, 0.40 = +40%).
“””
lev = leverage_for_ticker(ticker)

```
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
```

def load_price_table(market_data: pd.DataFrame) -> Dict[Tuple[str, str], dict]:
price_map: Dict[Tuple[str, str], dict] = {}
for _, row in market_data.iterrows():
key = (row[“date”], row[“ticker”])
price_map[key] = {
“open”: float(row[“open”]),
“high”: float(row[“high”]),
“low”: float(row[“low”]),
“close”: float(row[“close”]),
}
return price_map

def build_qqq_signal_table(scores: pd.DataFrame, signal_col: str) -> Dict[str, str]:
“””
Pre-build date -> QQQ signal_state lookup for fast regime checks at entry.
QQQ is tracked as the ‘Nasdaq Growth’ sector in sector_scores.
“””
qqq_rows = scores[scores[“sector”] == “Nasdaq Growth”]
return {row[“date”]: row[signal_col] for _, row in qqq_rows.iterrows()}

def signal_confirmed_for_entry(
scores: pd.DataFrame,
sector: str,
signal_date: str,
required_closes: int,
) -> bool:
subset = scores[(scores[“sector”] == sector) & (scores[“date”] <= signal_date)].sort_values(“date”)
if len(subset) < required_closes:
return False

```
tail = subset.tail(required_closes)
directions = tail["direction"].astype(str).tolist()
etfs = tail["selected_etf"].fillna("").astype(str).tolist()

if any(d != "long" for d in directions):
    return False
if any(e.strip() == "" for e in etfs):
    return False

return len(set(directions)) == 1 and len(set(etfs)) == 1
```

def latest_scores_for_date(scores: pd.DataFrame, date: str) -> pd.DataFrame:
day = scores[scores[“date”] == date].copy()
if day.empty:
return day

```
day["strength"] = day["total_score"].astype(float).abs()
day = day.sort_values(["strength", "sector"], ascending=[False, True]).reset_index(drop=True)
return day
```

def close_position(
position: Position,
exit_date: str,
exit_price: float,
exit_signal: str,
exit_type: str,
) -> dict:
gross_pnl = (exit_price - position.entry_price) * position.shares
return_pct = ((exit_price - position.entry_price) / position.entry_price) * 100.0
duration_days = (pd.to_datetime(exit_date) - pd.to_datetime(position.entry_date)).days

```
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
    "entry_strength": round(position.entry_strength, 4),
    "exit_signal": exit_signal,
    "gross_pnl_dollars": round(gross_pnl, 2),
    "return_pct": round(return_pct, 4),
    "trade_duration_days": int(duration_days),
    "exit_type": exit_type,
}
```

def performance_row(trade_log: pd.DataFrame) -> pd.DataFrame:
if trade_log.empty:
return pd.DataFrame([{
“total_trades”: 0,
“win_rate_pct”: 0.0,
“loss_rate_pct”: 0.0,
“average_gain_pct”: 0.0,
“average_loss_pct”: 0.0,
“largest_gain_pct”: 0.0,
“largest_loss_pct”: 0.0,
“total_return_pct”: 0.0,
“max_drawdown_pct”: 0.0,
“expectancy_per_trade_pct”: 0.0,
“gross_profit_dollars”: 0.0,
“gross_loss_dollars”: 0.0,
“net_profit_dollars”: 0.0,
}])

```
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
```

def main():
root = Path(”.”)
data_dir = root / “data”

```
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

# EXP05: sector filter
allowed_sectors: Optional[Set[str]] = None
sector_cfg = params.get("sectors", {})
if sector_cfg and "allowed" in sector_cfg:
    allowed_sectors = set(sector_cfg["allowed"])
    print(f"Sector filter active: {sorted(allowed_sectors)}")
else:
    print("No sector filter — all sectors eligible.")

# EXP06a: QQQ regime filter
# Only enter new positions when QQQ (Nasdaq Growth) is Bull or Strong Bull.
# QQQ is already scored daily — no new data needed.
# Exits are not affected; open positions run to completion regardless of QQQ.
qqq_filter_enabled: bool = params.get("filters", {}).get("require_qqq_bull", False)
qqq_signal_table = build_qqq_signal_table(scores, signal_col)
if qqq_filter_enabled:
    print("QQQ bull filter active: entries blocked when QQQ is Neutral or Bear.")
else:
    print("QQQ bull filter inactive.")

price_map = load_price_table(market)
max_positions = int(params["positions"]["max_concurrent_positions"])
required_closes = int(params["confirmation"]["required_consecutive_closes"])

# Minimum hold period for signal_change and direction_change exits.
# Trailing stops are NEVER blocked — they fire immediately at all times.
# A signal wobble in the first few days should not flush a position that
# hasn't had time to develop. Set to 0 to disable.
min_hold_days = int(params.get("exit", {}).get("min_hold_days", 0))
if min_hold_days:
    print(f"Minimum hold period: {min_hold_days} days (trailing stops exempt).")

# Dollar-based position sizing.
# account_size:      cash on hand
# margin_pct:        additional buying power as fraction of account (0.20 = 20%)
# buying_power:      total capital available across all positions
# dollars_per_trade: budget per position = buying_power / max_positions
# Shares = floor(dollars_per_trade / entry_open_price), minimum 1.
account_size      = float(params["positions"]["account_size"])
margin_pct        = float(params["positions"].get("margin_pct", 0.0))
buying_power      = account_size * (1.0 + margin_pct)
dollars_per_trade = buying_power / max_positions
print(f"Account: ${account_size:,.0f} | Margin: {margin_pct:.0%} | "
      f"Buying power: ${buying_power:,.0f} | Per position: ${dollars_per_trade:,.0f}")

active_positions: List[Position] = []
trade_log: List[dict] = []

# Yesterday's signal drives today's open
for i in range(1, len(all_dates)):
    signal_date = all_dates[i - 1]
    trade_date = all_dates[i]

    signal_day = latest_scores_for_date(scores, signal_date)
    signal_by_sector = {row["sector"]: row for _, row in signal_day.iterrows()}

    # 1) Exits — no filters applied; all open positions managed to completion
    survivors: List[Position] = []
    for position in active_positions:
        bar = price_map.get((trade_date, position.ticker))
        if not bar:
            survivors.append(position)
            continue

        current_gain_pct = (position.highest_price - position.entry_price) / position.entry_price
        position.stop_pct = stepped_stop_pct_for_ticker(position.ticker, current_gain_pct, params)
        position.trailing_stop = position.highest_price * (1 - position.stop_pct)

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
            continue

        signal_row = signal_by_sector.get(position.sector)
        if signal_row is None:
            survivors.append(position)
            continue

        raw_signal = normalize_signal(signal_row[signal_col])
        row_ticker = str(signal_row["selected_etf"]).strip()
        row_direction = str(signal_row["direction"]).strip().lower()

        exit_type: Optional[str] = None

        # Check whether the position has met the minimum hold requirement.
        # Trailing stops bypass this check entirely and always fire.
        days_held = (pd.to_datetime(trade_date) - pd.to_datetime(position.entry_date)).days
        hold_satisfied = days_held >= min_hold_days

        if not is_bullish_signal(raw_signal):
            # Only exit on signal change if minimum hold is met.
            # If not yet met, keep holding — the trailing stop still protects.
            if hold_satisfied:
                exit_type = "signal_change"
        elif row_direction != "long":
            if hold_satisfied:
                exit_type = "direction_change"
        elif row_ticker != "" and row_ticker != position.ticker:
            # Ticker rotation exits always fire — same as trailing stop,
            # the position is already meaningfully different.
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
        else:
            new_high = max(position.highest_price, bar["high"])
            position.highest_price = new_high
            current_gain_pct = (position.highest_price - position.entry_price) / position.entry_price
            position.stop_pct = stepped_stop_pct_for_ticker(position.ticker, current_gain_pct, params)
            position.trailing_stop = position.highest_price * (1 - position.stop_pct)
            survivors.append(position)

    active_positions = survivors

    # 2) Entries — all filters applied here only
    # EXP06a: check QQQ regime on signal date before evaluating any entries
    qqq_raw = qqq_signal_table.get(signal_date, "Neutral")
    qqq_is_bull = normalize_signal(qqq_raw) in {"Bull", "Strong Bull"}

    if qqq_filter_enabled and not qqq_is_bull:
        # QQQ not bullish — skip all entries today, exits above still processed
        continue

    candidates = []
    for _, row in signal_day.iterrows():
        sector = row["sector"]
        direction = str(row["direction"]).strip().lower()
        ticker = str(row["selected_etf"]).strip()
        total_score = float(row["total_score"])
        normalized_signal = normalize_signal(row[signal_col])

        # EXP05: sector filter
        if allowed_sectors is not None and sector not in allowed_sectors:
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

        stop_pct = base_stop_pct_for_ticker(candidate["ticker"], params)
        highest_price = float(bar["high"])
        trailing_stop = highest_price * (1 - stop_pct)

        active_positions.append(
            Position(
                sector=candidate["sector"],
                ticker=candidate["ticker"],
                direction="long",
                entry_date=trade_date,
                entry_price=float(bar["open"]),
                shares=max(1, int(dollars_per_trade / float(bar["open"]))),
                highest_price=highest_price,
                stop_pct=float(stop_pct),
                trailing_stop=trailing_stop,
                entry_signal=candidate["signal"],
                entry_strength=float(candidate["strength"]),
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

print(f"Paper trading complete. Open positions: {len(active_positions)} | Closed trades: {len(trade_log)}")
```

if **name** == “**main**”:
main()
