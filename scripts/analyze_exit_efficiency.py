#!/usr/bin/env python3
from __future__ import annotations

from pathlib import Path
from typing import Dict, Tuple

import pandas as pd
import yaml


def load_yaml(path: str | Path) -> dict:
    with open(path, "r", encoding="utf-8") as f:
        return yaml.safe_load(f)


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


def stepped_stop_pct_for_ticker(ticker: str, gain_pct: float) -> float:
    """
    Match EXP03 stepped trailing-stop logic exactly.
    gain_pct is decimal:
      0.10 = +10%
      0.20 = +20%
      0.40 = +40%
    """
    lev = leverage_for_ticker(ticker)

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


def load_price_table(market_data: pd.DataFrame) -> Dict[Tuple[pd.Timestamp, str], dict]:
    price_map: Dict[Tuple[pd.Timestamp, str], dict] = {}
    for _, row in market_data.iterrows():
        key = (row["date"], row["ticker"])
        price_map[key] = {
            "open": float(row["open"]),
            "high": float(row["high"]),
            "low": float(row["low"]),
            "close": float(row["close"]),
        }
    return price_map


def classify_stop_exit(
    exit_type: str,
    stop_price: float | None,
    exit_bar: dict | None,
) -> str:
    if exit_type != "trailing_stop":
        return "non_stop_exit"
    if stop_price is None or exit_bar is None:
        return "unknown_stop_exit"

    # If the open is already below the stop, it is effectively a gap through stop.
    if exit_bar["open"] < stop_price:
        return "gap_below_stop"

    # Otherwise if low <= stop, stop was hit intraday.
    if exit_bar["low"] <= stop_price:
        return "intraday_stop_hit"

    return "unknown_stop_exit"


def main() -> None:
    root = Path(".")
    data_dir = root / "data"

    # Config currently only needed to keep repo structure aligned.
    _params = load_yaml(root / "config" / "paper_trading_parameters.yaml")

    market = pd.read_csv(data_dir / "market_data.csv")
    trades = pd.read_csv(data_dir / "paper_trade_log.csv")

    market["date"] = pd.to_datetime(market["date"])
    trades["entry_date"] = pd.to_datetime(trades["entry_date"])
    trades["exit_date"] = pd.to_datetime(trades["exit_date"])

    market["ticker"] = market["ticker"].astype(str)
    trades["ticker"] = trades["ticker"].astype(str)

    price_map = load_price_table(market)

    audit_rows = []

    for _, trade in trades.iterrows():
        ticker = trade["ticker"]
        sector = trade["sector"]
        entry_date = trade["entry_date"]
        exit_date = trade["exit_date"]
        entry_price = float(trade["entry_price"])
        exit_price = float(trade["exit_price"])
        exit_type = str(trade["exit_type"])
        exit_signal = str(trade["exit_signal"])

        # Trade path from entry date through exit date, inclusive.
        trade_bars = market[
            (market["ticker"] == ticker) &
            (market["date"] >= entry_date) &
            (market["date"] <= exit_date)
        ].sort_values("date")

        if trade_bars.empty:
            continue

        peak_idx = trade_bars["high"].idxmax()
        peak_row = trade_bars.loc[peak_idx]
        peak_date = peak_row["date"]
        peak_price = float(peak_row["high"])
        peak_gain_pct = ((peak_price - entry_price) / entry_price) * 100.0

        stop_pct_at_peak = stepped_stop_pct_for_ticker(ticker, peak_gain_pct / 100.0)
        trailing_stop_at_peak = peak_price * (1 - stop_pct_at_peak)

        exit_return_pct = ((exit_price - entry_price) / entry_price) * 100.0
        giveback_pct = peak_gain_pct - exit_return_pct

        exit_bar = price_map.get((exit_date, ticker))
        stop_exit_class = classify_stop_exit(
            exit_type=exit_type,
            stop_price=trailing_stop_at_peak,
            exit_bar=exit_bar,
        )

        # Forward 5 trading days after exit
        future_bars = market[
            (market["ticker"] == ticker) &
            (market["date"] > exit_date)
        ].sort_values("date").head(5)

        if not future_bars.empty:
            post_exit_max_high = float(future_bars["high"].max())
            post_exit_continued_up = post_exit_max_high > exit_price
            post_exit_extra_pct = ((post_exit_max_high - exit_price) / exit_price) * 100.0
            post_exit_5d_bar_count = int(len(future_bars))
        else:
            post_exit_max_high = None
            post_exit_continued_up = None
            post_exit_extra_pct = None
            post_exit_5d_bar_count = 0

        audit_rows.append({
            "sector": sector,
            "ticker": ticker,
            "entry_date": entry_date.strftime("%Y-%m-%d"),
            "exit_date": exit_date.strftime("%Y-%m-%d"),
            "exit_type": exit_type,
            "exit_signal": exit_signal,
            "entry_price": round(entry_price, 4),
            "exit_price": round(exit_price, 4),
            "peak_date": peak_date.strftime("%Y-%m-%d"),
            "peak_price": round(peak_price, 4),
            "peak_gain_pct": round(peak_gain_pct, 4),
            "exit_return_pct": round(exit_return_pct, 4),
            "giveback_pct": round(giveback_pct, 4),
            "stop_pct_at_peak": round(stop_pct_at_peak * 100, 2),
            "trailing_stop_at_peak": round(trailing_stop_at_peak, 4),
            "stop_exit_class": stop_exit_class,
            "post_exit_5d_bar_count": post_exit_5d_bar_count,
            "post_exit_max_high_5d": round(post_exit_max_high, 4) if post_exit_max_high is not None else None,
            "post_exit_continued_up_5d": post_exit_continued_up,
            "post_exit_extra_pct_5d": round(post_exit_extra_pct, 4) if post_exit_extra_pct is not None else None,
        })

    audit_df = pd.DataFrame(audit_rows)

    if audit_df.empty:
        raise SystemExit("No audit rows produced. Check paper_trade_log.csv and market_data.csv.")

    # Full audit
    audit_df = audit_df.sort_values(
        ["giveback_pct", "peak_gain_pct"],
        ascending=[False, False]
    ).reset_index(drop=True)
    audit_df.to_csv(data_dir / "exit_efficiency_audit.csv", index=False)

    # Top profitable givebacks only
    top_givebacks = audit_df[
        (audit_df["exit_return_pct"] > 0) & (audit_df["giveback_pct"] > 0)
    ].sort_values(
        ["giveback_pct", "peak_gain_pct"],
        ascending=[False, False]
    ).reset_index(drop=True)
    top_givebacks.to_csv(data_dir / "top_givebacks.csv", index=False)

    # Helpful summary file
    summary = {
        "total_closed_trades": int(len(audit_df)),
        "trades_continued_higher_5d": int(audit_df["post_exit_continued_up_5d"].fillna(False).sum()),
        "pct_continued_higher_5d": round(
            100.0 * audit_df["post_exit_continued_up_5d"].fillna(False).mean(), 4
        ),
        "avg_post_exit_extra_pct_5d_all": round(
            float(audit_df["post_exit_extra_pct_5d"].dropna().mean()), 4
        ),
        "avg_post_exit_extra_pct_5d_when_higher": round(
            float(audit_df.loc[audit_df["post_exit_continued_up_5d"] == True, "post_exit_extra_pct_5d"].mean()),
            4,
        ),
        "total_giveback_pct_profitable_trades": round(
            float(top_givebacks["giveback_pct"].sum()), 4
        ),
        "gap_below_stop_count": int((audit_df["stop_exit_class"] == "gap_below_stop").sum()),
        "intraday_stop_hit_count": int((audit_df["stop_exit_class"] == "intraday_stop_hit").sum()),
        "non_stop_exit_count": int((audit_df["stop_exit_class"] == "non_stop_exit").sum()),
    }
    pd.DataFrame([summary]).to_csv(data_dir / "exit_efficiency_summary.csv", index=False)

    print("Created:")
    print(" - data/exit_efficiency_audit.csv")
    print(" - data/top_givebacks.csv")
    print(" - data/exit_efficiency_summary.csv")


if __name__ == "__main__":
    main()
