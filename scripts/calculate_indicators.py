#!/usr/bin/env python3

from __future__ import annotations
import math
from pathlib import Path

import pandas as pd
import yaml


def load_yaml(path: Path) -> dict:
    with path.open("r", encoding="utf-8") as f:
        return yaml.safe_load(f)


def load_data() -> pd.DataFrame:
    path = Path("data/market_data.csv")
    df = pd.read_csv(path)
    if df.empty:
        return df

    df["date"] = pd.to_datetime(df["date"])
    df = df.sort_values(["ticker", "date"]).reset_index(drop=True)
    return df


def atr(group: pd.DataFrame, period: int) -> pd.Series:
    prev_close = group["close"].shift(1)
    tr1 = group["high"] - group["low"]
    tr2 = (group["high"] - prev_close).abs()
    tr3 = (group["low"] - prev_close).abs()
    tr = pd.concat([tr1, tr2, tr3], axis=1).max(axis=1)
    return tr.rolling(period).mean()


def main():
    repo_root = Path(".")
    config_dir = repo_root / "config"

    sector_map = load_yaml(config_dir / "sector_map.yaml")
    params = load_yaml(config_dir / "system_parameters.yaml")["scoring"]

    trend_ma_period = int(params["trend_ma_period"])
    trend_ignition_ma_period = int(params["trend_ignition_ma_period"])
    trend_ignition_lookback_days = int(params["trend_ignition_lookback_days"])
    momentum_roc_period = int(params["momentum_roc_period"])
    momentum_threshold = float(params["momentum_threshold_pct"]) / 100.0
    rs_period = int(params["relative_strength_period"])
    rs_threshold = float(params["relative_strength_threshold_pct"]) / 100.0
    rs_persistence_days = int(params["rs_persistence_days"])
    exhaustion_short = int(params["exhaustion_short_roc_period"])
    exhaustion_long = int(params["exhaustion_long_roc_period"])
    exhaustion_threshold = float(params["exhaustion_threshold_pct"]) / 100.0
    atr_period = int(params["atr_period"])
    volatility_limit = float(params["volatility_limit_pct_of_price"]) / 100.0

    df = load_data()
    if df.empty:
        print("market_data.csv is empty")
        return

    # Keep only the signal ETFs plus VOO benchmark
    signal_rows = []
    for row in sector_map["sectors"]:
        signal_rows.append((row["sector"], row["signal_etf"]))
    signal_df = pd.DataFrame(signal_rows, columns=["sector", "signal_etf"])

    needed_tickers = set(signal_df["signal_etf"].tolist() + ["VOO"])
    df = df[df["ticker"].isin(needed_tickers)].copy()

    if df.empty:
        print("No signal ETF data found")
        return

    # Calculate per-ticker rolling values
    out_frames = []

    for ticker, group in df.groupby("ticker", group_keys=False):
        group = group.sort_values("date").copy()

        group["ma50"] = group["close"].rolling(trend_ma_period).mean()
        group["ma20"] = group["close"].rolling(trend_ignition_ma_period).mean()
        group["ma20_slope"] = group["ma20"] - group["ma20"].shift(trend_ignition_lookback_days)

        group["roc_5"] = group["close"] / group["close"].shift(momentum_roc_period) - 1
        group["roc_10"] = group["close"] / group["close"].shift(rs_period) - 1
        group["roc_3"] = group["close"] / group["close"].shift(exhaustion_short) - 1

        group["atr14"] = atr(group, atr_period)
        group["atr_pct"] = group["atr14"] / group["close"]

        out_frames.append(group)

    calc = pd.concat(out_frames, ignore_index=True)

    # Build benchmark map from VOO
    voo = calc[calc["ticker"] == "VOO"][["date", "roc_10"]].rename(columns={"roc_10": "voo_roc_10"})

    # Join sector names
    calc = calc.merge(signal_df, left_on="ticker", right_on="signal_etf", how="left")
    calc = calc.merge(voo, on="date", how="left")

    # Only keep sector rows
    calc = calc[calc["sector"].notna()].copy()

    # Trend score
    calc["trend_score"] = calc.apply(
        lambda r: 2 if pd.notna(r["ma50"]) and r["close"] > r["ma50"]
        else (-2 if pd.notna(r["ma50"]) and r["close"] < r["ma50"] else 0),
        axis=1,
    )

    # Trend ignition score
    calc["trend_ignition_score"] = calc.apply(
        lambda r: 1 if pd.notna(r["ma20_slope"]) and r["ma20_slope"] > 0
        else (-1 if pd.notna(r["ma20_slope"]) and r["ma20_slope"] < 0 else 0),
        axis=1,
    )

    # Momentum score
    calc["momentum_score"] = calc.apply(
        lambda r: 2 if pd.notna(r["roc_5"]) and r["roc_5"] >= momentum_threshold
        else (-2 if pd.notna(r["roc_5"]) and r["roc_5"] <= -momentum_threshold else 0),
        axis=1,
    )

    # Relative strength score vs VOO
    calc["rs_diff"] = calc["roc_10"] - calc["voo_roc_10"]
    calc["relative_strength_score"] = calc.apply(
        lambda r: 1 if pd.notna(r["rs_diff"]) and r["rs_diff"] >= rs_threshold
        else (-1 if pd.notna(r["rs_diff"]) and r["rs_diff"] <= -rs_threshold else 0),
        axis=1,
    )

    # RS persistence score
    calc = calc.sort_values(["sector", "date"]).copy()
    calc["rs_outperform_flag"] = calc["rs_diff"].apply(
        lambda x: 1 if pd.notna(x) and x >= rs_threshold
        else (-1 if pd.notna(x) and x <= -rs_threshold else 0)
    )

    rs_persistence_scores = []
    for sector, group in calc.groupby("sector", group_keys=False):
        streak = 0
        values = []
        for flag in group["rs_outperform_flag"]:
            if flag == 1:
                streak = streak + 1 if streak >= 0 else 1
            elif flag == -1:
                streak = streak - 1 if streak <= 0 else -1
            else:
                streak = 0

            if streak >= rs_persistence_days:
                values.append(1)
            elif streak <= -rs_persistence_days:
                values.append(-1)
            else:
                values.append(0)

        rs_persistence_scores.extend(values)

    calc["rs_persistence_score"] = rs_persistence_scores

    # Momentum exhaustion score
    calc["momentum_decay"] = calc["roc_3"] - calc["roc_10"]
    calc["momentum_exhaustion_score"] = calc["momentum_decay"].apply(
        lambda x: -1 if pd.notna(x) and x <= exhaustion_threshold else 0
    )

    # Volatility score
    calc["volatility_score"] = calc["atr_pct"].apply(
        lambda x: -1 if pd.notna(x) and x > volatility_limit else 0
    )

    final_cols = [
        "date",
        "sector",
        "signal_etf",
        "trend_score",
        "trend_ignition_score",
        "momentum_score",
        "relative_strength_score",
        "rs_persistence_score",
        "momentum_exhaustion_score",
        "volatility_score",
    ]

    final = calc[final_cols].copy()
    final["date"] = final["date"].dt.strftime("%Y-%m-%d")
    final = final.sort_values(["date", "sector"]).reset_index(drop=True)

    out_path = Path("data/indicators.csv")
    final.to_csv(out_path, index=False)
    print(f"Wrote {len(final)} rows to {out_path}")


if __name__ == "__main__":
    main()
