#!/usr/bin/env python3
from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Dict, List

import numpy as np
import pandas as pd
import yfinance as yf
import yaml


@dataclass
class Paths:
    universe: Path
    benchmark_cache: Path
    full_history: Path
    candidate_history: Path
    run_log: Path


def load_yaml(path: Path) -> dict:
    with path.open("r", encoding="utf-8") as f:
        return yaml.safe_load(f)


def ensure_parent(path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)


def load_universe(path: Path) -> List[str]:
    df = pd.read_csv(path)
    return sorted(set(df["ticker"].dropna().astype(str).str.upper().tolist()))


def download_history(ticker: str, period: str) -> pd.DataFrame:
    df = yf.download(
        ticker,
        period=period,
        interval="1d",
        progress=False,
        threads=False,
    )

    if df.empty:
        return df

    df = df.reset_index()
    return df[["Date", "Open", "High", "Low", "Close", "Volume"]]


def calc_atr(df: pd.DataFrame, window: int = 14) -> pd.Series:
    high = df["High"]
    low = df["Low"]
    close = df["Close"]
    prev_close = close.shift(1)

    tr = pd.concat(
        [
            high - low,
            (high - prev_close).abs(),
            (low - prev_close).abs(),
        ],
        axis=1,
    ).max(axis=1)

    return tr.rolling(window).mean()


def latest_metrics(df: pd.DataFrame, bench_ret_63: float) -> Dict[str, float]:
    close = df["Close"]
    volume = df["Volume"]

    ret_21 = close.iloc[-1] / close.shift(21).iloc[-1] - 1 if len(df) > 21 else np.nan
    ret_63 = close.iloc[-1] / close.shift(63).iloc[-1] - 1 if len(df) > 63 else np.nan
    ret_126 = close.iloc[-1] / close.shift(126).iloc[-1] - 1 if len(df) > 126 else np.nan

    ma_20 = close.rolling(20).mean().iloc[-1]
    ma_50 = close.rolling(50).mean().iloc[-1]
    ma_100 = close.rolling(100).mean().iloc[-1]

    avg_vol_20 = volume.rolling(20).mean().iloc[-1]
    avg_vol_50 = volume.rolling(50).mean().iloc[-1]

    vol_ratio = avg_vol_20 / avg_vol_50 if avg_vol_50 else np.nan

    atr_14 = calc_atr(df, 14).iloc[-1]
    atr_pct = atr_14 / close.iloc[-1] if close.iloc[-1] else np.nan

    extension_pct = (close.iloc[-1] - ma_20) / ma_20 if ma_20 else np.nan

    rs_63 = ret_63 - bench_ret_63 if pd.notna(ret_63) else np.nan

    return {
        "close": close.iloc[-1],
        "avg_volume_50": avg_vol_50,
        "ret_21": ret_21,
        "ret_63": ret_63,
        "ret_126": ret_126,
        "spy_ret_63": bench_ret_63,
        "rs_63": rs_63,
        "ma_20": ma_20,
        "ma_50": ma_50,
        "ma_100": ma_100,
        "vol_ratio": vol_ratio,
        "atr_pct": atr_pct,
        "extension_pct": extension_pct,
    }


def score_row(row: Dict[str, float], params: dict) -> Dict[str, int | str]:
    s = params["scoring"]

    rs_score = 2 if row["rs_63"] >= s["rs_strong"] else 1 if row["rs_63"] >= s["rs_moderate"] else 0

    positives = sum(
        x > 0 for x in [row["ret_21"], row["ret_63"], row["ret_126"]] if pd.notna(x)
    )
    trend_score = 2 if positives == 3 else 1 if positives >= 2 else 0

    structure_score = (
        1 if row["close"] > row["ma_20"] > row["ma_50"] > row["ma_100"] else 0
    )

    volume_score = 1 if row["vol_ratio"] >= s["volume_ratio"] else 0
    volatility_score = 1 if row["atr_pct"] <= s["atr_threshold"] else 0
    extension_score = 1 if row["extension_pct"] <= s["extension_threshold"] else 0

    total = (
        rs_score
        + trend_score
        + structure_score
        + volume_score
        + volatility_score
        + extension_score
    )

    if total >= s["strong_bullish"]:
        signal = "Strong Bullish"
    elif total >= s["bullish"]:
        signal = "Bullish"
    else:
        signal = "Neutral"

    return {
        "total_score": total,
        "signal": signal,
    }


def main():
    root = Path(".")
    params = load_yaml(root / "config" / "stocks" / "stocks_parameters.yaml")

    universe_path = root / "data" / "stocks" / "stock_universe.csv"
    scores_path = root / "data" / "stocks" / "stock_scores_history.csv"
    candidates_path = root / "data" / "stocks" / "stock_candidates_history.csv"
    run_log_path = root / "data" / "stocks" / "stock_system_run_log.csv"

    tickers = load_universe(universe_path)

    bench = download_history("SPY", "1y")
    spy_ret_63 = bench["Close"].iloc[-1] / bench["Close"].shift(63).iloc[-1] - 1

    today = str(pd.to_datetime(bench["Date"].iloc[-1]).date())

    rows = []

    for ticker in tickers:
        try:
            df = download_history(ticker, "1y")
            if df.empty or len(df) < 30:
                continue

            metrics = latest_metrics(df, spy_ret_63)
            scored = score_row(metrics, params)

            row = {
                "date": today,
                "ticker": ticker,
                **metrics,
                **scored,
            }

            rows.append(row)

        except Exception:
            continue

    if not rows:
        pd.DataFrame([{
            "date": today,
            "stocks_scored": 0,
            "candidates_found": 0,
            "status": "warning",
            "notes": "No rows survived filtering/scoring"
        }]).to_csv(run_log_path, mode="a", header=not run_log_path.exists(), index=False)

        print("WARNING: No stocks survived filtering/scoring")
        return

    df = pd.DataFrame(rows)

    df.to_csv(scores_path, index=False)

    candidates = df[df["signal"] == "Strong Bullish"]
    candidates.to_csv(candidates_path, index=False)

    pd.DataFrame([{
        "date": today,
        "stocks_scored": len(df),
        "candidates_found": len(candidates),
        "status": "success",
        "notes": "Run complete"
    }]).to_csv(run_log_path, mode="a", header=not run_log_path.exists(), index=False)

    print(f"Scored {len(df)} stocks. Found {len(candidates)} candidates.")


if __name__ == "__main__":
    main()
