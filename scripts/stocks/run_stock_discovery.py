#!/usr/bin/env python3
from __future__ import annotations

import math
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
    if not path.exists():
        raise SystemExit(
            f"Universe file not found: {path}. Run scripts/stocks/build_stock_universe.py first."
        )
    df = pd.read_csv(path)
    if "ticker" not in df.columns:
        raise SystemExit(f"Universe file missing 'ticker' column: {path}")
    tickers = sorted(set(df["ticker"].dropna().astype(str).str.upper().tolist()))
    if not tickers:
        raise SystemExit("Universe file has no tickers.")
    return tickers


def download_history(ticker: str, period: str) -> pd.DataFrame:
    df = yf.download(
        ticker,
        period=period,
        interval="1d",
        auto_adjust=False,
        progress=False,
        threads=False,
        group_by="column",
    )
    if df.empty:
        return df
    if hasattr(df.columns, "nlevels") and df.columns.nlevels > 1:
        df.columns = df.columns.get_level_values(0)
    df = df.rename(columns=str.title)
    df = df.reset_index()
    if "Date" not in df.columns:
        raise ValueError(f"No Date column for {ticker}")
    return df[["Date", "Open", "High", "Low", "Close", "Volume"]].copy()


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

    vol_ratio = avg_vol_20 / avg_vol_50 if avg_vol_50 and not np.isnan(avg_vol_50) else np.nan
    atr_14 = calc_atr(df, 14).iloc[-1]
    atr_pct = atr_14 / close.iloc[-1] if close.iloc[-1] and not np.isnan(atr_14) else np.nan
    extension_pct = (close.iloc[-1] - ma_20) / ma_20 if ma_20 and not np.isnan(ma_20) else np.nan

    rs_63 = ret_63 - bench_ret_63 if not np.isnan(ret_63) and not np.isnan(bench_ret_63) else np.nan

    return {
        "close": float(close.iloc[-1]),
        "avg_volume_50": float(avg_vol_50) if not np.isnan(avg_vol_50) else np.nan,
        "ret_21": float(ret_21) if not np.isnan(ret_21) else np.nan,
        "ret_63": float(ret_63) if not np.isnan(ret_63) else np.nan,
        "ret_126": float(ret_126) if not np.isnan(ret_126) else np.nan,
        "spy_ret_63": float(bench_ret_63) if not np.isnan(bench_ret_63) else np.nan,
        "rs_63": float(rs_63) if not np.isnan(rs_63) else np.nan,
        "ma_20": float(ma_20) if not np.isnan(ma_20) else np.nan,
        "ma_50": float(ma_50) if not np.isnan(ma_50) else np.nan,
        "ma_100": float(ma_100) if not np.isnan(ma_100) else np.nan,
        "avg_vol_20": float(avg_vol_20) if not np.isnan(avg_vol_20) else np.nan,
        "avg_vol_50": float(avg_vol_50) if not np.isnan(avg_vol_50) else np.nan,
        "vol_ratio": float(vol_ratio) if not np.isnan(vol_ratio) else np.nan,
        "atr_14": float(atr_14) if not np.isnan(atr_14) else np.nan,
        "atr_pct": float(atr_pct) if not np.isnan(atr_pct) else np.nan,
        "extension_pct": float(extension_pct) if not np.isnan(extension_pct) else np.nan,
    }


def classify_universe(close: float, market_cap: float, avg_vol_50: float, params: dict) -> str | None:
    ua = params["universe"]["universe_a"]
    ub = params["universe"]["universe_b"]
    if close >= ua["min_price"] and market_cap >= ua["min_market_cap"] and avg_vol_50 >= ua["min_volume"]:
        return "A"
    if close >= ub["min_price"] and market_cap >= ub["min_market_cap"] and avg_vol_50 >= ub["min_volume"]:
        return "B"
    return None


def score_row(row: Dict[str, float], params: dict) -> Dict[str, int | str]:
    s = params["scoring"]

    rs_score = 2 if row["rs_63"] >= s["rs_strong"] else 1 if row["rs_63"] >= s["rs_moderate"] else 0

    positives = sum(x > 0 for x in [row["ret_21"], row["ret_63"], row["ret_126"]] if not np.isnan(x))
    trend_score = 2 if positives == 3 else 1 if positives >= 2 else 0

    structure_score = 1 if row["close"] > row["ma_20"] > row["ma_50"] > row["ma_100"] else 0
    volume_score = 1 if row["vol_ratio"] >= s["volume_ratio"] else 0
    volatility_score = 1 if row["atr_pct"] <= s["atr_threshold"] else 0
    extension_score = 1 if row["extension_pct"] <= s["extension_threshold"] else 0

    total = rs_score + trend_score + structure_score + volume_score + volatility_score + extension_score

    if total >= s["strong_bullish"]:
        signal = "Strong Bullish"
    elif total >= s["bullish"]:
        signal = "Bullish"
    else:
        signal = "Neutral"

    return {
        "relative_strength_score": rs_score,
        "trend_score": trend_score,
        "structure_score": structure_score,
        "volume_score": volume_score,
        "volatility_score": volatility_score,
        "extension_score": extension_score,
        "total_score": total,
        "signal": signal,
    }


def load_or_empty(path: Path, columns: List[str]) -> pd.DataFrame:
    if path.exists():
        try:
            return pd.read_csv(path)
        except Exception:
            return pd.DataFrame(columns=columns)
    return pd.DataFrame(columns=columns)


def upsert_by_date(path: Path, today_df: pd.DataFrame, date_value: str, sort_cols: List[str]) -> None:
    ensure_parent(path)
    history = load_or_empty(path, list(today_df.columns))
    if "date" in history.columns:
        history = history[history["date"].astype(str) != date_value]
    merged = pd.concat([history, today_df], ignore_index=True)
    merged = merged.sort_values(sort_cols, ascending=[True] + [False] * (len(sort_cols) - 1))
    merged.to_csv(path, index=False)


def append_run_log(path: Path, row: Dict[str, object]) -> None:
    ensure_parent(path)
    cols = ["date", "stocks_scored", "candidates_found", "status", "notes"]
    exists = path.exists()
    with path.open("a", newline="", encoding="utf-8") as f:
        writer = pd.DataFrame([row], columns=cols)
        writer.to_csv(f, header=not exists, index=False)


def main() -> None:
    root = Path(".")
    params = load_yaml(root / "config" / "stocks" / "stocks_parameters.yaml")
    files = params["files"]
    paths = Paths(
        universe=root / files["universe"],
        benchmark_cache=root / files["benchmark_cache"],
        full_history=root / files["full_history"],
        candidate_history=root / files["candidate_history"],
        run_log=root / files["run_log"],
    )

    tickers = load_universe(paths.universe)
    benchmark_ticker = params["benchmark"]["ticker"]
    history_period = params["pull"]["history_period"]

    bench_df = download_history(benchmark_ticker, history_period)
    if bench_df.empty or len(bench_df) < 70:
        raise SystemExit(f"Benchmark download failed or insufficient history for {benchmark_ticker}")

    spy_ret_63 = bench_df["Close"].iloc[-1] / bench_df["Close"].shift(63).iloc[-1] - 1
    today = str(pd.to_datetime(bench_df["Date"].iloc[-1]).date())

    bench_df.to_csv(paths.benchmark_cache, index=False)

    rows: List[Dict[str, object]] = []

    for ticker in tickers:
        try:
            t = yf.Ticker(ticker)
            hist = download_history(ticker, history_period)
            if hist.empty or len(hist) < 130:
                continue

            info = {}
            try:
                info = t.fast_info or {}
            except Exception:
                info = {}

            market_cap = info.get("market_cap")
            if market_cap is None or pd.isna(market_cap):
                continue

            metrics = latest_metrics(hist, float(spy_ret_63))
            universe = classify_universe(metrics["close"], float(market_cap), metrics["avg_volume_50"], params)
            if universe is None:
                continue

            scored = score_row(metrics, params)
            row = {
                "date": today,
                "ticker": ticker,
                "universe": universe,
                "close": metrics["close"],
                "market_cap": float(market_cap),
                "avg_volume_50": metrics["avg_volume_50"],
                "ret_21": metrics["ret_21"],
                "ret_63": metrics["ret_63"],
                "ret_126": metrics["ret_126"],
                "spy_ret_63": metrics["spy_ret_63"],
                "rs_63": metrics["rs_63"],
                "ma_20": metrics["ma_20"],
                "ma_50": metrics["ma_50"],
                "ma_100": metrics["ma_100"],
                "avg_vol_20": metrics["avg_vol_20"],
                "avg_vol_50": metrics["avg_vol_50"],
                "vol_ratio": metrics["vol_ratio"],
                "atr_14": metrics["atr_14"],
                "atr_pct": metrics["atr_pct"],
                "extension_pct": metrics["extension_pct"],
                **scored,
            }
            rows.append(row)
        except Exception:
            continue

    if not rows:
    append_run_log(
        paths.run_log,
        {
            "date": today,
            "stocks_scored": 0,
            "candidates_found": 0,
            "status": "warning",
            "notes": "No rows survived filtering/scoring",
        },
    )
    print("WARNING: No stocks survived filtering/scoring")
    return

    full_df = pd.DataFrame(rows)
    full_df = full_df.sort_values(["date", "total_score", "ticker"], ascending=[True, False, True])

    candidate_df = full_df[full_df["signal"] == "Strong Bullish"][
        [
            "date",
            "ticker",
            "universe",
            "total_score",
            "signal",
            "close",
            "rs_63",
            "ret_63",
            "ret_126",
            "vol_ratio",
            "atr_pct",
            "extension_pct",
        ]
    ].copy()

    upsert_by_date(paths.full_history, full_df, today, ["date", "total_score"])
    if not candidate_df.empty:
        upsert_by_date(paths.candidate_history, candidate_df, today, ["date", "total_score"])
    else:
        # Still upsert an empty-day by removing existing rows for today only
        hist = load_or_empty(paths.candidate_history, list(candidate_df.columns))
        if "date" in hist.columns:
            hist = hist[hist["date"].astype(str) != today]
        ensure_parent(paths.candidate_history)
        hist.to_csv(paths.candidate_history, index=False)

    append_run_log(
        paths.run_log,
        {
            "date": today,
            "stocks_scored": len(full_df),
            "candidates_found": len(candidate_df),
            "status": "success",
            "notes": f"Benchmark={benchmark_ticker}",
        },
    )

    print(f"Scored {len(full_df)} stocks on {today}. Candidates: {len(candidate_df)}.")


if __name__ == "__main__":
    main()
