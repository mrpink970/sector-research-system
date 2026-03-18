#!/usr/bin/env python3
from __future__ import annotations

from pathlib import Path
from typing import Dict, List

import numpy as np
import pandas as pd
import yfinance as yf
import yaml


def load_yaml(path: Path) -> dict:
    with path.open("r", encoding="utf-8") as f:
        return yaml.safe_load(f)


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
        auto_adjust=False,
    )

    if df.empty:
        return df

    # flatten yfinance columns if needed
    if hasattr(df.columns, "nlevels") and df.columns.nlevels > 1:
        df.columns = df.columns.get_level_values(0)

    df = df.reset_index()

    needed = ["Date", "Open", "High", "Low", "Close", "Volume"]
    missing = [c for c in needed if c not in df.columns]
    if missing:
        raise ValueError(f"{ticker}: missing columns {missing}; got {list(df.columns)}")

    return df[needed].copy()


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

    vol_ratio = avg_vol_20 / avg_vol_50 if pd.notna(avg_vol_50) and avg_vol_50 != 0 else np.nan
    atr_14 = calc_atr(df, 14).iloc[-1]
    atr_pct = atr_14 / close.iloc[-1] if pd.notna(atr_14) and close.iloc[-1] != 0 else np.nan
    extension_pct = (close.iloc[-1] - ma_20) / ma_20 if pd.notna(ma_20) and ma_20 != 0 else np.nan
    rs_63 = ret_63 - bench_ret_63 if pd.notna(ret_63) else np.nan

    return {
        "close": float(close.iloc[-1]),
        "ret_21": float(ret_21) if pd.notna(ret_21) else np.nan,
        "ret_63": float(ret_63) if pd.notna(ret_63) else np.nan,
        "ret_126": float(ret_126) if pd.notna(ret_126) else np.nan,
        "spy_ret_63": float(bench_ret_63),
        "rs_63": float(rs_63) if pd.notna(rs_63) else np.nan,
        "ma_20": float(ma_20) if pd.notna(ma_20) else np.nan,
        "ma_50": float(ma_50) if pd.notna(ma_50) else np.nan,
        "ma_100": float(ma_100) if pd.notna(ma_100) else np.nan,
        "vol_ratio": float(vol_ratio) if pd.notna(vol_ratio) else np.nan,
        "atr_pct": float(atr_pct) if pd.notna(atr_pct) else np.nan,
        "extension_pct": float(extension_pct) if pd.notna(extension_pct) else np.nan,
    }


def score_row(row: Dict[str, float], params: dict) -> Dict[str, int | str]:
    s = params["scoring"]

    rs_score = 2 if row["rs_63"] >= s["rs_strong"] else 1 if row["rs_63"] >= s["rs_moderate"] else 0

    positives = sum(x > 0 for x in [row["ret_21"], row["ret_63"], row["ret_126"]] if pd.notna(x))
    trend_score = 2 if positives == 3 else 1 if positives >= 2 else 0

    structure_score = (
        1
        if pd.notna(row["ma_20"])
        and pd.notna(row["ma_50"])
        and pd.notna(row["ma_100"])
        and row["close"] > row["ma_20"] > row["ma_50"] > row["ma_100"]
        else 0
    )

    volume_score = 1 if pd.notna(row["vol_ratio"]) and row["vol_ratio"] >= s["volume_ratio"] else 0
    volatility_score = 1 if pd.notna(row["atr_pct"]) and row["atr_pct"] <= s["atr_threshold"] else 0
    extension_score = 1 if pd.notna(row["extension_pct"]) and row["extension_pct"] <= s["extension_threshold"] else 0

    total = rs_score + trend_score + structure_score + volume_score + volatility_score + extension_score

    if total >= s["strong_bullish"]:
        signal = "Strong Bullish"
    elif total >= s["bullish"]:
        signal = "Bullish"
    else:
        signal = "Neutral"

    return {"total_score": total, "signal": signal}


def append_run_log(path: Path, row: Dict[str, object]) -> None:
    exists = path.exists()
    pd.DataFrame([row]).to_csv(path, mode="a", header=not exists, index=False)


def main():
    root = Path(".")
    params = load_yaml(root / "config" / "stocks" / "stocks_parameters.yaml")

    universe_path = root / "data" / "stocks" / "stock_universe.csv"
    scores_path = root / "data" / "stocks" / "stock_scores_history.csv"
    candidates_path = root / "data" / "stocks" / "stock_candidates_history.csv"
    run_log_path = root / "data" / "stocks" / "stock_system_run_log.csv"

    tickers = load_universe(universe_path)

    bench = download_history("SPY", "1y")
    if bench.empty or len(bench) < 70:
        raise SystemExit("Benchmark SPY download failed")

    spy_ret_63 = bench["Close"].iloc[-1] / bench["Close"].shift(63).iloc[-1] - 1
    today = str(pd.to_datetime(bench["Date"].iloc[-1]).date())

    rows = []
    errors = []

    for ticker in tickers:
        try:
            df = download_history(ticker, "1y")
            if df.empty or len(df) < 30:
                errors.append(f"{ticker}: empty or too short ({len(df)})")
                continue

            metrics = latest_metrics(df, spy_ret_63)
            scored = score_row(metrics, params)

            rows.append(
                {
                    "date": today,
                    "ticker": ticker,
                    **metrics,
                    **scored,
                }
            )

        except Exception as e:
            errors.append(f"{ticker}: {type(e).__name__}: {e}")
            continue

    if errors:
        print("FIRST FEW TICKER ERRORS:")
        for msg in errors[:10]:
            print(msg)

    if not rows:
        append_run_log(
            run_log_path,
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

    out = pd.DataFrame(rows)
    out.to_csv(scores_path, index=False)

    candidates = out[out["signal"] == "Strong Bullish"].copy()
    candidates.to_csv(candidates_path, index=False)

    append_run_log(
        run_log_path,
        {
            "date": today,
            "stocks_scored": len(out),
            "candidates_found": len(candidates),
            "status": "success",
            "notes": "Run complete",
        },
    )

    print(f"Scored {len(out)} stocks. Found {len(candidates)} candidates.")


if __name__ == "__main__":
    main()
