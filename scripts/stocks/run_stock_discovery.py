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
    open_price = df["Open"].iloc[-1]
    high_price = df["High"].iloc[-1]
    low_price = df["Low"].iloc[-1]

    ret_21 = close.iloc[-1] / close.shift(21).iloc[-1] - 1 if len(df) > 21 else np.nan
    ret_63 = close.iloc[-1] / close.shift(63).iloc[-1] - 1 if len(df) > 63 else np.nan
    ret_126 = close.iloc[-1] / close.shift(126).iloc[-1] - 1 if len(df) > 126 else np.nan

    ma_20 = close.rolling(20).mean().iloc[-1]
    ma_50 = close.rolling(50).mean().iloc[-1]
    ma_100 = close.rolling(100).mean().iloc[-1]

    avg_vol_5 = volume.rolling(5).mean().iloc[-1]
    avg_vol_20 = volume.rolling(20).mean().iloc[-1]
    avg_vol_50 = volume.rolling(50).mean().iloc[-1]

    vol_ratio_20_50 = avg_vol_20 / avg_vol_50 if pd.notna(avg_vol_50) and avg_vol_50 != 0 else np.nan
    vol_acceleration = avg_vol_5 / avg_vol_20 if pd.notna(avg_vol_20) and avg_vol_20 != 0 else np.nan

    atr_14 = calc_atr(df, 14).iloc[-1]
    atr_pct = atr_14 / close.iloc[-1] if pd.notna(atr_14) and close.iloc[-1] != 0 else np.nan
    extension_pct = (close.iloc[-1] - ma_20) / ma_20 if pd.notna(ma_20) and ma_20 != 0 else np.nan
    rs_63 = ret_63 - bench_ret_63 if pd.notna(ret_63) else np.nan

    # For breakout scoring
    atr_50 = calc_atr(df, 50).iloc[-1] if len(df) > 50 else np.nan
    compression_ratio = atr_14 / atr_50 if pd.notna(atr_14) and pd.notna(atr_50) and atr_50 != 0 else np.nan

    high_20 = df["High"].rolling(20).max().iloc[-1]
    high_50 = df["High"].rolling(50).max().iloc[-1]
    proximity_20 = (close.iloc[-1] / high_20) - 1 if pd.notna(high_20) and high_20 != 0 else np.nan
    proximity_50 = (close.iloc[-1] / high_50) - 1 if pd.notna(high_50) and high_50 != 0 else np.nan

    rs_21 = ret_21 - bench_ret_63 if pd.notna(ret_21) else np.nan
    rs_acceleration = rs_21 - rs_63 if pd.notna(rs_21) and pd.notna(rs_63) else np.nan

    extension_50ma = (close.iloc[-1] - ma_50) / ma_50 if pd.notna(ma_50) and ma_50 != 0 else np.nan
    extension_20ma = (close.iloc[-1] - ma_20) / ma_20 if pd.notna(ma_20) and ma_20 != 0 else np.nan

    structure_condition = (
        pd.notna(close.iloc[-1]) and pd.notna(ma_20) and pd.notna(ma_50) and
        close.iloc[-1] > ma_20 and ma_20 > ma_50
    )

    return {
        "open": float(open_price),
        "high": float(high_price),
        "low": float(low_price),
        "close": float(close.iloc[-1]),
        "ret_21": float(ret_21) if pd.notna(ret_21) else np.nan,
        "ret_63": float(ret_63) if pd.notna(ret_63) else np.nan,
        "ret_126": float(ret_126) if pd.notna(ret_126) else np.nan,
        "spy_ret_63": float(bench_ret_63),
        "rs_63": float(rs_63) if pd.notna(rs_63) else np.nan,
        "ma_20": float(ma_20) if pd.notna(ma_20) else np.nan,
        "ma_50": float(ma_50) if pd.notna(ma_50) else np.nan,
        "ma_100": float(ma_100) if pd.notna(ma_100) else np.nan,
        "avg_vol_5": float(avg_vol_5) if pd.notna(avg_vol_5) else np.nan,
        "avg_vol_20": float(avg_vol_20) if pd.notna(avg_vol_20) else np.nan,
        "avg_vol_50": float(avg_vol_50) if pd.notna(avg_vol_50) else np.nan,
        "vol_ratio_20_50": float(vol_ratio_20_50) if pd.notna(vol_ratio_20_50) else np.nan,
        "vol_acceleration": float(vol_acceleration) if pd.notna(vol_acceleration) else np.nan,
        "atr_pct": float(atr_pct) if pd.notna(atr_pct) else np.nan,
        "extension_pct": float(extension_pct) if pd.notna(extension_pct) else np.nan,
        "compression_ratio": float(compression_ratio) if pd.notna(compression_ratio) else np.nan,
        "proximity_20": float(proximity_20) if pd.notna(proximity_20) else np.nan,
        "proximity_50": float(proximity_50) if pd.notna(proximity_50) else np.nan,
        "rs_acceleration": float(rs_acceleration) if pd.notna(rs_acceleration) else np.nan,
        "extension_50ma": float(extension_50ma) if pd.notna(extension_50ma) else np.nan,
        "extension_20ma": float(extension_20ma) if pd.notna(extension_20ma) else np.nan,
        "structure_condition": int(structure_condition),
    }


def score_trend(row: Dict[str, float], params: dict) -> Dict[str, int | str]:
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

    volume_score = 1 if pd.notna(row["vol_ratio_20_50"]) and row["vol_ratio_20_50"] >= s["volume_ratio"] else 0
    volatility_score = 1 if pd.notna(row["atr_pct"]) and row["atr_pct"] <= s["atr_threshold"] else 0
    extension_score = 1 if pd.notna(row["extension_pct"]) and row["extension_pct"] <= s["extension_threshold"] else 0

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


def score_breakout(row: Dict[str, float], params: dict) -> Dict[str, int | str]:
    b = params["breakout"]

    # A. Volatility Compression
    if pd.notna(row["compression_ratio"]):
        if row["compression_ratio"] < b["compression_strong"]:
            compression_score = 2
        elif row["compression_ratio"] < b["compression_moderate"]:
            compression_score = 1
        else:
            compression_score = 0
    else:
        compression_score = 0

    # B. Volume Expansion
    vol_ratio_ok = pd.notna(row["vol_ratio_20_50"]) and row["vol_ratio_20_50"] >= b["volume_ratio_strong"]
    vol_accel_ok = pd.notna(row["vol_acceleration"]) and row["vol_acceleration"] >= b["volume_acceleration_strong"]

    if vol_ratio_ok and vol_accel_ok:
        volume_score = 2
    elif (pd.notna(row["vol_ratio_20_50"]) and row["vol_ratio_20_50"] >= b["volume_ratio_moderate"]) or \
         (pd.notna(row["vol_acceleration"]) and row["vol_acceleration"] >= b["volume_acceleration_moderate"]):
        volume_score = 1
    else:
        volume_score = 0

    # C. Price Proximity to Breakout
    if pd.notna(row["proximity_20"]) and row["proximity_20"] >= -b["proximity_threshold"]:
        proximity_score = 2
    elif pd.notna(row["proximity_50"]) and row["proximity_50"] >= -b["proximity_threshold"]:
        proximity_score = 1
    else:
        proximity_score = 0

    # D. Relative Strength Acceleration
    rs_accel_strong = pd.notna(row["rs_acceleration"]) and row["rs_acceleration"] > b["rs_acceleration_strong"]
    rs_63_min_strong = pd.notna(row["rs_63"]) and row["rs_63"] > b["rs_min_strong"]

    if rs_accel_strong and rs_63_min_strong:
        rs_acceleration_score = 2
    elif (pd.notna(row["rs_acceleration"]) and row["rs_acceleration"] > b["rs_acceleration_moderate"]) and \
         (pd.notna(row["rs_63"]) and row["rs_63"] > b["rs_min_moderate"]):
        rs_acceleration_score = 1
    else:
        rs_acceleration_score = 0

    # E. Low Extension
    ext_50_strong = pd.notna(row["extension_50ma"]) and row["extension_50ma"] <= b["extension_50ma_strong"]
    ext_20_strong = pd.notna(row["extension_20ma"]) and row["extension_20ma"] <= b["extension_20ma_strong"]

    if ext_50_strong and ext_20_strong:
        extension_score = 2
    elif (pd.notna(row["extension_50ma"]) and row["extension_50ma"] <= b["extension_50ma_moderate"]) and \
         (pd.notna(row["extension_20ma"]) and row["extension_20ma"] <= b["extension_20ma_moderate"]):
        extension_score = 1
    else:
        extension_score = 0

    # F. Price Structure
    structure_score = row.get("structure_condition", 0)

    total = compression_score + volume_score + proximity_score + rs_acceleration_score + extension_score + structure_score

    if total >= b["strong_breakout_min"]:
        signal = "Strong Breakout Candidate"
    elif total >= b["breakout_watch_min"]:
        signal = "Breakout Watch"
    else:
        signal = "No Setup"

    return {
        "breakout_compression_score": compression_score,
        "breakout_volume_score": volume_score,
        "breakout_proximity_score": proximity_score,
        "breakout_rs_acceleration_score": rs_acceleration_score,
        "breakout_extension_score": extension_score,
        "breakout_structure_score": structure_score,
        "breakout_total_score": total,
        "breakout_signal": signal,
    }


def classify_tag(ret_21: float, ret_63: float, extension_pct: float) -> str:
    if pd.notna(ret_21) and ret_21 > 0.50:
        return "extended"

    if (
        pd.notna(ret_21)
        and pd.notna(ret_63)
        and ret_21 > 0.20
        and ret_63 > 0.30
    ):
        return "momentum"

    if pd.notna(ret_21) and ret_21 > 0.05:
        return "early_breakout"

    if pd.notna(extension_pct) and extension_pct > 0.12:
        return "extended"

    return "neutral"


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
            trend_scored = score_trend(metrics, params)
            breakout_scored = score_breakout(metrics, params)
            tag = classify_tag(metrics["ret_21"], metrics["ret_63"], metrics["extension_pct"])

            rows.append(
                {
                    "date": today,
                    "ticker": ticker,
                    "open": metrics["open"],
                    "high": metrics["high"],
                    "low": metrics["low"],
                    "close": metrics["close"],
                    **{k: v for k, v in metrics.items() if k not in ["open", "high", "low", "close"]},
                    **trend_scored,
                    **breakout_scored,
                    "tag": tag,
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
    out = out.sort_values(["date", "total_score", "ticker"], ascending=[True, False, True])
    
    # ============================================================
    # Append to history (keep all historical data)
    # ============================================================
    if scores_path.exists():
        existing = pd.read_csv(scores_path)
        today_str = out["date"].iloc[0] if not out.empty else None
        if today_str:
            existing = existing[existing["date"] != today_str]
        combined = pd.concat([existing, out], ignore_index=True)
        combined = combined.sort_values(["date", "total_score", "ticker"], ascending=[True, False, True])
        combined.to_csv(scores_path, index=False)
    else:
        out.to_csv(scores_path, index=False)

    # ============================================================
    # Trend candidates (sorted by trend score)
    # ============================================================
    trend_candidates = out[out["signal"] == "Strong Bullish"].copy()
    trend_candidates = trend_candidates.sort_values(
        ["date", "total_score", "ticker"], 
        ascending=[True, False, True]
    )
    trend_candidates.to_csv(candidates_path, index=False)

    # ============================================================
    # Breakout candidates (sorted by breakout score)
    # ============================================================
    breakout_candidates = out[out["breakout_signal"] == "Strong Breakout Candidate"].copy()
    breakout_candidates = breakout_candidates.sort_values(
        ["date", "breakout_total_score", "ticker"], 
        ascending=[True, False, True]
    )
    breakout_candidates_path = root / "data" / "stocks" / "stock_breakout_candidates_history.csv"
    breakout_candidates.to_csv(breakout_candidates_path, index=False)

    append_run_log(
        run_log_path,
        {
            "date": today,
            "stocks_scored": len(out),
            "candidates_found": len(trend_candidates),
            "breakout_candidates_found": len(breakout_candidates),
            "status": "success",
            "notes": "Run complete",
        },
    )

    print(f"Scored {len(out)} stocks. Found {len(trend_candidates)} trend candidates. Found {len(breakout_candidates)} breakout candidates.")


if __name__ == "__main__":
    main()
