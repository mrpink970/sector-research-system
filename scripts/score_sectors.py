#!/usr/bin/env python3

from __future__ import annotations
from pathlib import Path
import pandas as pd
import yaml


def load_yaml(path: Path) -> dict:
    with path.open("r", encoding="utf-8") as f:
        return yaml.safe_load(f)


def signal_state(score: int, params: dict) -> str:
    if score >= params["strong_bull_min"]:
        return "Strong Bull"
    if score >= params["bull_min"]:
        return "Bull"
    if params["neutral_min"] <= score <= params["neutral_max"]:
        return "Neutral"
    if score <= params["strong_bear_max"]:
        return "Strong Bear"
    if score <= params["bear_max"]:
        return "Bear"
    return "Neutral"


def main():
    indicators_path = Path("data/indicators.csv")
    if not indicators_path.exists():
        print("indicators.csv not found")
        return

    df = pd.read_csv(indicators_path)
    if df.empty:
        print("indicators.csv is empty")
        return

    params = load_yaml(Path("config/system_parameters.yaml"))["signal_states"]

    score_cols = [
        "trend_score",
        "trend_ignition_score",
        "momentum_score",
        "relative_strength_score",
        "rs_persistence_score",
        "momentum_exhaustion_score",
        "volatility_score",
    ]

    df["total_score"] = df[score_cols].sum(axis=1)
    df["signal_state"] = df["total_score"].apply(lambda x: signal_state(int(x), params))

    df = df.sort_values(["sector", "date"]).copy()
    df["score_change"] = df.groupby("sector")["total_score"].diff().fillna(0).astype(int)

    out = df[["date", "sector", "signal_etf", "total_score", "signal_state", "score_change"]].copy()
    out["rank"] = 0

    out = out.sort_values(["date", "sector"]).reset_index(drop=True)
    out.to_csv("data/sector_scores.csv", index=False)

    print(f"Wrote {len(out)} rows to data/sector_scores.csv")


if __name__ == "__main__":
    main()
