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


def build_sector_mapping() -> dict:
    config = load_yaml(Path("config/sector_map.yaml"))
    mapping = {}

    for item in config.get("sectors", []):
        sector = item.get("sector")
        if not sector:
            continue

        mapping[sector] = {
            "signal_etf": item.get("signal_etf", ""),
            "bull_etf": item.get("bull_etf", ""),
            "bear_etf": item.get("bear_etf", ""),
            "benchmark": item.get("benchmark", ""),
            "notes": item.get("notes", ""),
        }

    return mapping


def resolve_trade_fields(sector: str, state: str, sector_map: dict) -> tuple[str, str]:
    info = sector_map.get(sector, {})
    bull_etf = str(info.get("bull_etf", "") or "").strip().upper()
    bear_etf = str(info.get("bear_etf", "") or "").strip().upper()

    if state in ("Strong Bull", "Bull"):
        return bull_etf, "long" if bull_etf else "none"

    if state in ("Strong Bear", "Bear"):
        if bear_etf:
            return bear_etf, "short"
        return "", "none"

    return "", "none"


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
    sector_map = build_sector_mapping()

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

    # Normalize signal_etf from mapping so output always reflects current source of truth
    df["signal_etf"] = df["sector"].map(
        lambda s: sector_map.get(s, {}).get("signal_etf", "")
    )

    # Explicit tradable output
    trade_fields = df.apply(
        lambda row: resolve_trade_fields(row["sector"], row["signal_state"], sector_map),
        axis=1,
    )
    df["selected_etf"] = [x[0] for x in trade_fields]
    df["direction"] = [x[1] for x in trade_fields]

    out = df[
        [
            "date",
            "sector",
            "signal_etf",
            "selected_etf",
            "direction",
            "total_score",
            "signal_state",
            "score_change",
        ]
    ].copy()

    out["rank"] = 0
    out = out.sort_values(["date", "sector"]).reset_index(drop=True)
    out.to_csv("data/sector_scores.csv", index=False)

    print(f"Wrote {len(out)} rows to data/sector_scores.csv")


if __name__ == "__main__":
    main()
