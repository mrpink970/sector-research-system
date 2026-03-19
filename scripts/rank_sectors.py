#!/usr/bin/env python3

import pandas as pd
import yaml


INPUT_FILE = "data/sector_scores.csv"
OUTPUT_FILE = "data/sector_scores.csv"
SECTOR_MAP_FILE = "config/sector_map.yaml"


def load_sector_map():
    with open(SECTOR_MAP_FILE, "r") as f:
        config = yaml.safe_load(f)

    mapping = {}
    for item in config.get("sectors", []):
        sector = item.get("sector")
        if not sector:
            continue
        mapping[sector] = {
            "bull_etf": item.get("bull_etf", ""),
            "bear_etf": item.get("bear_etf", ""),
        }
    return mapping


def determine_direction(signal_state):
    if signal_state in ["Bullish", "Strong Bullish", "Bull", "Strong Bull"]:
        return "long"
    if signal_state in ["Bearish", "Strong Bearish", "Bear", "Strong Bear"]:
        return "short"
    return "none"


def main():
    df = pd.read_csv(INPUT_FILE)
    sector_map = load_sector_map()

    if df.empty:
        print("sector_scores.csv is empty")
        return

    # Support either signal_state or signal column names
    signal_col = "signal_state" if "signal_state" in df.columns else "signal"

    df["direction"] = df[signal_col].apply(determine_direction)

    def pick_etf(row):
        sector = row["sector"]
        direction = row["direction"]
        info = sector_map.get(sector, {})

        if direction == "long":
            return info.get("bull_etf", "")
        if direction == "short":
            return info.get("bear_etf", "") or ""
        return ""

    df["selected_etf"] = df.apply(pick_etf, axis=1)

    # Rank by date and score if total_score exists
    if "total_score" in df.columns:
        df = df.sort_values(["date", "total_score"], ascending=[True, False]).copy()
        df["rank"] = df.groupby("date")["total_score"].rank(method="dense", ascending=False).astype(int)

    df.to_csv(OUTPUT_FILE, index=False)
    print("Updated sector_scores.csv with direction and selected_etf")


if __name__ == "__main__":
    main()
