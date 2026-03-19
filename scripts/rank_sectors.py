#!/usr/bin/env python3

import pandas as pd
import yaml
from pathlib import Path


INPUT_FILE = "data/sector_scores_raw.csv"
OUTPUT_FILE = "data/sector_scores.csv"
SECTOR_MAP_FILE = "config/sector_map.yaml"


def load_sector_map():
    with open(SECTOR_MAP_FILE, "r") as f:
        return yaml.safe_load(f)


def determine_direction(signal):
    if signal in ["Bullish", "Strong Bullish"]:
        return "long"
    elif signal in ["Bearish", "Strong Bearish"]:
        return "short"
    else:
        return "none"


def run():
    df = pd.read_csv(INPUT_FILE)
    sector_map = load_sector_map()

    output_rows = []

    for _, row in df.iterrows():
        sector = row["sector"]
        signal = row["signal"]

        direction = determine_direction(signal)

        # --- ETF SELECTION ---
        selected_etf = None

        if direction == "long":
            selected_etf = sector_map.get(sector, {}).get("bull")
        elif direction == "short":
            selected_etf = sector_map.get(sector, {}).get("bear")

        # --- BUILD OUTPUT ROW ---
        out = row.to_dict()
        out["direction"] = direction
        out["selected_etf"] = selected_etf

        output_rows.append(out)

    output_df = pd.DataFrame(output_rows)

    # --- SORT (optional but useful) ---
    if "total_score" in output_df.columns:
        output_df = output_df.sort_values(
            ["date", "total_score"],
            ascending=[True, False]
        )

    output_df.to_csv(OUTPUT_FILE, index=False)

    print("Sector ranking complete → sector_scores.csv updated")


if __name__ == "__main__":
    run()
