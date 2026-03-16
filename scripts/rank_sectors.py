#!/usr/bin/env python3

from __future__ import annotations
from pathlib import Path
import pandas as pd


def main():
    path = Path("data/sector_scores.csv")
    if not path.exists():
        print("sector_scores.csv not found")
        return

    df = pd.read_csv(path)
    if df.empty:
        print("sector_scores.csv is empty")
        return

    df["rank"] = (
        df.groupby("date")["total_score"]
        .rank(method="dense", ascending=False)
        .astype(int)
    )

    df = df.sort_values(["date", "rank", "sector"]).reset_index(drop=True)
    df.to_csv(path, index=False)

    print(f"Updated ranks in {path}")


if __name__ == "__main__":
    main()
