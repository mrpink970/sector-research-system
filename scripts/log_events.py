#!/usr/bin/env python3

from __future__ import annotations
from pathlib import Path
import pandas as pd


def main():
    score_path = Path("data/sector_scores.csv")
    event_path = Path("data/event_log.csv")

    if not score_path.exists():
        print("sector_scores.csv not found")
        return

    scores = pd.read_csv(score_path)
    if scores.empty:
        print("sector_scores.csv is empty")
        return

    scores = scores.sort_values(["sector", "date"]).copy()

    events = []

    for sector, group in scores.groupby("sector", group_keys=False):
        group = group.sort_values("date").reset_index(drop=True)

        prev_signal = None
        prev_rank = None

        for _, row in group.iterrows():
            current_signal = row["signal_state"]
            current_rank = int(row["rank"])

            if prev_signal is not None and current_signal != prev_signal:
                events.append({
                    "date": row["date"],
                    "sector": row["sector"],
                    "signal_etf": row["signal_etf"],
                    "event_type": "Signal Change",
                    "old_value": prev_signal,
                    "new_value": current_signal,
                    "total_score": row["total_score"],
                    "signal_state": current_signal,
                    "rank": current_rank,
                    "notes": ""
                })

            if prev_rank is not None:
                if prev_rank != 1 and current_rank == 1:
                    events.append({
                        "date": row["date"],
                        "sector": row["sector"],
                        "signal_etf": row["signal_etf"],
                        "event_type": "Rank #1 Start",
                        "old_value": prev_rank,
                        "new_value": current_rank,
                        "total_score": row["total_score"],
                        "signal_state": current_signal,
                        "rank": current_rank,
                        "notes": ""
                    })
                elif prev_rank == 1 and current_rank != 1:
                    events.append({
                        "date": row["date"],
                        "sector": row["sector"],
                        "signal_etf": row["signal_etf"],
                        "event_type": "Rank #1 End",
                        "old_value": prev_rank,
                        "new_value": current_rank,
                        "total_score": row["total_score"],
                        "signal_state": current_signal,
                        "rank": current_rank,
                        "notes": ""
                    })

            prev_signal = current_signal
            prev_rank = current_rank

    event_df = pd.DataFrame(events, columns=[
        "date",
        "sector",
        "signal_etf",
        "event_type",
        "old_value",
        "new_value",
        "total_score",
        "signal_state",
        "rank",
        "notes",
    ])

    event_df.to_csv(event_path, index=False)
    print(f"Wrote {len(event_df)} events to {event_path}")


if __name__ == "__main__":
    main()
