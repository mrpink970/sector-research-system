# --- REVERTED DATA COLLECTION VERSION ---

# KEY CHANGES:
# - Entry is LESS strict (more trades)
# - Exit is FAST (no confirmation)
# - Designed to generate data, not optimize performance

#!/usr/bin/env python3
from __future__ import annotations

import pandas as pd
import yaml
from pathlib import Path

def load_yaml(path):
    with open(path, "r") as f:
        return yaml.safe_load(f)

def main():
    root = Path(".")
    data_dir = root / "data"

    params = load_yaml(root / "config" / "paper_trading_parameters.yaml")
    scores = pd.read_csv(data_dir / "sector_scores.csv")
    market = pd.read_csv(data_dir / "market_data.csv")

    scores["date"] = pd.to_datetime(scores["date"])
    market["date"] = pd.to_datetime(market["date"])

    positions = []
    trades = []

    shares = 100
    max_positions = 2

    dates = sorted(scores["date"].unique())

    for i in range(1, len(dates)):
        today = dates[i]
        yesterday = dates[i - 1]

        today_scores = scores[scores["date"] == yesterday]

        # --- EXIT LOGIC (FAST) ---
        new_positions = []
        for pos in positions:
            sector_data = today_scores[today_scores["sector"] == pos["sector"]]

            if sector_data.empty:
                new_positions.append(pos)
                continue

            row = sector_data.iloc[0]
            direction = str(row["direction"]).lower()

            # EXIT IMMEDIATELY if signal gone or changed
            if direction == "none" or row["selected_etf"] != pos["ticker"]:
                price_row = market[(market["date"] == today) & (market["ticker"] == pos["ticker"])]

                if not price_row.empty:
                    exit_price = price_row.iloc[0]["open"]
                    pnl = (exit_price - pos["entry_price"]) * shares

                    trades.append({
                        "ticker": pos["ticker"],
                        "entry_price": pos["entry_price"],
                        "exit_price": exit_price,
                        "pnl": pnl,
                        "exit_type": "signal_change"
                    })
                continue

            new_positions.append(pos)

        positions = new_positions

        # --- ENTRY LOGIC (LOOSE) ---
        if len(positions) < max_positions:
            sorted_scores = today_scores.sort_values("total_score", ascending=False)

            for _, row in sorted_scores.iterrows():
                if len(positions) >= max_positions:
                    break

                ticker = str(row["selected_etf"])
                direction = str(row["direction"]).lower()

                if direction == "none" or ticker == "":
                    continue

                if any(p["sector"] == row["sector"] for p in positions):
                    continue

                price_row = market[(market["date"] == today) & (market["ticker"] == ticker)]

                if price_row.empty:
                    continue

                entry_price = price_row.iloc[0]["open"]

                positions.append({
                    "sector": row["sector"],
                    "ticker": ticker,
                    "entry_price": entry_price
                })

    # Save outputs
    pd.DataFrame(trades).to_csv(data_dir / "paper_trade_log.csv", index=False)
    pd.DataFrame(positions).to_csv(data_dir / "paper_positions.csv", index=False)

    print("DATA MODE RUN COMPLETE")

if __name__ == "__main__":
    main()
