#!/usr/bin/env python3

import pandas as pd
from pathlib import Path


MAX_POSITIONS = 2
TRAILING_STOP_PCT = 0.10


def load_data():
    scores = pd.read_csv("data/sector_scores.csv")
    prices = pd.read_csv("data/market_data.csv")

    scores["date"] = pd.to_datetime(scores["date"])
    prices["date"] = pd.to_datetime(prices["date"])

    return scores, prices


def get_price(prices, ticker, date):
    row = prices[(prices["ticker"] == ticker) & (prices["date"] == date)]
    if row.empty:
        return None
    return float(row.iloc[0]["close"])


def run():
    scores, prices = load_data()

    dates = sorted(scores["date"].unique())

    positions = {}
    trade_log = []

    for date in dates:
        daily = scores[scores["date"] == date].copy()

        # rank by score
        daily = daily.sort_values("total_score", ascending=False)

        # --- EXIT LOGIC ---
        to_remove = []

        for sector, pos in positions.items():
            row = daily[daily["sector"] == sector]

            if row.empty:
                continue

            row = row.iloc[0]

            price = get_price(prices, pos["ticker"], date)
            if price is None:
                continue

            # update trailing high
            if price > pos["peak"]:
                pos["peak"] = price

            stop_price = pos["peak"] * (1 - TRAILING_STOP_PCT)

            # exit conditions
            exit_reason = None

            if row["direction"] == "none":
                exit_reason = "signal_neutral"

            elif row["selected_etf"] != pos["ticker"]:
                exit_reason = "etf_changed"

            elif price < stop_price:
                exit_reason = "trailing_stop"

            if exit_reason:
                trade_log.append({
                    "entry_date": pos["entry_date"],
                    "exit_date": date,
                    "ticker": pos["ticker"],
                    "entry_price": pos["entry_price"],
                    "exit_price": price,
                    "pnl_pct": (price / pos["entry_price"] - 1),
                    "reason": exit_reason
                })

                to_remove.append(sector)

        for sector in to_remove:
            del positions[sector]

        # --- ENTRY LOGIC ---
        for _, row in daily.iterrows():
            if len(positions) >= MAX_POSITIONS:
                break

            sector = row["sector"]
            ticker = row["selected_etf"]
            direction = row["direction"]

            if direction == "none":
                continue

            if sector in positions:
                continue

            price = get_price(prices, ticker, date)
            if price is None:
                continue

            positions[sector] = {
                "ticker": ticker,
                "entry_date": date,
                "entry_price": price,
                "peak": price
            }

    # save outputs
    pd.DataFrame(trade_log).to_csv("data/paper_trade_log.csv", index=False)

    print("Paper trading complete")


if __name__ == "__main__":
    run()
