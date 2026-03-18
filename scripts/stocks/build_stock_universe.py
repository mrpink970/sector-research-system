#!/usr/bin/env python3
from __future__ import annotations

import csv
from pathlib import Path
from typing import Iterable

DEFAULT_TICKERS = [
    # Large cap / liquid examples to get the file structure started.
    "AAPL","MSFT","NVDA","AMZN","META","GOOGL","TSLA","AVGO","NFLX","AMD",
    "PLTR","SMCI","CRWD","UBER","SHOP","ARM","ANET","DDOG","SNOW","PANW",
    "JPM","GS","BAC","WFC","C","BLK","KKR","AXP","COF","MS",
    "XOM","CVX","COP","SLB","HAL","VLO","MPC","OXY","FANG","DVN",
    "LLY","NVO","MRK","ABBV","ISRG","ABT","TMO","DHR","VRTX","REGN",
    "CAT","DE","GE","HON","ETN","PH","TT","EMR","ITW","PCAR",
    "COST","WMT","TJX","ORLY","AZO","ROST","CMG","HD","LOW","MCD",
    "CELH","APP","ELF","ONON","RKLB","IONQ","HIMS","CAVA","DUOL","RDDT"
]

def write_universe(path: Path, tickers: Iterable[str]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    rows = [{"ticker": t, "source": "seed"} for t in sorted(set(tickers))]
    with path.open("w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=["ticker", "source"])
        writer.writeheader()
        writer.writerows(rows)

def main() -> None:
    path = Path("data/stocks/stock_universe.csv")
    write_universe(path, DEFAULT_TICKERS)
    print(f"Wrote {path} with {len(set(DEFAULT_TICKERS))} seed tickers.")
    print("Replace or expand this file later with a broader universe as needed.")

if __name__ == "__main__":
    main()
