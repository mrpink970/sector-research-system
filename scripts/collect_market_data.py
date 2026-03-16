#!/usr/bin/env python3

import csv
from pathlib import Path
import yaml
import yfinance as yf


def load_yaml(path):
    with open(path, "r") as f:
        return yaml.safe_load(f)


def load_tickers():
    config = load_yaml("config/ticker_list.yaml")

    tickers = []

    for group in config:
        for ticker in config[group]:
            if ticker not in tickers:
                tickers.append(ticker)

    return tickers


def get_existing_keys():
    keys = set()

    path = Path("data/market_data.csv")

    if not path.exists():
        return keys

    with open(path, newline="") as f:
        reader = csv.DictReader(f)

        for row in reader:
            keys.add((row["date"], row["ticker"]))

    return keys


def fetch_price(ticker):

    data = yf.download(
        ticker,
        period="10d",
        interval="1d",
        progress=False
    )

    if data.empty:
        return None

    row = data.iloc[-1]
    date = data.index[-1].strftime("%Y-%m-%d")

    return [
    date,
    ticker,
    float(row["Open"].item()),
    float(row["High"].item()),
    float(row["Low"].item()),
    float(row["Close"].item()),
    int(row["Volume"].item())
]


def main():

    tickers = load_tickers()

    existing = get_existing_keys()

    new_rows = []

    for ticker in tickers:

        row = fetch_price(ticker)

        if row is None:
            print("no data", ticker)
            continue

        key = (row[0], row[1])

        if key in existing:
            print("duplicate", ticker)
            continue

        print("add", ticker, row[0])

        new_rows.append(row)

    if not new_rows:
        print("no new rows")
        return

    with open("data/market_data.csv", "a", newline="") as f:

        writer = csv.writer(f)

        for row in new_rows:
            writer.writerow(row)


if __name__ == "__main__":
    main()
