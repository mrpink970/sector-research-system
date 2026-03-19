#!/usr/bin/env python3

import csv
from pathlib import Path
import yaml
import yfinance as yf
import pandas as pd


MARKET_DATA_PATH = Path("data/market_data.csv")
LOOKBACK_PERIOD = "6mo"


def load_yaml(path):
    with open(path, "r") as f:
        return yaml.safe_load(f)


def load_tickers():
    config = load_yaml("config/sector_map.yaml")

    tickers = []

    for sector in config.get("sectors", []):
        signal_etf = sector.get("signal_etf")
        benchmark = sector.get("benchmark")

        if signal_etf and signal_etf not in tickers:
            tickers.append(signal_etf)

        if benchmark and benchmark not in tickers:
            tickers.append(benchmark)

    return tickers


def load_existing():
    if not MARKET_DATA_PATH.exists():
        return pd.DataFrame(columns=["date", "ticker", "open", "high", "low", "close", "volume"])

    df = pd.read_csv(MARKET_DATA_PATH)
    if df.empty:
        return pd.DataFrame(columns=["date", "ticker", "open", "high", "low", "close", "volume"])

    return df


def fetch_history(ticker):
    data = yf.download(
        ticker,
        period=LOOKBACK_PERIOD,
        interval="1d",
        progress=False,
        auto_adjust=False,
        threads=False,
    )

    if data.empty:
        return pd.DataFrame()

    if hasattr(data.columns, "nlevels") and data.columns.nlevels > 1:
        data.columns = data.columns.get_level_values(0)

    data = data.reset_index()

    out = pd.DataFrame({
        "date": pd.to_datetime(data["Date"]).dt.strftime("%Y-%m-%d"),
        "ticker": ticker,
        "open": data["Open"].astype(float),
        "high": data["High"].astype(float),
        "low": data["Low"].astype(float),
        "close": data["Close"].astype(float),
        "volume": data["Volume"].fillna(0).astype(int),
    })

    return out


def main():
    tickers = load_tickers()
    existing = load_existing()

    frames = []

    for ticker in tickers:
        hist = fetch_history(ticker)

        if hist.empty:
            print("no data", ticker)
            continue

        print("fetched", ticker, len(hist))
        frames.append(hist)

    if not frames:
        print("no data fetched")
        return

    fresh = pd.concat(frames, ignore_index=True)

    if existing.empty:
        combined = fresh
    else:
        combined = pd.concat([existing, fresh], ignore_index=True)
        combined = combined.drop_duplicates(subset=["date", "ticker"], keep="last")

    combined = combined.sort_values(["ticker", "date"]).reset_index(drop=True)
    combined.to_csv(MARKET_DATA_PATH, index=False)

    print(f"wrote {len(combined)} rows to {MARKET_DATA_PATH}")


if __name__ == "__main__":
    main()
