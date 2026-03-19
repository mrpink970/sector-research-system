#!/usr/bin/env python3

import pandas as pd
import yaml
import yfinance as yf


OUTPUT_FILE = "data/market_data.csv"
LOOKBACK_PERIOD = "2y"


def load_sector_map():
    with open("config/sector_map.yaml", "r") as f:
        return yaml.safe_load(f)


def get_all_tickers(config):
    tickers = set()

    for s in config["sectors"]:
        if s.get("signal_etf"):
            tickers.add(s["signal_etf"])
        if s.get("bull_etf"):
            tickers.add(s["bull_etf"])
        if s.get("bear_etf"):
            if s["bear_etf"] != "":
                tickers.add(s["bear_etf"])
        if s.get("benchmark"):
            tickers.add(s["benchmark"])

    return sorted(tickers)


def fetch_history(ticker):
    df = yf.download(
        ticker,
        period=LOOKBACK_PERIOD,
        interval="1d",
        progress=False,
        auto_adjust=False,
        threads=False,
    )

    if df.empty:
        return None

    if hasattr(df.columns, "nlevels") and df.columns.nlevels > 1:
        df.columns = df.columns.get_level_values(0)

    df = df.reset_index()

    out = pd.DataFrame({
        "date": pd.to_datetime(df["Date"]).dt.strftime("%Y-%m-%d"),
        "ticker": ticker,
        "open": df["Open"],
        "high": df["High"],
        "low": df["Low"],
        "close": df["Close"],
        "volume": df["Volume"],
    })

    return out


def main():
    config = load_sector_map()
    tickers = get_all_tickers(config)

    all_data = []

    for ticker in tickers:
        print(f"Fetching {ticker}")
        df = fetch_history(ticker)

        if df is None or df.empty:
            print(f"No data for {ticker}")
            continue

        all_data.append(df)

    if not all_data:
        print("No market data collected")
        return

    final = pd.concat(all_data, ignore_index=True)
    final = final.sort_values(["ticker", "date"]).reset_index(drop=True)

    final.to_csv(OUTPUT_FILE, index=False)
    print(f"Wrote {len(final)} rows to {OUTPUT_FILE}")


if __name__ == "__main__":
    main()
