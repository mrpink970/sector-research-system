#!/usr/bin/env python3
"""
Quick Baby Bond Data Fetcher
Fetches basic data from Yahoo Finance for a list of baby bond/preferred tickers.
"""

import yfinance as yf
import pandas as pd
from datetime import datetime

# Corrected tickers for Yahoo Finance
TICKERS = [
    "QVCD", "QVCC", 
    "BHFAL", "BHFAN", "BHFAO",
    "HOVNP", 
    "HPP-PRC",      # HPP/PRC → HPP-PRC
    "JSM", 
    "JXN-PRA",      # JXN/PRA → JXN-PRA
    "LNC-PRD",      # LNC/PRD → LNC-PRD
    "PBI-PRB",      # PBI/PRB → PBI-PRB
    "PSEC-PRA",     # PSEC/PRA → PSEC-PRA
    "SLMBP", 
    "CTBB", "CTDD",
    "DHCNI", "DHCNL", 
    "FGSN", 
    "FITBI", 
    "TDS-PRU",      # TDS/PRU → TDS-PRU
    "TDS-PRV",      # TDS/PRV → TDS-PRV
    "VNO-PRL",      # VNO/PRL → VNO-PRL
    "VNO-PRM",      # VNO/PRM → VNO-PRM
    "VNO-PRN"       # VNO/PRN → VNO-PRN
]

def fetch_baby_bond_data(ticker):
    """Fetch basic data for a single ticker"""
    try:
        stock = yf.Ticker(ticker)
        info = stock.info
        
        # Get historical data for 52-week range
        hist = stock.history(period="1y")
        if not hist.empty:
            year_high = hist['High'].max()
            year_low = hist['Low'].min()
            current_price = hist['Close'].iloc[-1]
        else:
            year_high = info.get('fiftyTwoWeekHigh', 'N/A')
            year_low = info.get('fiftyTwoWeekLow', 'N/A')
            current_price = info.get('currentPrice', info.get('regularMarketPrice', 'N/A'))
        
        # Extract data
        data = {
            'ticker': ticker,
            'current_price': current_price if isinstance(current_price, (int, float)) else 'N/A',
            '52_week_high': year_high if isinstance(year_high, (int, float)) else 'N/A',
            '52_week_low': year_low if isinstance(year_low, (int, float)) else 'N/A',
            'dividend_rate': info.get('dividendRate', 'N/A'),
            'dividend_yield': info.get('dividendYield', 'N/A'),
            'market_cap': info.get('marketCap', 'N/A'),
            'volume': info.get('volume', 'N/A'),
            'avg_volume': info.get('averageVolume', 'N/A'),
            'long_name': info.get('longName', 'N/A'),
        }
        
        # Calculate range width
        if isinstance(year_high, (int, float)) and isinstance(year_low, (int, float)) and year_low > 0:
            range_width = ((year_high - year_low) / year_low) * 100
            data['range_width_pct'] = round(range_width, 1)
        else:
            data['range_width_pct'] = 'N/A'
        
        # Calculate position in range
        if isinstance(current_price, (int, float)) and isinstance(year_low, (int, float)) and isinstance(year_high, (int, float)) and (year_high - year_low) > 0:
            position = ((current_price - year_low) / (year_high - year_low)) * 100
            data['position_in_range_pct'] = round(position, 1)
        else:
            data['position_in_range_pct'] = 'N/A'
        
        # Convert dividend yield to percentage
        if isinstance(data['dividend_yield'], (int, float)) and data['dividend_yield'] != 'N/A':
            if data['dividend_yield'] < 1:
                data['dividend_yield'] = round(data['dividend_yield'] * 100, 2)
        
        return data
        
    except Exception as e:
        print(f"Error fetching {ticker}: {e}")
        return None

def main():
    print("=" * 80)
    print("Baby Bond Data Fetcher - Yahoo Finance")
    print(f"Fetching data for {len(TICKERS)} tickers...")
    print("=" * 80)
    
    results = []
    failed = []
    
    for ticker in TICKERS:
        print(f"Fetching {ticker}...", end=" ")
        data = fetch_baby_bond_data(ticker)
        if data:
            results.append(data)
            price = data['current_price']
            if isinstance(price, (int, float)):
                print(f"OK - ${price:.2f} | Range: {data['range_width_pct']}% | Yield: {data['dividend_yield']}%")
            else:
                print(f"OK - No price data")
        else:
            failed.append(ticker)
            print("FAILED")
    
    if results:
        df = pd.DataFrame(results)
        
        # Filter numeric columns for sorting
        df_sorted = df.copy()
        df_sorted['dividend_yield_num'] = pd.to_numeric(df_sorted['dividend_yield'], errors='coerce')
        df_sorted = df_sorted.sort_values('dividend_yield_num', ascending=False)
        
        print("\n" + "=" * 80)
        print("SUMMARY - Sorted by Dividend Yield")
        print("=" * 80)
        
        # Display key columns
        display_df = df_sorted[['ticker', 'current_price', '52_week_low', '52_week_high', 
                                 'range_width_pct', 'position_in_range_pct', 'dividend_yield']]
        print(display_df.to_string(index=False))
        
        print("\n" + "=" * 80)
        print("STATISTICS")
        print("=" * 80)
        print(f"Total tickers in universe: {len(TICKERS)}")
        print(f"Successfully fetched: {len(results)}")
        print(f"Failed: {len(failed)}")
        if failed:
            print(f"Failed tickers: {', '.join(failed)}")
        
        # Filter for good candidates
        df_numeric = df.copy()
        df_numeric['current_price'] = pd.to_numeric(df_numeric['current_price'], errors='coerce')
        df_numeric['dividend_yield'] = pd.to_numeric(df_numeric['dividend_yield'], errors='coerce')
        df_numeric['range_width_pct'] = pd.to_numeric(df_numeric['range_width_pct'], errors='coerce')
        
        good_candidates = df_numeric[
            (df_numeric['current_price'] < 20) & 
            (df_numeric['dividend_yield'] > 8) & 
            (df_numeric['range_width_pct'] > 30)
        ]
        
        print(f"\n✅ Good candidates (price < $20, yield > 8%, range > 30%): {len(good_candidates)}")
        if not good_candidates.empty:
            print(good_candidates[['ticker', 'current_price', 'dividend_yield', 'range_width_pct', 'position_in_range_pct']].to_string(index=False))
        
        # Save to CSV
        filename = f"baby_bond_data_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"
        df.to_csv(filename, index=False)
        print(f"\n📁 Full data saved to: {filename}")
        
    else:
        print("\n❌ No data fetched successfully.")

if __name__ == "__main__":
    main()
