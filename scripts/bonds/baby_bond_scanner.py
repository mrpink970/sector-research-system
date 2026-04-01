#!/usr/bin/env python3
"""
Baby Bond Scanner
Reads from baby_bond_universe.csv and fetches current data from Yahoo Finance.
"""

import yfinance as yf
import pandas as pd
from datetime import datetime
from pathlib import Path


def load_baby_bond_universe(file_path: Path) -> pd.DataFrame:
    """Load the baby bond universe from CSV"""
    if not file_path.exists():
        print(f"Error: {file_path} not found")
        return pd.DataFrame()
    
    df = pd.read_csv(file_path)
    print(f"Loaded {len(df)} bonds from universe")
    return df


def fetch_baby_bond_data(ticker: str) -> dict:
    """Fetch current data for a single ticker"""
    try:
        stock = yf.Ticker(ticker)
        info = stock.info
        
        # Get historical data for 52-week range
        hist = stock.history(period="1y")
        if not hist.empty:
            year_high = hist['High'].max()
            year_low = hist['Low'].min()
            current_price = hist['Close'].iloc[-1]
            current_volume = hist['Volume'].iloc[-1]
            avg_volume = hist['Volume'].mean()
        else:
            year_high = info.get('fiftyTwoWeekHigh', 'N/A')
            year_low = info.get('fiftyTwoWeekLow', 'N/A')
            current_price = info.get('currentPrice', info.get('regularMarketPrice', 'N/A'))
            current_volume = info.get('volume', 'N/A')
            avg_volume = info.get('averageVolume', 'N/A')
        
        # Get dividend/yield data
        dividend_rate = info.get('dividendRate', 'N/A')
        dividend_yield = info.get('dividendYield', 'N/A')
        
        # Convert dividend yield to percentage
        if isinstance(dividend_yield, (int, float)) and dividend_yield != 'N/A':
            if dividend_yield < 1:
                dividend_yield = round(dividend_yield * 100, 2)
        
        return {
            'ticker': ticker,
            'current_price': current_price if isinstance(current_price, (int, float)) else 'N/A',
            '52_week_high': year_high if isinstance(year_high, (int, float)) else 'N/A',
            '52_week_low': year_low if isinstance(year_low, (int, float)) else 'N/A',
            'volume': current_volume if isinstance(current_volume, (int, float)) else 'N/A',
            'avg_volume': avg_volume if isinstance(avg_volume, (int, float)) else 'N/A',
            'dividend_rate': dividend_rate,
            'dividend_yield': dividend_yield,
        }
        
    except Exception as e:
        print(f"  Error fetching {ticker}: {e}")
        return None


def calculate_metrics(row: dict, bond_info: dict) -> dict:
    """Calculate additional metrics from bond data and current price"""
    result = {}
    
    # Current price
    current_price = bond_info.get('current_price', 'N/A')
    result['current_price'] = current_price
    
    # Discount to par
    par_value = bond_info.get('par_value', 25)
    if isinstance(current_price, (int, float)) and current_price > 0:
        discount_pct = ((par_value - current_price) / par_value) * 100
        result['discount_to_par_pct'] = round(discount_pct, 1)
        result['is_discount'] = discount_pct > 0
    else:
        result['discount_to_par_pct'] = 'N/A'
        result['is_discount'] = False
    
    # 52-week range width
    year_high = bond_info.get('52_week_high', 'N/A')
    year_low = bond_info.get('52_week_low', 'N/A')
    if isinstance(year_high, (int, float)) and isinstance(year_low, (int, float)) and year_low > 0:
        range_width = ((year_high - year_low) / year_low) * 100
        result['range_width_pct'] = round(range_width, 1)
        
        # Position in range (0 = at low, 100 = at high)
        if isinstance(current_price, (int, float)):
            position = ((current_price - year_low) / (year_high - year_low)) * 100
            result['position_in_range_pct'] = round(position, 1)
        else:
            result['position_in_range_pct'] = 'N/A'
    else:
        result['range_width_pct'] = 'N/A'
        result['position_in_range_pct'] = 'N/A'
    
    # Yield
    result['current_yield_pct'] = bond_info.get('dividend_yield', 'N/A')
    
    # Coupon from universe
    result['coupon_pct'] = bond_info.get('coupon', 'N/A')
    
    # Redemption date
    redemption_date = bond_info.get('redemption_date', 'N/A')
    if redemption_date != 'N/A':
        try:
            rd = datetime.strptime(redemption_date, '%Y-%m-%d')
            today = datetime.now()
            days_to_redemption = (rd - today).days
            result['days_to_redemption'] = days_to_redemption
            result['years_to_redemption'] = round(days_to_redemption / 365, 1)
        except:
            result['days_to_redemption'] = 'N/A'
            result['years_to_redemption'] = 'N/A'
    else:
        result['days_to_redemption'] = 'N/A'
        result['years_to_redemption'] = 'N/A'
    
    return result


def main():
    # Paths
    repo_root = Path(".")
    universe_file = repo_root / "data" / "bonds" / "baby_bond_universe.csv"
    output_file = repo_root / "data" / "bonds" / "baby_bond_scores.csv"
    
    # Load universe
    print("=" * 80)
    print("Baby Bond Scanner")
    print("=" * 80)
    
    universe = load_baby_bond_universe(universe_file)
    if universe.empty:
        return
    
    # Filter to active bonds only
    active = universe[universe['status'] == 'active']
    print(f"Active bonds to scan: {len(active)}")
    print("-" * 80)
    
    # Fetch data for each ticker
    results = []
    failed = []
    
    for idx, row in active.iterrows():
        ticker = row['ticker']
        print(f"Fetching {ticker}...", end=" ", flush=True)
        
        bond_data = fetch_baby_bond_data(ticker)
        if bond_data and bond_data.get('current_price') != 'N/A':
            # Combine with universe data
            combined = {
                'ticker': ticker,
                'description': row.get('description', ''),
                'coupon': row.get('coupon', 'N/A'),
                'par_value': row.get('par_value', 25),
                'maturity': row.get('maturity', 'N/A'),
                'redemption_date': row.get('redemption_date', 'N/A'),
                'exchange': row.get('exchange', 'N/A'),
            }
            combined.update(bond_data)
            
            # Calculate metrics
            metrics = calculate_metrics(row.to_dict(), combined)
            combined.update(metrics)
            
            results.append(combined)
            
            # Print summary
            price = combined.get('current_price', 'N/A')
            discount = combined.get('discount_to_par_pct', 'N/A')
            yield_pct = combined.get('current_yield_pct', 'N/A')
            range_width = combined.get('range_width_pct', 'N/A')
            position = combined.get('position_in_range_pct', 'N/A')
            
            print(f"OK - ${price:.2f} | Discount: {discount}% | Yield: {yield_pct}% | Range: {range_width}% | Pos: {position}%")
        else:
            failed.append(ticker)
            print("FAILED")
    
    # Create DataFrame
    if results:
        df = pd.DataFrame(results)
        
        # Sort by discount (highest first)
        df_sorted = df.copy()
        df_sorted['discount_num'] = pd.to_numeric(df_sorted['discount_to_par_pct'], errors='coerce')
        df_sorted = df_sorted.sort_values('discount_num', ascending=False)
        
        print("\n" + "=" * 80)
        print("TOP DISCOUNT CANDIDATES")
        print("=" * 80)
        
        # Show top 20 by discount
        display_cols = ['ticker', 'current_price', 'discount_to_par_pct', 'current_yield_pct', 
                        'range_width_pct', 'position_in_range_pct', 'coupon', 'redemption_date']
        print(df_sorted[display_cols].head(20).to_string(index=False))
        
        # Filter for good candidates
        df['discount_num'] = pd.to_numeric(df['discount_to_par_pct'], errors='coerce')
        df['yield_num'] = pd.to_numeric(df['current_yield_pct'], errors='coerce')
        df['range_num'] = pd.to_numeric(df['range_width_pct'], errors='coerce')
        
        good_candidates = df[
            (df['discount_num'] > 20) & 
            (df['yield_num'] > 8) & 
            (df['range_num'] > 30)
        ].copy()
        
        print("\n" + "=" * 80)
        print(f"GOOD CANDIDATES (Discount > 20%, Yield > 8%, Range > 30%): {len(good_candidates)}")
        print("=" * 80)
        
        if not good_candidates.empty:
            print(good_candidates[['ticker', 'current_price', 'discount_to_par_pct', 
                                    'current_yield_pct', 'range_width_pct', 'position_in_range_pct']].to_string(index=False))
        else:
            print("None found with current filters")
        
        # Save to CSV
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        output_filename = f"baby_bond_scores_{timestamp}.csv"
        output_path = repo_root / "data" / "bonds" / output_filename
        df.to_csv(output_path, index=False)
        
        print("\n" + "=" * 80)
        print("STATISTICS")
        print("=" * 80)
        print(f"Total bonds in universe: {len(universe)}")
        print(f"Active bonds: {len(active)}")
        print(f"Successfully fetched: {len(results)}")
        print(f"Failed: {len(failed)}")
        if failed:
            print(f"Failed tickers: {', '.join(failed[:20])}")
            if len(failed) > 20:
                print(f"... and {len(failed) - 20} more")
        
        print(f"\n✅ Full data saved to: {output_filename}")
        print(f"📁 Location: data/bonds/{output_filename}")
        
        # Save the good candidates separately
        if not good_candidates.empty:
            good_filename = f"baby_bond_candidates_{timestamp}.csv"
            good_path = repo_root / "data" / "bonds" / good_filename
            good_candidates.to_csv(good_path, index=False)
            print(f"✅ Good candidates saved to: {good_filename}")
        
    else:
        print("\n❌ No data fetched successfully.")


if __name__ == "__main__":
    main()
