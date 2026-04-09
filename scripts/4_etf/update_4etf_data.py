#!/usr/bin/env python3
"""
ETF Daily Data Updater
Fetches OHLC data for SOXL, TQQQ, SOXS, SQQQ, SMH, QQQ, and SOXX
Updates the Excel workbook without losing existing data
"""

import sys
import pandas as pd
import yfinance as yf
from openpyxl import load_workbook
from openpyxl.utils import get_column_letter
from datetime import datetime, timedelta
import warnings
warnings.filterwarnings('ignore')

# Tickers to fetch
TICKERS = {
    'SOXL': 'SOXL',
    'TQQQ': 'TQQQ',
    'SOXS': 'SOXS',
    'SQQQ': 'SQQQ',
    'SMH': 'SMH',
    'QQQ': 'QQQ',
    'SOXX': 'SOXX',  # NEW: Added SOXX
}

# Column suffixes for each ticker
COLUMN_SUFFIXES = ['_Open', '_High', '_Low', '_Close', '_%Chg', '_3D', '_5D']

# Required columns for Daily_Data sheet (will be auto-created if missing)
REQUIRED_COLUMNS = []
for ticker in TICKERS.keys():
    for suffix in COLUMN_SUFFIXES:
        REQUIRED_COLUMNS.append(f"{ticker}{suffix}")


def ensure_columns_exist(worksheet, required_columns):
    """Automatically add missing columns to the worksheet without disturbing existing data"""
    existing_columns = []
    for col in range(1, worksheet.max_column + 1):
        cell_value = worksheet.cell(row=1, column=col).value
        if cell_value:
            existing_columns.append(str(cell_value).strip())
    
    missing_columns = [col for col in required_columns if col not in existing_columns]
    
    if missing_columns:
        print(f"Adding {len(missing_columns)} missing columns: {missing_columns[:5]}...")
        
        # Add missing columns to the right of existing data
        next_col = worksheet.max_column + 1
        for col_name in missing_columns:
            worksheet.cell(row=1, column=next_col, value=col_name)
            next_col += 1
        
        return True
    return False


def fetch_historical_data(ticker, start_date, end_date):
    """Fetch historical OHLC data from Yahoo Finance"""
    try:
        stock = yf.Ticker(ticker)
        data = stock.history(start=start_date, end=end_date)
        if data.empty:
            print(f"Warning: No data for {ticker}")
            return None
        return data
    except Exception as e:
        print(f"Error fetching {ticker}: {e}")
        return None


def calculate_returns(df, ticker):
    """Calculate 1D, 3D, 5D returns from close prices"""
    if df is None or df.empty:
        return {}
    
    # Calculate returns
    returns_1d = df['Close'].pct_change() * 100
    returns_3d = df['Close'].pct_change(periods=3) * 100
    returns_5d = df['Close'].pct_change(periods=5) * 100
    
    # Create return columns
    result = {}
    for date in df.index:
        date_str = date.strftime('%Y-%m-%d')
        result[date_str] = {
            f"{ticker}_%Chg": round(returns_1d.get(date, 0), 4) if date in returns_1d.index else 0,
            f"{ticker}_3D": round(returns_3d.get(date, 0), 4) if date in returns_3d.index else 0,
            f"{ticker}_5D": round(returns_5d.get(date, 0), 4) if date in returns_5d.index else 0,
        }
    
    return result


def update_workbook(workbook_path):
    """Main function to update the workbook with latest data"""
    
    print("=" * 60)
    print("ETF DAILY DATA UPDATER")
    print("=" * 60)
    
    # Load workbook
    try:
        wb = load_workbook(workbook_path)
        print(f"Loaded workbook: {workbook_path}")
    except Exception as e:
        print(f"Error loading workbook: {e}")
        return False
    
    # Ensure Daily_Data sheet exists
    if 'Daily_Data' not in wb.sheetnames:
        print("Creating Daily_Data sheet...")
        ws = wb.create_sheet('Daily_Data')
        # Add Date column
        ws.cell(row=1, column=1, value='Date')
    else:
        ws = wb['Daily_Data']
        print("Daily_Data sheet found")
    
    # Auto-add missing columns
    if ensure_columns_exist(ws, REQUIRED_COLUMNS):
        print("Added missing columns - existing data preserved")
    
    # Read existing data into DataFrame
    data_rows = []
    headers = []
    
    # Get headers from first row
    for col in range(1, ws.max_column + 1):
        header = ws.cell(row=1, column=col).value
        if header:
            headers.append(str(header).strip())
    
    # Read existing data rows
    for row in range(2, ws.max_row + 1):
        row_data = {}
        has_data = False
        for col_idx, header in enumerate(headers, start=1):
            cell_value = ws.cell(row=row, column=col_idx).value
            if cell_value is not None and str(cell_value).strip() != '':
                has_data = True
            row_data[header] = cell_value
        if has_data:
            data_rows.append(row_data)
    
    existing_df = pd.DataFrame(data_rows) if data_rows else pd.DataFrame()
    
    # Determine date range
    end_date = datetime.now()
    start_date = end_date - timedelta(days=365)  # Get 1 year of history
    
    print(f"\nFetching data from {start_date.date()} to {end_date.date()}")
    
    # Fetch data for all tickers
    all_data = {}
    all_returns = {}
    
    for ticker_symbol in TICKERS.keys():
        print(f"Fetching {ticker_symbol}...")
        df = fetch_historical_data(ticker_symbol, start_date, end_date)
        if df is not None:
            all_data[ticker_symbol] = df
            all_returns[ticker_symbol] = calculate_returns(df, ticker_symbol)
    
    # Build complete dataset
    all_dates = set()
    for ticker, df in all_data.items():
        all_dates.update(df.index.strftime('%Y-%m-%d'))
    
    all_dates = sorted(all_dates)
    
    # Create complete DataFrame
    complete_data = {}
    for date in all_dates:
        complete_data[date] = {'Date': date}
        
        for ticker, df in all_data.items():
            date_obj = datetime.strptime(date, '%Y-%m-%d')
            if date_obj in df.index:
                row = df.loc[date_obj]
                complete_data[date][f"{ticker}_Open"] = round(row['Open'], 6)
                complete_data[date][f"{ticker}_High"] = round(row['High'], 6)
                complete_data[date][f"{ticker}_Low"] = round(row['Low'], 6)
                complete_data[date][f"{ticker}_Close"] = round(row['Close'], 6)
            else:
                complete_data[date][f"{ticker}_Open"] = None
                complete_data[date][f"{ticker}_High"] = None
                complete_data[date][f"{ticker}_Low"] = None
                complete_data[date][f"{ticker}_Close"] = None
            
            # Add return data
            if ticker in all_returns and date in all_returns[ticker]:
                complete_data[date].update(all_returns[ticker][date])
    
    # Convert to DataFrame
    new_df = pd.DataFrame.from_dict(complete_data, orient='index')
    new_df = new_df.sort_index()  # Sort by date
    
    # Merge with existing data (preserve existing values)
    if not existing_df.empty and 'Date' in existing_df.columns:
        # Convert existing DataFrame dates to string for merging
        existing_df['Date'] = existing_df['Date'].astype(str)
        new_df['Date'] = new_df['Date'].astype(str)
        
        # Merge: new data overwrites existing, but keep existing columns not in new data
        merged_df = existing_df.set_index('Date').combine_first(new_df.set_index('Date')).reset_index()
        merged_df.rename(columns={'index': 'Date'}, inplace=True)
        final_df = merged_df.sort_values('Date')
    else:
        final_df = new_df
    
    # Write back to Excel
    print("\nWriting data to Excel...")
    
    # Clear existing data (keep headers)
    for row in range(2, ws.max_row + 1):
        for col in range(1, ws.max_column + 1):
            ws.cell(row=row, column=col, value=None)
    
    # Write headers
    headers = ['Date'] + [col for col in final_df.columns if col != 'Date']
    for col_idx, header in enumerate(headers, start=1):
        ws.cell(row=1, column=col_idx, value=header)
    
    # Write data rows
    for row_idx, (_, row) in enumerate(final_df.iterrows(), start=2):
        for col_idx, header in enumerate(headers, start=1):
            value = row[header] if header in row.index else None
            if value is not None and not pd.isna(value):
                ws.cell(row=row_idx, column=col_idx, value=value)
    
    # Auto-adjust column widths
    for col in range(1, ws.max_column + 1):
        max_length = 0
        col_letter = get_column_letter(col)
        for row in range(1, min(ws.max_row, 100) + 1):
            cell_value = ws.cell(row=row, column=col).value
            if cell_value:
                max_length = max(max_length, len(str(cell_value)))
        adjusted_width = min(max_length + 2, 20)
        ws.column_dimensions[col_letter].width = adjusted_width
    
    # Save workbook
    try:
        wb.save(workbook_path)
        print(f"\n✅ Successfully updated {workbook_path}")
        print(f"   Total rows: {len(final_df)}")
        print(f"   Date range: {final_df['Date'].min()} to {final_df['Date'].max()}")
        print(f"   Columns: {len(headers)}")
        return True
    except Exception as e:
        print(f"Error saving workbook: {e}")
        return False


def main():
    if len(sys.argv) > 1:
        workbook_path = sys.argv[1]
    else:
        workbook_path = "4_ETF_Trading_Workbook_Template.xlsx"
    
    success = update_workbook(workbook_path)
    sys.exit(0 if success else 1)


if __name__ == "__main__":
    main()
