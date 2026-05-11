def fetch_market_data(tickers: List[str]) -> pd.DataFrame:
    """Fetch daily closing prices for all tickers plus QQQ"""
    all_tickers = list(set(tickers + ["QQQ"]))
    start_date = (datetime.now() - timedelta(days=60)).strftime("%Y-%m-%d")
    end_date = datetime.now().strftime("%Y-%m-%d")
    
    print(f"  Fetching {len(all_tickers)} tickers from {start_date} to {end_date}")
    
    data = yf.download(all_tickers, start=start_date, end=end_date, progress=False)
    
    if data.empty:
        print("  WARNING: No data returned")
        return pd.DataFrame()
    
    # Extract Close prices (handles both single and multiple tickers)
    if isinstance(data.columns, pd.MultiIndex):
        df = data['Close'].copy()
    else:
        df = pd.DataFrame(data['Close']) if 'Close' in data else data
    
    return df.dropna()
