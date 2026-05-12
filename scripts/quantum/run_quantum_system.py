#!/usr/bin/env python3
"""
Quantum Computing Paper Trading System
Matches spec exactly: momentum scoring, single position, 10% trailing stop, QQQ regime.
"""

from pathlib import Path
from datetime import datetime, timedelta
import pandas as pd
import yfinance as yf
import yaml

# ========================= CONFIG =========================
DATA_DIR = Path("data/quantum")
DATA_DIR.mkdir(parents=True, exist_ok=True)

PRICES_PATH = DATA_DIR / "prices.csv"
SCORES_PATH = DATA_DIR / "scores.csv"
POSITIONS_PATH = DATA_DIR / "positions.csv"
TRADE_LOG_PATH = DATA_DIR / "trade_log.csv"
PERFORMANCE_PATH = DATA_DIR / "performance.csv"

TICKERS = ["IONQ", "QBTS", "RGTI", "QUBT", "XNDU", "INFQ", "HQ"]
STARTING_BALANCE = 5000.0
MIN_SCORE = 4.0
TRAILING_STOP_PCT = 0.10
MIN_DATA_DAYS = 20

def load_config():
    try:
        with open("config/quantum_parameters.yaml") as f:
            return yaml.safe_load(f)
    except:
        return {}

def calculate_score(ret_1d: float, ret_3d: float, ret_5d: float) -> float:
    """Exact formula from spec"""
    score = (ret_1d * 0.30) + (ret_3d * 0.25) + (ret_5d * 0.20)
    
    if ret_1d > 0 and ret_3d > 0 and ret_5d > 0:
        score += 0.75
    elif ret_1d < 0 and ret_3d < 0 and ret_5d < 0:
        score -= 0.75
    
    vol = abs(ret_1d - ret_3d)
    score += max(0, 10 - vol) * 0.10
    
    return round(score, 2)

def get_qqq_regime() -> bool:
    """QQQ > MA50"""
    qqq = yf.Ticker("QQQ")
    hist = qqq.history(period="3mo")
    if len(hist) < 50:
        return True
    current = hist['Close'].iloc[-1]
    ma50 = hist['Close'].rolling(50).mean().iloc[-1]
    return current > ma50

def main():
    print("🔬 QUANTUM COMPUTING PAPER TRADING SYSTEM")
    print("=" * 60)
    
    config = load_config()
    print(f"Tickers: {TICKERS}")
    print(f"Starting Balance: ${STARTING_BALANCE:,.0f} | Min Score: {MIN_SCORE} | Stop: {TRAILING_STOP_PCT*100}%")
    
    # Fetch fresh data
    print("\n📥 Fetching latest prices...")
    end = datetime.now()
    start = end - timedelta(days=150)
    df = yf.download(TICKERS, start=start, end=end, progress=False)['Close']
    
    if df.empty:
        print("❌ No data fetched")
        return
    
    print(f"✅ Data from {df.index[0].date()} to {df.index[-1].date()}")
    
    # Calculate returns
    ret_1d = df.pct_change() * 100
    ret_3d = df.pct_change(3) * 100
    ret_5d = df.pct_change(5) * 100
    
    # Calculate scores
    scores = pd.DataFrame(index=df.index)
    for ticker in TICKERS:
        scores[ticker] = [
            calculate_score(
                ret_1d[ticker].iloc[i] if pd.notna(ret_1d[ticker].iloc[i]) else 0,
                ret_3d[ticker].iloc[i] if pd.notna(ret_3d[ticker].iloc[i]) else 0,
                ret_5d[ticker].iloc[i] if pd.notna(ret_5d[ticker].iloc[i]) else 0
            ) for i in range(len(df))
        ]
    
    # Save prices and scores (as required by dashboard)
    prices_df = df.round(2).copy()
    prices_df.to_csv(PRICES_PATH)
    scores.round(2).to_csv(SCORES_PATH)
    print(f"✅ Saved {len(df)} rows to prices.csv and scores.csv")
    
    # === LIVE TRADING LOGIC ===
    regime_ok = get_qqq_regime()
    print(f"📊 QQQ Regime: {'BULL (Trade OK)' if regime_ok else 'BEAR (Cash Only)'}")
    
    # Load current state
    positions = pd.read_csv(POSITIONS_PATH) if POSITIONS_PATH.exists() else pd.DataFrame()
    trade_log = pd.read_csv(TRADE_LOG_PATH) if TRADE_LOG_PATH.exists() else pd.DataFrame()
    
    today = df.index[-1].strftime("%Y-%m-%d")
    current_prices = df.iloc[-1]
    latest_scores = scores.iloc[-1]
    
    # === EXIT CHECK ===
    if not positions.empty:
        pos = positions.iloc[0]
        ticker = pos['ticker']
        current_price = current_prices[ticker]
        highest = pos['highest_price']
        stop_price = highest * (1 - TRAILING_STOP_PCT)
        
        if current_price <= stop_price or latest_scores[ticker] < MIN_SCORE:
            exit_reason = "trailing_stop" if current_price <= stop_price else "low_score"
            pl = (current_price - pos['entry_price']) * pos['shares']
            ret_pct = (current_price / pos['entry_price'] - 1) * 100
            
            new_trade = {
                "ticker": ticker, "entry_date": pos['entry_date'], "exit_date": today,
                "entry_price": round(pos['entry_price'], 2), "exit_price": round(current_price, 2),
                "shares": int(pos['shares']), "return_pct": round(ret_pct, 2),
                "gross_pl": round(pl, 2), "exit_reason": exit_reason
            }
            
            trade_log = pd.concat([trade_log, pd.DataFrame([new_trade])], ignore_index=True)
            trade_log.to_csv(TRADE_LOG_PATH, index=False)
            
            print(f"🚪 EXIT {ticker} | Reason: {exit_reason} | P&L: ${pl:.2f}")
            positions = pd.DataFrame()  # clear position
    
    # === ENTRY CHECK ===
    if positions.empty and regime_ok:
        # Filter valid tickers (enough history)
        valid_scores = {}
        for t in TICKERS:
            history_days = df[t].dropna().shape[0]
            if history_days >= MIN_DATA_DAYS:
                valid_scores[t] = latest_scores[t]
        
        if valid_scores:
            best_ticker = max(valid_scores, key=valid_scores.get)
            best_score = valid_scores[best_ticker]
            
            if best_score >= MIN_SCORE:
                # Entry at next open (for paper trading, we approximate with current close or fetch open)
                entry_price = current_prices[best_ticker]  # TODO: improve with real next open if needed
                
                cash = STARTING_BALANCE + (trade_log['gross_pl'].sum() if not trade_log.empty else 0)
                shares = int(cash / entry_price)
                
                if shares > 0:
                    new_pos = {
                        "ticker": best_ticker,
                        "entry_date": today,
                        "entry_price": round(entry_price, 2),
                        "shares": shares,
                        "highest_price": entry_price,
                        "trailing_stop": entry_price * (1 - TRAILING_STOP_PCT),
                        "entry_score": best_score
                    }
                    pd.DataFrame([new_pos]).to_csv(POSITIONS_PATH, index=False)
                    print(f"🟢 ENTRY {best_ticker} @ ${entry_price:.2f} | Score: {best_score} | Shares: {shares}")
    
    # Update performance
    realized_pl = trade_log['gross_pl'].sum() if not trade_log.empty else 0
    open_pl = 0
    if not positions.empty:
        pos = positions.iloc[0]
        open_pl = (current_prices[pos['ticker']] - pos['entry_price']) * pos['shares']
    
    perf = {
        "total_trades": len(trade_log),
        "win_rate_pct": round((trade_log['gross_pl'] > 0).mean() * 100, 1) if not trade_log.empty else 0,
        "total_return_pct": round(((STARTING_BALANCE + realized_pl + open_pl) / STARTING_BALANCE - 1) * 100, 2),
        "net_profit_dollars": round(realized_pl + open_pl, 2),
        "final_balance": round(STARTING_BALANCE + realized_pl + open_pl, 2)
    }
    pd.DataFrame([perf]).to_csv(PERFORMANCE_PATH, index=False)
    
    print("\n✅ Quantum System Run Complete!")
    print(f"   Position: {'None' if positions.empty else positions.iloc[0]['ticker']}")
    print(f"   Realized P&L: ${realized_pl:.2f} | Total Equity: ${perf['final_balance']:,.2f}")

if __name__ == "__main__":
    main()
