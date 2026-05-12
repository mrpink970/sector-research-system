#!/usr/bin/env python3
"""
Quantum Computing Paper Trading System - EOD Version
"""

from pathlib import Path
from datetime import datetime, timedelta
import pandas as pd
import yfinance as yf

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

def calculate_score(ret_1d: float, ret_3d: float, ret_5d: float) -> float:
    score = (ret_1d * 0.30) + (ret_3d * 0.25) + (ret_5d * 0.20)
    if ret_1d > 0 and ret_3d > 0 and ret_5d > 0:
        score += 0.75
    elif ret_1d < 0 and ret_3d < 0 and ret_5d < 0:
        score -= 0.75
    vol = abs(ret_1d - ret_3d)
    score += max(0, 10 - vol) * 0.10
    return round(score, 2)

def get_qqq_regime() -> bool:
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
    
    print(f"Tickers: {TICKERS}")
    print(f"Starting Balance: ${STARTING_BALANCE:,.0f} | Min Score: {MIN_SCORE} | Stop: {TRAILING_STOP_PCT*100}%")
    
    # Fetch data
    print("\n📥 Fetching latest prices...")
    end = datetime.now()
    start = end - timedelta(days=200)
    data = yf.download(TICKERS + ["QQQ"], start=start, end=end, progress=False)
    closes = data['Close'][TICKERS] if isinstance(data.columns, pd.MultiIndex) else data['Close']
    
    print(f"✅ Data up to {closes.index[-1].date()}")
    
    # Calculate scores
    ret_1d = closes.pct_change() * 100
    ret_3d = closes.pct_change(3) * 100
    ret_5d = closes.pct_change(5) * 100
    
    scores_df = pd.DataFrame(index=closes.index)
    for ticker in TICKERS:
        scores_df[ticker] = [calculate_score(
            ret_1d[ticker].iloc[i] if pd.notna(ret_1d[ticker].iloc[i]) else 0,
            ret_3d[ticker].iloc[i] if pd.notna(ret_3d[ticker].iloc[i]) else 0,
            ret_5d[ticker].iloc[i] if pd.notna(ret_5d[ticker].iloc[i]) else 0
        ) for i in range(len(closes))]
    
    closes.round(2).to_csv(PRICES_PATH)
    scores_df.round(2).to_csv(SCORES_PATH)
    print(f"✅ Saved prices.csv and scores.csv")
    
    regime_ok = get_qqq_regime()
    print(f"📊 QQQ Regime: {'BULL (Trade OK)' if regime_ok else 'BEAR (Cash Only)'}")
    
    today = closes.index[-1].strftime("%Y-%m-%d")
    current_prices = closes.iloc[-1]
    latest_scores = scores_df.iloc[-1]
    
    # Load state
    if POSITIONS_PATH.exists():
        positions = pd.read_csv(POSITIONS_PATH)
    else:
        positions = pd.DataFrame(columns=["ticker","entry_date","entry_price","shares","highest_price","trailing_stop","entry_score"])
    
    if TRADE_LOG_PATH.exists():
        trade_log = pd.read_csv(TRADE_LOG_PATH)
    else:
        trade_log = pd.DataFrame(columns=["ticker","entry_date","exit_date","entry_price","exit_price","shares","return_pct","gross_pl","exit_reason"])
    
    cash = STARTING_BALANCE + (trade_log['gross_pl'].sum() if not trade_log.empty else 0.0)
    
    # === EXIT CHECK ===
    if not positions.empty:
        pos = positions.iloc[0].copy()
        ticker = pos['ticker']
        curr_price = float(current_prices[ticker])
        highest = max(float(pos['highest_price']), curr_price)
        
        stop_price = highest * (1 - TRAILING_STOP_PCT)
        
        if curr_price <= stop_price or latest_scores[ticker] < MIN_SCORE:
            exit_reason = "trailing_stop" if curr_price <= stop_price else "low_score"
            ret_pct = (curr_price / float(pos['entry_price']) - 1) * 100
            pl = (curr_price - float(pos['entry_price'])) * int(pos['shares'])
            
            new_trade = {
                "ticker": ticker, "entry_date": pos['entry_date'], "exit_date": today,
                "entry_price": round(float(pos['entry_price']),2), 
                "exit_price": round(curr_price,2),
                "shares": int(pos['shares']), 
                "return_pct": round(ret_pct,2),
                "gross_pl": round(pl,2), 
                "exit_reason": exit_reason
            }
            trade_log = pd.concat([trade_log, pd.DataFrame([new_trade])], ignore_index=True)
            trade_log.to_csv(TRADE_LOG_PATH, index=False)
            print(f"🚪 EXIT {ticker} | {exit_reason} | P&L: ${pl:.2f}")
            positions = pd.DataFrame(columns=positions.columns)  # clear
        else:
            # Update highest
            pos['highest_price'] = highest
            positions.iloc[0] = pos
            positions.to_csv(POSITIONS_PATH, index=False)
            print(f"📍 Holding {ticker} | Highest: ${highest:.2f}")
    
    # === ENTRY CHECK ===
    if positions.empty and regime_ok:
        valid = {}
        for t in TICKERS:
            if closes[t].dropna().shape[0] >= MIN_DATA_DAYS:
                valid[t] = latest_scores[t]
        
        if valid:
            best_ticker = max(valid, key=valid.get)
            best_score = valid[best_ticker]
            
            if best_score >= MIN_SCORE:
                entry_price = float(current_prices[best_ticker])
                shares = int(cash // entry_price)
                
                if shares > 0:
                    new_pos = {
                        "ticker": best_ticker,
                        "entry_date": today,
                        "entry_price": round(entry_price, 2),
                        "shares": shares,
                        "highest_price": round(entry_price, 2),
                        "trailing_stop": round(entry_price * (1 - TRAILING_STOP_PCT), 2),
                        "entry_score": best_score
                    }
                    pd.DataFrame([new_pos]).to_csv(POSITIONS_PATH, index=False)
                    print(f"🟢 ENTRY {best_ticker} @ ${entry_price:.2f} | Score: {best_score} | Shares: {shares}")
    
    # === PERFORMANCE ===
    realized = trade_log['gross_pl'].sum() if not trade_log.empty else 0.0
    open_pl = 0.0
    current_pos = None
    if not positions.empty:
        pos = positions.iloc[0]
        current_pos = pos['ticker']
        open_pl = (float(current_prices[pos['ticker']]) - float(pos['entry_price'])) * int(pos['shares'])
    
    total_equity = STARTING_BALANCE + realized + open_pl
    
    perf = pd.DataFrame([{
        "total_trades": len(trade_log),
        "win_rate_pct": round((trade_log['gross_pl'] > 0).mean()*100, 1) if not trade_log.empty else 0,
        "total_return_pct": round((total_equity / STARTING_BALANCE - 1)*100, 2),
        "net_profit_dollars": round(realized + open_pl, 2),
        "final_balance": round(total_equity, 2)
    }])
    perf.to_csv(PERFORMANCE_PATH, index=False)
    
    print("\n✅ Run Complete!")
    print(f"   Current Position: {current_pos or 'None'}")
    print(f"   Realized P&L: ${realized:.2f} | Unrealized: ${open_pl:.2f}")
    print(f"   Total Equity: ${total_equity:,.2f}")

if __name__ == "__main__":
    main()
