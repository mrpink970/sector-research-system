#!/usr/bin/env python3
"""
Quantum Computing Paper Trading System - Stable EOD
"""

from pathlib import Path
from datetime import datetime, timedelta
import pandas as pd
import yfinance as yf

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

def calculate_score(ret_1d, ret_3d, ret_5d):
    score = (ret_1d * 0.30) + (ret_3d * 0.25) + (ret_5d * 0.20)
    if ret_1d > 0 and ret_3d > 0 and ret_5d > 0:
        score += 0.75
    elif ret_1d < 0 and ret_3d < 0 and ret_5d < 0:
        score -= 0.75
    vol = abs(ret_1d - ret_3d)
    score += max(0, 10 - vol) * 0.10
    return round(score, 2)

def main():
    print("🔬 QUANTUM COMPUTING PAPER TRADING SYSTEM")
    print("=" * 60)
    
    print("\n📥 Fetching latest prices...")
    data = yf.download(TICKERS, period="6mo", progress=False)
    if isinstance(data.columns, pd.MultiIndex):
        closes = data['Close'][TICKERS]
    else:
        closes = data[TICKERS]
    
    print(f"✅ Data up to {closes.index[-1].date()}")
    
    ret_1d = closes.pct_change() * 100
    ret_3d = closes.pct_change(3) * 100
    ret_5d = closes.pct_change(5) * 100

    scores_df = pd.DataFrame(index=closes.index)
    for t in TICKERS:
        scores_df[t] = [calculate_score(
            ret_1d[t].iloc[i] if pd.notna(ret_1d[t].iloc[i]) else 0,
            ret_3d[t].iloc[i] if pd.notna(ret_3d[t].iloc[i]) else 0,
            ret_5d[t].iloc[i] if pd.notna(ret_5d[t].iloc[i]) else 0
        ) for i in range(len(closes))]

    closes.round(2).to_csv(PRICES_PATH)
    scores_df.round(2).to_csv(SCORES_PATH)

    today = closes.index[-1].strftime("%Y-%m-%d")
    current_prices = closes.iloc[-1]
    latest_scores = scores_df.iloc[-1]

    # Load state
    positions = pd.read_csv(POSITIONS_PATH) if POSITIONS_PATH.exists() else pd.DataFrame(columns=["ticker","entry_date","entry_price","shares","highest_price","trailing_stop","entry_score"])
    trade_log = pd.read_csv(TRADE_LOG_PATH) if TRADE_LOG_PATH.exists() else pd.DataFrame(columns=["ticker","entry_date","exit_date","entry_price","exit_price","shares","return_pct","gross_pl","exit_reason"])

    cash = STARTING_BALANCE + (trade_log['gross_pl'].sum() if not trade_log.empty else 0)

    # EXIT
    if not positions.empty:
        pos = positions.iloc[0].copy()
        ticker = pos['ticker']
        curr = float(current_prices[ticker])
        highest = max(float(pos['highest_price']), curr)

        if curr <= highest * (1 - TRAILING_STOP_PCT) or latest_scores[ticker] < MIN_SCORE:
            reason = "trailing_stop" if curr <= highest * (1 - TRAILING_STOP_PCT) else "low_score"
            pl = (curr - float(pos['entry_price'])) * int(pos['shares'])
            print(f"🚪 EXIT {ticker} | {reason} | P&L: ${pl:.2f}")
            new_row = pd.DataFrame([{"ticker": ticker, "entry_date": pos['entry_date'], "exit_date": today,
                "entry_price": round(float(pos['entry_price']),2), "exit_price": round(curr,2),
                "shares": int(pos['shares']), "return_pct": round((curr/float(pos['entry_price'])-1)*100,2),
                "gross_pl": round(pl,2), "exit_reason": reason}])
            trade_log = pd.concat([trade_log, new_row], ignore_index=True)
            trade_log.to_csv(TRADE_LOG_PATH, index=False)
            positions = pd.DataFrame(columns=positions.columns)
        else:
            pos['highest_price'] = highest
            positions.iloc[0] = pos
            positions.to_csv(POSITIONS_PATH, index=False)
            print(f"📍 Holding {ticker} | Score: {latest_scores[ticker]:.2f} | Highest: ${highest:.2f}")

    # ENTRY
    if positions.empty:
        valid = {t: latest_scores[t] for t in TICKERS if closes[t].dropna().count() >= MIN_DATA_DAYS}
        if valid:
            best = max(valid, key=valid.get)
            score = valid[best]
            if score >= MIN_SCORE:
                price = float(current_prices[best])
                shares = int(cash // price)
                if shares > 0:
                    new_pos = pd.DataFrame([{
                        "ticker": best, "entry_date": today, "entry_price": round(price,2),
                        "shares": shares, "highest_price": round(price,2),
                        "trailing_stop": round(price*(1-TRAILING_STOP_PCT),2), "entry_score": score
                    }])
                    new_pos.to_csv(POSITIONS_PATH, index=False)
                    print(f"🟢 ENTRY {best} @ ${price:.2f} | Score: {score} | Shares: {shares}")

    # Reload positions
    positions = pd.read_csv(POSITIONS_PATH) if POSITIONS_PATH.exists() else pd.DataFrame(columns=["ticker","entry_date","entry_price","shares","highest_price","trailing_stop","entry_score"])

    # Performance
    realized = trade_log['gross_pl'].sum() if not trade_log.empty else 0.0
    open_pl = 0.0
    current_pos = None
    if not positions.empty:
        p = positions.iloc[0]
        current_pos = p['ticker']
        open_pl = (float(current_prices[p['ticker']]) - float(p['entry_price'])) * int(p['shares'])

    total = STARTING_BALANCE + realized + open_pl
    perf = pd.DataFrame([{
        "total_trades": len(trade_log),
        "win_rate_pct": round((trade_log['gross_pl']>0).mean()*100,1) if len(trade_log)>0 else 0,
        "total_return_pct": round((total/STARTING_BALANCE-1)*100,2),
        "net_profit_dollars": round(realized+open_pl,2),
        "final_balance": round(total,2)
    }])
    perf.to_csv(PERFORMANCE_PATH, index=False)

    print("\n✅ Run Complete!")
    print(f"   Position: {current_pos or 'None'}")
    print(f"   Equity: ${total:,.2f} | Realized: ${realized:.2f} | Unrealized: ${open_pl:.2f}")

if __name__ == "__main__":
    main()
