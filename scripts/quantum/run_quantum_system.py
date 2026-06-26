#!/usr/bin/env python3
"""
Quantum Computing Paper Trading System - FIXED VERSION
- 25% trailing stop
- 18.0 min score (dashboard aligned)
- 35/35/30 weights (dashboard aligned)
- 5-day cooldown on re-entry
- 5-day time stop
- Position sizing: 100% of cash (unchanged)
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
HISTORICAL_QUOTES_PATH = DATA_DIR / "historical_quotes.csv"

TICKERS = ["IONQ", "QBTS", "RGTI", "QUBT", "XNDU", "INFQ", "HQ"]
STARTING_BALANCE = 5000.0

# ===== PARAMETERS (Dashboard aligned) =====
MIN_SCORE = 18.0                    # Dashboard: 18.0
TRAILING_STOP_PCT = 0.25            # Dashboard: 25%
MIN_DATA_DAYS = 20
COOLDOWN_DAYS = 5                   # NEW: Don't re-enter same ticker for 5 days
MAX_HOLD_DAYS = 5                   # NEW: Time stop after 5 days
TIME_STOP_MIN_RETURN = 3.0          # NEW: Exit if < 3% after 5 days

# Score weights (Dashboard: 35/35/30)
WEIGHT_1D = 0.35
WEIGHT_3D = 0.35
WEIGHT_5D = 0.30

# Track last exited ticker for cooldown
_last_exit_ticker = None
_last_exit_date = None

def calculate_score(ret_1d, ret_3d, ret_5d):
    """Calculate score with 35/35/30 weights (dashboard aligned)"""
    score = (ret_1d * WEIGHT_1D) + (ret_3d * WEIGHT_3D) + (ret_5d * WEIGHT_5D)
    
    # Trend bonus/penalty
    if ret_1d > 0 and ret_3d > 0 and ret_5d > 0:
        score += 0.75
    elif ret_1d < 0 and ret_3d < 0 and ret_5d < 0:
        score -= 0.75
    
    # Volatility adjustment (removed per dashboard)
    # score += max(0, 10 - vol) * 0.10  ← REMOVED
    
    return round(score, 2)

def main():
    print("🔬 QUANTUM COMPUTING PAPER TRADING SYSTEM (FIXED)")
    print("=" * 60)
    print(f"   Min Score: {MIN_SCORE}")
    print(f"   Trailing Stop: {TRAILING_STOP_PCT * 100}%")
    print(f"   Cooldown: {COOLDOWN_DAYS} days")
    print(f"   Time Stop: {MAX_HOLD_DAYS} days")
    print("=" * 60)
    
    print("\n📥 Fetching latest prices...")
    data = yf.download(TICKERS, period="6mo", progress=False)
    
    # Extract OHLCV components
    if isinstance(data.columns, pd.MultiIndex):
        closes = data['Close'][TICKERS]
        opens = data['Open'][TICKERS]
        highs = data['High'][TICKERS]
        lows = data['Low'][TICKERS]
        volumes = data['Volume'][TICKERS]
    else:
        closes = data[TICKERS]
        opens = data[TICKERS]
        highs = data[TICKERS]
        lows = data[TICKERS]
        volumes = pd.DataFrame(index=data.index, columns=TICKERS)
    
    print(f"✅ Data up to {closes.index[-1].date()}")
    
    # ============================================================
    # SAVE OHLCV DATA FOR DASHBOARD
    # ============================================================
    ohlcv_df = pd.DataFrame(index=closes.index)
    for t in TICKERS:
        ohlcv_df[f"{t}_Open"] = opens[t].round(2)
        ohlcv_df[f"{t}_High"] = highs[t].round(2)
        ohlcv_df[f"{t}_Low"] = lows[t].round(2)
        ohlcv_df[f"{t}_Close"] = closes[t].round(2)
        volume_series = volumes[t].fillna(0).astype(int)
        ohlcv_df[f"{t}_Volume"] = volume_series
    
    ohlcv_df.to_csv(HISTORICAL_QUOTES_PATH)
    print(f"✅ Saved OHLCV history to {HISTORICAL_QUOTES_PATH}")
    
    # Calculate returns
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

    # ============================================================
    # EXIT LOGIC
    # ============================================================
    if not positions.empty:
        pos = positions.iloc[0].copy()
        ticker = pos['ticker']
        curr = float(current_prices[ticker])
        highest = max(float(pos['highest_price']), curr)
        entry_price = float(pos['entry_price'])
        entry_date = datetime.strptime(pos['entry_date'], "%Y-%m-%d")
        hold_days = (datetime.now() - entry_date).days
        return_pct = (curr / entry_price - 1) * 100
        score = latest_scores[ticker]

        # Exit reasons (checked in order)
        exit_reason = None

        # 1. TIME STOP: Exit after MAX_HOLD_DAYS if return < TIME_STOP_MIN_RETURN
        if hold_days >= MAX_HOLD_DAYS and return_pct < TIME_STOP_MIN_RETURN:
            exit_reason = f"time_stop_{hold_days}d_{return_pct:.1f}pct"
        
        # 2. TRAILING STOP: 25% from highest
        elif curr <= highest * (1 - TRAILING_STOP_PCT):
            exit_reason = "trailing_stop"
        
        # 3. LOW SCORE: Below MIN_SCORE (18.0)
        elif score < MIN_SCORE:
            exit_reason = f"low_score_{score:.1f}"
        
        # Execute exit if any condition met
        if exit_reason:
            pl = (curr - entry_price) * int(pos['shares'])
            print(f"🚪 EXIT {ticker} | {exit_reason} | Return: {return_pct:.1f}% | P&L: ${pl:.2f}")
            
            new_row = pd.DataFrame([{
                "ticker": ticker,
                "entry_date": pos['entry_date'],
                "exit_date": today,
                "entry_price": round(entry_price, 2),
                "exit_price": round(curr, 2),
                "shares": int(pos['shares']),
                "return_pct": round(return_pct, 2),
                "gross_pl": round(pl, 2),
                "exit_reason": exit_reason
            }])
            trade_log = pd.concat([trade_log, new_row], ignore_index=True)
            trade_log.to_csv(TRADE_LOG_PATH, index=False)
            
            # NEW: Store last exited ticker for cooldown
            global _last_exit_ticker, _last_exit_date
            _last_exit_ticker = ticker
            _last_exit_date = today
            
            positions = pd.DataFrame(columns=positions.columns)
            positions.to_csv(POSITIONS_PATH, index=False)
            print(f"📭 Position cleared. Cooldown started for {ticker} ({COOLDOWN_DAYS} days)")
        else:
            # Update trailing stop
            pos['highest_price'] = highest
            pos['trailing_stop'] = round(highest * (1 - TRAILING_STOP_PCT), 2)
            positions.iloc[0] = pos
            positions.to_csv(POSITIONS_PATH, index=False)
            print(f"📍 Holding {ticker} | Score: {score:.1f} | Highest: ${highest:.2f} | Days: {hold_days}")

    # ============================================================
    # ENTRY LOGIC
    # ============================================================
    if positions.empty:
        # Filter tickers with enough data
        valid = {t: latest_scores[t] for t in TICKERS if closes[t].dropna().count() >= MIN_DATA_DAYS}
        
        if valid:
            # Pick highest scoring ticker
            best = max(valid, key=valid.get)
            score = valid[best]
            price = float(current_prices[best])
            
            # ---- ENTRY FILTERS ----
            
            # Filter 1: Min score threshold (18.0)
            if score < MIN_SCORE:
                print(f"⛔ {best} score {score:.1f} < {MIN_SCORE} — skipping")
            
            # Filter 2: COOLDOWN — Don't re-enter same ticker within COOLDOWN_DAYS
            elif _last_exit_ticker == best and _last_exit_date:
                days_since = (datetime.now() - datetime.strptime(_last_exit_date, "%Y-%m-%d")).days
                if days_since <= COOLDOWN_DAYS:
                    print(f"⛔ Cooldown: {best} exited {days_since} days ago, skipping")
                else:
                    # Cooldown expired, reset
                    _last_exit_ticker = None
                    _last_exit_date = None
            
            # Filter 3: 20-day SMA filter (dashboard: ON)
            else:
                close_series = closes[best].dropna()
                if len(close_series) >= 20:
                    sma20 = close_series.tail(20).mean()
                    if price <= sma20:
                        print(f"⛔ {best} price ${price:.2f} below 20-day SMA ${sma20:.2f} — skipping")
                    else:
                        # ALL FILTERS PASSED → ENTER
                        shares = int(cash // price)  # 100% of cash (unchanged)
                        if shares > 0:
                            new_pos = pd.DataFrame([{
                                "ticker": best,
                                "entry_date": today,
                                "entry_price": round(price, 2),
                                "shares": shares,
                                "highest_price": round(price, 2),
                                "trailing_stop": round(price * (1 - TRAILING_STOP_PCT), 2),
                                "entry_score": score
                            }])
                            new_pos.to_csv(POSITIONS_PATH, index=False)
                            print(f"🟢 ENTRY {best} @ ${price:.2f} | Score: {score:.1f} | Shares: {shares}")
                            print(f"   Stop: ${price * (1 - TRAILING_STOP_PCT):.2f} (-{TRAILING_STOP_PCT*100}%)")
                        else:
                            print(f"⚠️ Insufficient cash to enter {best}")
                else:
                    print(f"⛔ {best} has insufficient data (need 20 days)")

    # Reload positions
    positions = pd.read_csv(POSITIONS_PATH) if POSITIONS_PATH.exists() else pd.DataFrame(columns=["ticker","entry_date","entry_price","shares","highest_price","trailing_stop","entry_score"])

    # ============================================================
    # PERFORMANCE
    # ============================================================
    realized = trade_log['gross_pl'].sum() if not trade_log.empty else 0.0
    open_pl = 0.0
    current_pos = None
    
    if not positions.empty:
        p = positions.iloc[0]
        current_pos = p['ticker']
        current_price = float(current_prices[p['ticker']])
        entry_price = float(p['entry_price'])
        shares = int(p['shares'])
        open_pl = (current_price - entry_price) * shares

    total = STARTING_BALANCE + realized + open_pl
    total_return_pct = ((total - STARTING_BALANCE) / STARTING_BALANCE) * 100
    
    # Win rate
    wins = len(trade_log[trade_log['gross_pl'] > 0]) if not trade_log.empty else 0
    losses = len(trade_log[trade_log['gross_pl'] < 0]) if not trade_log.empty else 0
    win_rate = (wins / len(trade_log) * 100) if len(trade_log) > 0 else 0

    perf = pd.DataFrame([{
        "total_trades": len(trade_log),
        "win_rate_pct": round(win_rate, 1),
        "total_return_pct": round(total_return_pct, 2),
        "net_profit_dollars": round(realized + open_pl, 2),
        "final_balance": round(total, 2)
    }])
    perf.to_csv(PERFORMANCE_PATH, index=False)

    # ============================================================
    # SUMMARY
    # ============================================================
    print("\n" + "=" * 60)
    print("✅ RUN COMPLETE")
    print("=" * 60)
    print(f"   Date: {today}")
    print(f"   Position: {current_pos or 'None'}")
    print(f"   Cash: ${cash:,.2f}")
    print(f"   Equity: ${total:,.2f}")
    print(f"   Realized: ${realized:,.2f}")
    print(f"   Unrealized: ${open_pl:,.2f}")
    print(f"   Total Return: {total_return_pct:+.1f}%")
    print(f"   Trades: {len(trade_log)}")
    print(f"   Win Rate: {win_rate:.1f}%")
    print("=" * 60)

if __name__ == "__main__":
    main()
