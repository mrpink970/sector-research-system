#!/usr/bin/env python3
"""
Quantum Computing Paper Trading System - Stable EOD
Updated: 25% trailing stop, min_score 18.0, no volatility penalty, trend filter
Now saves full OHLCV historical data for dashboard
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

# UPDATED PARAMETERS
MIN_SCORE = 18.0  # Was 4.0 - only top 3-4 names qualify
TRAILING_STOP_PCT = 0.25  # Was 0.10 - 25% for quantum volatility
MIN_DATA_DAYS = 20
TREND_FILTER = True  # New: require price above 20-day SMA
TREND_MA_PERIOD = 20

def calculate_score(ret_1d, ret_3d, ret_5d):
    """
    Pure momentum score - NO volatility penalty.
    Quantum stocks are volatile by nature; penalizing that kills performance.
    """
    # Weighted momentum: recent days get highest weight
    score = (ret_1d * 0.35) + (ret_3d * 0.35) + (ret_5d * 0.30)
    
    # Alignment bonus/penalty for consistent direction
    if ret_1d > 0 and ret_3d > 0 and ret_5d > 0:
        score += 1.0  # Strong bullish alignment
    elif ret_1d < 0 and ret_3d < 0 and ret_5d < 0:
        score -= 0.75  # Strong bearish alignment
    
    # No volatility penalty - removed completely
    
    return round(score, 2)

def save_historical_ohlcv(closes, opens, highs, lows, volumes):
    """Save full OHLCV data to CSV for dashboard use"""
    df = pd.DataFrame(index=closes.index)
    for t in TICKERS:
        df[f"{t}_Open"] = opens[t].round(2)
        df[f"{t}_High"] = highs[t].round(2)
        df[f"{t}_Low"] = lows[t].round(2)
        df[f"{t}_Close"] = closes[t].round(2)
        df[f"{t}_Volume"] = volumes[t].astype(int)
    
    df.to_csv(HISTORICAL_QUOTES_PATH)
    print(f"✅ Saved OHLCV history to {HISTORICAL_QUOTES_PATH}")

def main():
    print("🔬 QUANTUM COMPUTING PAPER TRADING SYSTEM")
    print("=" * 60)
    print(f"   Trailing Stop: {TRAILING_STOP_PCT*100:.0f}%")
    print(f"   Min Score: {MIN_SCORE}")
    print(f"   Trend Filter: {'ON (20-day SMA)' if TREND_FILTER else 'OFF'}")
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
        # Fallback for unexpected data structure
        closes = data[TICKERS]
        opens = data[TICKERS]
        highs = data[TICKERS]
        lows = data[TICKERS]
        volumes = pd.DataFrame(index=data.index, columns=TICKERS)
    
    print(f"✅ Data up to {closes.index[-1].date()}")
    
    # Save historical OHLCV data for dashboard
    save_historical_ohlcv(closes, opens, highs, lows, volumes)
    
    # Calculate returns
    ret_1d = closes.pct_change() * 100
    ret_3d = closes.pct_change(3) * 100
    ret_5d = closes.pct_change(5) * 100
    
    # Calculate SMA for trend filter
    sma20 = closes.rolling(window=TREND_MA_PERIOD).mean()
    
    # Calculate scores for each day
    scores_df = pd.DataFrame(index=closes.index)
    for t in TICKERS:
        scores_df[t] = [
            calculate_score(
                ret_1d[t].iloc[i] if pd.notna(ret_1d[t].iloc[i]) else 0,
                ret_3d[t].iloc[i] if pd.notna(ret_3d[t].iloc[i]) else 0,
                ret_5d[t].iloc[i] if pd.notna(ret_5d[t].iloc[i]) else 0
            ) for i in range(len(closes))
        ]
    
    # Save data
    closes.round(2).to_csv(PRICES_PATH)
    scores_df.round(2).to_csv(SCORES_PATH)
    
    # Today's data
    today = closes.index[-1].strftime("%Y-%m-%d")
    current_prices = closes.iloc[-1]
    latest_scores = scores_df.iloc[-1]
    latest_sma20 = sma20.iloc[-1]
    
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
        current_score = latest_scores[ticker]
        
        # Check exit conditions
        stop_hit = curr <= highest * (1 - TRAILING_STOP_PCT)
        low_score = current_score < MIN_SCORE
        
        if stop_hit or low_score:
            if stop_hit:
                reason = "trailing_stop"
                print(f"🚪 EXIT {ticker} | {reason} (hit {TRAILING_STOP_PCT*100:.0f}% stop from ${highest:.2f} high)")
            else:
                reason = "low_score"
                print(f"🚪 EXIT {ticker} | {reason} (score {current_score:.2f} < {MIN_SCORE})")
            
            pl = (curr - float(pos['entry_price'])) * int(pos['shares'])
            
            new_row = pd.DataFrame([{
                "ticker": ticker, 
                "entry_date": pos['entry_date'], 
                "exit_date": today,
                "entry_price": round(float(pos['entry_price']), 2), 
                "exit_price": round(curr, 2),
                "shares": int(pos['shares']), 
                "return_pct": round((curr/float(pos['entry_price'])-1)*100, 2),
                "gross_pl": round(pl, 2), 
                "exit_reason": reason
            }])
            trade_log = pd.concat([trade_log, new_row], ignore_index=True)
            trade_log.to_csv(TRADE_LOG_PATH, index=False)
            positions = pd.DataFrame(columns=positions.columns)
            # Delete positions file
            if POSITIONS_PATH.exists():
                POSITIONS_PATH.unlink()
            print(f"   P&L: ${pl:.2f} | Return: {((curr/float(pos['entry_price'])-1)*100):.2f}%")
        else:
            # Update highest price
            pos['highest_price'] = highest
            pos['trailing_stop'] = round(highest * (1 - TRAILING_STOP_PCT), 2)
            positions.iloc[0] = pos
            positions.to_csv(POSITIONS_PATH, index=False)
            print(f"📍 HOLDING {ticker} | Price: ${curr:.2f} | Score: {current_score:.2f} | Highest: ${highest:.2f} | Stop: ${pos['trailing_stop']:.2f}")
    
    # ============================================================
    # ENTRY LOGIC
    # ============================================================
    if positions.empty:
        # Find valid candidates
        valid = {}
        for t in TICKERS:
            # Check minimum data days
            if closes[t].dropna().count() < MIN_DATA_DAYS:
                print(f"⏭️ {t}: insufficient data ({closes[t].dropna().count()} days)")
                continue
            
            # Check trend filter (price above SMA20)
            if TREND_FILTER:
                price = float(current_prices[t])
                sma = float(latest_sma20[t])
                if pd.notna(sma) and price <= sma:
                    print(f"⏭️ {t}: below 20-day SMA (${sma:.2f}) - trend filter blocks entry")
                    continue
            
            # Passed all filters
            valid[t] = latest_scores[t]
        
        if valid:
            best = max(valid, key=valid.get)
            score = valid[best]
            
            if score >= MIN_SCORE:
                price = float(current_prices[best])
                
                # Position sizing: 100% of cash as specified
                shares = int(cash // price)
                
                if shares > 0:
                    stop_price = round(price * (1 - TRAILING_STOP_PCT), 2)
                    new_pos = pd.DataFrame([{
                        "ticker": best, 
                        "entry_date": today, 
                        "entry_price": round(price, 2),
                        "shares": shares, 
                        "highest_price": round(price, 2),
                        "trailing_stop": stop_price, 
                        "entry_score": score
                    }])
                    new_pos.to_csv(POSITIONS_PATH, index=False)
                    print(f"🟢 ENTRY {best} @ ${price:.2f}")
                    print(f"   Score: {score:.2f} | Shares: {shares} | Position size: ${price * shares:,.2f}")
                    print(f"   Initial stop: ${stop_price} ({TRAILING_STOP_PCT*100:.0f}%)")
                    
                    if TREND_FILTER:
                        sma = float(latest_sma20[best])
                        print(f"   20-day SMA: ${sma:.2f} (price above by {((price/sma)-1)*100:.1f}%)")
                else:
                    print(f"⚠️ Cannot afford {best} at ${price:.2f} (cash: ${cash:.2f})")
            else:
                best_ticker = max(valid, key=valid.get)
                print(f"⏭️ Best candidate {best_ticker} score {valid[best_ticker]:.2f} < {MIN_SCORE} - no entry")
    
    # ============================================================
    # PERFORMANCE SUMMARY
    # ============================================================
    # Reload positions for accurate summary
    positions = pd.read_csv(POSITIONS_PATH) if POSITIONS_PATH.exists() else pd.DataFrame(columns=["ticker","entry_date","entry_price","shares","highest_price","trailing_stop","entry_score"])
    
    realized = trade_log['gross_pl'].sum() if not trade_log.empty else 0.0
    open_pl = 0.0
    current_pos = None
    
    if not positions.empty:
        p = positions.iloc[0]
        current_pos = p['ticker']
        current_price = float(current_prices[p['ticker']])
        open_pl = (current_price - float(p['entry_price'])) * int(p['shares'])
    
    total = STARTING_BALANCE + realized + open_pl
    
    # Calculate win rate and avg loss
    if len(trade_log) > 0:
        wins = trade_log['gross_pl'] > 0
        win_rate = round((wins).mean() * 100, 1)
        avg_loss_pct = round(trade_log[trade_log['gross_pl'] < 0]['return_pct'].mean(), 1) if len(trade_log[trade_log['gross_pl'] < 0]) > 0 else 0
        avg_win_pct = round(trade_log[wins]['return_pct'].mean(), 1) if len(trade_log[wins]) > 0 else 0
    else:
        win_rate = 0.0
        avg_loss_pct = 0.0
        avg_win_pct = 0.0
    
    perf = pd.DataFrame([{
        "date": today,
        "total_trades": len(trade_log),
        "win_rate_pct": win_rate,
        "avg_win_pct": avg_win_pct,
        "avg_loss_pct": avg_loss_pct,
        "total_return_pct": round((total/STARTING_BALANCE-1)*100, 2),
        "net_profit_dollars": round(realized + open_pl, 2),
        "final_balance": round(total, 2),
        "realized_pl": round(realized, 2),
        "unrealized_pl": round(open_pl, 2)
    }])
    perf.to_csv(PERFORMANCE_PATH, index=False)
    
    print("\n" + "=" * 60)
    print("📊 PERFORMANCE SUMMARY")
    print("=" * 60)
    print(f"   Current Position: {current_pos or 'None'}")
    print(f"   Total Equity: ${total:,.2f}")
    print(f"   Realized P&L: ${realized:,.2f}")
    print(f"   Unrealized P&L: ${open_pl:,.2f}")
    print(f"   Total Return: {((total/STARTING_BALANCE-1)*100):.1f}%")
    print(f"   Win Rate: {win_rate}% ({len(trade_log[trade_log['gross_pl'] > 0]) if len(trade_log) > 0 else 0}/{len(trade_log)} trades)")
    if avg_loss_pct != 0:
        print(f"   Avg Loss: {avg_loss_pct}%")
    if avg_win_pct != 0:
        print(f"   Avg Win: {avg_win_pct}%")
    print("=" * 60)
    print("\n✅ Run Complete!")

if __name__ == "__main__":
    main()
