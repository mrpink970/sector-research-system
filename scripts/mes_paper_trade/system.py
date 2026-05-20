#!/usr/bin/env python3
"""
MES Automated Paper Trading System
- Fetches live prices
- Generates buy/sell signals
- Auto-paper trades and logs everything to CSV
- Dashboard with trade instructions
"""

import os
import yfinance as yf
import pandas as pd
import numpy as np
from pathlib import Path
from datetime import datetime, time
import smtplib
from email.message import EmailMessage
import json
import time as time_module

# ============================================================
# CONFIGURATION
# ============================================================
DATA_DIR = Path("data/mes_paper")
POSITIONS_FILE = DATA_DIR / "positions.csv"
TRADES_FILE = DATA_DIR / "trades.csv"
SIGNALS_FILE = DATA_DIR / "signals.csv"
DASHBOARD_DATA = DATA_DIR / "dashboard_data.json"

os.makedirs(DATA_DIR, exist_ok=True)

# System Parameters
CONTRACTS = 6
STOP_POINTS = 8.0
TARGET_POINTS = 16.0
POINT_VALUE = 5.0

# EMA Parameters
EMA_FAST = 9
EMA_SLOW = 21

# ============================================================
# DATA FETCHING - MES TICKER
# ============================================================
def get_mes_data():
    """Get MES 1-hour data using ticker MES=F"""
    ticker = "MES=F"
    
    # Get 1-hour data for last 5 days
    data = yf.download(ticker, period="5d", interval="1h", progress=False)
    
    if data.empty:
        # Fallback to ES=F which tracks the same index (just 10x size)
        data = yf.download("ES=F", period="5d", interval="1h", progress=False)
    
    return data

def get_current_price():
    """Get current MES price from real-time data"""
    ticker = yf.Ticker("MES=F")
    
    # Try 1-minute data for current price
    data = ticker.history(period="1d", interval="1m", progress=False)
    
    if not data.empty:
        return data['Close'].iloc[-1]
    
    # Fallback to 1-hour
    data = ticker.history(period="1d", interval="1h", progress=False)
    if not data.empty:
        return data['Close'].iloc[-1]
    
    return None

# ============================================================
# INDICATORS
# ============================================================
def calculate_ema(data, period):
    """Calculate Exponential Moving Average"""
    return data['Close'].ewm(span=period, adjust=False).mean()

def check_signal(data):
    """Check for 9/21 EMA crossover signal"""
    if len(data) < 22:
        return None, {}
    
    ema_fast = calculate_ema(data, EMA_FAST)
    ema_slow = calculate_ema(data, EMA_SLOW)
    
    current_fast = ema_fast.iloc[-1]
    current_slow = ema_slow.iloc[-1]
    prev_fast = ema_fast.iloc[-2]
    prev_slow = ema_slow.iloc[-2]
    
    current_price = data['Close'].iloc[-1]
    
    # Bullish crossover
    if prev_fast <= prev_slow and current_fast > current_slow:
        return 'BUY', {
            'price': round(current_price, 2),
            'ema_fast': round(current_fast, 2),
            'ema_slow': round(current_slow, 2),
            'type': 'CROSSOVER'
        }
    
    # Bearish crossover
    elif prev_fast >= prev_slow and current_fast < current_slow:
        return 'SELL', {
            'price': round(current_price, 2),
            'ema_fast': round(current_fast, 2),
            'ema_slow': round(current_slow, 2),
            'type': 'CROSSOVER'
        }
    
    return None, {}

# ============================================================
# PAPER TRADE MANAGEMENT
# ============================================================
def load_current_position():
    """Load current open position from CSV"""
    if not POSITIONS_FILE.exists():
        return None
    
    df = pd.read_csv(POSITIONS_FILE)
    open_positions = df[df['status'] == 'OPEN']
    
    if open_positions.empty:
        return None
    
    return open_positions.iloc[-1].to_dict()

def save_position(position):
    """Save open position to CSV"""
    df = pd.DataFrame([position])
    
    if POSITIONS_FILE.exists():
        existing = pd.read_csv(POSITIONS_FILE)
        # Mark existing open positions as closed
        existing.loc[existing['status'] == 'OPEN', 'status'] = 'CLOSED'
        updated = pd.concat([existing, df], ignore_index=True)
    else:
        updated = df
    
    updated.to_csv(POSITIONS_FILE, index=False)

def open_paper_trade(signal, price, signal_details):
    """Open a new paper trade"""
    if signal == 'BUY':
        stop_price = price - STOP_POINTS
        target_price = price + TARGET_POINTS
    else:  # SELL
        stop_price = price + STOP_POINTS
        target_price = price - TARGET_POINTS
    
    position = {
        'ticker': 'MES',
        'entry_time': datetime.now().isoformat(),
        'direction': signal,
        'entry_price': round(price, 2),
        'contracts': CONTRACTS,
        'stop_price': round(stop_price, 2),
        'target_price': round(target_price, 2),
        'risk_points': STOP_POINTS,
        'target_points': TARGET_POINTS,
        'risk_dollars': round(STOP_POINTS * POINT_VALUE * CONTRACTS, 2),
        'target_dollars': round(TARGET_POINTS * POINT_VALUE * CONTRACTS, 2),
        'status': 'OPEN',
        'signal_details': json.dumps(signal_details)
    }
    
    save_position(position)
    return position

def check_exit_conditions(position, current_price):
    """Check if position should exit"""
    if position['direction'] == 'BUY':
        # Check stop loss
        if current_price <= position['stop_price']:
            return True, 'STOP_LOSS', position['stop_price']
        # Check take profit
        if current_price >= position['target_price']:
            return True, 'TAKE_PROFIT', position['target_price']
    else:  # SELL
        if current_price >= position['stop_price']:
            return True, 'STOP_LOSS', position['stop_price']
        if current_price <= position['target_price']:
            return True, 'TAKE_PROFIT', position['target_price']
    
    return False, None, None

def close_paper_trade(position, exit_price, exit_reason):
    """Close a paper trade and record results"""
    if position['direction'] == 'BUY':
        profit_points = exit_price - position['entry_price']
    else:
        profit_points = position['entry_price'] - exit_price
    
    profit_dollars = profit_points * POINT_VALUE * position['contracts']
    
    trade_record = {
        'ticker': 'MES',
        'entry_time': position['entry_time'],
        'exit_time': datetime.now().isoformat(),
        'direction': position['direction'],
        'entry_price': position['entry_price'],
        'exit_price': exit_price,
        'contracts': position['contracts'],
        'profit_points': round(profit_points, 2),
        'profit_dollars': round(profit_dollars, 2),
        'exit_reason': exit_reason,
        'stop_price': position['stop_price'],
        'target_price': position['target_price']
    }
    
    # Save to trades history
    trades_df = pd.DataFrame([trade_record])
    
    if TRADES_FILE.exists():
        existing = pd.read_csv(TRADES_FILE)
        updated = pd.concat([existing, trades_df], ignore_index=True)
    else:
        updated = trades_df
    
    updated.to_csv(TRADES_FILE, index=False)
    
    # Update position status
    position['status'] = 'CLOSED'
    position['exit_time'] = datetime.now().isoformat()
    position['exit_price'] = exit_price
    position['exit_reason'] = exit_reason
    
    if POSITIONS_FILE.exists():
        df = pd.read_csv(POSITIONS_FILE)
        for i, row in df.iterrows():
            if row['entry_time'] == position['entry_time']:
                df.loc[i] = pd.Series(position)
                break
        df.to_csv(POSITIONS_FILE, index=False)
    
    return trade_record

# ============================================================
# DASHBOARD DATA
# ============================================================
def update_dashboard_data(current_price, signal, signal_details, position, recent_trades):
    """Update JSON file for dashboard"""
    dashboard_data = {
        'timestamp': datetime.now().isoformat(),
        'current_price': current_price,
        'signal': signal,
        'signal_details': signal_details,
        'active_position': position,
        'recent_trades': recent_trades[:10] if recent_trades else [],
        'system_status': 'ACTIVE'
    }
    
    with open(DASHBOARD_DATA, 'w') as f:
        json.dump(dashboard_data, f, indent=2)

# ============================================================
# EMAIL ALERTS
# ============================================================
def send_trade_alert(signal, position, signal_details):
    """Send email with trade instructions"""
    mail_username = os.environ.get("MAIL_USERNAME")
    mail_password = os.environ.get("MAIL_PASSWORD")
    
    if not mail_username or not mail_password:
        print("❌ Email credentials not found")
        return
    
    date_str = datetime.now().strftime("%Y-%m-%d %H:%M ET")
    
    if signal == 'BUY':
        subject = f"📈 MES BUY SIGNAL - 6 Contracts at {position['entry_price']}"
        action = "BUY 6 MES CONTRACTS"
    else:
        subject = f"📉 MES SELL SIGNAL - 6 Contracts at {position['entry_price']}"
        action = "SELL 6 MES CONTRACTS"
    
    body = f"""
═══════════════════════════════════════════════════════════
  📊 MES PAPER TRADING SYSTEM - TRADE ALERT
═══════════════════════════════════════════════════════════

⏰ TIME: {date_str}
📊 INSTRUMENT: MES (Micro E-mini S&P 500)
💰 ACCOUNT: $100,000 Topstep Evaluation
🎯 STRATEGY: 9/21 EMA Crossover

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

🚨 TRADE SIGNAL: {signal}

   WHAT TO DO: {action}

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

💰 YOUR TRADE LEVELS:

   📍 ENTRY PRICE: ${position['entry_price']:.2f}
   🛑 STOP LOSS: ${position['stop_price']:.2f} (-{STOP_POINTS} points)
   🎯 TAKE PROFIT: ${position['target_price']:.2f} (+{TARGET_POINTS} points)

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

📊 SIGNAL DETAILS:

   Fast EMA (9): {signal_details['ema_fast']:.2f}
   Slow EMA (21): {signal_details['ema_slow']:.2f}
   
   The 9 EMA has crossed {'ABOVE' if signal == 'BUY' else 'BELOW'} the 21 EMA

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

💰 RISK VS REWARD (6 CONTRACTS):

   Risk per trade: ${position['risk_dollars']}
   Reward per trade: ${position['target_dollars']}
   Risk:Reward Ratio: 1:2

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

📋 HOW TO EXECUTE:

   1️⃣ Open your trading platform
   2️⃣ Enter {action}
   3️⃣ Set STOP LOSS at ${position['stop_price']:.2f}
   4️⃣ Set TAKE PROFIT at ${position['target_price']:.2f}
   5️⃣ DO NOT move your stop loss

═══════════════════════════════════════════════════════════
  PAPER TRADE - Auto-logged to CSV
  Dashboard: https://mrpink970.github.io/.../mes_dashboard.html
═══════════════════════════════════════════════════════════
"""
    
    msg = EmailMessage()
    msg.set_content(body)
    msg["Subject"] = subject
    msg["From"] = mail_username
    msg["To"] = mail_username
    
    try:
        with smtplib.SMTP_SSL("smtp.gmail.com", 465) as smtp:
            smtp.login(mail_username, mail_password)
            smtp.send_message(msg)
        print(f"✅ Trade alert sent")
    except Exception as e:
        print(f"❌ Email failed: {e}")

def send_exit_alert(trade_record):
    """Send email when a trade exits"""
    mail_username = os.environ.get("MAIL_USERNAME")
    mail_password = os.environ.get("MAIL_PASSWORD")
    
    if not mail_username or not mail_password:
        return
    
    date_str = datetime.now().strftime("%Y-%m-%d %H:%M ET")
    
    profit_symbol = "+" if trade_record['profit_dollars'] > 0 else ""
    
    subject = f"🔒 MES POSITION CLOSED - {trade_record['exit_reason']} - {profit_symbol}${trade_record['profit_dollars']:.2f}"
    body = f"""
═══════════════════════════════════════════════════════════
  📊 MES PAPER TRADING - POSITION CLOSED
═══════════════════════════════════════════════════════════

⏰ CLOSED: {date_str}
📊 INSTRUMENT: MES
📈 EXIT REASON: {trade_record['exit_reason']}

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

💰 TRADE RESULT:

   Direction: {trade_record['direction']}
   Entry: ${trade_record['entry_price']:.2f}
   Exit: ${trade_record['exit_price']:.2f}
   Contracts: {trade_record['contracts']}
   
   Profit/Loss: {profit_symbol}${trade_record['profit_dollars']:.2f}

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

📈 ACCOUNT SUMMARY:

   Open Positions: None
   Ready for next signal

═══════════════════════════════════════════════════════════
  Dashboard: https://mrpink970.github.io/.../mes_dashboard.html
═══════════════════════════════════════════════════════════
"""
    
    msg = EmailMessage()
    msg.set_content(body)
    msg["Subject"] = subject
    msg["From"] = mail_username
    msg["To"] = mail_username
    
    try:
        with smtplib.SMTP_SSL("smtp.gmail.com", 465) as smtp:
            smtp.login(mail_username, mail_password)
            smtp.send_message(msg)
        print(f"✅ Exit alert sent")
    except Exception as e:
        print(f"❌ Email failed: {e}")

# ============================================================
# PERFORMANCE REPORT
# ============================================================
def get_performance_report():
    """Calculate performance metrics from trades history"""
    if not TRADES_FILE.exists():
        return None
    
    df = pd.read_csv(TRADES_FILE)
    if df.empty:
        return None
    
    total_trades = len(df)
    wins = len(df[df['profit_dollars'] > 0])
    losses = len(df[df['profit_dollars'] < 0])
    
    win_rate = (wins / total_trades * 100) if total_trades > 0 else 0
    total_profit = df['profit_dollars'].sum()
    avg_win = df[df['profit_dollars'] > 0]['profit_dollars'].mean() if wins > 0 else 0
    avg_loss = abs(df[df['profit_dollars'] < 0]['profit_dollars'].mean()) if losses > 0 else 0
    
    # Calculate max drawdown from cumulative equity
    df['cumulative'] = df['profit_dollars'].cumsum()
    running_max = df['cumulative'].cummax()
    df['drawdown'] = running_max - df['cumulative']
    max_drawdown = df['drawdown'].max()
    
    return {
        'total_trades': total_trades,
        'wins': wins,
        'losses': losses,
        'win_rate': round(win_rate, 1),
        'total_profit': round(total_profit, 2),
        'avg_win': round(avg_win, 2),
        'avg_loss': round(avg_loss, 2),
        'max_drawdown': round(max_drawdown, 2),
        'profit_factor': round(abs(total_profit / df[df['profit_dollars'] < 0]['profit_dollars'].sum()), 2) if losses > 0 else 0
    }

# ============================================================
# MAIN LOOP
# ============================================================
def main():
    print("=" * 60)
    print("MES PAPER TRADING SYSTEM")
    print(f"6 Contracts | {STOP_POINTS}pt Stop | {TARGET_POINTS}pt Target")
    print("=" * 60)
    
    # Get current price
    current_price = get_current_price()
    if current_price is None:
        print("❌ Could not fetch MES price. Check ticker MES=F")
        return
    
    print(f"\n📊 Current MES price: ${current_price:.2f}")
    print(f"   Time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    
    # Check existing position
    existing_position = load_current_position()
    
    # If we have an open position, check exit conditions
    if existing_position:
        print(f"\n📌 Open position: {existing_position['direction']} at ${existing_position['entry_price']:.2f}")
        
        should_exit, exit_reason, exit_price = check_exit_conditions(existing_position, current_price)
        
        if should_exit:
            trade_record = close_paper_trade(existing_position, exit_price, exit_reason)
            print(f"\n🔴 POSITION CLOSED: {exit_reason}")
            print(f"   Profit: ${trade_record['profit_dollars']:.2f}")
            send_exit_alert(trade_record)
        else:
            print(f"\n✅ Position still active")
            print(f"   Stop: ${existing_position['stop_price']:.2f}")
            print(f"   Target: ${existing_position['target_price']:.2f}")
    
    # Check for new signals
    else:
        print("\n🔍 Checking for EMA crossover signal...")
        data = get_mes_data()
        
        if data is not None and not data.empty:
            signal, signal_details = check_signal(data)
            
            if signal:
                print(f"\n🎯 SIGNAL DETECTED: {signal}")
                print(f"   Fast EMA (9): {signal_details['ema_fast']:.2f}")
                print(f"   Slow EMA (21): {signal_details['ema_slow']:.2f}")
                
                position = open_paper_trade(signal, signal_details['price'], signal_details)
                
                print(f"\n📈 PAPER TRADE OPENED:")
                print(f"   Direction: {signal}")
                print(f"   Entry: ${position['entry_price']:.2f}")
                print(f"   Stop: ${position['stop_price']:.2f}")
                print(f"   Target: ${position['target_price']:.2f}")
                print(f"   Risk: ${position['risk_dollars']} | Reward: ${position['target_dollars']}")
                
                send_trade_alert(signal, position, signal_details)
            else:
                print("\n🔍 No signal - waiting for EMA crossover")
        else:
            print("\n❌ Could not fetch historical data")
    
    # Get and show performance report
    performance = get_performance_report()
    if performance:
        print(f"\n📊 PERFORMANCE SUMMARY:")
        print(f"   Total Trades: {performance['total_trades']}")
        print(f"   Win Rate: {performance['win_rate']}%")
        print(f"   Total Profit: ${performance['total_profit']:.2f}")
        print(f"   Max Drawdown: ${performance['max_drawdown']:.2f}")
    
    # Update dashboard data
    recent_trades = None
    if TRADES_FILE.exists():
        df = pd.read_csv(TRADES_FILE)
        recent_trades = df.tail(10).to_dict('records') if not df.empty else None
    
    update_dashboard_data(
        current_price=float(current_price),
        signal=signal if 'signal' in locals() else None,
        signal_details=signal_details if 'signal_details' in locals() else None,
        position=existing_position,
        recent_trades=recent_trades
    )
    
    print("\n" + "=" * 60)
    print("✅ System run complete")

if __name__ == "__main__":
    main()
