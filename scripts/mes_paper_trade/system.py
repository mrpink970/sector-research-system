#!/usr/bin/env python3
"""
MES Automated Paper Trading System - SIMPLIFIED
Uses ES=F (which tracks MES price, just 10x multiplier)
"""

import os
import yfinance as yf
import pandas as pd
from pathlib import Path
from datetime import datetime
import smtplib
from email.message import EmailMessage
import json

# ============================================================
# CONFIGURATION
# ============================================================
DATA_DIR = Path("data/mes_paper")
POSITIONS_FILE = DATA_DIR / "positions.csv"
TRADES_FILE = DATA_DIR / "trades.csv"
DASHBOARD_DATA = DATA_DIR / "dashboard_data.json"

os.makedirs(DATA_DIR, exist_ok=True)

# System Parameters
CONTRACTS = 6
STOP_POINTS = 8.0
TARGET_POINTS = 16.0
POINT_VALUE = 5.0  # MES is $5 per point

# EMA Parameters
EMA_FAST = 9
EMA_SLOW = 21

# ============================================================
# DATA FETCHING - Using ES=F (reliable, tracks same price)
# ============================================================
def get_mes_price():
    """Get current MES price using ES=F (ES price = MES price)"""
    try:
        ticker = yf.Ticker("ES=F")
        data = ticker.history(period="1d", interval="5m")
        if not data.empty:
            return round(float(data['Close'].iloc[-1]), 2)
        return None
    except Exception as e:
        print(f"Error fetching price: {e}")
        return None

def get_historical_data():
    """Get historical 1-hour data for EMA calculation"""
    try:
        # Using ES=F - reliable data source
        data = yf.download("ES=F", period="5d", interval="1h")
        if data.empty:
            return None
        
        # Clean up - remove timezone info and NaN
        data = data.dropna()
        
        return data
    except Exception as e:
        print(f"Error fetching historical data: {e}")
        return None

# ============================================================
# INDICATORS - Simple EMA calculation
# ============================================================
def calculate_ema(prices, period):
    """Calculate EMA from a list of prices"""
    if len(prices) < period:
        return None
    
    # Simple EMA calculation
    multiplier = 2 / (period + 1)
    ema = prices[0]
    
    for price in prices[1:]:
        ema = (price - ema) * multiplier + ema
    
    return round(ema, 2)

def check_signal(data):
    """Check for 9/21 EMA crossover using simple lists"""
    if len(data) < 25:
        return None, {}
    
    # Get closing prices as a list
    closes = data['Close'].tolist()
    
    # Calculate EMAs for last few points
    ema_9_values = []
    ema_21_values = []
    
    for i in range(1, len(closes) + 1):
        window = closes[:i]
        if len(window) >= EMA_FAST:
            ema_9 = calculate_ema(window[-EMA_FAST:], EMA_FAST)
            if ema_9 is not None:
                ema_9_values.append(ema_9)
        
        if len(window) >= EMA_SLOW:
            ema_21 = calculate_ema(window[-EMA_SLOW:], EMA_SLOW)
            if ema_21 is not None:
                ema_21_values.append(ema_21)
    
    # Need at least 2 values for each to detect crossover
    if len(ema_9_values) < 2 or len(ema_21_values) < 2:
        return None, {}
    
    # Current and previous values
    current_fast = ema_9_values[-1]
    current_slow = ema_21_values[-1]
    prev_fast = ema_9_values[-2]
    prev_slow = ema_21_values[-2]
    
    current_price = round(closes[-1], 2)
    
    # Detect crossover
    if prev_fast <= prev_slow and current_fast > current_slow:
        return 'BUY', {
            'price': current_price,
            'ema_fast': current_fast,
            'ema_slow': current_slow
        }
    elif prev_fast >= prev_slow and current_fast < current_slow:
        return 'SELL', {
            'price': current_price,
            'ema_fast': current_fast,
            'ema_slow': current_slow
        }
    
    return None, {}

# ============================================================
# PAPER TRADE MANAGEMENT
# ============================================================
def load_current_position():
    """Load current open position"""
    if not POSITIONS_FILE.exists():
        return None
    
    df = pd.read_csv(POSITIONS_FILE)
    open_positions = df[df['status'] == 'OPEN']
    
    if open_positions.empty:
        return None
    
    return open_positions.iloc[-1].to_dict()

def save_position(position):
    """Save open position"""
    df = pd.DataFrame([position])
    
    if POSITIONS_FILE.exists():
        existing = pd.read_csv(POSITIONS_FILE)
        existing.loc[existing['status'] == 'OPEN', 'status'] = 'CLOSED'
        updated = pd.concat([existing, df], ignore_index=True)
    else:
        updated = df
    
    updated.to_csv(POSITIONS_FILE, index=False)

def open_paper_trade(signal, price):
    """Open a new paper trade"""
    if signal == 'BUY':
        stop_price = price - STOP_POINTS
        target_price = price + TARGET_POINTS
    else:
        stop_price = price + STOP_POINTS
        target_price = price - TARGET_POINTS
    
    position = {
        'ticker': 'MES',
        'entry_time': datetime.now().isoformat(),
        'direction': signal,
        'entry_price': price,
        'contracts': CONTRACTS,
        'stop_price': round(stop_price, 2),
        'target_price': round(target_price, 2),
        'risk_points': STOP_POINTS,
        'target_points': TARGET_POINTS,
        'risk_dollars': round(STOP_POINTS * POINT_VALUE * CONTRACTS, 2),
        'target_dollars': round(TARGET_POINTS * POINT_VALUE * CONTRACTS, 2),
        'status': 'OPEN'
    }
    
    save_position(position)
    return position

def check_exit_conditions(position, current_price):
    """Check if position should exit"""
    entry = float(position['entry_price'])
    stop = float(position['stop_price'])
    target = float(position['target_price'])
    direction = position['direction']
    
    if direction == 'BUY':
        if current_price <= stop:
            return True, 'STOP_LOSS', stop
        if current_price >= target:
            return True, 'TAKE_PROFIT', target
    else:
        if current_price >= stop:
            return True, 'STOP_LOSS', stop
        if current_price <= target:
            return True, 'TAKE_PROFIT', target
    
    return False, None, None

def close_paper_trade(position, exit_price, exit_reason):
    """Close a paper trade"""
    if position['direction'] == 'BUY':
        profit_points = exit_price - position['entry_price']
    else:
        profit_points = position['entry_price'] - exit_price
    
    profit_dollars = profit_points * POINT_VALUE * position['contracts']
    
    trade_record = {
        'entry_time': position['entry_time'],
        'exit_time': datetime.now().isoformat(),
        'direction': position['direction'],
        'entry_price': position['entry_price'],
        'exit_price': exit_price,
        'contracts': position['contracts'],
        'profit_dollars': round(profit_dollars, 2),
        'exit_reason': exit_reason
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
# EMAIL ALERTS
# ============================================================
def send_email(subject, body):
    """Send email notification"""
    mail_username = os.environ.get("MAIL_USERNAME")
    mail_password = os.environ.get("MAIL_PASSWORD")
    
    if not mail_username or not mail_password:
        print("❌ Email credentials not found")
        return
    
    msg = EmailMessage()
    msg.set_content(body)
    msg["Subject"] = subject
    msg["From"] = mail_username
    msg["To"] = mail_username
    
    try:
        with smtplib.SMTP_SSL("smtp.gmail.com", 465) as smtp:
            smtp.login(mail_username, mail_password)
            smtp.send_message(msg)
        print(f"✅ Email sent")
    except Exception as e:
        print(f"❌ Email failed: {e}")

def send_trade_alert(signal, position):
    """Send trade alert email"""
    date_str = datetime.now().strftime("%Y-%m-%d %H:%M ET")
    
    if signal == 'BUY':
        subject = f"📈 MES BUY SIGNAL - 6 Contracts at {position['entry_price']}"
        action = "BUY 6 MES CONTRACTS"
    else:
        subject = f"📉 MES SELL SIGNAL - 6 Contracts at {position['entry_price']}"
        action = "SELL 6 MES CONTRACTS"
    
    body = f"""
MES TRADE SIGNAL - {date_str}

ACTION: {action}
ENTRY: ${position['entry_price']}
STOP LOSS: ${position['stop_price']} (-{STOP_POINTS} pts)
TAKE PROFIT: ${position['target_price']} (+{TARGET_POINTS} pts)

RISK: ${position['risk_dollars']}
REWARD: ${position['target_dollars']}

Execute now. Do not move stop loss.
"""
    
    send_email(subject, body)

def send_exit_alert(trade_record):
    """Send exit alert email"""
    date_str = datetime.now().strftime("%Y-%m-%d %H:%M ET")
    profit_symbol = "+" if trade_record['profit_dollars'] > 0 else ""
    
    subject = f"🔒 MES CLOSED - {trade_record['exit_reason']} - {profit_symbol}${trade_record['profit_dollars']:.2f}"
    body = f"""
MES POSITION CLOSED - {date_str}

Exit Reason: {trade_record['exit_reason']}
Direction: {trade_record['direction']}
Entry: ${trade_record['entry_price']}
Exit: ${trade_record['exit_price']}
Profit: {profit_symbol}${trade_record['profit_dollars']:.2f}
"""
    
    send_email(subject, body)

# ============================================================
# PERFORMANCE REPORT
# ============================================================
def get_performance_report():
    """Calculate performance metrics"""
    if not TRADES_FILE.exists():
        return None
    
    df = pd.read_csv(TRADES_FILE)
    if df.empty:
        return None
    
    total_trades = len(df)
    wins = len(df[df['profit_dollars'] > 0])
    total_profit = df['profit_dollars'].sum()
    win_rate = (wins / total_trades * 100) if total_trades > 0 else 0
    
    # Calculate max drawdown
    df['cumulative'] = df['profit_dollars'].cumsum()
    running_max = df['cumulative'].cummax()
    df['drawdown'] = running_max - df['cumulative']
    max_drawdown = df['drawdown'].max()
    
    return {
        'total_trades': total_trades,
        'win_rate': round(win_rate, 1),
        'total_profit': round(total_profit, 2),
        'max_drawdown': round(max_drawdown, 2)
    }

# ============================================================
# DASHBOARD DATA
# ============================================================
def update_dashboard(current_price, signal, position, recent_trades, performance):
    """Update dashboard JSON"""
    dashboard_data = {
        'timestamp': datetime.now().isoformat(),
        'current_price': current_price,
        'signal': signal,
        'active_position': position,
        'recent_trades': recent_trades[:10] if recent_trades else [],
        'performance': performance
    }
    
    with open(DASHBOARD_DATA, 'w') as f:
        json.dump(dashboard_data, f, indent=2)

# ============================================================
# MAIN
# ============================================================
def main():
    print("=" * 60)
    print("MES PAPER TRADING SYSTEM")
    print(f"{CONTRACTS} Contracts | {STOP_POINTS}pt Stop | {TARGET_POINTS}pt Target")
    print("=" * 60)
    
    # Get current price
    current_price = get_mes_price()
    if current_price is None:
        print("❌ Could not fetch MES price")
        return
    
    print(f"\n📊 Current MES price: ${current_price:.2f}")
    print(f"   Time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    
    # Check existing position
    existing_position = load_current_position()
    
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
        data = get_historical_data()
        
        if data is not None and not data.empty:
            signal, signal_details = check_signal(data)
            
            if signal:
                print(f"\n🎯 SIGNAL DETECTED: {signal}")
                print(f"   Fast EMA (9): {signal_details['ema_fast']:.2f}")
                print(f"   Slow EMA (21): {signal_details['ema_slow']:.2f}")
                
                position = open_paper_trade(signal, signal_details['price'])
                
                print(f"\n📈 PAPER TRADE OPENED:")
                print(f"   Direction: {signal}")
                print(f"   Entry: ${position['entry_price']:.2f}")
                print(f"   Stop: ${position['stop_price']:.2f}")
                print(f"   Target: ${position['target_price']:.2f}")
                print(f"   Risk: ${position['risk_dollars']} | Reward: ${position['target_dollars']}")
                
                send_trade_alert(signal, position)
            else:
                print("\n🔍 No signal - waiting for EMA crossover")
        else:
            print("\n❌ Could not fetch historical data")
    
    # Get performance
    performance = get_performance_report()
    if performance:
        print(f"\n📊 PERFORMANCE:")
        print(f"   Trades: {performance['total_trades']} | Win Rate: {performance['win_rate']}%")
        print(f"   Total P&L: ${performance['total_profit']:.2f} | Max DD: ${performance['max_drawdown']:.2f}")
    
    # Update dashboard
    recent_trades = None
    if TRADES_FILE.exists():
        df = pd.read_csv(TRADES_FILE)
        recent_trades = df.tail(10).to_dict('records') if not df.empty else None
    
    update_dashboard(
        current_price=current_price,
        signal=signal if 'signal' in locals() else None,
        position=existing_position,
        recent_trades=recent_trades,
        performance=performance
    )
    
    print("\n" + "=" * 60)
    print("✅ System run complete")

if __name__ == "__main__":
    main()
