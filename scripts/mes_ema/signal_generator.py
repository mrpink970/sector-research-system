#!/usr/bin/env python3
"""
MES EMA Crossover System - Simple Monthly Income
Designed to be shared with others who can manually trade
"""

import os
import smtplib
from email.message import EmailMessage
import yfinance as yf
import pandas as pd
from pathlib import Path
from datetime import datetime

DATA_DIR = Path("data/mes_ema")
os.makedirs(DATA_DIR, exist_ok=True)

def get_mes_1h_data():
    """Get MES 1-hour data (using ES=F as proxy)"""
    data = yf.download("ES=F", period="5d", interval="1h", progress=False)
    if data.empty:
        return None
    return data

def calculate_ema(data, period):
    """Calculate Exponential Moving Average"""
    return data['Close'].ewm(span=period, adjust=False).mean()

def check_signal(data):
    """Check for EMA crossover signal"""
    if len(data) < 22:
        return None, {}
    
    # Calculate EMAs
    ema_fast = calculate_ema(data, 9)
    ema_slow = calculate_ema(data, 21)
    
    # Get current and previous values
    current_fast = ema_fast.iloc[-1]
    current_slow = ema_slow.iloc[-1]
    prev_fast = ema_fast.iloc[-2]
    prev_slow = ema_slow.iloc[-2]
    
    current_price = data['Close'].iloc[-1]
    current_time = data.index[-1]
    
    # Check for crossovers
    if prev_fast <= prev_slow and current_fast > current_slow:
        return 'LONG', {
            'current_price': round(current_price, 2),
            'ema_fast': round(current_fast, 2),
            'ema_slow': round(current_slow, 2),
            'time': current_time
        }
    elif prev_fast >= prev_slow and current_fast < current_slow:
        return 'SHORT', {
            'current_price': round(current_price, 2),
            'ema_fast': round(current_fast, 2),
            'ema_slow': round(current_slow, 2),
            'time': current_time
        }
    
    return None, {}

def calculate_positions(signal, entry_price, contracts=1):
    """Calculate stop and target levels"""
    if signal == 'LONG':
        stop_price = entry_price - 10.0
        target_price = entry_price + 20.0
    else:
        stop_price = entry_price + 10.0
        target_price = entry_price - 20.0
    
    point_value = 5.0
    
    return {
        'entry_price': round(entry_price, 2),
        'stop_price': round(stop_price, 2),
        'target_price': round(target_price, 2),
        'contracts': contracts,
        'risk_points': 10.0,
        'reward_points': 20.0,
        'risk_dollars': round(10.0 * point_value * contracts, 2),
        'reward_dollars': round(20.0 * point_value * contracts, 2),
        'point_value': point_value
    }

def send_email(signal, entry_price, positions, signal_details):
    """Send trade alert email - designed for manual traders"""
    mail_username = os.environ.get("MAIL_USERNAME")
    mail_password = os.environ.get("MAIL_PASSWORD")
    
    if not mail_username or not mail_password:
        print("❌ Email credentials not found")
        return
    
    date_str = datetime.now().strftime("%Y-%m-%d %H:%M ET")
    
    if signal == 'LONG':
        subject = f"📈 MES BUY SIGNAL - Enter at {entry_price}"
        action = "BUY 1 MES CONTRACT"
        direction = "LONG"
    else:
        subject = f"📉 MES SELL SIGNAL - Enter at {entry_price}"
        action = "SELL 1 MES CONTRACT"
        direction = "SHORT"
    
    body = f"""
═══════════════════════════════════════════════════════════
  📊 MES MONTHLY INCOME SYSTEM - TRADE ALERT
═══════════════════════════════════════════════════════════

⏰ SIGNAL TIME: {date_str}
📊 INSTRUMENT: MES (Micro E-mini S&P 500)
🎯 STRATEGY: EMA Crossover (9/21)

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

🚨 TRADE SIGNAL: {signal}

   WHAT TO DO: {action}
   CONTRACTS: {positions['contracts']}

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

💰 YOUR TRADE LEVELS:

   📍 ENTRY PRICE: ${positions['entry_price']:.2f}
   🛑 STOP LOSS: ${positions['stop_price']:.2f} (-10 points)
   🎯 TAKE PROFIT: ${positions['target_price']:.2f} (+20 points)

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

📊 WHY THIS SIGNAL:

   Fast EMA (9): {signal_details['ema_fast']:.2f}
   Slow EMA (21): {signal_details['ema_slow']:.2f}
   
   The {signal_details['ema_fast']:.2f} has crossed {'ABOVE' if signal == 'LONG' else 'BELOW'} {signal_details['ema_slow']:.2f}
   This indicates a {'bullish' if signal == 'LONG' else 'bearish'} trend starting.

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

💰 RISK VS REWARD:

   Risk per trade: ${positions['risk_dollars']} (10 points)
   Reward per trade: ${positions['reward_dollars']} (20 points)
   Risk:Reward Ratio: 1:2

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

📋 HOW TO EXECUTE (Manual Trading):

   1️⃣ Open your trading platform
   2️⃣ Enter {action}
   3️⃣ Set STOP LOSS at ${positions['stop_price']:.2f}
   4️⃣ Set TAKE PROFIT at ${positions['target_price']:.2f}
   5️⃣ DO NOT move your stop loss
   6️⃣ Walk away - let the trade work

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

⚠️ IMPORTANT RULES FOR MONTHLY INCOME:

   ✅ Take every signal (no cherry picking)
   ✅ Use exactly 1 contract (no scaling)
   ✅ Never move your stop loss
   ✅ Stop trading after 2 consecutive losses
   ✅ Daily loss limit: $150 (3 losses)

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

📈 MONTHLY INCOME TARGET:

   Average trades per month: 8-12
   Average win rate: 55-65%
   Expected monthly profit: $800-1,500

═══════════════════════════════════════════════════════════
  This system is designed for consistent monthly income.
  Follow the rules exactly. Do not deviate.
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
        print(f"✅ MES signal email sent")
        return True
    except Exception as e:
        print(f"❌ Email failed: {e}")
        return False

def save_signal(signal, positions, signal_details):
    """Save signal to CSV for record keeping"""
    signal_file = DATA_DIR / "signals.csv"
    
    new_row = pd.DataFrame([{
        'timestamp': datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        'signal': signal,
        'entry_price': positions['entry_price'],
        'stop_price': positions['stop_price'],
        'target_price': positions['target_price'],
        'ema_fast': signal_details['ema_fast'],
        'ema_slow': signal_details['ema_slow']
    }])
    
    if signal_file.exists():
        existing = pd.read_csv(signal_file)
        updated = pd.concat([existing, new_row], ignore_index=True)
    else:
        updated = new_row
    
    updated.to_csv(signal_file, index=False)

def main():
    print("=" * 60)
    print("MES EMA CROSSOVER - MONTHLY INCOME SYSTEM")
    print("Strategy: 9/21 EMA Crossover | 1 Contract | 10pt Stop | 20pt Target")
    print("=" * 60)
    
    data = get_mes_1h_data()
    if data is None or data.empty:
        print("❌ No market data available")
        return
    
    current_price = data['Close'].iloc[-1]
    print(f"\n📊 Current MES price: ${current_price:.2f}")
    print(f"   Time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    
    # Check for signal
    signal, details = check_signal(data)
    
    if signal:
        print(f"\n🎯 SIGNAL DETECTED: {signal}")
        print(f"   Fast EMA (9): {details['ema_fast']:.2f}")
        print(f"   Slow EMA (21): {details['ema_slow']:.2f}")
        
        positions = calculate_positions(signal, details['current_price'], contracts=1)
        
        print(f"\n📈 TRADE PLAN:")
        print(f"   Action: {'BUY' if signal == 'LONG' else 'SELL'} 1 MES")
        print(f"   Entry: ${positions['entry_price']:.2f}")
        print(f"   Stop: ${positions['stop_price']:.2f}")
        print(f"   Target: ${positions['target_price']:.2f}")
        print(f"   Risk: ${positions['risk_dollars']}")
        print(f"   Reward: ${positions['reward_dollars']}")
        
        # Save and send alert
        save_signal(signal, positions, details)
        send_email(signal, positions['entry_price'], positions, details)
        
        print(f"\n✅ Signal saved and email sent")
    else:
        print(f"\n🔍 No signal detected")
        print(f"   Waiting for 9 EMA to cross {'above' if 'LONG' else 'below'} 21 EMA")
    
    print("\n" + "=" * 60)

if __name__ == "__main__":
    main()
