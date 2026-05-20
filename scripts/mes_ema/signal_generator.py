#!/usr/bin/env python3
"""
MES EMA Crossover - 6 Contract Design Phase
Topstep $100K Compatible
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

CONTRACTS = 6
POINT_VALUE = 5.0
STOP_POINTS = 8.0
TARGET_POINTS = 16.0

def get_mes_1h_data():
    data = yf.download("ES=F", period="5d", interval="1h", progress=False)
    if data.empty:
        return None
    return data

def calculate_ema(data, period):
    return data['Close'].ewm(span=period, adjust=False).mean()

def check_signal(data):
    if len(data) < 22:
        return None, {}
    
    ema_fast = calculate_ema(data, 9)
    ema_slow = calculate_ema(data, 21)
    
    current_fast = ema_fast.iloc[-1]
    current_slow = ema_slow.iloc[-1]
    prev_fast = ema_fast.iloc[-2]
    prev_slow = ema_slow.iloc[-2]
    
    current_price = data['Close'].iloc[-1]
    
    if prev_fast <= prev_slow and current_fast > current_slow:
        return 'LONG', {
            'current_price': round(current_price, 2),
            'ema_fast': round(current_fast, 2),
            'ema_slow': round(current_slow, 2)
        }
    elif prev_fast >= prev_slow and current_fast < current_slow:
        return 'SHORT', {
            'current_price': round(current_price, 2),
            'ema_fast': round(current_fast, 2),
            'ema_slow': round(current_slow, 2)
        }
    
    return None, {}

def calculate_positions(signal, entry_price):
    if signal == 'LONG':
        stop_price = entry_price - STOP_POINTS
        target_price = entry_price + TARGET_POINTS
    else:
        stop_price = entry_price + STOP_POINTS
        target_price = entry_price - TARGET_POINTS
    
    risk_dollars = STOP_POINTS * POINT_VALUE * CONTRACTS
    reward_dollars = TARGET_POINTS * POINT_VALUE * CONTRACTS
    
    return {
        'entry_price': round(entry_price, 2),
        'stop_price': round(stop_price, 2),
        'target_price': round(target_price, 2),
        'contracts': CONTRACTS,
        'stop_points': STOP_POINTS,
        'target_points': TARGET_POINTS,
        'risk_dollars': round(risk_dollars, 2),
        'reward_dollars': round(reward_dollars, 2)
    }

def send_email(signal, entry_price, positions, signal_details):
    mail_username = os.environ.get("MAIL_USERNAME")
    mail_password = os.environ.get("MAIL_PASSWORD")
    
    if not mail_username or not mail_password:
        print("❌ Email credentials not found")
        return False
    
    date_str = datetime.now().strftime("%Y-%m-%d %H:%M ET")
    
    if signal == 'LONG':
        subject = f"📈 MES BUY SIGNAL - 6 Contracts at {entry_price}"
        action = "BUY 6 MES CONTRACTS"
    else:
        subject = f"📉 MES SELL SIGNAL - 6 Contracts at {entry_price}"
        action = "SELL 6 MES CONTRACTS"
    
    body = f"""
═══════════════════════════════════════════════════════════
  📊 MES EMA CROSSOVER - 6 CONTRACT DESIGN PHASE
═══════════════════════════════════════════════════════════

⏰ SIGNAL TIME: {date_str}
📊 INSTRUMENT: MES (6 contracts)
💰 ACCOUNT: $100,000 Topstep Evaluation

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

🚨 TRADE SIGNAL: {signal}

   {action}

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

💰 TRADE LEVELS:

   ENTRY: ${positions['entry_price']:.2f}
   STOP: ${positions['stop_price']:.2f} (-{STOP_POINTS} pts)
   TARGET: ${positions['target_price']:.2f} (+{TARGET_POINTS} pts)

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

📊 SIGNAL:

   9 EMA ({signal_details['ema_fast']:.2f}) crossed {'ABOVE' if signal == 'LONG' else 'BELOW'} 
   21 EMA ({signal_details['ema_slow']:.2f})

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

💰 RISK/REWARD (6 contracts):

   RISK: ${positions['risk_dollars']}
   REWARD: ${positions['reward_dollars']}
   RATIO: 1:2

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

⚠️ DESIGN PHASE RULES:

   ✅ Take every signal (collect data)
   ✅ Track win rate and consecutive losses
   ✅ Do NOT scale up until backtest validated
   ✅ Stop after 3 consecutive losses

📈 EXPECTED (once validated):

   55-65% win rate → $1,200-2,100/month

═══════════════════════════════════════════════════════════
  DESIGN PHASE - Collecting data for validation
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
        print(f"✅ Email sent")
        return True
    except Exception as e:
        print(f"❌ Email failed: {e}")
        return False

def save_signal(signal, positions, signal_details):
    signal_file = DATA_DIR / "signals.csv"
    
    new_row = pd.DataFrame([{
        'timestamp': datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        'signal': signal,
        'entry_price': positions['entry_price'],
        'stop_price': positions['stop_price'],
        'target_price': positions['target_price'],
        'contracts': CONTRACTS,
        'risk_dollars': positions['risk_dollars'],
        'reward_dollars': positions['reward_dollars']
    }])
    
    if signal_file.exists():
        existing = pd.read_csv(signal_file)
        updated = pd.concat([existing, new_row], ignore_index=True)
    else:
        updated = new_row
    
    updated.to_csv(signal_file, index=False)

def main():
    print("=" * 60)
    print("MES EMA CROSSOVER - DESIGN PHASE")
    print(f"6 Contracts | {STOP_POINTS}pt Stop | {TARGET_POINTS}pt Target")
    print("=" * 60)
    
    data = get_mes_1h_data()
    if data is None or data.empty:
        print("❌ No market data")
        return
    
    current_price = data['Close'].iloc[-1]
    print(f"\n📊 Current MES: ${current_price:.2f}")
    
    signal, details = check_signal(data)
    
    if signal:
        print(f"\n🎯 {signal} SIGNAL")
        positions = calculate_positions(signal, details['current_price'])
        
        print(f"\n📈 TRADE:")
        print(f"   {'BUY' if signal == 'LONG' else 'SELL'} {CONTRACTS} MES @ ${positions['entry_price']:.2f}")
        print(f"   Stop: ${positions['stop_price']:.2f} | Target: ${positions['target_price']:.2f}")
        print(f"   Risk: ${positions['risk_dollars']} | Reward: ${positions['reward_dollars']}")
        
        save_signal(signal, positions, details)
        send_email(signal, positions['entry_price'], positions, details)
        print("\n✅ Signal saved and emailed")
    else:
        print("\n🔍 No signal - waiting for EMA crossover")
    
    print("\n" + "=" * 60)

if __name__ == "__main__":
    main()
