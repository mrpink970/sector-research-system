#!/usr/bin/env python3
"""
Trade The Pool - SOXX Signal Generator (with Email)
Determines Green Day status based on: 1h return, MA20, RSI
Volume requirement removed
"""

import os
import smtplib
from email.message import EmailMessage
import pandas as pd
import yaml
from pathlib import Path
from datetime import datetime

# Paths
CONFIG_PATH = Path("config/ttp_config.yaml")
DATA_DIR = Path("data/ttp")
SIGNALS_PATH = DATA_DIR / "signals.csv"


def load_config():
    with open(CONFIG_PATH, 'r') as f:
        return yaml.safe_load(f)


def load_latest_data():
    """Load the most recent price data"""
    log_path = DATA_DIR / "price_log.csv"
    if not log_path.exists():
        return None
    
    df = pd.read_csv(log_path)
    if df.empty:
        return None
    
    return df.iloc[-1].to_dict()


def check_green_day(data: dict, config: dict) -> tuple[bool, list]:
    """Check if conditions meet Green Day criteria"""
    conditions = []
    all_met = True
    
    # Check 1-hour return
    min_return = config['entry_conditions']['min_1h_return']
    if data['return_1h_pct'] >= min_return:
        conditions.append(f"✅ 1h return: {data['return_1h_pct']:.2f}% >= {min_return}%")
    else:
        conditions.append(f"❌ 1h return: {data['return_1h_pct']:.2f}% < {min_return}%")
        all_met = False
    
    # Check MA20
    if config['entry_conditions']['above_ma20']:
        if data['above_ma20']:
            conditions.append(f"✅ Above MA20 (${data['ma20']:.2f})")
        else:
            conditions.append(f"❌ Below MA20 (${data['ma20']:.2f})")
            all_met = False
    
    # Check RSI
    rsi_min = config['entry_conditions']['rsi_min']
    if data['rsi'] >= rsi_min:
        conditions.append(f"✅ RSI: {data['rsi']:.1f} >= {rsi_min}")
    else:
        conditions.append(f"❌ RSI: {data['rsi']:.1f} < {rsi_min}")
        all_met = False
    
    # Volume check removed
    
    return all_met, conditions


def check_fast_breakout(data: dict, recent_data: list) -> tuple[bool, dict]:
    """
    Check for fast breakout on 15-minute timeframe
    Catches moves that happen too quickly for 1-hour checks
    """
    if not recent_data or len(recent_data) < 3:
        return False, {}
    
    current_price = data['price']
    
    # Get recent 15-minute high/low (last 3 data points)
    recent_high = max([d['price'] for d in recent_data[-3:]])
    recent_low = min([d['price'] for d in recent_data[-3:]])
    
    # Fast breakout conditions (volume no longer required)
    breakout_up = current_price > recent_high * 1.005  # 0.5% above recent high
    momentum = data['rsi'] > 60
    above_ma20 = data['above_ma20']
    
    is_fast_long = breakout_up and momentum and above_ma20
    
    details = {
        'recent_high': round(recent_high, 2),
        'recent_low': round(recent_low, 2),
        'breakout_up': breakout_up,
        'momentum_met': momentum,
    }
    
    return is_fast_long, details


def load_recent_data_points() -> list:
    """Load recent data points for fast breakout detection"""
    log_path = DATA_DIR / "price_log.csv"
    if not log_path.exists():
        return []
    
    df = pd.read_csv(log_path)
    if df.empty:
        return []
    
    # Get last 2 hours of data (approx 8 data points for 15-min intervals)
    recent = df.tail(8).to_dict('records')
    return recent


def calculate_positions(data: dict, config: dict, is_fast: bool = False) -> dict:
    """Calculate entry, stop, and target prices"""
    price = data['price']
    shares = config['trade_management']['shares_per_trade']
    
    if is_fast:
        # Tighter stops for fast breakout trades
        stop_pct = 1.5  # 1.5% stop instead of 2%
        target_pct = 3.0  # 3% target instead of 6%
    else:
        stop_pct = config['exit_rules']['stop_loss_pct']
        target_pct = config['exit_rules']['take_profit_pct']
    
    stop_price = round(price * (1 - stop_pct / 100), 2)
    target_price = round(price * (1 + target_pct / 100), 2)
    
    profit_per_share = target_price - price
    total_profit = profit_per_share * shares
    
    commission_per_share = config['commission']['per_share']
    min_commission = config['commission']['min_per_order']
    commission = max(min_commission, shares * commission_per_share) * 2
    
    net_profit = total_profit - commission
    
    return {
        'entry_price': price,
        'stop_price': stop_price,
        'target_price': target_price,
        'shares': shares,
        'gross_profit': round(total_profit, 2),
        'commission': round(commission, 2),
        'net_profit': round(net_profit, 2),
        'stop_loss_pct': stop_pct,
        'target_pct': target_pct
    }


def save_signal(data: dict, is_green: bool, conditions: list, positions: dict, is_fast: bool = False):
    """Save signal to CSV"""
    signal_type = 'GREEN' if is_green else 'RED'
    if is_fast and is_green:
        signal_type = 'FAST_BREAKOUT'
    
    new_row = pd.DataFrame([{
        'timestamp': datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        'session': data['session'],
        'signal': signal_type,
        'price': data['price'],
        'stop_price': positions['stop_price'],
        'target_price': positions['target_price'],
        'shares': positions['shares'],
        'net_profit': positions['net_profit'],
        'conditions': ' | '.join(conditions)
    }])
    
    if SIGNALS_PATH.exists():
        existing = pd.read_csv(SIGNALS_PATH)
        updated = pd.concat([existing, new_row], ignore_index=True)
    else:
        updated = new_row
    
    updated.to_csv(SIGNALS_PATH, index=False)


def send_email(is_green: bool, data: dict, conditions: list, positions: dict, is_fast: bool = False):
    """Send email notification"""
    mail_username = os.environ.get("MAIL_USERNAME")
    mail_password = os.environ.get("MAIL_PASSWORD")
    
    if not mail_username or not mail_password:
        print("❌ Email credentials not found")
        return
    
    date_str = datetime.now().strftime("%Y-%m-%d %H:%M ET")
    dashboard_url = "https://mrpink970.github.io/sector-research-system/docs/ttp/ttp_dashboard.html"
    trade_entry_url = "https://mrpink970.github.io/sector-research-system/docs/ttp/trade_entry.html"
    
    if is_fast and is_green:
        subject = f"⚡ FAST BREAKOUT - BUY SOXX NOW ({date_str})"
        body = f"""
═══════════════════════════════════════════════════════════
  ⚡ SOXX FAST BREAKOUT SYSTEM - IMMEDIATE ACTION ⚡
═══════════════════════════════════════════════════════════

🟢 FAST BREAKOUT DETECTED - {date_str}

📊 SOXX DATA:
   Price: ${data['price']:.2f}
   1h Return: {data['return_1h_pct']:.2f}%
   RSI: {data['rsi']:.1f}
   Above MA20: YES

⚡ FAST BREAKOUT CONDITIONS:
   Price broke above 15-min high by 0.5%+
   RSI > 60: YES

📈 TRADE PLAN (TIGHTER STOPS):
   Action: BUY {positions['shares']} SHARES SOXX
   Entry: ${positions['entry_price']:.2f} (market)
   Stop Loss: ${positions['stop_price']:.2f} (-{positions['stop_loss_pct']}%)
   Take Profit: ${positions['target_price']:.2f} (+{positions['target_pct']}%)
   Expected Net Profit: ${positions['net_profit']:.2f}

🔗 DASHBOARD: {dashboard_url}
📝 TRADE ENTRY: {trade_entry_url}

═══════════════════════════════════════════════════════════
  ⚠️ FAST SYSTEM - Execute within 5 minutes
  System checks every 15 minutes during market hours
═══════════════════════════════════════════════════════════
"""
    elif is_green:
        subject = f"🟢 TTP GREEN DAY - BUY SOXX ({date_str})"
        body = f"""
═══════════════════════════════════════════════════════════
  TTP SOXX GREEN DAY SYSTEM - BUY SIGNAL
═══════════════════════════════════════════════════════════

🟢 GREEN DAY CONFIRMED - {date_str}

📊 SOXX DATA:
   Price: ${data['price']:.2f}
   1h Return: {data['return_1h_pct']:.2f}%
   RSI: {data['rsi']:.1f}
   Above MA20: YES

📈 TRADE PLAN:
   Action: BUY {positions['shares']} SHARES SOXX
   Entry: ${positions['entry_price']:.2f} (market)
   Stop Loss: ${positions['stop_price']:.2f} (-{positions['stop_loss_pct']}%)
   Take Profit: ${positions['target_price']:.2f} (+{positions['target_pct']}%)
   Expected Net Profit: ${positions['net_profit']:.2f}

✅ CONDITIONS MET:
{chr(10).join(conditions)}

🔗 DASHBOARD: {dashboard_url}
📝 TRADE ENTRY: {trade_entry_url}

═══════════════════════════════════════════════════════════
  Sell both shares at +6%. No partial sells.
  Max daily loss: $60. Stop trading if hit.
  System checks every 15 minutes during market hours.
═══════════════════════════════════════════════════════════
"""
    else:
        subject = f"🔴 TTP RED DAY - WAIT ({date_str})"
        body = f"""
═══════════════════════════════════════════════════════════
  TTP SOXX GREEN DAY SYSTEM - WAIT
═══════════════════════════════════════════════════════════

🔴 RED DAY - NO TRADE

📊 SOXX DATA:
   Price: ${data['price']:.2f}
   1h Return: {data['return_1h_pct']:.2f}%
   RSI: {data['rsi']:.1f}
   Above MA20: {data['above_ma20']}

❌ CONDITIONS NOT MET:
{chr(10).join(conditions)}

🔗 DASHBOARD: {dashboard_url}
📝 TRADE ENTRY: {trade_entry_url}

═══════════════════════════════════════════════════════════
  System checks every 15 minutes during market hours.
  Next check in 15 minutes.
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
        print(f"✅ Email sent to {mail_username}")
    except Exception as e:
        print(f"❌ Failed to send email: {e}")


def main():
    print("=" * 50)
    print("TTP SOXX Signal Generator (with Fast Breakout)")
    print("Volume requirement removed")
    print("=" * 50)
    
    config = load_config()
    data = load_latest_data()
    
    if not data:
        print("❌ No data available. Run collect_data.py first.")
        return
    
    print(f"Processing {data['session']} data from {data['timestamp']}")
    print(f"Current SOXX price: ${data['price']:.2f}")
    
    # Check standard Green Day conditions
    is_green, conditions = check_green_day(data, config)
    
    # Check for fast breakout (only if not already green)
    is_fast = False
    fast_details = {}
    
    if not is_green:
        recent_data = load_recent_data_points()
        is_fast, fast_details = check_fast_breakout(data, recent_data)
        
        if is_fast:
            print("\n⚡ FAST BREAKOUT DETECTED!")
            print(f"   Recent high: ${fast_details.get('recent_high', 0):.2f}")
    
    print("\n📊 Conditions Check:")
    for c in conditions:
        print(f"   {c}")
    
    if is_fast:
        print("\n⚡ SIGNAL: FAST BREAKOUT - BUY NOW")
        positions = calculate_positions(data, config, is_fast=True)
        
        print(f"\n📈 Trade Plan (Tighter Stops):")
        print(f"   Buy: {positions['shares']} shares @ ${positions['entry_price']:.2f}")
        print(f"   Stop Loss: ${positions['stop_price']:.2f} (-{positions['stop_loss_pct']}%)")
        print(f"   Take Profit: ${positions['target_price']:.2f} (+{positions['target_pct']}%)")
        print(f"   Net Profit: ${positions['net_profit']:.2f}")
        
        save_signal(data, True, conditions + [f"⚡ Fast breakout: broke {fast_details.get('recent_high', 0):.2f}"], positions, is_fast=True)
        send_email(True, data, conditions, positions, is_fast=True)
        
    elif is_green:
        print("\n🟢 SIGNAL: GREEN DAY - READY TO BUY")
        positions = calculate_positions(data, config, is_fast=False)
        
        print(f"\n📈 Trade Plan:")
        print(f"   Buy: {positions['shares']} shares @ ${positions['entry_price']:.2f}")
        print(f"   Stop Loss: ${positions['stop_price']:.2f} (-{positions['stop_loss_pct']}%)")
        print(f"   Take Profit: ${positions['target_price']:.2f} (+{positions['target_pct']}%)")
        print(f"   Net Profit: ${positions['net_profit']:.2f}")
        
        save_signal(data, is_green, conditions, positions)
        send_email(is_green, data, conditions, positions)
        
    else:
        print("\n🔴 SIGNAL: RED DAY - WAIT")
        positions = {
            'entry_price': data['price'],
            'stop_price': 0,
            'target_price': 0,
            'shares': 2,
            'net_profit': 0
        }
        
        save_signal(data, is_green, conditions, positions)
        send_email(is_green, data, conditions, positions)
    
    print("\n✅ Signal saved and email sent")


if __name__ == "__main__":
    main()
