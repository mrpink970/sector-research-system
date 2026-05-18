#!/usr/bin/env python3
"""
Trade The Pool - SOXX Signal Generator (with Email)
Determines Green Day status and sends email alerts
"""

import os
import smtplib
from email.message import EmailMessage
import pandas as pd
import yaml
from pathlib import Path
from datetime import datetime
import sys

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
    
    min_return = config['entry_conditions']['min_1h_return']
    if data['return_1h_pct'] >= min_return:
        conditions.append(f"✅ 1h return: {data['return_1h_pct']:.2f}% >= {min_return}%")
    else:
        conditions.append(f"❌ 1h return: {data['return_1h_pct']:.2f}% < {min_return}%")
        all_met = False
    
    if config['entry_conditions']['above_ma20']:
        if data['above_ma20']:
            conditions.append(f"✅ Above MA20 (${data['ma20']:.2f})")
        else:
            conditions.append(f"❌ Below MA20 (${data['ma20']:.2f})")
            all_met = False
    
    min_volume = config['entry_conditions']['min_volume_ratio']
    if data['volume_ratio'] >= min_volume:
        conditions.append(f"✅ Volume ratio: {data['volume_ratio']:.2f} >= {min_volume}")
    else:
        conditions.append(f"❌ Volume ratio: {data['volume_ratio']:.2f} < {min_volume}")
        all_met = False
    
    rsi_min = config['entry_conditions']['rsi_min']
    if data['rsi'] >= rsi_min:
        conditions.append(f"✅ RSI: {data['rsi']:.1f} >= {rsi_min}")
    else:
        conditions.append(f"❌ RSI: {data['rsi']:.1f} < {rsi_min}")
        all_met = False
    
    return all_met, conditions


def calculate_positions(data: dict, config: dict) -> dict:
    """Calculate entry, stop, and target prices"""
    price = data['price']
    shares = config['trade_management']['shares_per_trade']
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


def save_signal(data: dict, is_green: bool, conditions: list, positions: dict):
    """Save signal to CSV"""
    new_row = pd.DataFrame([{
        'timestamp': datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        'session': data['session'],
        'signal': 'GREEN' if is_green else 'RED',
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


def send_email(is_green: bool, data: dict, conditions: list, positions: dict):
    """Send email notification"""
    mail_username = os.environ.get("MAIL_USERNAME")
    mail_password = os.environ.get("MAIL_PASSWORD")
    
    if not mail_username or not mail_password:
        print("❌ Email credentials not found")
        return
    
    date_str = datetime.now().strftime("%Y-%m-%d %H:%M ET")
    dashboard_url = "https://mrpink970.github.io/sector-research-system/docs/ttp/ttp_dashboard.html"
    trade_entry_url = "https://mrpink970.github.io/sector-research-system/docs/ttp/trade_entry.html"
    
    if is_green:
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
   Volume Ratio: {data['volume_ratio']:.2f}

📈 TRADE PLAN:
   Action: BUY 2 SHARES SOXX
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
   Volume Ratio: {data['volume_ratio']:.2f}

❌ CONDITIONS NOT MET:
{chr(10).join(conditions)}

🔗 DASHBOARD: {dashboard_url}
📝 TRADE ENTRY: {trade_entry_url}

═══════════════════════════════════════════════════════════
  Wait for next check at 10 AM, 1 PM, or 3:30 PM ET.
═══════════════════════════════════════════════════════════
"""
    
    msg = EmailMessage()
    msg.set_content(body)
    msg["Subject"] = subject
    msg["From"] = mail_username
    msg["To"] = mail_username  # Only you
    
    try:
        with smtplib.SMTP_SSL("smtp.gmail.com", 465) as smtp:
            smtp.login(mail_username, mail_password)
            smtp.send_message(msg)
        print(f"✅ Email sent to {mail_username}")
    except Exception as e:
        print(f"❌ Failed to send email: {e}")


def main():
    print("=" * 50)
    print("TTP SOXX Signal Generator")
    print("=" * 50)
    
    config = load_config()
    data = load_latest_data()
    
    if not data:
        print("❌ No data available. Run collect_data.py first.")
        return
    
    print(f"Processing {data['session']} data from {data['timestamp']}")
    print(f"Current SOXX price: ${data['price']:.2f}")
    
    is_green, conditions = check_green_day(data, config)
    
    print("\n📊 Conditions Check:")
    for c in conditions:
        print(f"   {c}")
    
    if is_green:
        print("\n🟢 SIGNAL: GREEN DAY - READY TO BUY")
        positions = calculate_positions(data, config)
        
        print(f"\n📈 Trade Plan:")
        print(f"   Buy: {positions['shares']} shares @ ${positions['entry_price']:.2f}")
        print(f"   Stop Loss: ${positions['stop_price']:.2f} (-{positions['stop_loss_pct']}%)")
        print(f"   Take Profit: ${positions['target_price']:.2f} (+{positions['target_pct']}%)")
        print(f"   Net Profit: ${positions['net_profit']:.2f}")
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
