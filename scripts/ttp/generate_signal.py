#!/usr/bin/env python3
"""
Trade The Pool - SOXX Signal Generator (Email Only)
Sends pre-market email with Green Day/Red Day signal and TTP rule warnings
No CSV updates, no position tracking, no dashboard
Supports multiple email recipients from config file
"""

import os
import smtplib
from email.message import EmailMessage
from email.utils import formataddr
import pandas as pd
import yaml
from pathlib import Path
from datetime import datetime
import sys

# Add parent directory to path for imports
sys.path.insert(0, str(Path(__file__).parent.parent.parent))

# Paths
CONFIG_PATH = Path("config/ttp_config.yaml")
DATA_DIR = Path("data/ttp")
SIGNALS_PATH = DATA_DIR / "signals.csv"

# Import TTP compliance module
compliance_available = False
can_enter_swing_trade = None
get_upcoming_events = None

try:
    from .compliance import can_enter_swing_trade as _can_enter, get_upcoming_events as _get_events
    can_enter_swing_trade = _can_enter
    get_upcoming_events = _get_events
    compliance_available = True
    print("✅ Compliance module loaded")
except ImportError:
    try:
        from scripts.ttp.compliance import can_enter_swing_trade as _can_enter, get_upcoming_events as _get_events
        can_enter_swing_trade = _can_enter
        get_upcoming_events = _get_events
        compliance_available = True
        print("✅ Compliance module loaded")
    except ImportError:
        try:
            from compliance import can_enter_swing_trade as _can_enter, get_upcoming_events as _get_events
            can_enter_swing_trade = _can_enter
            get_upcoming_events = _get_events
            compliance_available = True
            print("✅ Compliance module loaded")
        except ImportError:
            print("⚠️ Compliance module not found.")
            def can_enter_swing_trade(config):
                return True, "Compliance checks skipped"
            def get_upcoming_events(config):
                return {'earnings': []}


def load_config():
    with open(CONFIG_PATH, 'r') as f:
        return yaml.safe_load(f)


def get_email_recipients(config):
    """Get list of email recipients from config"""
    recipients = config.get('email_recipients', [])
    if not recipients:
        # Fallback to environment variable or default
        mail_username = os.environ.get("MAIL_USERNAME")
        if mail_username:
            return [mail_username]
        return []
    return recipients


def load_latest_data():
    """Load the most recent price data"""
    log_path = DATA_DIR / "price_log.csv"
    if not log_path.exists():
        return None
    
    df = pd.read_csv(log_path)
    if df.empty:
        return None
    
    return df.iloc[-1].to_dict()


def check_green_day(data: dict, config: dict) -> tuple:
    """
    Check if conditions meet Green Day criteria.
    PRIMARY: Day return from market open (>= 0.5%)
    """
    conditions = []
    all_met = False
    
    min_return = config['entry_conditions']['min_1h_return']
    
    # Get day return
    day_return = data.get('day_return_pct', 0)
    one_hour_return = data['return_1h_pct']
    session = data.get('session', 'unknown')
    
    # Fallback: use 1h return if day_return is 0 and not pre-market
    if day_return == 0 and session != 'premarket':
        day_return = one_hour_return
    
    # Primary condition: Day return >= 0.5%
    if day_return >= min_return:
        conditions.append(f"✅ DAY RETURN: {day_return:.2f}%")
        all_met = True
    else:
        conditions.append(f"❌ DAY RETURN: {day_return:.2f}%")
    
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
        conditions.append(f"✅ RSI: {data['rsi']:.1f}")
    else:
        conditions.append(f"❌ RSI: {data['rsi']:.1f}")
        all_met = False
    
    return all_met, conditions


def send_email(is_green: bool, data: dict, conditions: list, recipients: list):
    """Send decision email to multiple recipients with TTP rule warnings"""
    mail_username = os.environ.get("MAIL_USERNAME")
    mail_password = os.environ.get("MAIL_PASSWORD")
    
    if not mail_username or not mail_password:
        print("❌ Email credentials not found")
        return False
    
    if not recipients:
        print("❌ No email recipients configured")
        return False
    
    date_str = datetime.now().strftime("%Y-%m-%d %H:%M ET")
    trade_entry_url = "https://mrpink970.github.io/sector-research-system/docs/ttp/trade_entry.html"
    day_return = data.get('day_return_pct', 0)
    
    if is_green:
        subject = f"🟢 TTP DECISION ENGINE - GREEN DAY - Prepare to Buy ({date_str})"
        action = "PREPARE TO BUY"
        signal_icon = "🟢"
        signal_text = "GREEN DAY"
    else:
        subject = f"🔴 TTP DECISION ENGINE - RED DAY - No Trade ({date_str})"
        Action = "WAIT - Check again next run"
        signal_icon = "🔴"
        signal_text = "RED DAY"
    
    body = f"""
═══════════════════════════════════════════════════════════
  TTP DECISION ENGINE - {date_str}
═══════════════════════════════════════════════════════════

{signal_icon} MARKET CONDITION: {signal_text} (Day Return: {day_return:.1f}%)

   → Action: {action}

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

{'📈 TRADE PLAN (to be entered at order placement):' if is_green else '⏸️ ⏸️ Conditions not met this run. Market may change. Check again at next scheduled run..'}

{'   Symbol: SOXX' if is_green else ''}
{'   Direction: BUY' if is_green else ''}
{'   Quantity: 2 SHARES' if is_green else ''}
{'   Order Type: LIMIT (enter your desired price)' if is_green else ''}
{'   Good For: DAY' if is_green else ''}
{'   Overnight: OFF' if is_green else ''}
{'' if is_green else ''}
{'   When you enter your limit price, use the Trade Entry Calculator:' if is_green else ''}
{'   - STOP LOSS: 2% below entry = X,XXX TICKS' if is_green else ''}
{'   - TAKE PROFIT: 6% above entry = $XXX.XX (in dollars)' if is_green else ''}

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

⚠️ ⚠️ ⚠️ CRITICAL RULE WARNINGS ⚠️ ⚠️ ⚠️

   ❌ MAX DRAWDOWN: 7% ($140 from peak)
      → Violation will cause IMMEDIATE ACCOUNT TERMINATION
      → All profits will be FORFEITED

   ❌ MAX POSITION PROFIT: 50% of target ($150)
      → Any single trade exceeding $150 profit will be INVALIDATED
      → You may need additional trades to pass

   ❌ MINIMUM TRADES: 5 trades required to scale to next level
      → Cannot pass evaluation or scale without 5 completed trades

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

✅ COMPLIANCE CHECK:
   ✅ No earnings expected today
   ✅ No dividend restrictions

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

📊 CURRENT MARKET DATA (for reference):

   Current Price: ${data['price']:.2f}
   Day Return: {day_return:.1f}%
   1h Return: {data['return_1h_pct']:.1f}%
   RSI: {data['rsi']:.1f}
   Above MA20: {'YES' if data['above_ma20'] else 'NO'}

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

📊 CONDITIONS CHECK:
{chr(10).join([f'   {c}' for c in conditions])}

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

🔗 TRADE ENTRY CALCULATOR:
   {trade_entry_url}

   Enter your limit price → Get stop loss in TICKS + take profit in DOLLARS

═══════════════════════════════════════════════════════════
"""
    
    # Create message
    msg = EmailMessage()
    msg.set_content(body)
    msg["Subject"] = subject
    msg["From"] = formataddr(("TTP Decision Engine", mail_username))
    msg["To"] = ", ".join(recipients)
    
    # Send to all recipients
    try:
        with smtplib.SMTP_SSL("smtp.gmail.com", 465) as smtp:
            smtp.login(mail_username, mail_password)
            smtp.send_message(msg)
        print(f"✅ Decision email sent to {len(recipients)} recipient(s): {', '.join(recipients)}")
        return True
    except Exception as e:
        print(f"❌ Failed to send email: {e}")
        return False


def send_compliance_email(data: dict, compliance_reason: str, recipients: list):
    """Send compliance block email to multiple recipients"""
    mail_username = os.environ.get("MAIL_USERNAME")
    mail_password = os.environ.get("MAIL_PASSWORD")
    
    if not mail_username or not mail_password:
        print("❌ Email credentials not found")
        return False
    
    if not recipients:
        print("❌ No email recipients configured")
        return False
    
    date_str = datetime.now().strftime("%Y-%m-%d %H:%M ET")
    trade_entry_url = "https://mrpink970.github.io/sector-research-system/docs/ttp/trade_entry.html"
    day_return = data.get('day_return_pct', 0)
    
    body = f"""
═══════════════════════════════════════════════════════════
  TTP DECISION ENGINE - COMPLIANCE BLOCKED - {date_str}
═══════════════════════════════════════════════════════════

⚠️ COMPLIANCE RESTRICTION: {compliance_reason}

   → Action: DO NOT TRADE TODAY

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

📊 CURRENT MARKET DATA:
   Price: ${data['price']:.2f}
   Day Return: {day_return:.1f}%

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

🔗 TRADE ENTRY CALCULATOR:
   {trade_entry_url}

═══════════════════════════════════════════════════════════
"""
    
    msg = EmailMessage()
    msg.set_content(body)
    msg["Subject"] = f"⚠️ TTP COMPLIANCE - No Trading Today ({date_str})"
    msg["From"] = formataddr(("TTP Decision Engine", mail_username))
    msg["To"] = ", ".join(recipients)
    
    try:
        with smtplib.SMTP_SSL("smtp.gmail.com", 465) as smtp:
            smtp.login(mail_username, mail_password)
            smtp.send_message(msg)
        print(f"✅ Compliance email sent to {len(recipients)} recipient(s)")
        return True
    except Exception as e:
        print(f"❌ Failed to send email: {e}")
        return False


def save_signal(data: dict, is_green: bool, conditions: list):
    """Save signal to CSV for reference (no position tracking)"""
    signal_type = 'GREEN' if is_green else 'RED'
    
    new_row = pd.DataFrame([{
        'timestamp': datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        'session': data['session'],
        'signal': signal_type,
        'price': data['price'],
        'day_return': data.get('day_return_pct', 0),
        'one_hour_return': data.get('return_1h_pct', 0),
        'rsi': data['rsi'],
        'above_ma20': data['above_ma20'],
        'conditions': ' | '.join(conditions)
    }])
    
    if SIGNALS_PATH.exists():
        existing = pd.read_csv(SIGNALS_PATH)
        updated = pd.concat([existing, new_row], ignore_index=True).tail(100)
    else:
        DATA_DIR.mkdir(parents=True, exist_ok=True)
        updated = new_row
    
    updated.to_csv(SIGNALS_PATH, index=False)


def main():
    print("=" * 50)
    print("TTP DECISION ENGINE (Email Only)")
    print("=" * 50)
    
    config = load_config()
    
    # Get email recipients from config
    recipients = get_email_recipients(config)
    print(f"📧 Email recipients: {len(recipients)}")
    for r in recipients:
        print(f"   - {r}")
    
    data = load_latest_data()
    
    if not data:
        print("❌ No data available. Run collect_data.py first.")
        return
    
    current_price = data['price']
    session = data.get('session', 'unknown')
    day_return = data.get('day_return_pct', 0)
    
    print(f"Current SOXX price: ${current_price:.2f}")
    print(f"Day Return: {day_return:.2f}%")
    print(f"Session: {session}")
    
    # Check compliance
    if compliance_available:
        can_enter, compliance_reason = can_enter_swing_trade(config)
    else:
        can_enter, compliance_reason = True, "Compliance checks skipped"
    
    if not can_enter:
        print(f"\n⚠️ COMPLIANCE RESTRICTION: {compliance_reason}")
        print("   Sending compliance warning email")
        send_compliance_email(data, compliance_reason, recipients)
        return
    
    # Check for Green Day signal
    is_green, conditions = check_green_day(data, config)
    
    print(f"\n📊 Signal: {'GREEN DAY' if is_green else 'RED DAY'}")
    for c in conditions:
        print(f"   {c}")
    
    # Send email to all recipients
    send_email(is_green, data, conditions, recipients)
    
    # Save signal to CSV for reference
    save_signal(data, is_green, conditions)
    
    print("\n✅ Decision engine complete")
    print("=" * 50)


if __name__ == "__main__":
    main()
