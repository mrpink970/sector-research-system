#!/usr/bin/env python3
"""
Trade The Pool - SOXX Signal Generator (with Email)
Sends emails 2-3 times per day for manual trading
"""

import os
import smtplib
from email.message import EmailMessage
import pandas as pd
import yaml
from pathlib import Path
from datetime import datetime
import json
import sys

# Add parent directory to path for imports
sys.path.insert(0, str(Path(__file__).parent.parent.parent))

# Paths
CONFIG_PATH = Path("config/ttp_config.yaml")
DATA_DIR = Path("data/ttp")
SIGNALS_PATH = DATA_DIR / "signals.csv"
TRADES_PATH = DATA_DIR / "trades.csv"

# Import TTP compliance module with fallbacks
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


def load_latest_data():
    """Load the most recent price data"""
    log_path = DATA_DIR / "price_log.csv"
    if not log_path.exists():
        return None
    
    df = pd.read_csv(log_path)
    if df.empty:
        return None
    
    return df.iloc[-1].to_dict()


def load_open_position():
    """Load open position from trades.csv (manually entered)"""
    if not TRADES_PATH.exists():
        return None
    
    df = pd.read_csv(TRADES_PATH)
    if df.empty:
        return None
    
    open_trades = df[df['status'] == 'open']
    if open_trades.empty:
        return None
    
    return open_trades.iloc[-1].to_dict()


def get_session():
    """Determine current trading session based on time"""
    now = datetime.now()
    hour = now.hour
    
    if 9 <= hour < 12:
        return "morning", "10:00 AM Outlook"
    elif 12 <= hour < 15:
        return "afternoon", "1:00 PM Midday Update"
    elif 15 <= hour < 17:
        return "close", "3:30 PM End of Day Summary"
    else:
        return "off_hours", "Market Closed"


def check_green_day(data: dict, config: dict) -> tuple:
    """Check if conditions meet Green Day criteria"""
    conditions = []
    all_met = True
    
    min_return = config['entry_conditions']['min_1h_return']
    if data['return_1h_pct'] >= min_return:
        conditions.append(f"✅ 1h return: {data['return_1h_pct']:.2f}%")
    else:
        conditions.append(f"❌ 1h return: {data['return_1h_pct']:.2f}% (needs {min_return}%+)")
        all_met = False
    
    if config['entry_conditions']['above_ma20']:
        if data['above_ma20']:
            conditions.append(f"✅ Above MA20 (${data['ma20']:.2f})")
        else:
            conditions.append(f"❌ Below MA20 (${data['ma20']:.2f})")
            all_met = False
    
    rsi_min = config['entry_conditions']['rsi_min']
    if data['rsi'] >= rsi_min:
        conditions.append(f"✅ RSI: {data['rsi']:.1f}")
    else:
        conditions.append(f"❌ RSI: {data['rsi']:.1f} (needs {rsi_min}+)")
        all_met = False
    
    return all_met, conditions


def check_fast_breakout(data: dict, recent_data: list) -> tuple:
    """Check for fast breakout on 15-minute timeframe"""
    if not recent_data or len(recent_data) < 3:
        return False, {}
    
    current_price = data['price']
    recent_high = max([d['price'] for d in recent_data[-3:]])
    breakout_up = current_price > recent_high * 1.005
    momentum = data['rsi'] > 60
    above_ma20 = data['above_ma20']
    
    is_fast_long = breakout_up and momentum and above_ma20
    
    return is_fast_long, {'recent_high': round(recent_high, 2)}


def load_recent_data_points() -> list:
    """Load recent data points for fast breakout detection"""
    log_path = DATA_DIR / "price_log.csv"
    if not log_path.exists():
        return []
    
    df = pd.read_csv(log_path)
    if df.empty:
        return []
    
    return df.tail(8).to_dict('records')


def calculate_positions(data: dict, config: dict, is_fast: bool = False) -> dict:
    """Calculate entry, stop, and target prices"""
    price = data['price']
    shares = config['trade_management']['shares_per_trade']
    
    if is_fast:
        stop_pct = 1.5
        target_pct = 3.0
        stop_label = "-1.5% (TIGHTER)"
    else:
        stop_pct = config['exit_rules']['stop_loss_pct']
        target_pct = config['exit_rules']['take_profit_pct']
        stop_label = f"-{stop_pct}%"
    
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
        'net_profit': net_profit,
        'stop_label': stop_label,
        'target_pct': target_pct
    }


def send_email(data: dict, signal_type: str, conditions: list, positions: dict, 
               has_open_position: bool, open_position: dict = None, 
               compliance_restricted: bool = False, compliance_reason: str = "",
               session: str = "morning", session_label: str = ""):
    """Send email based on session and conditions"""
    mail_username = os.environ.get("MAIL_USERNAME")
    mail_password = os.environ.get("MAIL_PASSWORD")
    
    if not mail_username or not mail_password:
        print("❌ Email credentials not found")
        return
    
    date_str = datetime.now().strftime("%Y-%m-%d %H:%M ET")
    dashboard_url = "https://mrpink970.github.io/sector-research-system/docs/ttp/ttp_dashboard.html"
    
    # Determine email subject and priority
    if compliance_restricted:
        subject = f"⚠️ TTP COMPLIANCE - No Entry ({session_label})"
        priority = "HIGH"
    elif signal_type == "FAST_BREAKOUT" and not has_open_position:
        subject = f"⚡ FAST BREAKOUT - BUY SOXX NOW ({session_label})"
        priority = "HIGH"
    elif signal_type == "GREEN" and not has_open_position:
        subject = f"🟢 GREEN DAY - BUY SOXX ({session_label})"
        priority = "NORMAL"
    elif signal_type == "RED":
        subject = f"🔴 NO SIGNAL - WAIT ({session_label})"
        priority = "LOW"
    else:
        subject = f"📊 SOXX Market Update ({session_label})"
        priority = "LOW"
    
    # Build email body
    body_lines = []
    body_lines.append("═" * 60)
    body_lines.append(f"  TTP SOXX GREEN DAY SYSTEM - {session_label}")
    body_lines.append("═" * 60)
    body_lines.append("")
    body_lines.append(f"📅 {date_str}")
    body_lines.append("")
    
    # Open position section
    if has_open_position and open_position:
        body_lines.append("📌 CURRENT OPEN POSITION")
    else:
        body_lines.append("📌 NO OPEN POSITION")
    
    if has_open_position and open_position:
        entry_date = open_position.get('entry_date', '').split('T')[0] if open_position.get('entry_date') else 'Unknown'
        entry_price = open_position.get('entry_price', 0)
        shares = open_position.get('shares', 2)
        stop_price = open_position.get('stop_price', 0)
        target_price = open_position.get('target_price', 0)
        
        body_lines.append(f"   Ticker: SOXX")
        body_lines.append(f"   Entry Date: {entry_date}")
        body_lines.append(f"   Entry Price: ${entry_price:.2f}")
        body_lines.append(f"   Shares: {shares}")
        body_lines.append(f"   Stop Loss: ${stop_price:.2f}")
        body_lines.append(f"   Take Profit: ${target_price:.2f}")
        
        # Calculate unrealized P&L
        current_price = data['price']
        unrealized = (current_price - entry_price) * shares
        unrealized_pct = (current_price - entry_price) / entry_price * 100
        body_lines.append(f"   Current Price: ${current_price:.2f}")
        body_lines.append(f"   Unrealized P&L: +${unrealized:.2f} (+{unrealized_pct:.1f}%)")
    else:
        body_lines.append("   No positions currently open")
    
    body_lines.append("")
    body_lines.append("─" * 60)
    body_lines.append("")
    
    # Compliance section
    if compliance_restricted:
        body_lines.append("⚠️ COMPLIANCE RESTRICTION ⚠️")
        body_lines.append("")
        body_lines.append(f"   {compliance_reason}")
        body_lines.append("")
        body_lines.append("   ❌ CANNOT ENTER NEW TRADE")
        body_lines.append("")
        body_lines.append("─" * 60)
        body_lines.append("")
    
    # Signal section (only if no open position and not compliance restricted)
    if not has_open_position and not compliance_restricted:
        if signal_type == "FAST_BREAKOUT":
            body_lines.append("⚡ FAST BREAKOUT DETECTED - TIGHTER STOPS ⚡")
            body_lines.append("")
            body_lines.append(f"   Action: BUY {positions['shares']} SHARES SOXX")
            body_lines.append(f"   Entry: ${positions['entry_price']:.2f} (market)")
            body_lines.append(f"   Stop Loss: ${positions['stop_price']:.2f} {positions['stop_label']}")
            body_lines.append(f"   Take Profit: ${positions['target_price']:.2f} (+{positions['target_pct']}%)")
            body_lines.append(f"   Expected Net Profit: ${positions['net_profit']:.2f}")
            
        elif signal_type == "GREEN":
            body_lines.append("🟢 GREEN DAY CONFIRMED - READY TO BUY 🟢")
            body_lines.append("")
            body_lines.append(f"   Action: BUY {positions['shares']} SHARES SOXX")
            body_lines.append(f"   Entry: ${positions['entry_price']:.2f} (market)")
            body_lines.append(f"   Stop Loss: ${positions['stop_price']:.2f} {positions['stop_label']}")
            body_lines.append(f"   Take Profit: ${positions['target_price']:.2f} (+{positions['target_pct']}%)")
            body_lines.append(f"   Expected Net Profit: ${positions['net_profit']:.2f}")
            body_lines.append("")
            body_lines.append("   📈 TRAILING STOP RULES:")
            body_lines.append("   - Initial stop at -2%")
            body_lines.append("   - After +3% move, trailing stop activates")
            body_lines.append("   - Stop trails 2% below highest price")
            
        elif signal_type == "RED":
            body_lines.append("🔴 RED DAY - NO TRADE 🔴")
            body_lines.append("")
            body_lines.append("   Conditions not met for entry.")
            
        # Add conditions breakdown
        if conditions:
            body_lines.append("")
            body_lines.append("📊 CONDITIONS CHECK:")
            for c in conditions:
                body_lines.append(f"   {c}")
    
    elif has_open_position:
        body_lines.append("📊 MARKET CONDITIONS (for reference):")
        body_lines.append(f"   Current Price: ${data['price']:.2f}")
        body_lines.append(f"   1h Return: {data['return_1h_pct']:.2f}%")
        body_lines.append(f"   RSI: {data['rsi']:.1f}")
        body_lines.append(f"   MA20: ${data['ma20']:.2f}")
        body_lines.append(f"   Above MA20: {'YES' if data['above_ma20'] else 'NO'}")
    
    # Add TTP targets
    body_lines.append("")
    body_lines.append("─" * 60)
    body_lines.append("")
    body_lines.append("🎯 TTP EVALUATION STATUS:")
    
    # Try to get progress from trade_manager
    try:
        from scripts.ttp.trade_manager import check_ready_for_review
        status = check_ready_for_review()
        body_lines.append(f"   Profit: ${status['total_profit']:.2f} / ${status['profit_target']}")
        body_lines.append(f"   Trades: {status['trades_completed']} / {status['min_trades_required']}")
        if status['ready_for_review']:
            body_lines.append("   ✅ READY FOR TTP REVIEW!")
        else:
            if status['profit_remaining'] > 0:
                body_lines.append(f"   Need ${status['profit_remaining']:.2f} more profit")
            if status['trades_needed'] > 0:
                body_lines.append(f"   Need {status['trades_needed']} more trades")
    except:
        body_lines.append("   (Load trade_manager.py for status)")
    
    body_lines.append("")
    body_lines.append("─" * 60)
    body_lines.append("")
    
    # Upcoming compliance events
    if compliance_available:
        events = get_upcoming_events(config)
        if events['earnings']:
            body_lines.append("📅 UPCOMING EARNINGS (will block entry):")
            for e in events['earnings'][:3]:
                body_lines.append(f"   {e['symbol']}: {e['date']} ({e['days']} days)")
            body_lines.append("")
    
    body_lines.append("🔗 DASHBOARD: " + dashboard_url)
    body_lines.append("")
    body_lines.append("═" * 60)
    
    if session == "close":
        body_lines.append("  Market closed. Next update tomorrow at 10:00 AM ET.")
    else:
        body_lines.append("  Next update at next scheduled interval.")
    body_lines.append("═" * 60)
    
    # Send email
    msg = EmailMessage()
    msg.set_content("\n".join(body_lines))
    msg["Subject"] = subject
    msg["From"] = mail_username
    msg["To"] = mail_username
    
    try:
        with smtplib.SMTP_SSL("smtp.gmail.com", 465) as smtp:
            smtp.login(mail_username, mail_password)
            smtp.send_message(msg)
        print(f"✅ Email sent: {subject}")
    except Exception as e:
        print(f"❌ Failed to send email: {e}")


def main():
    print("=" * 50)
    print("TTP SOXX Signal Generator (Manual Trading)")
    print("=" * 50)
    
    config = load_config()
    data = load_latest_data()
    
    if not data:
        print("❌ No data available. Run collect_data.py first.")
        return
    
    current_price = data['price']
    session, session_label = get_session()
    
    print(f"Current SOXX price: ${current_price:.2f}")
    print(f"Time: {data['timestamp']}")
    print(f"Session: {session_label}")
    
    # Check for existing open position (manually entered)
    open_position = load_open_position()
    has_open_position = open_position is not None
    
    # Check compliance
    can_enter, compliance_reason = can_enter_swing_trade(config) if compliance_available else (True, "Compliance skipped")
    compliance_restricted = not can_enter
    
    # Get signal (for display and email)
    is_fast = False
    is_green = False
    conditions = []
    
    if not has_open_position:
        # Check standard Green Day conditions
        is_green, conditions = check_green_day(data, config)
        
        # If not green, check for fast breakout
        if not is_green:
            recent_data = load_recent_data_points()
            is_fast, fast_details = check_fast_breakout(data, recent_data)
        
        # Determine signal type
        if is_fast and not compliance_restricted:
            signal_type = "FAST_BREAKOUT"
            positions = calculate_positions(data, config, is_fast=True)
        elif is_green and not compliance_restricted:
            signal_type = "GREEN"
            positions = calculate_positions(data, config, is_fast=False)
        else:
            signal_type = "RED"
            positions = calculate_positions(data, config, is_fast=False)
    else:
        signal_type = "HOLD"  # Just status update
        positions = calculate_positions(data, config, is_fast=False)
    
    # Print to console
    print("\n📊 Signal Summary:")
    print(f"   Open Position: {'YES' if has_open_position else 'NO'}")
    print(f"   Compliance OK: {'YES' if not compliance_restricted else 'NO'}")
    print(f"   Signal: {signal_type}")
    
    # ALWAYS SEND EMAIL - 2-3 times per day
    # Only skip if off_hours
    if session != "off_hours":
        print(f"\n📧 Sending {session_label} email...")
        send_email(
            data=data,
            signal_type=signal_type,
            conditions=conditions,
            positions=positions,
            has_open_position=has_open_position,
            open_position=open_position,
            compliance_restricted=compliance_restricted,
            compliance_reason=compliance_reason,
            session=session,
            session_label=session_label
        )
    else:
        print(f"\n⏸️ Off hours - no email sent")
    
    # Save signal to CSV for dashboard
    signal_record = {
        'timestamp': datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        'session': session,
        'signal': signal_type,
        'price': current_price,
        'has_position': has_open_position,
        'compliance_ok': not compliance_restricted
    }
    
    # Append to signals CSV
    new_row = pd.DataFrame([signal_record])
    if SIGNALS_PATH.exists():
        existing = pd.read_csv(SIGNALS_PATH)
        updated = pd.concat([existing, new_row], ignore_index=True).tail(100)
    else:
        updated = new_row
    updated.to_csv(SIGNALS_PATH, index=False)
    
    print("\n✅ Signal generation complete")
    print("=" * 50)


if __name__ == "__main__":
    main()
