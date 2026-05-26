#!/usr/bin/env python3
"""
Trade The Pool - SOXX Signal Generator (with Email)
Determines Green Day status and fast breakouts
Includes TRAILING STOP logic for active positions
Includes TTP COMPLIANCE CHECKS and MAX PROFIT HARD STOP
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

# Import TTP compliance module with multiple fallbacks
compliance_available = False
can_enter_swing_trade = None
get_upcoming_events = None

try:
    from .compliance import can_enter_swing_trade as _can_enter, get_upcoming_events as _get_events
    can_enter_swing_trade = _can_enter
    get_upcoming_events = _get_events
    compliance_available = True
    print("✅ Compliance module loaded (relative import)")
except ImportError:
    try:
        from scripts.ttp.compliance import can_enter_swing_trade as _can_enter, get_upcoming_events as _get_events
        can_enter_swing_trade = _can_enter
        get_upcoming_events = _get_events
        compliance_available = True
        print("✅ Compliance module loaded (absolute import)")
    except ImportError:
        try:
            from compliance import can_enter_swing_trade as _can_enter, get_upcoming_events as _get_events
            can_enter_swing_trade = _can_enter
            get_upcoming_events = _get_events
            compliance_available = True
            print("✅ Compliance module loaded (direct import)")
        except ImportError:
            print("⚠️ Compliance module not found. Creating dummy functions.")
            def can_enter_swing_trade(config):
                return True, "Compliance module not available - skipping checks"
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
    """Load open position from trades.csv"""
    if not TRADES_PATH.exists():
        return None
    
    df = pd.read_csv(TRADES_PATH)
    if df.empty:
        return None
    
    open_trades = df[df['status'] == 'open']
    if open_trades.empty:
        return None
    
    return open_trades.iloc[-1].to_dict()


def save_open_position(position: dict):
    """Save or update open position in trades.csv with all tracking fields"""
    trades = []
    
    if TRADES_PATH.exists():
        df = pd.read_csv(TRADES_PATH)
        trades = df.to_dict('records')
        
        # Find and update existing open position
        found = False
        for i, trade in enumerate(trades):
            if trade.get('status') == 'open':
                # Update with latest tracking values
                trades[i].update({
                    'stop_price': position.get('stop_price', trade.get('stop_price')),
                    'highest_price': position.get('highest_price', trade.get('highest_price', position.get('entry_price'))),
                    'initial_stop': position.get('initial_stop', trade.get('initial_stop')),
                    'status': 'open'
                })
                found = True
                break
        
        if not found:
            trades.append(position)
    else:
        trades = [position]
    
    # Ensure all required columns exist with proper values
    all_trades = []
    for t in trades:
        row = {
            'ticker': t.get('ticker', 'SOXX'),
            'entry_date': t.get('entry_date'),
            'entry_price': t.get('entry_price'),
            'shares': t.get('shares'),
            'stop_price': t.get('stop_price', 0),
            'target_price': t.get('target_price', 0),
            'status': t.get('status'),
            'exit_date': t.get('exit_date'),
            'exit_price': t.get('exit_price'),
            'profit': t.get('profit'),
            'initial_stop': t.get('initial_stop', t.get('stop_price', 0)),
            'highest_price': t.get('highest_price', t.get('entry_price', 0)),
            'exit_reason': t.get('exit_reason')
        }
        all_trades.append(row)
    
    df = pd.DataFrame(all_trades)
    df.to_csv(TRADES_PATH, index=False)


def update_trailing_stop(position: dict, current_price: float, config: dict) -> dict:
    """
    Update trailing stop based on highest price reached
    Returns updated position dict with new stop price and highest price
    """
    entry_price = position['entry_price']
    highest_price = position.get('highest_price', entry_price)
    current_stop = position.get('stop_price', 0)
    
    trailing_pct = config['exit_rules']['trailing_stop_pct']
    activation_pct = config['exit_rules']['trailing_activation_pct']
    
    # Calculate initial stop if not set
    if current_stop == 0:
        current_stop = round(entry_price * (1 - config['exit_rules']['stop_loss_pct'] / 100), 2)
        position['initial_stop'] = current_stop
    
    # Update highest price seen
    if current_price > highest_price:
        highest_price = current_price
        print(f"   📈 New high: ${highest_price:.2f}")
    
    # Check if trailing should activate
    profit_pct = (highest_price - entry_price) / entry_price * 100
    
    if profit_pct >= activation_pct:
        # Calculate new trailing stop
        new_stop = round(highest_price * (1 - trailing_pct / 100), 2)
        
        # Only move stop UP, never down
        if new_stop > current_stop:
            current_stop = new_stop
            print(f"   🔼 Trailing stop moved up to ${current_stop:.2f} (profit: {profit_pct:.1f}%)")
    
    # Update position
    position['highest_price'] = highest_price
    position['stop_price'] = current_stop
    
    return position


def check_max_profit_limit(position: dict, current_price: float, config: dict) -> tuple:
    """
    HARD STOP at $150 profit per trade (50% of $300 target)
    Returns: (should_exit, exit_price, exit_reason)
    """
    max_profit = config['exit_rules'].get('max_profit_per_trade', 150)
    entry_price = position['entry_price']
    shares = position['shares']
    
    unrealized_profit = (current_price - entry_price) * shares
    
    if unrealized_profit >= max_profit:
        return True, current_price, f"MAX_PROFIT_LIMIT (${unrealized_profit:.2f} >= ${max_profit})"
    
    return False, None, None


def check_trailing_stop_exit(position: dict, current_price: float, config: dict) -> tuple:
    """
    Check if trailing stop has been hit
    Returns: (should_exit, exit_price, exit_reason)
    """
    stop_price = position.get('stop_price', 0)
    
    if stop_price > 0 and current_price <= stop_price:
        return True, stop_price, "TRAILING_STOP"
    
    return False, None, None


def check_initial_exit(position: dict, current_price: float, config: dict) -> tuple:
    """
    Check initial stop loss and take profit
    """
    entry_price = position['entry_price']
    stop_loss = position.get('initial_stop', position.get('stop_price', 0))
    take_profit = position.get('target_price', 0)
    
    # Check stop loss
    if stop_loss > 0 and current_price <= stop_loss:
        return True, stop_loss, "STOP_LOSS"
    
    # Check take profit
    if take_profit > 0 and current_price >= take_profit:
        return True, take_profit, "TAKE_PROFIT"
    
    return False, None, None


def close_position(exit_price: float, exit_reason: str, position: dict):
    """Close an open position and record profit"""
    trades = []
    
    if TRADES_PATH.exists():
        df = pd.read_csv(TRADES_PATH)
        trades = df.to_dict('records')
        
        for i, trade in enumerate(trades):
            if trade.get('status') == 'open':
                profit = (exit_price - trade['entry_price']) * trade['shares']
                trades[i]['status'] = 'completed'
                trades[i]['exit_date'] = datetime.now().isoformat()
                trades[i]['exit_price'] = exit_price
                trades[i]['profit'] = round(profit, 2)
                trades[i]['exit_reason'] = exit_reason
                # Preserve tracking fields
                trades[i]['highest_price'] = position.get('highest_price', trade.get('highest_price', trade['entry_price']))
                trades[i]['initial_stop'] = position.get('initial_stop', trade.get('initial_stop', trade.get('stop_price')))
                trades[i]['stop_price'] = position.get('stop_price', trade.get('stop_price'))
                break
    
    df = pd.DataFrame(trades)
    df.to_csv(TRADES_PATH, index=False)
    
    print(f"   🔴 POSITION CLOSED: {exit_reason} at ${exit_price:.2f}")


def check_green_day(data: dict, config: dict) -> tuple:
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
    
    rsi_min = config['entry_conditions']['rsi_min']
    if data['rsi'] >= rsi_min:
        conditions.append(f"✅ RSI: {data['rsi']:.1f} >= {rsi_min}")
    else:
        conditions.append(f"❌ RSI: {data['rsi']:.1f} < {rsi_min}")
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
    
    details = {
        'recent_high': round(recent_high, 2),
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
    
    recent = df.tail(8).to_dict('records')
    return recent


def calculate_positions(data: dict, config: dict, is_fast: bool = False) -> dict:
    """Calculate entry, stop, and target prices"""
    price = data['price']
    shares = config['trade_management']['shares_per_trade']
    
    if is_fast:
        stop_pct = 1.5
        target_pct = 3.0
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
        'initial_stop': stop_price,
        'highest_price': price,
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
   RSI: {data['rsi']:.1f}
   Above MA20: YES

⚡ FAST BREAKOUT CONDITIONS:
   Price broke above 15-min high by 0.5%+

📈 TRADE PLAN (TIGHTER STOPS):
   Action: BUY {positions['shares']} SHARES SOXX
   Entry: ${positions['entry_price']:.2f} (market)
   Initial Stop: ${positions['stop_price']:.2f} (-{positions['stop_loss_pct']}%)
   Take Profit: ${positions['target_price']:.2f} (+{positions['target_pct']}%)
   Expected Net Profit: ${positions['net_profit']:.2f}

🔗 DASHBOARD: {dashboard_url}
📝 TRADE ENTRY: {trade_entry_url}

═══════════════════════════════════════════════════════════
  ⚠️ FAST SYSTEM - Execute within 5 minutes
  Tighter stops (1.5%) = smaller losses
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
   Initial Stop: ${positions['stop_price']:.2f} (-{positions['stop_loss_pct']}%)
   Take Profit: ${positions['target_price']:.2f} (+{positions['target_pct']}%)

   📈 TRAILING STOP RULES:
   - Initial stop at -2%
   - After price moves up +3%, trailing stop activates
   - Stop trails 2% below highest price reached
   - Stop only moves UP, never down

✅ CONDITIONS MET:
{chr(10).join(conditions)}

🔗 DASHBOARD: {dashboard_url}
📝 TRADE ENTRY: {trade_entry_url}

═══════════════════════════════════════════════════════════
  Trailing stop locks in profits as price rises.
  Max profit per trade hard stop: ${positions.get('max_profit_limit', 150)} (50% of $300 target)
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


def send_trailing_stop_email(position: dict, new_stop: float, current_price: float):
    """Send notification when trailing stop moves up"""
    mail_username = os.environ.get("MAIL_USERNAME")
    mail_password = os.environ.get("MAIL_PASSWORD")
    
    if not mail_username or not mail_password:
        return
    
    date_str = datetime.now().strftime("%Y-%m-%d %H:%M ET")
    profit_pct = (current_price - position['entry_price']) / position['entry_price'] * 100
    
    subject = f"🔒 Trailing Stop Moved Up - SOXX at ${current_price:.2f}"
    body = f"""
═══════════════════════════════════════════════════════════
  TRAILING STOP UPDATE
═══════════════════════════════════════════════════════════

📊 POSITION UPDATE - {date_str}

   Entry Price: ${position['entry_price']:.2f}
   Current Price: ${current_price:.2f}
   Unrealized Profit: +{profit_pct:.1f}%

🔒 STOP LOSS UPDATED:

   New Stop Loss: ${new_stop:.2f}
   Locked Profit: ${(new_stop - position['entry_price']) * position['shares']:.2f}

═══════════════════════════════════════════════════════════
  Your stop loss has been raised to lock in profits.
  No action needed - stop will continue trailing up.
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
        print(f"✅ Trailing stop email sent")
    except Exception as e:
        print(f"❌ Failed to send email: {e}")


def send_compliance_block_email(data: dict, compliance_reason: str):
    """Send notification when compliance blocks entry"""
    mail_username = os.environ.get("MAIL_USERNAME")
    mail_password = os.environ.get("MAIL_PASSWORD")
    
    if not mail_username or not mail_password:
        return
    
    date_str = datetime.now().strftime("%Y-%m-%d %H:%M ET")
    
    subject = f"⚠️ TTP COMPLIANCE - Entry Blocked ({date_str})"
    body = f"""
═══════════════════════════════════════════════════════════
  TTP COMPLIANCE RESTRICTION - ENTRY BLOCKED
═══════════════════════════════════════════════════════════

⚠️ SWING TRADE ENTRY BLOCKED - {date_str}

📊 SOXX DATA:
   Price: ${data['price']:.2f}
   1h Return: {data['return_1h_pct']:.2f}%
   RSI: {data['rsi']:.1f}

🚫 COMPLIANCE REASON:
   {compliance_reason}

📋 TTP RULES:
   - Cannot hold overnight when component stocks have earnings within 1 day
   - Cannot hold overnight before SOXX ex-dividend date

🔗 DASHBOARD: https://mrpink970.github.io/sector-research-system/docs/ttp/ttp_dashboard.html

═══════════════════════════════════════════════════════════
  System will check again at next interval.
  No action required.
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
        print(f"✅ Compliance block email sent")
    except Exception as e:
        print(f"❌ Failed to send email: {e}")


def main():
    print("=" * 50)
    print("TTP SOXX Signal Generator (with Trailing Stop & Compliance)")
    print("=" * 50)
    
    config = load_config()
    data = load_latest_data()
    
    if not data:
        print("❌ No data available. Run collect_data.py first.")
        return
    
    current_price = data['price']
    print(f"Current SOXX price: ${current_price:.2f}")
    print(f"Time: {data['timestamp']}")
    
    # Check for existing open position
    open_position = load_open_position()
    
    # If there's an open position, manage trailing stop and check max profit
    if open_position:
        print(f"\n📌 Open position exists:")
        print(f"   Entry: ${open_position['entry_price']:.2f}")
        print(f"   Current stop: ${open_position.get('stop_price', 'N/A')}")
        print(f"   Highest price: ${open_position.get('highest_price', open_position['entry_price']):.2f}")
        
        # Update trailing stop with current price
        updated_position = update_trailing_stop(open_position, current_price, config)
        
        # === CHECK MAX PROFIT LIMIT FIRST (HARD STOP at $150) ===
        should_exit, exit_price, exit_reason = check_max_profit_limit(updated_position, current_price, config)
        
        if not should_exit:
            # Check trailing stop
            should_exit, exit_price, exit_reason = check_trailing_stop_exit(updated_position, current_price, config)
        
        if not should_exit:
            # Check initial stop/take profit
            should_exit, exit_price, exit_reason = check_initial_exit(updated_position, current_price, config)
        
        if should_exit:
            close_position(exit_price, exit_reason, updated_position)
            print(f"\n🔴 Position closed: {exit_reason} at ${exit_price:.2f}")
        else:
            # Save updated position with new stop and highest price
            save_open_position(updated_position)
            print(f"\n✅ Position updated")
            print(f"   Stop price: ${updated_position['stop_price']:.2f}")
            print(f"   Highest price: ${updated_position['highest_price']:.2f}")
            
            # Send email if trailing stop moved significantly
            old_stop = open_position.get('stop_price', 0)
            if updated_position['stop_price'] > old_stop:
                send_trailing_stop_email(updated_position, updated_position['stop_price'], current_price)
        
        print("\n" + "=" * 50)
        return
    
    # No open position - check TTP compliance before entering
    print("\n🔍 No open position. Checking compliance for new entry...")
    
    # === TTP COMPLIANCE CHECK ===
    if compliance_available:
        can_enter, compliance_reason = can_enter_swing_trade(config)
    else:
        can_enter, compliance_reason = True, "Compliance checks skipped (module not loaded)"
    
    if not can_enter:
        print(f"\n⚠️ COMPLIANCE RESTRICTION: {compliance_reason}")
        print("   Cannot enter swing trade at this time")
        
        # Send notification email
        positions = {
            'entry_price': data['price'],
            'stop_price': 0,
            'target_price': 0,
            'shares': config['trade_management']['shares_per_trade'],
            'net_profit': 0
        }
        send_compliance_block_email(data, compliance_reason)
        save_signal(data, False, [f"COMPLIANCE: {compliance_reason}"], positions)
        print("\n" + "=" * 50)
        return
    
    print(f"✅ Compliance passed: {compliance_reason}")
    
    # Check for fast breakout
    is_fast = False
    fast_details = {}
    
    # Check standard Green Day conditions
    is_green, conditions = check_green_day(data, config)
    
    # If not green, check for fast breakout
    if not is_green:
        recent_data = load_recent_data_points()
        is_fast, fast_details = check_fast_breakout(data, recent_data)
        
        if is_fast:
            print("\n⚡ FAST BREAKOUT DETECTED!")
    
    print("\n📊 Conditions Check:")
    for c in conditions:
        print(f"   {c}")
    
    max_profit_limit = config['exit_rules'].get('max_profit_per_trade', 150)
    
    if is_fast:
        print("\n⚡ SIGNAL: FAST BREAKOUT - BUY NOW")
        positions = calculate_positions(data, config, is_fast=True)
        
        print(f"\n📈 Trade Plan (Tighter Stops):")
        print(f"   Buy: {positions['shares']} shares @ ${positions['entry_price']:.2f}")
        print(f"   Stop Loss: ${positions['stop_price']:.2f} (-{positions['stop_loss_pct']}%)")
        print(f"   Take Profit: ${positions['target_price']:.2f} (+{positions['target_pct']}%)")
        print(f"   ⚠️ HARD STOP: Will close automatically at ${max_profit_limit} profit")
        
        # Save as open position with tracking fields
        position_record = {
            'ticker': 'SOXX',
            'entry_date': datetime.now().isoformat(),
            'entry_price': positions['entry_price'],
            'shares': positions['shares'],
            'stop_price': positions['stop_price'],
            'target_price': positions['target_price'],
            'initial_stop': positions['stop_price'],
            'highest_price': positions['entry_price'],
            'status': 'open'
        }
        save_open_position(position_record)
        save_signal(data, True, conditions, positions, is_fast=True)
        send_email(True, data, conditions, positions, is_fast=True)
        
    elif is_green:
        print("\n🟢 SIGNAL: GREEN DAY - READY TO BUY")
        positions = calculate_positions(data, config, is_fast=False)
        
        print(f"\n📈 Trade Plan:")
        print(f"   Buy: {positions['shares']} shares @ ${positions['entry_price']:.2f}")
        print(f"   Initial Stop: ${positions['stop_price']:.2f} (-{positions['stop_loss_pct']}%)")
        print(f"   Take Profit: ${positions['target_price']:.2f} (+{positions['target_pct']}%)")
        print(f"   ⚠️ HARD STOP: Will close automatically at ${max_profit_limit} profit")
        print(f"\n   📈 Trailing Stop Rules:")
        print(f"   - After price moves up +3%, trailing stop activates")
        print(f"   - Stop trails 2% below highest price")
        print(f"   - Stop only moves UP, never down")
        
        # Save as open position with tracking fields
        position_record = {
            'ticker': 'SOXX',
            'entry_date': datetime.now().isoformat(),
            'entry_price': positions['entry_price'],
            'shares': positions['shares'],
            'stop_price': positions['stop_price'],
            'target_price': positions['target_price'],
            'initial_stop': positions['stop_price'],
            'highest_price': positions['entry_price'],
            'status': 'open'
        }
        save_open_position(position_record)
        save_signal(data, is_green, conditions, positions)
        send_email(is_green, data, conditions, positions)
        
    else:
        print("\n🔴 SIGNAL: RED DAY - WAIT")
        positions = {
            'entry_price': data['price'],
            'stop_price': 0,
            'target_price': 0,
            'shares': config['trade_management']['shares_per_trade'],
            'net_profit': 0
        }
        save_signal(data, is_green, conditions, positions)
        send_email(is_green, data, conditions, positions)
    
    # Display upcoming compliance events if module is available
    if compliance_available:
        events = get_upcoming_events(config)
        if events['earnings']:
            print("\n📅 Upcoming earnings (watch for compliance blocks):")
            for e in events['earnings'][:3]:
                print(f"   {e['symbol']}: {e['date']} ({e['days']} days)")
    
    print("\n✅ Signal saved and email sent")
    print("=" * 50)


if __name__ == "__main__":
    main()
