#!/usr/bin/env python3
"""
MES Automated Paper Trading System - MY FUNDED FUTURES OPTIMIZED
Specifically configured to pass $50K Core evaluation in 1 month
"""

import os
import yfinance as yf
import pandas as pd
import numpy as np
from pathlib import Path
from datetime import datetime, timedelta
import smtplib
from email.message import EmailMessage
import json
import yaml

# ============================================================
# CONFIGURATION
# ============================================================
CONFIG_PATH = Path("config/mes_config.yaml")
DATA_DIR = Path("data/mes_paper")
POSITIONS_FILE = DATA_DIR / "positions.csv"
TRADES_FILE = DATA_DIR / "trades.csv"
DASHBOARD_DATA = DATA_DIR / "dashboard_data.json"
DAILY_LOG_FILE = DATA_DIR / "daily_log.csv"
PROGRESS_FILE = DATA_DIR / "progress.json"

os.makedirs(DATA_DIR, exist_ok=True)

# EVALUATION PARAMETERS (My Funded Futures $50K Core)
ACCOUNT_SIZE = 50000
PROFIT_TARGET = 3000
MAX_DRAWDOWN = 2000          # EOD trailing drawdown
MAX_DAILY_PROFIT = 1500      # 50% consistency rule
MIN_TRADING_DAYS = 2
DAILY_LOSS_LIMIT = 2000      # EOD loss limit

# TRADE PARAMETERS (Conservative for evaluation)
CONTRACTS = 2                # Start with 2 MES during eval
POINT_VALUE = 5.0
STOP_POINTS = 8.0
TARGET_POINTS = 16.0
RISK_REWARD_RATIO = 2.0

# EMA Parameters
EMA_FAST = 9
EMA_SLOW = 21

# Trading hours (ET)
TRADING_START_HOUR = 9
TRADING_END_HOUR = 16


def load_config():
    """Load MES configuration"""
    if CONFIG_PATH.exists():
        with open(CONFIG_PATH, 'r') as f:
            return yaml.safe_load(f)
    return None


def get_config_value(key, default):
    """Get config value with fallback"""
    config = load_config()
    if config:
        keys = key.split('.')
        value = config
        for k in keys:
            value = value.get(k, default)
            if value == default:
                break
        return value
    return default


# ============================================================
# EVALUATION TRACKING
# ============================================================

def load_daily_log():
    """Load daily P&L log"""
    if DAILY_LOG_FILE.exists():
        df = pd.read_csv(DAILY_LOG_FILE)
        return df.to_dict('records')
    return []


def save_daily_log_entry(date, daily_pnl, peak_equity, current_equity, day_count):
    """Save daily P&L entry"""
    entries = load_daily_log()
    
    today_str = date.strftime("%Y-%m-%d")
    existing = [e for e in entries if e.get('date') == today_str]
    
    if existing:
        for e in entries:
            if e['date'] == today_str:
                e['daily_pnl'] = daily_pnl
                e['peak_equity'] = peak_equity
                e['current_equity'] = current_equity
                e['day_count'] = day_count
    else:
        entries.append({
            'date': today_str,
            'daily_pnl': daily_pnl,
            'peak_equity': peak_equity,
            'current_equity': current_equity,
            'day_count': day_count
        })
    
    df = pd.DataFrame(entries)
    df.to_csv(DAILY_LOG_FILE, index=False)


def get_today_pnl():
    """Get today's P&L from closed trades"""
    if not TRADES_FILE.exists():
        return 0
    
    df = pd.read_csv(TRADES_FILE)
    if df.empty:
        return 0
    
    today = datetime.now().strftime("%Y-%m-%d")
    if 'exit_time' in df.columns:
        today_trades = df[df['exit_time'].str.startswith(today) if len(df) > 0 else pd.Series()]
        if 'profit_dollars' in df.columns and len(today_trades) > 0:
            return today_trades['profit_dollars'].sum()
    
    return 0


def get_total_profit():
    """Get total profit from all closed trades"""
    if not TRADES_FILE.exists():
        return 0
    
    df = pd.read_csv(TRADES_FILE)
    if df.empty or 'profit_dollars' not in df.columns:
        return 0
    
    return df['profit_dollars'].sum()


def get_trading_days_count():
    """Get number of unique trading days with trades"""
    if not TRADES_FILE.exists():
        return 0
    
    df = pd.read_csv(TRADES_FILE)
    if df.empty or 'exit_time' not in df.columns:
        return 0
    
    df['date'] = pd.to_datetime(df['exit_time']).dt.date
    return df['date'].nunique()


def get_max_daily_profit():
    """Get maximum daily profit (for consistency rule)"""
    if not TRADES_FILE.exists():
        return 0
    
    df = pd.read_csv(TRADES_FILE)
    if df.empty or 'exit_time' not in df.columns or 'profit_dollars' not in df.columns:
        return 0
    
    df['date'] = pd.to_datetime(df['exit_time']).dt.date
    daily_profits = df.groupby('date')['profit_dollars'].sum()
    
    return daily_profits.max() if len(daily_profits) > 0 else 0


def get_peak_equity():
    """Get peak equity from daily log"""
    log = load_daily_log()
    if not log:
        return ACCOUNT_SIZE
    
    peaks = [entry.get('peak_equity', ACCOUNT_SIZE) for entry in log]
    return max(peaks) if peaks else ACCOUNT_SIZE


def check_evaluation_rules():
    """Check all evaluation rules before trading"""
    today_pnl = get_today_pnl()
    total_profit = get_total_profit()
    trading_days = get_trading_days_count()
    max_daily_profit = get_max_daily_profit()
    current_equity = ACCOUNT_SIZE + total_profit
    peak_equity = get_peak_equity()
    drawdown = peak_equity - current_equity
    
    # Rule 1: Max drawdown (EOD trailing)
    if drawdown >= MAX_DRAWDOWN:
        return False, f"MAX DRAWDOWN HIT: ${drawdown:.2f} loss from peak of ${peak_equity:.0f}"
    
    # Rule 2: Daily loss limit (EOD)
    if today_pnl <= -DAILY_LOSS_LIMIT:
        return False, f"DAILY LOSS LIMIT HIT: -${abs(today_pnl):.2f} today"
    
    # Rule 3: Consistency rule (no single day >50% of target)
    if total_profit > 0 and max_daily_profit > MAX_DAILY_PROFIT:
        remaining_needed = PROFIT_TARGET - total_profit
        if remaining_needed > 0:
            return False, f"CONSISTENCY RULE: Best day ${max_daily_profit:.0f} > ${MAX_DAILY_PROFIT}"
    
    # Rule 4: Profit target reached
    if total_profit >= PROFIT_TARGET and trading_days >= MIN_TRADING_DAYS:
        return True, "READY FOR REVIEW - Target met!"
    
    # Rule 5: Stop trading if we're close to drawdown
    remaining_buffer = MAX_DRAWDOWN - drawdown
    if remaining_buffer < 500:
        return False, f"LOW BUFFER: Only ${remaining_buffer:.0f} left before drawdown"
    
    return True, "OK to trade"


def update_progress():
    """Update progress.json for dashboard"""
    total_profit = get_total_profit()
    trading_days = get_trading_days_count()
    today_pnl = get_today_pnl()
    max_daily_profit = get_max_daily_profit()
    peak_equity = get_peak_equity()
    current_equity = ACCOUNT_SIZE + total_profit
    drawdown = peak_equity - current_equity
    
    # Calculate days remaining (assuming 2 trades/day at 60% win rate)
    trades_needed = max(0, (PROFIT_TARGET - total_profit) / (CONTRACTS * TARGET_POINTS * POINT_VALUE * 0.6))
    days_remaining = int(trades_needed / 2) + 1
    
    progress = {
        'timestamp': datetime.now().isoformat(),
        'total_profit': round(total_profit, 2),
        'profit_target': PROFIT_TARGET,
        'profit_remaining': round(max(0, PROFIT_TARGET - total_profit), 2),
        'percent_complete': round(min(100, (total_profit / PROFIT_TARGET) * 100), 1),
        'trading_days': trading_days,
        'min_days_required': MIN_TRADING_DAYS,
        'today_pnl': round(today_pnl, 2),
        'max_daily_profit': round(max_daily_profit, 2),
        'max_allowed_daily': MAX_DAILY_PROFIT,
        'peak_equity': round(peak_equity, 2),
        'current_equity': round(current_equity, 2),
        'drawdown': round(drawdown, 2),
        'max_drawdown': MAX_DRAWDOWN,
        'remaining_buffer': round(MAX_DRAWDOWN - drawdown, 2),
        'estimated_days_remaining': days_remaining,
        'can_trade': check_evaluation_rules()[0]
    }
    
    with open(PROGRESS_FILE, 'w') as f:
        json.dump(progress, f, indent=2)
    
    return progress


# ============================================================
# DATA FETCHING (Using SPY as proxy for strategy validation)
# ============================================================

def get_current_price():
    """Get current price using SPY as proxy for MES"""
    try:
        ticker = yf.Ticker("SPY")
        data = ticker.history(period="1d", interval="5m")
        if data is not None and len(data) > 0:
            return round(float(data['Close'].iloc[-1]), 2)
        return None
    except Exception as e:
        print(f"Error fetching price: {e}")
        return None


def get_historical_data():
    """Get historical 1-hour data for EMA calculation"""
    try:
        data = yf.download("SPY", period="7d", interval="1h", progress=False)
        
        if data is None or data.empty:
            return None
        
        if hasattr(data.columns, 'get_level_values'):
            data.columns = data.columns.get_level_values(0)
        
        data = data.dropna()
        return data
    except Exception as e:
        print(f"Error fetching historical data: {e}")
        return None


def check_trading_hours():
    """Check if current time is within trading window"""
    now = datetime.now()
    current_hour = now.hour
    
    # Only trade 9 AM - 4 PM ET
    if TRADING_START_HOUR <= current_hour < TRADING_END_HOUR:
        return True
    return False


# ============================================================
# EMA CALCULATION
# ============================================================

def calculate_ema(prices, period):
    """Calculate EMA from a list of prices"""
    if len(prices) < period:
        return None
    
    multiplier = 2 / (period + 1)
    ema = prices[0]
    
    for price in prices[1:]:
        ema = (price - ema) * multiplier + ema
    
    return round(ema, 2)


def check_signal(data):
    """Check for 9/21 EMA crossover"""
    if data is None or data.empty or len(data) < 25:
        return None, {}
    
    try:
        closes = data['Close'].values.tolist()
        
        if len(closes) < 25:
            return None, {}
        
        ema_9_list = []
        ema_21_list = []
        
        for i in range(EMA_SLOW, len(closes)):
            prices_9 = closes[i-EMA_FAST:i]
            ema_9 = calculate_ema(prices_9, EMA_FAST)
            if ema_9:
                ema_9_list.append(ema_9)
            
            prices_21 = closes[i-EMA_SLOW:i]
            ema_21 = calculate_ema(prices_21, EMA_SLOW)
            if ema_21:
                ema_21_list.append(ema_21)
        
        if len(ema_9_list) < 2 or len(ema_21_list) < 2:
            return None, {}
        
        curr_fast = ema_9_list[-1]
        curr_slow = ema_21_list[-1]
        prev_fast = ema_9_list[-2]
        prev_slow = ema_21_list[-2]
        current_price = round(closes[-1], 2)
        
        if prev_fast <= prev_slow and curr_fast > curr_slow:
            return 'BUY', {
                'price': current_price,
                'ema_fast': curr_fast,
                'ema_slow': curr_slow
            }
        
        if prev_fast >= prev_slow and curr_fast < curr_slow:
            return 'SELL', {
                'price': current_price,
                'ema_fast': curr_fast,
                'ema_slow': curr_slow
            }
        
        return None, {}
        
    except Exception as e:
        print(f"Error checking signal: {e}")
        return None, {}


# ============================================================
# PAPER TRADE MANAGEMENT
# ============================================================

def load_current_position():
    """Load current open position"""
    if not POSITIONS_FILE.exists():
        return None
    
    try:
        df = pd.read_csv(POSITIONS_FILE)
        open_positions = df[df['status'] == 'OPEN']
        if open_positions.empty:
            return None
        return open_positions.iloc[-1].to_dict()
    except Exception:
        return None


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
        'entry_price': round(price, 2),
        'contracts': CONTRACTS,
        'stop_price': round(stop_price, 2),
        'target_price': round(target_price, 2),
        'stop_points': STOP_POINTS,
        'target_points': TARGET_POINTS,
        'risk_dollars': round(STOP_POINTS * POINT_VALUE * CONTRACTS, 2),
        'target_dollars': round(TARGET_POINTS * POINT_VALUE * CONTRACTS, 2),
        'status': 'OPEN'
    }
    
    save_position(position)
    return position


def check_exit_conditions(position, current_price):
    """Check if position should exit"""
    try:
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
    except Exception as e:
        print(f"Error checking exit: {e}")
    
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
        'exit_price': round(exit_price, 2),
        'contracts': position['contracts'],
        'profit_points': round(profit_points, 2),
        'profit_dollars': round(profit_dollars, 2),
        'exit_reason': exit_reason
    }
    
    trades_df = pd.DataFrame([trade_record])
    
    if TRADES_FILE.exists():
        existing = pd.read_csv(TRADES_FILE)
        updated = pd.concat([existing, trades_df], ignore_index=True)
    else:
        updated = trades_df
    
    updated.to_csv(TRADES_FILE, index=False)
    
    position['status'] = 'CLOSED'
    position['exit_time'] = datetime.now().isoformat()
    position['exit_price'] = exit_price
    position['exit_reason'] = exit_reason
    
    if POSITIONS_FILE.exists():
        df = pd.read_csv(POSITIONS_FILE)
        for i, row in df.iterrows():
            if str(row['entry_time']) == str(position['entry_time']):
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
        return False
    
    msg = EmailMessage()
    msg.set_content(body)
    msg["Subject"] = subject
    msg["From"] = mail_username
    msg["To"] = mail_username
    
    try:
        with smtplib.SMTP_SSL("smtp.gmail.com", 465) as smtp:
            smtp.login(mail_username, mail_password)
            smtp.send_message(msg)
        print(f"✅ Email sent: {subject}")
        return True
    except Exception as e:
        print(f"❌ Email failed: {e}")
        return False


def send_trade_alert(signal, position):
    """Send trade alert email"""
    date_str = datetime.now().strftime("%Y-%m-%d %H:%M ET")
    risk_reward = f"1:{position['target_points']/position['stop_points']:.1f}"
    
    if signal == 'BUY':
        subject = f"📈 MES BUY SIGNAL - {position['contracts']} Contracts at {position['entry_price']}"
        action = f"BUY {position['contracts']} MES CONTRACTS"
    else:
        subject = f"📉 MES SELL SIGNAL - {position['contracts']} Contracts at {position['entry_price']}"
        action = f"SELL {position['contracts']} MES CONTRACTS"
    
    body = f"""
═══════════════════════════════════════════════════════════
  MES PAPER TRADING SYSTEM - TRADE ALERT
═══════════════════════════════════════════════════════════

TIME: {date_str}

🚨 TRADE SIGNAL: {signal}
   {action}

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

ENTRY: ${position['entry_price']}
STOP: ${position['stop_price']} (-{position['stop_points']} pts)
TARGET: ${position['target_price']} (+{position['target_points']} pts)

RISK: ${position['risk_dollars']}
REWARD: ${position['target_dollars']}
R:R: {risk_reward}

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

⚠️ My Funded Futures Rules:
   - Daily consistency: max ${MAX_DAILY_PROFIT} profit per day
   - Max drawdown: ${MAX_DRAWDOWN} from peak
   - Current progress: ${get_total_profit():.0f} / ${PROFIT_TARGET}

═══════════════════════════════════════════════════════════
"""
    
    send_email(subject, body)


def send_exit_alert(trade_record, progress):
    """Send exit alert email with evaluation status"""
    date_str = datetime.now().strftime("%Y-%m-%d %H:%M ET")
    profit_symbol = "+" if trade_record['profit_dollars'] > 0 else ""
    
    subject = f"🔒 MES CLOSED - {trade_record['exit_reason']} - {profit_symbol}${trade_record['profit_dollars']:.2f}"
    body = f"""
═══════════════════════════════════════════════════════════
  MES PAPER TRADING - POSITION CLOSED
═══════════════════════════════════════════════════════════

TIME: {date_str}

EXIT REASON: {trade_record['exit_reason']}
DIRECTION: {trade_record['direction']}
ENTRY: ${trade_record['entry_price']}
EXIT: ${trade_record['exit_price']}
PROFIT: {profit_symbol}${trade_record['profit_dollars']:.2f}

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

📊 EVALUATION PROGRESS:
   Total Profit: ${progress['total_profit']:.0f} / ${PROFIT_TARGET}
   Trading Days: {progress['trading_days']} / {MIN_TRADING_DAYS}
   Drawdown: ${progress['drawdown']:.0f} / ${MAX_DRAWDOWN}

⚠️ Remaining to pass: ${progress['profit_remaining']:.0f}

═══════════════════════════════════════════════════════════
"""
    
    send_email(subject, body)


def send_status_email(progress):
    """Send daily status email"""
    date_str = datetime.now().strftime("%Y-%m-%d %H:%M ET")
    
    subject = f"📊 MES Evaluation - {progress['percent_complete']:.0f}% to ${PROFIT_TARGET}"
    
    body = f"""
═══════════════════════════════════════════════════════════
  MY FUNDED FUTURES - $50K CORE EVALUATION
═══════════════════════════════════════════════════════════

TIME: {date_str}

🎯 PROFIT TARGET:
   Current: ${progress['total_profit']:.2f}
   Target: ${PROFIT_TARGET}
   Remaining: ${progress['profit_remaining']:.2f}
   Complete: {progress['percent_complete']:.1f}%

📅 TRADING DAYS:
   Days Traded: {progress['trading_days']} / {MIN_TRADING_DAYS}
   Today's P&L: ${progress['today_pnl']:.2f}

⚠️ RULES CHECK:
   Max Daily Profit: ${progress['max_daily_profit']:.0f} / ${MAX_DAILY_PROFIT}
   Drawdown: ${progress['drawdown']:.0f} / ${MAX_DRAWDOWN}
   Buffer Remaining: ${progress['remaining_buffer']:.0f}

📈 ESTIMATED COMPLETION:
   ~{progress['estimated_days_remaining']} more trading days at current pace

✅ CAN TRADE: {'YES' if progress['can_trade'] else 'NO'}

═══════════════════════════════════════════════════════════
"""
    
    send_email(subject, body)


# ============================================================
# DASHBOARD DATA
# ============================================================

def update_dashboard(current_price, signal, position, recent_trades, progress):
    """Update dashboard JSON"""
    dashboard_data = {
        'timestamp': datetime.now().isoformat(),
        'current_price': current_price,
        'signal': signal,
        'active_position': position,
        'recent_trades': recent_trades[:10] if recent_trades else [],
        'progress': progress
    }
    
    with open(DASHBOARD_DATA, 'w') as f:
        json.dump(dashboard_data, f, indent=2)


# ============================================================
# MAIN
# ============================================================

def main():
    print("=" * 60)
    print("MES PAPER TRADING - MY FUNDED FUTURES $50K CORE")
    print("=" * 60)
    
    print(f"\n📋 EVALUATION RULES:")
    print(f"   Profit Target: ${PROFIT_TARGET}")
    print(f"   Max Drawdown: ${MAX_DRAWDOWN}")
    print(f"   Max Daily Profit: ${MAX_DAILY_PROFIT}")
    print(f"   Min Trading Days: {MIN_TRADING_DAYS}")
    print(f"   Contracts: {CONTRACTS} MES")
    
    # Check trading hours
    if not check_trading_hours():
        print(f"\n⏸️ Outside trading hours ({TRADING_START_HOUR}:00-{TRADING_END_HOUR}:00 ET)")
        print("   System will check again next hour")
        
        # Still update progress
        progress = update_progress()
        update_dashboard(None, None, None, None, progress)
        return
    
    # Get current price
    current_price = get_current_price()
    if current_price is None:
        print("❌ Could not fetch price data")
        progress = update_progress()
        update_dashboard(None, None, None, None, progress)
        return
    
    print(f"\n📊 Current price (SPY): ${current_price:.2f}")
    print(f"   Time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    
    # Update progress
    progress = update_progress()
    
    # Check evaluation rules
    can_trade, rule_status = check_evaluation_rules()
    print(f"\n📋 Evaluation Status: {rule_status}")
    
    if not can_trade:
        print(f"\n❌ TRADING BLOCKED: {rule_status}")
        send_status_email(progress)
        update_dashboard(current_price, None, None, None, progress)
        
        # Check if ready for review
        if "READY FOR REVIEW" in rule_status:
            print("\n✅✅✅ READY TO REQUEST REVIEW! ✅✅✅")
            print("   Go to My Funded Futures dashboard and request evaluation completion")
        return
    
    # Check existing position
    existing_position = load_current_position()
    
    if existing_position:
        print(f"\n📌 Open position: {existing_position['direction']} at ${existing_position['entry_price']:.2f}")
        
        should_exit, exit_reason, exit_price = check_exit_conditions(existing_position, current_price)
        
        if should_exit:
            trade_record = close_paper_trade(existing_position, exit_price, exit_reason)
            print(f"\n🔴 POSITION CLOSED: {exit_reason}")
            print(f"   Profit: ${trade_record['profit_dollars']:.2f}")
            send_exit_alert(trade_record, update_progress())
        else:
            print(f"\n✅ Position still active")
            print(f"   Stop: ${existing_position['stop_price']:.2f}")
            print(f"   Target: ${existing_position['target_price']:.2f}")
    
    # Check for new signals
    else:
        # Check daily profit limit before entering new trade
        today_pnl = get_today_pnl()
        if today_pnl >= MAX_DAILY_PROFIT:
            print(f"\n⚠️ Daily profit limit reached: ${today_pnl:.0f} / ${MAX_DAILY_PROFIT}")
            print("   No more trades today (consistency rule)")
            send_status_email(progress)
            update_dashboard(current_price, None, None, None, progress)
            return
        
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
                print(f"   Contracts: {position['contracts']}")
                print(f"   Entry: ${position['entry_price']:.2f}")
                print(f"   Stop: ${position['stop_price']:.2f} (-{position['stop_points']} pts)")
                print(f"   Target: ${position['target_price']:.2f} (+{position['target_points']} pts)")
                print(f"   Risk: ${position['risk_dollars']} | Reward: ${position['target_dollars']}")
                
                send_trade_alert(signal, position)
            else:
                print("\n🔍 No signal - waiting for EMA crossover")
        else:
            print("\n❌ Could not fetch historical data")
    
    # Get recent trades
    recent_trades = None
    if TRADES_FILE.exists():
        try:
            df = pd.read_csv(TRADES_FILE)
            recent_trades = df.tail(10).to_dict('records') if not df.empty else None
        except Exception:
            pass
    
    # Send status at end of day
    current_hour = datetime.now().hour
    if current_hour >= 15:
        send_status_email(progress)
    
    # Update dashboard
    update_dashboard(
        current_price=current_price,
        signal=signal if 'signal' in locals() else None,
        position=existing_position if existing_position else None,
        recent_trades=recent_trades,
        progress=progress
    )
    
    print("\n" + "=" * 60)
    print("✅ System run complete")
    print(f"   Total Profit: ${progress['total_profit']:.2f} / ${PROFIT_TARGET}")
    print(f"   Trading Days: {progress['trading_days']}")
    print("=" * 60)


if __name__ == "__main__":
    main()
