#!/usr/bin/env python3
"""
MES Automated Paper Trading System - PROP FIRM READY
Features:
- Daily loss limit enforcement
- Max drawdown protection  
- Minimum trading days tracking
- Consistency rule (max daily profit %)
- Dynamic position sizing
- ATR-based stops
- Economic calendar checks
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

# Default parameters (will be overridden by config)
CONTRACTS = 6
STOP_POINTS = 8.0
TARGET_POINTS = 16.0
POINT_VALUE = 5.0
ACCOUNT_SIZE = 100000
DAILY_LOSS_LIMIT = 3000
MAX_DRAWDOWN = 10000
MAX_DAILY_PROFIT_PCT = 30  # Consistency rule: no single day >30% of total profit
MIN_TRADING_DAYS = 5
USE_ATR_STOPS = True
ATR_PERIOD = 14
ATR_MULTIPLIER = 1.5

# EMA Parameters
EMA_FAST = 9
EMA_SLOW = 21

# Economic events that block trading
BLOCKED_EVENTS = ['FOMC', 'Nonfarm Payrolls', 'CPI', 'PPI', 'Fed Chair']


def load_config():
    """Load MES configuration from config folder"""
    if CONFIG_PATH.exists():
        with open(CONFIG_PATH, 'r') as f:
            config = yaml.safe_load(f)
            return config
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
# PROP FIRM TRACKING
# ============================================================

def load_daily_log():
    """Load daily P&L log"""
    if DAILY_LOG_FILE.exists():
        df = pd.read_csv(DAILY_LOG_FILE)
        return df.to_dict('records')
    return []


def save_daily_log_entry(date, daily_pnl, peak_equity, current_equity):
    """Save daily P&L entry"""
    entries = load_daily_log()
    
    # Check if today already logged
    today_str = date.strftime("%Y-%m-%d")
    existing = [e for e in entries if e.get('date') == today_str]
    
    if existing:
        for e in entries:
            if e['date'] == today_str:
                e['daily_pnl'] = daily_pnl
                e['peak_equity'] = peak_equity
                e['current_equity'] = current_equity
    else:
        entries.append({
            'date': today_str,
            'daily_pnl': daily_pnl,
            'peak_equity': peak_equity,
            'current_equity': current_equity
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


def get_max_daily_profit_pct():
    """Get max daily profit as percentage of total profit (consistency rule)"""
    if not TRADES_FILE.exists():
        return 0
    
    df = pd.read_csv(TRADES_FILE)
    if df.empty or 'exit_time' not in df.columns or 'profit_dollars' not in df.columns:
        return 0
    
    df['date'] = pd.to_datetime(df['exit_time']).dt.date
    daily_profits = df.groupby('date')['profit_dollars'].sum()
    total_profit = daily_profits.sum()
    
    if total_profit == 0:
        return 0
    
    max_daily = daily_profits.max()
    return (max_daily / total_profit) * 100


def get_peak_equity():
    """Get peak equity from daily log"""
    log = load_daily_log()
    if not log:
        return ACCOUNT_SIZE
    
    peaks = [entry.get('peak_equity', ACCOUNT_SIZE) for entry in log]
    return max(peaks) if peaks else ACCOUNT_SIZE


def check_prop_firm_rules():
    """Check all prop firm rules before trading"""
    today_pnl = get_today_pnl()
    total_profit = get_total_profit()
    trading_days = get_trading_days_count()
    max_daily_pct = get_max_daily_profit_pct()
    
    daily_limit = get_config_value('system.daily_loss_limit', DAILY_LOSS_LIMIT)
    max_drawdown = get_config_value('system.max_drawdown', MAX_DRAWDOWN)
    min_days = get_config_value('system.min_trading_days', MIN_TRADING_DAYS)
    max_daily_pct_limit = get_config_value('system.max_daily_profit_pct', MAX_DAILY_PROFIT_PCT)
    profit_target = get_config_value('system.profit_target', 5000)
    
    # Check daily loss limit
    if today_pnl <= -daily_limit:
        return False, f"Daily loss limit hit: ${today_pnl:.2f} <= -${daily_limit}"
    
    # Check max drawdown (from peak)
    peak_equity = get_peak_equity()
    current_equity = ACCOUNT_SIZE + total_profit
    drawdown = peak_equity - current_equity
    if drawdown >= max_drawdown:
        return False, f"Max drawdown hit: ${drawdown:.2f} >= ${max_drawdown}"
    
    # Check consistency rule (if we have enough profit)
    if total_profit > 1000 and max_daily_pct > max_daily_pct_limit:
        return False, f"Consistency rule: max daily profit {max_daily_pct:.1f}% > {max_daily_pct_limit}%"
    
    # Check if we've already passed (profit target met)
    if total_profit >= profit_target and trading_days >= min_days:
        return True, "READY FOR REVIEW - Target met!"
    
    return True, "OK to trade"


def update_progress():
    """Update progress.json for dashboard"""
    total_profit = get_total_profit()
    trading_days = get_trading_days_count()
    today_pnl = get_today_pnl()
    max_daily_pct = get_max_daily_profit_pct()
    peak_equity = get_peak_equity()
    current_equity = ACCOUNT_SIZE + total_profit
    
    profit_target = get_config_value('system.profit_target', 5000)
    min_days = get_config_value('system.min_trading_days', MIN_TRADING_DAYS)
    daily_limit = get_config_value('system.daily_loss_limit', DAILY_LOSS_LIMIT)
    max_drawdown_limit = get_config_value('system.max_drawdown', MAX_DRAWDOWN)
    
    progress = {
        'timestamp': datetime.now().isoformat(),
        'total_profit': round(total_profit, 2),
        'profit_target': profit_target,
        'profit_remaining': round(max(0, profit_target - total_profit), 2),
        'percent_complete': round(min(100, (total_profit / profit_target) * 100), 1) if profit_target > 0 else 0,
        'trading_days': trading_days,
        'min_days_required': min_days,
        'today_pnl': round(today_pnl, 2),
        'daily_loss_limit': daily_limit,
        'max_daily_profit_pct': round(max_daily_pct, 1),
        'max_allowed_pct': 30,
        'peak_equity': round(peak_equity, 2),
        'current_equity': round(current_equity, 2),
        'drawdown_from_peak': round(peak_equity - current_equity, 2),
        'max_drawdown_limit': max_drawdown_limit,
        'can_trade': check_prop_firm_rules()[0]
    }
    
    with open(PROGRESS_FILE, 'w') as f:
        json.dump(progress, f, indent=2)
    
    return progress


# ============================================================
# ECONOMIC CALENDAR CHECK
# ============================================================

def is_high_impact_event_today():
    """Check if today has a high-impact economic event"""
    today = datetime.now()
    
    # First Friday of month = Nonfarm Payrolls
    if today.weekday() == 4 and today.day <= 7:
        return True, "Nonfarm Payrolls (first Friday)"
    
    # Wednesday before third Thursday of month = FOMC (approx)
    if today.weekday() == 2 and 12 <= today.day <= 20:
        return True, "Possible FOMC meeting"
    
    # CPI/PPI around 10th-15th
    if 10 <= today.day <= 15:
        return True, "Possible CPI/PPI release"
    
    return False, None


# ============================================================
# DATA FETCHING
# ============================================================

def get_mes_price():
    """Get current MES futures price using MES=F ticker"""
    try:
        ticker = yf.Ticker("MES=F")
        data = ticker.history(period="1d", interval="5m")
        if data is not None and len(data) > 0:
            return round(float(data['Close'].iloc[-1]), 2)
        
        # If 5m data fails, try 1m
        data = ticker.history(period="1d", interval="1m")
        if data is not None and len(data) > 0:
            return round(float(data['Close'].iloc[-1]), 2)
        
        print("❌ No MES=F price data available")
        return None
    except Exception as e:
        print(f"Error fetching MES price: {e}")
        return None


def get_historical_data():
    """Get historical 1-hour data for EMA calculation"""
    try:
        # Download MES futures data
        data = yf.download("MES=F", period="7d", interval="1h", progress=False)
        
        if data is None or data.empty:
            print("❌ No MES=F historical data available")
            return None
        
        # Flatten MultiIndex if present
        if hasattr(data.columns, 'get_level_values'):
            data.columns = data.columns.get_level_values(0)
        
        data = data.dropna()
        
        # Verify we have enough data points
        if len(data) < 25:
            print(f"⚠️ Only {len(data)} data points, need 25+ for EMA calculation")
            return None
            
        return data
    except Exception as e:
        print(f"Error fetching historical data: {e}")
        return None


def calculate_atr(data, period=ATR_PERIOD):
    """Calculate Average True Range"""
    if data is None or data.empty or len(data) < period:
        return STOP_POINTS
    
    high = data['High']
    low = data['Low']
    close = data['Close'].shift(1)
    
    tr1 = high - low
    tr2 = abs(high - close)
    tr3 = abs(low - close)
    
    tr = pd.concat([tr1, tr2, tr3], axis=1).max(axis=1)
    atr = tr.rolling(window=period).mean().iloc[-1]
    
    # Convert to points (MES moves in 0.25 increments, but ATR in index points)
    return round(atr, 2)


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
        
        # Calculate EMAs for recent points
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
        
        # Bullish crossover
        if prev_fast <= prev_slow and curr_fast > curr_slow:
            return 'BUY', {
                'price': current_price,
                'ema_fast': curr_fast,
                'ema_slow': curr_slow
            }
        
        # Bearish crossover
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
# POSITION SIZING
# ============================================================

def calculate_position_size(current_price, atr):
    """Dynamic position sizing based on risk"""
    account_size = get_config_value('system.account_size', ACCOUNT_SIZE)
    risk_per_trade_pct = get_config_value('system.risk_per_trade_pct', 1.0)  # 1% risk per trade
    
    max_risk_dollars = account_size * (risk_per_trade_pct / 100)
    
    use_atr = get_config_value('entry_conditions.use_atr_stops', USE_ATR_STOPS)
    
    if use_atr:
        stop_points = atr * ATR_MULTIPLIER
        stop_points = max(stop_points, 5)  # Minimum 5 points stop
        stop_points = min(stop_points, 15)  # Maximum 15 points stop
    else:
        stop_points = STOP_POINTS
    
    risk_per_contract = stop_points * POINT_VALUE
    contracts = int(max_risk_dollars / risk_per_contract)
    max_contracts = get_config_value('system.max_contracts', 10)
    contracts = max(1, min(contracts, max_contracts))
    
    return contracts, stop_points


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


def open_paper_trade(signal, price, atr):
    """Open a new paper trade with dynamic sizing"""
    contracts, stop_points = calculate_position_size(price, atr)
    risk_reward = get_config_value('exit_rules.risk_reward_ratio', 2.0)
    
    if signal == 'BUY':
        stop_price = price - stop_points
        target_points = stop_points * risk_reward
        target_price = price + target_points
    else:
        stop_price = price + stop_points
        target_points = stop_points * risk_reward
        target_price = price - target_points
    
    position = {
        'ticker': 'MES',
        'entry_time': datetime.now().isoformat(),
        'direction': signal,
        'entry_price': round(price, 2),
        'contracts': contracts,
        'stop_price': round(stop_price, 2),
        'target_price': round(target_price, 2),
        'stop_points': round(stop_points, 2),
        'target_points': round(target_points, 2),
        'risk_dollars': round(stop_points * POINT_VALUE * contracts, 2),
        'target_dollars': round(target_points * POINT_VALUE * contracts, 2),
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
            if str(row['entry_time']) == str(position['entry_time']):
                df.loc[i] = pd.Series(position)
                break
        df.to_csv(POSITIONS_FILE, index=False)
    
    # Update daily log with new P&L
    update_progress()
    
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

⚠️ DO NOT move your stop loss.
⚠️ Take profit at target - no partials.

═══════════════════════════════════════════════════════════
"""
    
    send_email(subject, body)


def send_exit_alert(trade_record):
    """Send exit alert email"""
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

═══════════════════════════════════════════════════════════
"""
    
    send_email(subject, body)


def send_status_email(progress):
    """Send daily status email with prop firm progress"""
    date_str = datetime.now().strftime("%Y-%m-%d %H:%M ET")
    
    subject = f"📊 MES System Status - {progress['percent_complete']:.0f}% to Target"
    
    body = f"""
═══════════════════════════════════════════════════════════
  MES PAPER TRADING - PROP FIRM PROGRESS
═══════════════════════════════════════════════════════════

TIME: {date_str}

🎯 TARGET PROGRESS:
   Profit: ${progress['total_profit']:.2f} / ${progress['profit_target']}
   Complete: {progress['percent_complete']:.1f}%
   Remaining: ${progress['profit_remaining']:.2f}

📅 TRADING DAYS:
   Days Traded: {progress['trading_days']} / {progress['min_days_required']}
   Today's P&L: ${progress['today_pnl']:.2f}
   Daily Limit: ${progress['daily_loss_limit']}

📈 CONSISTENCY:
   Max Daily Profit: {progress['max_daily_profit_pct']:.1f}% (limit 30%)
   
⚠️ DRAWDOWN:
   Peak Equity: ${progress['peak_equity']:.2f}
   Current Equity: ${progress['current_equity']:.2f}
   Drawdown: ${progress['drawdown_from_peak']:.2f}
   Max Allowed: ${progress['max_drawdown_limit']}

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
    print("MES PAPER TRADING SYSTEM - PROP FIRM READY")
    print("=" * 60)
    
    # Load configuration
    daily_limit = get_config_value('system.daily_loss_limit', DAILY_LOSS_LIMIT)
    max_dd = get_config_value('system.max_drawdown', MAX_DRAWDOWN)
    profit_target = get_config_value('system.profit_target', 5000)
    
    print(f"\n📋 PROP FIRM RULES:")
    print(f"   Daily Loss Limit: ${daily_limit}")
    print(f"   Max Drawdown: ${max_dd}")
    print(f"   Profit Target: ${profit_target}")
    print(f"   Min Trading Days: {get_config_value('system.min_trading_days', MIN_TRADING_DAYS)}")
    
    # Get current price
    current_price = get_mes_price()
    if current_price is None:
        print("❌ Could not fetch MES price")
        # Still update dashboard with error
        progress = update_progress()
        update_dashboard(None, None, None, None, progress)
        return
    
    print(f"\n📊 Current MES price: ${current_price:.2f}")
    print(f"   Time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    
    # Update progress first
    progress = update_progress()
    
    # Check prop firm rules
    can_trade, rule_status = check_prop_firm_rules()
    print(f"\n📋 Prop Firm Status: {rule_status}")
    
    if not can_trade:
        print(f"\n❌ TRADING BLOCKED: {rule_status}")
        send_status_email(progress)
        
        # Still update dashboard
        update_dashboard(current_price, None, None, None, progress)
        print("\n✅ Dashboard updated (trading blocked)")
        return
    
    # Check economic calendar
    has_event, event_name = is_high_impact_event_today()
    if has_event:
        print(f"\n⚠️ HIGH IMPACT EVENT: {event_name}")
        print("   Skipping trades today")
        send_status_email(progress)
        update_dashboard(current_price, None, None, None, progress)
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
            send_exit_alert(trade_record)
            
            # Update progress after closing
            progress = update_progress()
        else:
            print(f"\n✅ Position still active")
            print(f"   Stop: ${existing_position['stop_price']:.2f}")
            print(f"   Target: ${existing_position['target_price']:.2f}")
    
    # Check for new signals
    else:
        print("\n🔍 Checking for EMA crossover signal...")
        data = get_historical_data()
        
        if data is not None and not data.empty:
            # Calculate ATR for dynamic stops
            use_atr = get_config_value('entry_conditions.use_atr_stops', USE_ATR_STOPS)
            atr = calculate_atr(data) if use_atr else STOP_POINTS
            print(f"   Current ATR: {atr:.2f} points")
            
            signal, signal_details = check_signal(data)
            
            if signal:
                print(f"\n🎯 SIGNAL DETECTED: {signal}")
                print(f"   Fast EMA (9): {signal_details['ema_fast']:.2f}")
                print(f"   Slow EMA (21): {signal_details['ema_slow']:.2f}")
                
                position = open_paper_trade(signal, signal_details['price'], atr)
                
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
    
    # Get recent trades for dashboard
    recent_trades = None
    if TRADES_FILE.exists():
        try:
            df = pd.read_csv(TRADES_FILE)
            recent_trades = df.tail(10).to_dict('records') if not df.empty else None
        except Exception:
            pass
    
    # Send daily status at end of day (after 4 PM)
    current_hour = datetime.now().hour
    if current_hour >= 16:
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
    print(f"   Total Profit: ${progress['total_profit']:.2f}")
    print(f"   Trading Days: {progress['trading_days']}")
    print("=" * 60)


if __name__ == "__main__":
    main()
