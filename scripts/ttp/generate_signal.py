#!/usr/bin/env python3
"""
Trade The Pool - SOXX Signal Generator with Window Analysis
Analyzes all 15-minute data since last email
Always sends email with market context
ADDED: ATR-based Green Day detection (replaces fixed 0.5%)
ADDED: pytz for correct Eastern Time (no more UTC offset)
"""

import os
import smtplib
from email.message import EmailMessage
from email.utils import formataddr
import pandas as pd
import numpy as np
import yaml
from pathlib import Path
from datetime import datetime, timedelta
import pytz
import sys

sys.path.insert(0, str(Path(__file__).parent.parent.parent))

CONFIG_PATH = Path("config/ttp_config.yaml")
DATA_DIR = Path("data/ttp")
SIGNALS_PATH = DATA_DIR / "signals.csv"
LAST_RUN_PATH = DATA_DIR / "last_email_run.txt"

# Eastern Time Zone
ET = pytz.timezone('US/Eastern')

def get_eastern_now():
    """Return current datetime in Eastern Time"""
    return datetime.now(ET)

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
    recipients = config.get('email_recipients', [])
    if not recipients:
        mail_username = os.environ.get("MAIL_USERNAME")
        if mail_username:
            return [mail_username]
        return []
    return recipients


def get_last_run_time():
    """Get timestamp of last email sent (in Eastern Time)"""
    if LAST_RUN_PATH.exists():
        with open(LAST_RUN_PATH, 'r') as f:
            timestamp_str = f.read().strip()
            try:
                # Parse and make timezone-aware
                dt = datetime.fromisoformat(timestamp_str)
                if dt.tzinfo is None:
                    dt = ET.localize(dt)
                return dt
            except:
                # Default to today at 9:30 AM ET
                return get_eastern_now().replace(hour=9, minute=30, second=0, microsecond=0)
    # No previous run - default to today at 9:30 AM ET
    return get_eastern_now().replace(hour=9, minute=30, second=0, microsecond=0)


def save_last_run_time(dt):
    """Save timestamp of this email (store as ISO string)"""
    with open(LAST_RUN_PATH, 'w') as f:
        f.write(dt.isoformat())


def load_window_data(last_run, current_time):
    """Load all 15-minute data rows between last_run and current_time"""
    log_path = DATA_DIR / "price_log.csv"
    if not log_path.exists():
        return None
    
    df = pd.read_csv(log_path)
    if df.empty:
        return None
    
    df['timestamp_dt'] = pd.to_datetime(df['timestamp'])
    # Make timezone-aware for comparison
    df['timestamp_dt'] = df['timestamp_dt'].dt.tz_localize(ET)
    
    mask = (df['timestamp_dt'] > last_run) & (df['timestamp_dt'] <= current_time)
    window_df = df[mask].copy()
    
    if window_df.empty:
        return None
    
    return window_df


def calculate_atr_from_returns(returns_series, period=14):
    """Calculate ATR from return series using rolling standard deviation"""
    if len(returns_series) < period:
        return None
    
    rolling_std = returns_series.rolling(window=period).std()
    atr_value = rolling_std.iloc[-1]
    
    if pd.isna(atr_value):
        return None
    
    return round(atr_value, 4)


def analyze_trend(returns_list):
    if len(returns_list) < 2:
        return "FLAT"
    
    start_return = returns_list[0]
    end_return = returns_list[-1]
    change = end_return - start_return
    
    if change > 0.2:
        return "UP"
    elif change < -0.2:
        return "DOWN"
    else:
        return "FLAT"


def get_volume_conviction(volume_ratio):
    if pd.isna(volume_ratio) or volume_ratio == 0:
        return "Unknown", ""
    
    if volume_ratio >= 1.5:
        return "High conviction", "✅"
    elif volume_ratio >= 0.8:
        return "Normal", "📊"
    else:
        return "Low volume — caution", "⚠️"


def check_green_day_atr(returns_list, atr_value, multiplier=0.5):
    if atr_value is None or len(returns_list) == 0:
        return False, 0.5, 0.5
    
    threshold = multiplier * atr_value
    threshold_pct = round(threshold * 100, 2)
    
    for r in returns_list:
        if r >= threshold_pct:
            return True, threshold_pct, threshold_pct
    
    return False, threshold_pct, threshold_pct


def analyze_window(window_df):
    if window_df is None or window_df.empty:
        return None
    
    soxx_returns = window_df['soxx_day_return_pct'].tolist()
    
    returns_series = window_df['soxx_day_return_pct']
    atr_value = calculate_atr_from_returns(returns_series, period=14)
    
    green_day, threshold_pct, used_threshold = check_green_day_atr(soxx_returns, atr_value, multiplier=0.5)
    
    fixed_green_day = any(r >= 0.5 for r in soxx_returns)
    
    best_idx = window_df['soxx_day_return_pct'].idxmax()
    worst_idx = window_df['soxx_day_return_pct'].idxmin()
    
    best_candle = {
        'time': window_df.loc[best_idx, 'timestamp'],
        'return': round(window_df.loc[best_idx, 'soxx_day_return_pct'], 2)
    }
    worst_candle = {
        'time': window_df.loc[worst_idx, 'timestamp'],
        'return': round(window_df.loc[worst_idx, 'soxx_day_return_pct'], 2)
    }
    
    trend = analyze_trend(soxx_returns)
    
    qqq_returns = window_df['qqq_day_return_pct'].tolist()
    qqq_trend = analyze_trend(qqq_returns)
    
    window_start = window_df.iloc[0]['timestamp']
    window_end = window_df.iloc[-1]['timestamp']
    start_return = round(soxx_returns[0], 2)
    end_return = round(soxx_returns[-1], 2)
    qqq_start = round(qqq_returns[0], 2)
    qqq_end = round(qqq_returns[-1], 2)
    
    last_row = window_df.iloc[-1]
    
    volume_ratio = last_row.get('soxx_volume_ratio', 1.0)
    if pd.isna(volume_ratio):
        volume_ratio = 1.0
    volume_conviction, volume_icon = get_volume_conviction(volume_ratio)
    
    soxx_current = {
        'price': round(last_row['soxx_price'], 2) if not pd.isna(last_row['soxx_price']) else 0,
        'day_return': round(last_row['soxx_day_return_pct'], 2) if not pd.isna(last_row['soxx_day_return_pct']) else 0,
        'rsi': round(last_row['soxx_rsi'], 1) if not pd.isna(last_row['soxx_rsi']) else 50,
        'above_ma20': last_row['soxx_above_ma20'] if not pd.isna(last_row.get('soxx_above_ma20')) else False,
        'volume_ratio': round(volume_ratio, 1),
        'volume_conviction': volume_conviction,
        'volume_icon': volume_icon
    }
    qqq_current = {
        'price': round(last_row['qqq_price'], 2) if not pd.isna(last_row['qqq_price']) else 0,
        'day_return': round(last_row['qqq_day_return_pct'], 2) if not pd.isna(last_row['qqq_day_return_pct']) else 0,
        'rsi': round(last_row['qqq_rsi'], 1) if not pd.isna(last_row['qqq_rsi']) else 50,
        'above_ma20': last_row['qqq_above_ma20'] if not pd.isna(last_row.get('qqq_above_ma20')) else False
    }
    
    best_green = max([r for r in soxx_returns if r >= used_threshold], default=None)
    
    return {
        'window_start': window_start,
        'window_end': window_end,
        'num_candles': len(window_df),
        'trend': trend,
        'start_return': start_return,
        'end_return': end_return,
        'best_candle': best_candle,
        'worst_candle': worst_candle,
        'soxx_current': soxx_current,
        'qqq_trend': qqq_trend,
        'qqq_start': qqq_start,
        'qqq_end': qqq_end,
        'qqq_current': qqq_current,
        'green_day': green_day,
        'fixed_green_day': fixed_green_day,
        'best_green_return': best_green,
        'atr_value': atr_value,
        'atr_threshold_pct': used_threshold,
        'window_df': window_df
    }


def get_action_recommendation(analysis):
    if not analysis:
        return "⚠️ No data available. Check data collection."
    
    if not analysis['green_day']:
        return f"🔴 No Green Day conditions met (ATR threshold: {analysis['atr_threshold_pct']}%). Wait for next window."
    
    volume_ratio = analysis['soxx_current']['volume_ratio']
    volume_conviction = analysis['soxx_current']['volume_conviction']
    
    if volume_ratio < 0.8:
        return f"⚠️ Green Day but {volume_conviction}. Wait for volume confirmation."
    elif analysis['trend'] == "UP" and analysis['qqq_trend'] != "DOWN":
        return f"✅ Green Day with building momentum (ATR: {analysis['atr_threshold_pct']}%). Volume: {volume_conviction}. Ready to buy on pullback."
    elif analysis['trend'] == "FLAT" and analysis['qqq_trend'] != "DOWN":
        return f"🟡 Green Day but choppy (ATR: {analysis['atr_threshold_pct']}%). Volume: {volume_conviction}. Wait for confirmation candle."
    elif analysis['trend'] == "DOWN":
        return f"⚠️ Green Day but momentum faded (ATR: {analysis['atr_threshold_pct']}%). Volume: {volume_conviction}. Caution - wait for reversal."
    else:
        return f"🟢 Green Day confirmed (ATR: {analysis['atr_threshold_pct']}%). Volume: {volume_conviction}. Use standard entry rules."


def send_email(analysis, recipients):
    mail_username = os.environ.get("MAIL_USERNAME")
    mail_password = os.environ.get("MAIL_PASSWORD")
    
    if not mail_username or not mail_password:
        print("❌ Email credentials not found")
        return False
    
    if not recipients:
        print("❌ No email recipients configured")
        return False
    
    # Use Eastern Time for email timestamp
    now_et = get_eastern_now()
    date_str = now_et.strftime("%Y-%m-%d %I:%M %p ET")
    separator = "━" * 60
    
    if not analysis:
        subject = f"⚠️ TTP DATA ERROR - No Data Available ({date_str})"
        body = f"{separator}\n  TTP DECISION ENGINE - DATA ERROR - {date_str}\n{separator}\n❌ No price data found.\n{separator}"
    else:
        signal_icon = "🟢" if analysis['green_day'] else "🔴"
        signal_text = "GREEN DAY" if analysis['green_day'] else "RED DAY"
        
        atr_info = f"ATR: {analysis['atr_value']}% | Threshold: ≥{analysis['atr_threshold_pct']}%"
        
        if analysis['green_day']:
            best_info = f"Best: {analysis['best_candle']['time']} (+{analysis['best_green_return']}%) | {atr_info}"
        else:
            best_info = f"No qualifying candles (threshold: {analysis['atr_threshold_pct']}%) | Fixed 0.5%: {'YES' if analysis['fixed_green_day'] else 'NO'}"
        
        subject = f"{signal_icon} TTP ANALYSIS - {signal_text} - {date_str}"
        action = get_action_recommendation(analysis)
        
        body = f"""
{separator}
  TTP DECISION ENGINE - {date_str}
{separator}

{signal_icon} SIGNAL: {signal_text}
   {best_info}

📈 WINDOW ({analysis['window_start']} → {analysis['window_end']}):
   Trend: {analysis['trend']} ({analysis['start_return']}% → {analysis['end_return']}%)
   Best: {analysis['best_candle']['time']} (+{analysis['best_candle']['return']}%)
   Worst: {analysis['worst_candle']['time']} ({analysis['worst_candle']['return']}%)

📊 ATR (Volatility):
   ATR (14): {analysis['atr_value']}%
   Green Day: ≥{analysis['atr_threshold_pct']}% (0.5x ATR)
   Fixed 0.5%: {'GREEN' if analysis['fixed_green_day'] else 'RED'}

📊 QQQ:
   Trend: {analysis['qqq_trend']} ({analysis['qqq_start']}% → {analysis['qqq_end']}%)
   Current: {analysis['qqq_current']['day_return']}% | RSI: {analysis['qqq_current']['rsi']}

📊 SOXX:
   Current: {analysis['soxx_current']['day_return']}% | RSI: {analysis['soxx_current']['rsi']}
   Volume: {analysis['soxx_current']['volume_ratio']}x {analysis['soxx_current']['volume_icon']}

{action}

{separator}
⚠️ RULES: $140 DD | $90/trade | $60/day | 5 trades min
🔗 https://mrpink970.github.io/sector-research-system/docs/ttp/trade_entry.html
{separator}
"""
    
    msg = EmailMessage()
    msg.set_content(body)
    msg["Subject"] = subject
    msg["From"] = ("TTP Engine", mail_username)
    msg["To"] = ", ".join(recipients)
    
    try:
        with smtplib.SMTP_SSL("smtp.gmail.com", 465) as smtp:
            smtp.login(mail_username, mail_password)
            smtp.send_message(msg)
        print(f"✅ Email sent to {len(recipients)} recipient(s)")
        return True
    except Exception as e:
        print(f"❌ Failed: {e}")
        return False


def save_signal(analysis):
    if not analysis:
        return
    
    signal_type = 'GREEN' if analysis['green_day'] else 'RED'
    
    # Use Eastern Time for signal timestamp
    now_et = get_eastern_now()
    
    new_row = pd.DataFrame([{
        'timestamp': now_et.strftime("%Y-%m-%d %H:%M:%S"),
        'signal': signal_type,
        'window_start': analysis['window_start'],
        'window_end': analysis['window_end'],
        'trend': analysis['trend'],
        'best_candle_return': analysis['best_candle']['return'],
        'soxx_current_return': analysis['soxx_current']['day_return'],
        'soxx_volume_ratio': analysis['soxx_current']['volume_ratio'],
        'atr_value': analysis['atr_value'],
        'atr_threshold': analysis['atr_threshold_pct'],
        'fixed_green_day': analysis['fixed_green_day']
    }])
    
    if SIGNALS_PATH.exists():
        existing = pd.read_csv(SIGNALS_PATH)
        updated = pd.concat([existing, new_row], ignore_index=True).tail(500)
    else:
        DATA_DIR.mkdir(parents=True, exist_ok=True)
        updated = new_row
    
    updated.to_csv(SIGNALS_PATH, index=False)


def main():
    print("=" * 60)
    print("TTP ANALYSIS ENGINE (ATR-Based Green Day)")
    print("=" * 60)
    
    config = load_config()
    recipients = get_email_recipients(config)
    print(f"📧 Recipients: {len(recipients)}")
    
    if compliance_available:
        can_enter, reason = can_enter_swing_trade(config)
        if not can_enter:
            print(f"⚠️ Compliance: {reason}")
            send_email(None, recipients)
            return
    
    last_run = get_last_run_time()
    current_time = get_eastern_now()
    
    print(f"📅 Last run (ET): {last_run.strftime('%Y-%m-%d %I:%M:%S %p')}")
    print(f"📅 Current (ET): {current_time.strftime('%Y-%m-%d %I:%M:%S %p')}")
    
    window_df = load_window_data(last_run, current_time)
    
    if window_df is None or window_df.empty:
        print("⚠️ No new data since last email")
        send_email(None, recipients)
        save_last_run_time(current_time)
        return
    
    print(f"📊 Found {len(window_df)} new candles")
    
    analysis = analyze_window(window_df)
    
    if analysis:
        print(f"\n📈 Results:")
        print(f"   ATR: {analysis['atr_value']}%")
        print(f"   Threshold: ≥{analysis['atr_threshold_pct']}%")
        print(f"   Green Day (ATR): {analysis['green_day']}")
        print(f"   Green Day (fixed): {analysis['fixed_green_day']}")
        print(f"   Volume ratio: {analysis['soxx_current']['volume_ratio']}x")
    
    send_email(analysis, recipients)
    save_signal(analysis)
    save_last_run_time(current_time)
    
    print("\n✅ Complete")
    print("=" * 60)


if __name__ == "__main__":
    main()
