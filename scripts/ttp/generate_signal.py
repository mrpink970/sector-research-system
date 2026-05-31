#!/usr/bin/env python3
"""
Trade The Pool - SOXX Signal Generator with Window Analysis
Analyzes all 15-minute data since last email
Always sends email with market context
Supports scheduled and manual runs
ADDED: Volume ratio tracking and conviction scoring
ADDED: ATR-based Green Day detection (replaces fixed 0.5%)
ADDED: Pre-market volume confirmation
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
import sys

# Add parent directory to path for imports
sys.path.insert(0, str(Path(__file__).parent.parent.parent))

# Paths
CONFIG_PATH = Path("config/ttp_config.yaml")
DATA_DIR = Path("data/ttp")
SIGNALS_PATH = DATA_DIR / "signals.csv"
LAST_RUN_PATH = DATA_DIR / "last_email_run.txt"

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
        mail_username = os.environ.get("MAIL_USERNAME")
        if mail_username:
            return [mail_username]
        return []
    return recipients


def get_last_run_time():
    """Get timestamp of last email sent"""
    if LAST_RUN_PATH.exists():
        with open(LAST_RUN_PATH, 'r') as f:
            timestamp_str = f.read().strip()
            try:
                return datetime.fromisoformat(timestamp_str)
            except:
                return datetime.now().replace(hour=9, minute=30, second=0, microsecond=0)
    return datetime.now().replace(hour=9, minute=30, second=0, microsecond=0)


def save_last_run_time(dt):
    """Save timestamp of this email"""
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
    """Determine if trend is UP, FLAT, or DOWN based on start-to-end change"""
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
    """Return conviction level based on volume ratio"""
    if pd.isna(volume_ratio) or volume_ratio == 0:
        return "Unknown", ""
    
    if volume_ratio >= 1.5:
        return "High conviction", "✅"
    elif volume_ratio >= 0.8:
        return "Normal", "📊"
    else:
        return "Low volume — caution", "⚠️"


def get_premarket_conviction(premarket_ratio):
    """Return conviction level based on pre-market volume ratio"""
    if pd.isna(premarket_ratio) or premarket_ratio == 0:
        return "Unknown", ""
    
    if premarket_ratio >= 1.2:
        return "Strong pre-market interest", "🔥"
    elif premarket_ratio >= 0.7:
        return "Moderate pre-market interest", "📈"
    else:
        return "Weak pre-market interest — caution", "⚠️"


def check_green_day_atr(returns_list, atr_value, multiplier=0.5):
    """ATR-based Green Day detection"""
    if atr_value is None or len(returns_list) == 0:
        return False, None, 0.5
    
    threshold = multiplier * atr_value
    threshold_pct = round(threshold * 100, 2)
    
    for r in returns_list:
        if r >= threshold_pct:
            return True, threshold_pct, threshold_pct
    
    return False, threshold_pct, threshold_pct


def analyze_window(window_df):
    """Analyze the window and return market context with ATR and pre-market volume"""
    if window_df is None or window_df.empty:
        return None
    
    # Get SOXX returns in window
    soxx_returns = window_df['soxx_day_return_pct'].tolist()
    
    # Calculate ATR from returns
    returns_series = window_df['soxx_day_return_pct']
    atr_value = calculate_atr_from_returns(returns_series, period=14)
    
    # ATR-based Green Day detection
    green_day, threshold_pct, used_threshold = check_green_day_atr(soxx_returns, atr_value, multiplier=0.5)
    
    # Fixed threshold for comparison
    fixed_green_day = any(r >= 0.5 for r in soxx_returns)
    
    # Get pre-market volume from the most recent row
    last_row = window_df.iloc[-1]
    premarket_volume_ratio = last_row.get('soxx_premarket_volume_ratio', 1.0)
    if pd.isna(premarket_volume_ratio):
        premarket_volume_ratio = 1.0
    
    premarket_ok = premarket_volume_ratio >= 0.7
    
    # Final signal: Green Day AND pre-market volume OK
    final_green_day = green_day and premarket_ok
    
    # Find best and worst candles
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
    
    # Trend analysis
    trend = analyze_trend(soxx_returns)
    
    # QQQ trend
    qqq_returns = window_df['qqq_day_return_pct'].tolist()
    qqq_trend = analyze_trend(qqq_returns)
    
    # Start and end values
    window_start = window_df.iloc[0]['timestamp']
    window_end = window_df.iloc[-1]['timestamp']
    start_return = round(soxx_returns[0], 2)
    end_return = round(soxx_returns[-1], 2)
    qqq_start = round(qqq_returns[0], 2)
    qqq_end = round(qqq_returns[-1], 2)
    
    # Volume ratio and conviction
    volume_ratio = last_row.get('soxx_volume_ratio', 1.0)
    if pd.isna(volume_ratio):
        volume_ratio = 1.0
    volume_conviction, volume_icon = get_volume_conviction(volume_ratio)
    
    # Pre-market conviction
    premarket_conviction, premarket_icon = get_premarket_conviction(premarket_volume_ratio)
    
    soxx_current = {
        'price': round(last_row['soxx_price'], 2) if not pd.isna(last_row['soxx_price']) else 0,
        'day_return': round(last_row['soxx_day_return_pct'], 2) if not pd.isna(last_row['soxx_day_return_pct']) else 0,
        'rsi': round(last_row['soxx_rsi'], 1) if not pd.isna(last_row['soxx_rsi']) else 50,
        'above_ma20': last_row['soxx_above_ma20'] if not pd.isna(last_row.get('soxx_above_ma20')) else False,
        'volume_ratio': round(volume_ratio, 1),
        'volume_conviction': volume_conviction,
        'volume_icon': volume_icon,
        'premarket_volume_ratio': round(premarket_volume_ratio, 1),
        'premarket_conviction': premarket_conviction,
        'premarket_icon': premarket_icon
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
        'green_day': final_green_day,
        'raw_green_day': green_day,
        'premarket_ok': premarket_ok,
        'fixed_green_day': fixed_green_day,
        'best_green_return': best_green,
        'atr_value': atr_value,
        'atr_threshold_pct': used_threshold,
        'premarket_volume_ratio': premarket_volume_ratio,
        'window_df': window_df
    }


def get_action_recommendation(analysis):
    """Generate action recommendation based on analysis"""
    if not analysis:
        return "⚠️ No data available. Check data collection."
    
    if not analysis['green_day']:
        if not analysis['raw_green_day']:
            return f"🔴 No Green Day conditions met (ATR threshold: {analysis['atr_threshold_pct']}%). Wait for next window."
        elif not analysis['premarket_ok']:
            return f"⚠️ Green Day detected but pre-market volume weak (ratio: {analysis['premarket_volume_ratio']}x). Wait for stronger pre-market confirmation."
    
    volume_ratio = analysis['soxx_current']['volume_ratio']
    volume_conviction = analysis['soxx_current']['volume_conviction']
    premarket_ratio = analysis['premarket_volume_ratio']
    premarket_conviction = analysis['soxx_current']['premarket_conviction']
    
    if volume_ratio < 0.8:
        return f"⚠️ Green Day confirmed but low volume ({volume_conviction}). Pre-market: {premarket_conviction}. Consider smaller position."
    elif analysis['trend'] == "UP" and analysis['qqq_trend'] != "DOWN":
        return f"✅ Green Day with building momentum (ATR: {analysis['atr_threshold_pct']}%). Volume: {volume_conviction}. Pre-market: {premarket_conviction}. Ready to buy on pullback."
    elif analysis['trend'] == "FLAT" and analysis['qqq_trend'] != "DOWN":
        return f"🟡 Green Day but choppy (ATR: {analysis['atr_threshold_pct']}%). Volume: {volume_conviction}. Pre-market: {premarket_conviction}. Wait for confirmation candle."
    elif analysis['trend'] == "DOWN":
        return f"⚠️ Green Day but momentum faded (ATR: {analysis['atr_threshold_pct']}%). Volume: {volume_conviction}. Pre-market: {premarket_conviction}. Caution - wait for reversal."
    else:
        return f"🟢 Green Day confirmed (ATR: {analysis['atr_threshold_pct']}%). Volume: {volume_conviction}. Pre-market: {premarket_conviction}. Use standard entry rules."


def send_email(analysis, recipients, is_manual=False):
    """Send analysis email"""
    mail_username = os.environ.get("MAIL_USERNAME")
    mail_password = os.environ.get("MAIL_PASSWORD")
    
    if not mail_username or not mail_password:
        print("❌ Email credentials not found")
        return False
    
    if not recipients:
        print("❌ No email recipients configured")
        return False
    
    date_str = datetime.now().strftime("%Y-%m-%d %I:%M %p ET")
    separator = "━" * 60
    
    if not analysis:
        subject = f"⚠️ TTP DATA ERROR - No Data Available ({date_str})"
        body = f"""
{separator}
  TTP DECISION ENGINE - DATA ERROR - {date_str}
{separator}

❌ No price data found in window.

Run collect_data.py first and ensure data is being collected.

{separator}
"""
    else:
        signal_icon = "🟢" if analysis['green_day'] else ("🟡" if analysis['raw_green_day'] else "🔴")
        
        if analysis['green_day']:
            signal_text = "GREEN DAY - CONFIRMED"
        elif analysis['raw_green_day'] and not analysis['premarket_ok']:
            signal_text = "GREEN DAY - PRE-MARKET REJECTED"
        else:
            signal_text = "RED DAY - NO TRADE"
        
        atr_info = f"ATR: {analysis['atr_value']}% | Threshold: ≥{analysis['atr_threshold_pct']}%"
        
        if analysis['green_day']:
            best_info = f"Best signal: {analysis['best_candle']['time']} (+{analysis['best_green_return']}%) | {atr_info} | Pre-market: {analysis['premarket_volume_ratio']}x ✅"
        elif analysis['raw_green_day'] and not analysis['premarket_ok']:
            best_info = f"Best signal: {analysis['best_candle']['time']} (+{analysis['best_green_return']}%) | {atr_info} | Pre-market: {analysis['premarket_volume_ratio']}x ❌ (needs ≥0.7x)"
        else:
            best_info = f"No qualifying candles (ATR threshold: {analysis['atr_threshold_pct']}%) | Fixed 0.5%: {'YES' if analysis['fixed_green_day'] else 'NO'}"
        
        subject = f"{signal_icon} TTP MARKET ANALYSIS - {signal_text} - {date_str}"
        
        action = get_action_recommendation(analysis)
        
        body = f"""
{separator}
  TTP DECISION ENGINE - {date_str}
{separator}

{signal_icon} MARKET CONDITION: {signal_text}
   {best_info}

📈 WINDOW ANALYSIS ({analysis['window_start']} → {analysis['window_end']} ET):
   Trend: {analysis['trend']} (start: {analysis['start_return']}% → end: {analysis['end_return']}%)
   Best candle: {analysis['best_candle']['time']} (+{analysis['best_candle']['return']}%)
   Worst candle: {analysis['worst_candle']['time']} ({analysis['worst_candle']['return']}%)
   Candles in window: {analysis['num_candles']}

📊 VOLATILITY (ATR):
   ATR (14-period): {analysis['atr_value']}%
   Green Day threshold: ≥{analysis['atr_threshold_pct']}% (0.5x ATR)
   Fixed 0.5% threshold would show: {'GREEN DAY' if analysis['fixed_green_day'] else 'RED DAY'}

📊 PRE-MARKET CONFIRMATION:
   Pre-market volume ratio: {analysis['premarket_volume_ratio']}x {analysis['soxx_current']['premarket_icon']}
   Status: {'✅ CONFIRMED' if analysis['premarket_ok'] else '❌ REJECTED (needs ≥0.7x)'}

📊 QQQ CONTEXT:
   Trend: {analysis['qqq_trend']} (start: {analysis['qqq_start']}% → end: {analysis['qqq_end']}%)
   Current: +{analysis['qqq_current']['day_return']}% | Price: ${analysis['qqq_current']['price']}
   RSI: {analysis['qqq_current']['rsi']} | Above MA20: {'YES' if analysis['qqq_current']['above_ma20'] else 'NO'}

📊 SOXX CONTEXT:
   Current: +{analysis['soxx_current']['day_return']}% | Price: ${analysis['soxx_current']['price']}
   RSI: {analysis['soxx_current']['rsi']} | Above MA20: {'YES' if analysis['soxx_current']['above_ma20'] else 'NO'}
   Volume ratio: {analysis['soxx_current']['volume_ratio']}x {analysis['soxx_current']['volume_icon']} ({analysis['soxx_current']['volume_conviction']})

{action}

{separator}

⚠️ RULE REMINDERS:
   ❌ Max Drawdown: $140 (7%)
   ❌ Max Profit Per Trade: $90 (30% of $300)
   ❌ Daily Loss Limit: $60
   ✅ Minimum 5 trades to pass
   ✅ ATR-based Green Day: Adapts to market volatility
   ✅ Pre-market volume: Requires ≥0.7x average

{separator}

🔗 TRADE ENTRY CALCULATOR:
   https://mrpink970.github.io/sector-research-system/docs/ttp/trade_entry.html

{separator}
"""
    
    msg = EmailMessage()
    msg.set_content(body)
    msg["Subject"] = subject
    msg["From"] = formataddr(("TTP Decision Engine", mail_username))
    msg["To"] = ", ".join(recipients)
    
    try:
        with smtplib.SMTP_SSL("smtp.gmail.com", 465) as smtp:
            smtp.login(mail_username, mail_password)
            smtp.send_message(msg)
        print(f"✅ Analysis email sent to {len(recipients)} recipient(s)")
        return True
    except Exception as e:
        print(f"❌ Failed to send email: {e}")
        return False


def save_signal(analysis):
    """Save signal analysis to CSV"""
    if not analysis:
        return
    
    signal_type = 'GREEN' if analysis['green_day'] else ('PREMARKET_REJECTED' if analysis['raw_green_day'] else 'RED')
    
    new_row = pd.DataFrame([{
        'timestamp': datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        'signal': signal_type,
        'window_start': analysis['window_start'],
        'window_end': analysis['window_end'],
        'trend': analysis['trend'],
        'best_candle_time': analysis['best_candle']['time'],
        'best_candle_return': analysis['best_candle']['return'],
        'worst_candle_return': analysis['worst_candle']['return'],
        'soxx_current_return': analysis['soxx_current']['day_return'],
        'soxx_volume_ratio': analysis['soxx_current']['volume_ratio'],
        'soxx_premarket_ratio': analysis['premarket_volume_ratio'],
        'qqq_current_return': analysis['qqq_current']['day_return'],
        'action': get_action_recommendation(analysis),
        'atr_value': analysis['atr_value'],
        'atr_threshold': analysis['atr_threshold_pct'],
        'fixed_green_day': analysis['fixed_green_day'],
        'premarket_ok': analysis['premarket_ok']
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
    print("TTP MARKET ANALYSIS ENGINE (ATR + Pre-Market Volume)")
    print("=" * 60)
    
    config = load_config()
    recipients = get_email_recipients(config)
    print(f"📧 Email recipients: {len(recipients)}")
    
    if compliance_available:
        can_enter, compliance_reason = can_enter_swing_trade(config)
    else:
        can_enter, compliance_reason = True, "Compliance checks skipped"
    
    if not can_enter:
        print(f"\n⚠️ COMPLIANCE RESTRICTION: {compliance_reason}")
        analysis = None
        send_email(analysis, recipients)
        return
    
    last_run = get_last_run_time()
    current_time = datetime.now()
    
    print(f"\n📅 Last email: {last_run}")
    print(f"📅 Current run: {current_time}")
    
    window_df = load_window_data(last_run, current_time)
    
    if window_df is None or window_df.empty:
        print("⚠️ No new data since last email")
        send_email(None, recipients)
        save_last_run_time(current_time)
        return
    
    print(f"📊 Found {len(window_df)} new 15-min candles")
    
    analysis = analyze_window(window_df)
    
    if analysis:
        print(f"\n📈 Analysis complete:")
        print(f"   ATR Value: {analysis['atr_value']}%")
        print(f"   Green Day threshold: ≥{analysis['atr_threshold_pct']}%")
        print(f"   Raw Green Day (ATR): {analysis['raw_green_day']}")
        print(f"   Pre-market ratio: {analysis['premarket_volume_ratio']}x")
        print(f"   Pre-market OK: {analysis['premarket_ok']}")
        print(f"   FINAL SIGNAL: {analysis['green_day']}")
        print(f"   Fixed 0.5% comparison: {analysis['fixed_green_day']}")
        print(f"   Trend: {analysis['trend']}")
        print(f"   QQQ Trend: {analysis['qqq_trend']}")
        print(f"   Volume Ratio: {analysis['soxx_current']['volume_ratio']}x")
    
    send_email(analysis, recipients)
    save_signal(analysis)
    save_last_run_time(current_time)
    
    print("\n✅ Analysis complete")
    print("=" * 60)


if __name__ == "__main__":
    main()
