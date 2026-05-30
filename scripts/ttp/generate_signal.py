#!/usr/bin/env python3
"""
Trade The Pool - SOXX Signal Generator with Window Analysis
Analyzes all 15-minute data since last email
Always sends email with market context
Supports scheduled and manual runs
"""

import os
import smtplib
from email.message import EmailMessage
from email.utils import formataddr
import pandas as pd
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
                # Default to today at 9:30 AM if invalid
                return datetime.now().replace(hour=9, minute=30, second=0, microsecond=0)
    # No previous run - default to today at 9:30 AM ET
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
    
    # Parse timestamps
    df['timestamp_dt'] = pd.to_datetime(df['timestamp'])
    
    # Filter to window
    mask = (df['timestamp_dt'] > last_run) & (df['timestamp_dt'] <= current_time)
    window_df = df[mask].copy()
    
    if window_df.empty:
        return None
    
    return window_df


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


def analyze_window(window_df):
    """Analyze the window and return market context"""
    if window_df is None or window_df.empty:
        return None
    
    # SOXX returns in window
    soxx_returns = window_df['soxx_day_return_pct'].tolist()
    
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
    
    # Current state (last row)
    last_row = window_df.iloc[-1]
    soxx_current = {
        'price': round(last_row['soxx_price'], 2),
        'day_return': round(last_row['soxx_day_return_pct'], 2),
        'rsi': round(last_row['soxx_rsi'], 1),
        'above_ma20': last_row['soxx_above_ma20']
    }
    qqq_current = {
        'price': round(last_row['qqq_price'], 2),
        'day_return': round(last_row['qqq_day_return_pct'], 2),
        'rsi': round(last_row['qqq_rsi'], 1),
        'above_ma20': last_row['qqq_above_ma20']
    }
    
    # Check if Green Day (any candle ≥ 0.5%)
    green_day = any(r >= 0.5 for r in soxx_returns)
    best_green = max([r for r in soxx_returns if r >= 0.5], default=None)
    
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
        'best_green_return': best_green,
        'window_df': window_df
    }


def get_action_recommendation(analysis):
    """Generate action recommendation based on analysis"""
    if not analysis:
        return "⚠️ No data available. Check data collection."
    
    if not analysis['green_day']:
        return "🔴 No Green Day conditions met. Wait for next window."
    
    # Green Day exists
    if analysis['trend'] == "UP" and analysis['qqq_trend'] != "DOWN":
        return "✅ Green Day with building momentum. Ready to buy on pullback confirmation."
    elif analysis['trend'] == "FLAT" and analysis['qqq_trend'] != "DOWN":
        return "🟡 Green Day but choppy. Wait for pullback and confirmation candle."
    elif analysis['trend'] == "DOWN":
        return "⚠️ Green Day occurred but momentum faded. Caution - wait for reversal confirmation."
    else:
        return "🟢 Green Day confirmed. Use standard entry rules."


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
        body = f"{separator}\n  TTP DECISION ENGINE - DATA ERROR - {date_str}\n{separator}\n\n❌ No price data found in window.\n\nRun collect_data.py first and ensure data is being collected."
    else:
        signal_icon = "🟢" if analysis['green_day'] else "🔴"
        signal_text = "GREEN DAY" if analysis['green_day'] else "RED DAY"
        best_info = f"Best signal: {analysis['best_candle']['time']} (+{analysis['best_green_return']}%)" if analysis['green_day'] else "No qualifying candles"
        
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

📊 QQQ CONTEXT:
   Trend: {analysis['qqq_trend']} (start: {analysis['qqq_start']}% → end: {analysis['qqq_end']}%)
   Current: +{analysis['qqq_current']['day_return']}% | Price: ${analysis['qqq_current']['price']}
   RSI: {analysis['qqq_current']['rsi']} | Above MA20: {'YES' if analysis['qqq_current']['above_ma20'] else 'NO'}

📊 SOXX CONTEXT:
   Current: +{analysis['soxx_current']['day_return']}% | Price: ${analysis['soxx_current']['price']}
   RSI: {analysis['soxx_current']['rsi']} | Above MA20: {'YES' if analysis['soxx_current']['above_ma20'] else 'NO'}

{action}

{separator}

⚠️ RULE REMINDERS:
   ❌ Max Drawdown: $140 (7%)
   ❌ Max Profit Per Trade: $90 (30% of $300)
   ❌ Daily Loss Limit: $60
   ✅ Minimum 5 trades to pass

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
    
    signal_type = 'GREEN' if analysis['green_day'] else 'RED'
    
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
        'qqq_current_return': analysis['qqq_current']['day_return'],
        'action': get_action_recommendation(analysis)
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
    print("TTP MARKET ANALYSIS ENGINE (Window-Based)")
    print("=" * 60)
    
    config = load_config()
    recipients = get_email_recipients(config)
    print(f"📧 Email recipients: {len(recipients)}")
    
    # Check compliance first
    if compliance_available:
        can_enter, compliance_reason = can_enter_swing_trade(config)
    else:
        can_enter, compliance_reason = True, "Compliance checks skipped"
    
    if not can_enter:
        print(f"\n⚠️ COMPLIANCE RESTRICTION: {compliance_reason}")
        # Still send email but with warning
        analysis = None
        send_email(None, recipients)
        return
    
    # Get last run time and current time
    last_run = get_last_run_time()
    current_time = datetime.now()
    
    print(f"\n📅 Last email: {last_run}")
    print(f"📅 Current run: {current_time}")
    
    # Load window data
    window_df = load_window_data(last_run, current_time)
    
    if window_df is None or window_df.empty:
        print("⚠️ No new data since last email")
        # Still send email to indicate no data
        send_email(None, recipients)
        save_last_run_time(current_time)
        return
    
    print(f"📊 Found {len(window_df)} new 15-min candles")
    
    # Analyze window
    analysis = analyze_window(window_df)
    
    if analysis:
        print(f"\n📈 Analysis complete:")
        print(f"   Green Day: {analysis['green_day']}")
        print(f"   Trend: {analysis['trend']}")
        print(f"   Best candle: {analysis['best_candle']['time']} (+{analysis['best_candle']['return']}%)")
        print(f"   QQQ Trend: {analysis['qqq_trend']}")
    
    # Send email
    send_email(analysis, recipients)
    
    # Save signal to CSV
    save_signal(analysis)
    
    # Update last run time
    save_last_run_time(current_time)
    
    print("\n✅ Analysis complete")
    print("=" * 60)


if __name__ == "__main__":
    main()
