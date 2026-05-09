#!/usr/bin/env python3
"""
Decision Engine - Morning analysis and trade recommendations
Runs at 10:00 AM ET to capture opening action
Saves decision log for later review and backtesting
"""

import os
import smtplib
from email.message import EmailMessage
from datetime import datetime, timedelta
from pathlib import Path
from typing import Dict, List, Tuple, Optional

import pandas as pd
import yfinance as yf
import yaml


# ============================================================
# CONFIGURATION
# ============================================================
CONFIG_PATH = Path("config/master_config.yaml")


def load_config() -> dict:
    """Load master configuration file"""
    if not CONFIG_PATH.exists():
        raise FileNotFoundError(f"Config not found: {CONFIG_PATH}")
    
    with open(CONFIG_PATH, "r", encoding="utf-8") as f:
        return yaml.safe_load(f)


def safe_float(value, default=0.0) -> float:
    """Safely convert to float"""
    if value is None:
        return default
    try:
        return float(value)
    except (TypeError, ValueError):
        return default


# ============================================================
# DATA FETCHING
# ============================================================

def fetch_yesterdays_close(ticker: str) -> float:
    """Fetch yesterday's closing price"""
    try:
        stock = yf.Ticker(ticker)
        hist = stock.history(period="2d")
        if len(hist) >= 2:
            return float(hist['Close'].iloc[-2])
        elif len(hist) == 1:
            return float(hist['Close'].iloc[-1])
        return 0.0
    except Exception as e:
        print(f"Error fetching {ticker}: {e}")
        return 0.0


def fetch_current_price(ticker: str) -> float:
    """Fetch current price (10 AM ET, after free data delay)"""
    try:
        stock = yf.Ticker(ticker)
        hist = stock.history(period="1d")
        if not hist.empty:
            return float(hist['Close'].iloc[-1])
        return 0.0
    except Exception as e:
        print(f"Error fetching current {ticker}: {e}")
        return 0.0


def calculate_ma50(ticker: str) -> float:
    """Calculate 50-day moving average"""
    try:
        stock = yf.Ticker(ticker)
        hist = stock.history(period="2mo")
        if len(hist) >= 50:
            ma50 = hist['Close'].tail(50).mean()
            return float(ma50)
        return 0.0
    except Exception as e:
        print(f"Error calculating MA50 for {ticker}: {e}")
        return 0.0


# ============================================================
# SYSTEM PERFORMANCE DATA
# ============================================================

def get_system_performance() -> Dict[str, dict]:
    """Read performance data from all three systems"""
    systems = {}
    
    # Sector system
    perf_path = Path("data/paper_performance.csv")
    if perf_path.exists():
        try:
            df = pd.read_csv(perf_path)
            if not df.empty:
                row = df.iloc[0]
                systems['sector'] = {
                    'name': 'Sector System',
                    'return': safe_float(row.get('total_return_pct', 0)),
                    'win_rate': safe_float(row.get('win_rate_pct', 0)),
                    'trades': int(safe_float(row.get('total_trades', 0))),
                }
        except Exception:
            pass
    
    # 2 ETF system
    perf_path = Path("data/4_etf/etf_paper_performance.csv")
    if perf_path.exists():
        try:
            df = pd.read_csv(perf_path)
            if not df.empty:
                row = df.iloc[0]
                total_pl = safe_float(row.get('total_gross_pl', 0))
                start_balance = 5000
                total_equity = start_balance + total_pl
                # Get open position value from positions file
                pos_path = Path("data/4_etf/etf_paper_positions.csv")
                if pos_path.exists():
                    pos_df = pd.read_csv(pos_path)
                    if not pos_df.empty:
                        for _, pos in pos_df.iterrows():
                            shares = safe_float(pos.get('shares', 0))
                            highest = safe_float(pos.get('highest_price', 0))
                            entry = safe_float(pos.get('entry_price', 0))
                            total_equity += (highest - entry) * shares
                
                total_return = (total_equity / start_balance - 1) * 100
                systems['two_etf'] = {
                    'name': '2 ETF Bull',
                    'return': total_return,
                    'win_rate': safe_float(row.get('win_rate', 0)) * 100,
                    'trades': int(safe_float(row.get('total_trades', 0))),
                }
        except Exception:
            pass
    
    # Stock system
    perf_path = Path("data/stocks/stock_performance.csv")
    if perf_path.exists():
        try:
            df = pd.read_csv(perf_path)
            if not df.empty:
                total_balance = df['balance'].sum() if 'balance' in df.columns else 2000
                start_balance = 2000
                total_return = (total_balance / start_balance - 1) * 100
                total_trades = df['total_trades'].sum() if 'total_trades' in df.columns else 0
                
                # Weighted win rate
                if 'total_trades' in df.columns and 'win_rate' in df.columns:
                    total_trades_sum = df['total_trades'].sum()
                    if total_trades_sum > 0:
                        win_rate = (df['total_trades'] * df['win_rate']).sum() / total_trades_sum
                    else:
                        win_rate = 0
                else:
                    win_rate = 0
                
                systems['stock'] = {
                    'name': 'Stock System',
                    'return': total_return,
                    'win_rate': win_rate,
                    'trades': int(total_trades),
                }
        except Exception:
            pass
    
    return systems


# ============================================================
# ANALYSIS
# ============================================================

def get_regime(qqq_current: float, qqq_ma50: float) -> str:
    """Determine market regime"""
    if qqq_current > qqq_ma50:
        return "BULL"
    return "CASH"


def generate_recommendations(
    qqq_change: float,
    soxl_change: float,
    tqqq_change: float,
    regime: str,
    systems: Dict
) -> List[str]:
    """Generate trading recommendations"""
    recommendations = []
    
    # Market direction
    if qqq_change > 1.0:
        recommendations.append(f"📈 QQQ is up {qqq_change:.1f}% pre-market → bullish bias")
    elif qqq_change < -1.0:
        recommendations.append(f"📉 QQQ is down {qqq_change:.1f}% pre-market → bearish bias")
    else:
        recommendations.append(f"➡️ QQQ is flat ({qqq_change:+.1f}%) → wait for confirmation")
    
    # Sector strength (semis vs tech)
    if soxl_change > tqqq_change:
        recommendations.append(f"🔌 Semiconductors leading ({soxl_change:+.1f}%) → favor SOXL")
    else:
        recommendations.append(f"💻 Tech leading ({tqqq_change:+.1f}%) → favor TQQQ")
    
    # Current regime
    if regime == "BULL":
        recommendations.append("✅ Regime is BULL (QQQ > MA50) → entries allowed")
    else:
        recommendations.append("⚠️ Regime is CASH (QQQ < MA50) → no new entries")
    
    # System ranking
    if systems:
        sorted_systems = sorted(systems.items(), key=lambda x: x[1]['return'], reverse=True)
        best = sorted_systems[0][1]
        recommendations.append(f"🏆 Best performing system: {best['name']} (+{best['return']:.1f}%)")
    
    return recommendations


# ============================================================
# DECISION LOGGING
# ============================================================

def save_decision_log(
    timestamp: str,
    qqq_price: float,
    qqq_change: float,
    soxl_price: float,
    soxl_change: float,
    tqqq_price: float,
    tqqq_change: float,
    regime: str,
    recommendations: List[str],
    systems: Dict
) -> None:
    """Save decision log for later review and backtesting"""
    
    log_path = Path("data/decision_log.csv")
    
    # Create row
    new_row = {
        'timestamp': timestamp,
        'qqq_price': round(qqq_price, 2),
        'qqq_change_pct': round(qqq_change, 2),
        'soxl_price': round(soxl_price, 2),
        'soxl_change_pct': round(soxl_change, 2),
        'tqqq_price': round(tqqq_price, 2),
        'tqqq_change_pct': round(tqqq_change, 2),
        'regime': regime,
        'recommendations': " | ".join(recommendations),
        'sector_return_pct': round(systems.get('sector', {}).get('return', 0), 2),
        'two_etf_return_pct': round(systems.get('two_etf', {}).get('return', 0), 2),
        'stock_return_pct': round(systems.get('stock', {}).get('return', 0), 2),
        'sector_win_rate': round(systems.get('sector', {}).get('win_rate', 0), 2),
        'two_etf_win_rate': round(systems.get('two_etf', {}).get('win_rate', 0), 2),
        'stock_win_rate': round(systems.get('stock', {}).get('win_rate', 0), 2),
    }
    
    # Append to CSV
    new_df = pd.DataFrame([new_row])
    if log_path.exists():
        existing_df = pd.read_csv(log_path)
        updated_df = pd.concat([existing_df, new_df], ignore_index=True)
    else:
        updated_df = new_df
    
    updated_df.to_csv(log_path, index=False)
    print(f"✅ Decision logged to {log_path}")


# ============================================================
# EMAIL
# ============================================================

def send_email(
    qqq_current: float,
    qqq_close: float,
    qqq_change: float,
    soxl_current: float,
    soxl_close: float,
    soxl_change: float,
    tqqq_current: float,
    tqqq_close: float,
    tqqq_change: float,
    regime: str,
    recommendations: List[str],
    recipients: List[str]
) -> bool:
    """Send decision engine email"""
    mail_username = os.environ.get("MAIL_USERNAME")
    mail_password = os.environ.get("MAIL_PASSWORD")
    
    if not mail_username or not mail_password:
        print("❌ Email credentials not found")
        return False
    
    date_str = datetime.now().strftime("%Y-%m-%d %H:%M")
    
    # Build recommendations text
    rec_text = "\n".join([f"  {r}" for r in recommendations])
    
    body = f"""
═══════════════════════════════════════════════════════════
  DECISION ENGINE - {date_str} ET
═══════════════════════════════════════════════════════════

📊 PRE-MARKET / OPENING ACTION

  QQQ:   ${qqq_current:.2f} (vs close ${qqq_close:.2f}) → {qqq_change:+.2f}%
  SOXL:  ${soxl_current:.2f} (vs close ${soxl_close:.2f}) → {soxl_change:+.2f}%
  TQQQ:  ${tqqq_current:.2f} (vs close ${tqqq_close:.2f}) → {tqqq_change:+.2f}%

🎯 MARKET REGIME

  Current: {regime} (QQQ vs MA50)

💡 RECOMMENDATIONS

{rec_text}

═══════════════════════════════════════════════════════════
  Actionable before 11:00 AM ET
═══════════════════════════════════════════════════════════
"""
    
    msg = EmailMessage()
    msg.set_content(body)
    msg["Subject"] = f"🎯 Decision Engine - {datetime.now().strftime('%Y-%m-%d')}"
    msg["From"] = mail_username
    msg["To"] = recipients[0] if recipients else mail_username
    msg["Cc"] = recipients[1:] if len(recipients) > 1 else []
    
    try:
        with smtplib.SMTP_SSL("smtp.gmail.com", 465) as smtp:
            smtp.login(mail_username, mail_password)
            smtp.send_message(msg)
        print(f"✅ Decision email sent to {len(recipients)} recipient(s)")
        return True
    except Exception as e:
        print(f"❌ Failed to send email: {e}")
        return False


# ============================================================
# MAIN
# ============================================================

def main():
    print("=" * 60)
    print("DECISION ENGINE - Morning Analysis")
    print("=" * 60)
    
    # Load config for recipients
    try:
        config = load_config()
        recipients = config.get('email_recipients', [])
        print(f"✅ Loaded config, {len(recipients)} recipient(s)")
    except Exception as e:
        print(f"⚠️ Could not load config: {e}")
        recipients = ["mrpink970@gmail.com"]
    
    # Fetch current prices
    print("\n📊 Fetching market data...")
    
    qqq_ma50 = calculate_ma50("QQQ")
    
    qqq_close = fetch_yesterdays_close("QQQ")
    qqq_current = fetch_current_price("QQQ")
    qqq_change = ((qqq_current - qqq_close) / qqq_close * 100) if qqq_close > 0 else 0
    
    soxl_close = fetch_yesterdays_close("SOXL")
    soxl_current = fetch_current_price("SOXL")
    soxl_change = ((soxl_current - soxl_close) / soxl_close * 100) if soxl_close > 0 else 0
    
    tqqq_close = fetch_yesterdays_close("TQQQ")
    tqqq_current = fetch_current_price("TQQQ")
    tqqq_change = ((tqqq_current - tqqq_close) / tqqq_close * 100) if tqqq_close > 0 else 0
    
    # Get system performance
    systems = get_system_performance()
    
    # Determine regime
    regime = get_regime(qqq_current, qqq_ma50)
    
    # Generate recommendations
    recommendations = generate_recommendations(
        qqq_change, soxl_change, tqqq_change, regime, systems
    )
    
    # Print summary
    print(f"\n   QQQ:  {qqq_change:+.2f}% ({regime})")
    print(f"   SOXL: {soxl_change:+.2f}%")
    print(f"   TQQQ: {tqqq_change:+.2f}%")
    print(f"\n   Systems active: {len(systems)}")
    
    # Save decision log
    print("\n💾 Saving decision log...")
    save_decision_log(
        timestamp=datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        qqq_price=qqq_current,
        qqq_change=qqq_change,
        soxl_price=soxl_current,
        soxl_change=soxl_change,
        tqqq_price=tqqq_current,
        tqqq_change=tqqq_change,
        regime=regime,
        recommendations=recommendations,
        systems=systems
    )
    
    # Send email
    print("\n📧 Sending decision email...")
    send_email(
        qqq_current, qqq_close, qqq_change,
        soxl_current, soxl_close, soxl_change,
        tqqq_current, tqqq_close, tqqq_change,
        regime, recommendations, recipients
    )
    
    print("\n" + "=" * 60)
    print("DECISION ENGINE COMPLETE")
    print("=" * 60)


if __name__ == "__main__":
    main()
