#!/usr/bin/env python3
"""
Decision Engine - Morning analysis and trade recommendations
Runs at 10:00 AM ET
Tells you which system to hold, when to switch, when to go to cash
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
SWITCH_LOG_PATH = Path("data/decision_switch_log.csv")


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


def safe_int(value, default=0) -> int:
    """Safely convert to int"""
    return int(safe_float(value, default))


# ============================================================
# SYSTEM PERFORMANCE DATA
# ============================================================

def get_system_performance() -> Dict[str, dict]:
    """Read performance data from all systems and calculate 0-100 scores"""
    systems = {}
    
    # 1. 2 ETF System
    perf_path = Path("data/4_etf/etf_paper_performance.csv")
    pos_path = Path("data/4_etf/etf_paper_positions.csv")
    
    if perf_path.exists():
        try:
            df = pd.read_csv(perf_path)
            if not df.empty:
                row = df.iloc[0]
                total_pl = safe_float(row.get('total_gross_pl', 0))
                win_rate = safe_float(row.get('win_rate', 0)) * 100
                trades = safe_int(row.get('total_trades', 0))
                
                # Calculate return from balance
                start_balance = 5000
                total_equity = start_balance + total_pl
                
                # Get open position value if exists
                if pos_path.exists():
                    pos_df = pd.read_csv(pos_path)
                    if not pos_df.empty:
                        for _, pos in pos_df.iterrows():
                            shares = safe_float(pos.get('shares', 0))
                            highest = safe_float(pos.get('highest_price', 0))
                            entry = safe_float(pos.get('entry_price', 0))
                            total_equity += (highest - entry) * shares
                
                total_return = (total_equity / start_balance - 1) * 100
                
                # Max drawdown (estimate if not available)
                max_dd = 1.6  # From your dashboard
                
                systems['two_etf'] = {
                    'name': '2 ETF System',
                    'return': total_return,
                    'win_rate': win_rate,
                    'trades': trades,
                    'max_dd': max_dd,
                    'current_holding': 'SOXL',
                }
        except Exception as e:
            print(f"Error reading 2 ETF system: {e}")
    
    # 2. Sector System
    perf_path = Path("data/paper_performance.csv")
    pos_path = Path("data/paper_positions.csv")
    
    if perf_path.exists():
        try:
            df = pd.read_csv(perf_path)
            if not df.empty:
                row = df.iloc[0]
                systems['sector'] = {
                    'name': 'Sector System',
                    'return': safe_float(row.get('total_return_pct', 0)),
                    'win_rate': safe_float(row.get('win_rate_pct', 0)),
                    'trades': safe_int(row.get('total_trades', 0)),
                    'max_dd': safe_float(row.get('max_drawdown_pct', 0)),
                    'current_holding': 'SOXL',
                }
        except Exception as e:
            print(f"Error reading Sector system: {e}")
    
    # 3. Stock System
    perf_path = Path("data/stocks/stock_performance.csv")
    
    if perf_path.exists():
        try:
            df = pd.read_csv(perf_path)
            if not df.empty:
                total_balance = df['balance'].sum() if 'balance' in df.columns else 2000
                start_balance = 2000
                total_return = (total_balance / start_balance - 1) * 100
                total_trades = df['total_trades'].sum() if 'total_trades' in df.columns else 0
                
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
                    'max_dd': 7.8,  # From your dashboard
                    'current_holding': 'VARIOUS',
                }
        except Exception as e:
            print(f"Error reading Stock system: {e}")
    
    # 4. AI System
    perf_path = Path("data/ai/performance.csv")
    pos_path = Path("data/ai/positions.csv")
    
    if perf_path.exists():
        try:
            df = pd.read_csv(perf_path)
            if not df.empty:
                row = df.iloc[0]
                current_holding = 'N/A'
                if pos_path.exists():
                    pos_df = pd.read_csv(pos_path)
                    if not pos_df.empty and 'ticker' in pos_df.columns:
                        current_holding = pos_df.iloc[0]['ticker']
                
                systems['ai'] = {
                    'name': 'AI System',
                    'return': safe_float(row.get('total_return_pct', 0)),
                    'win_rate': safe_float(row.get('win_rate_pct', 0)),
                    'trades': safe_int(row.get('total_trades', 0)),
                    'max_dd': 0,
                    'current_holding': current_holding,
                }
        except Exception as e:
            print(f"Error reading AI system: {e}")
    
    return systems


def calculate_system_scores(systems: Dict[str, dict]) -> Dict[str, dict]:
    """
    Calculate 0-100 scores for each system based on:
    - Return (40% weight)
    - Win rate (25% weight)
    - Drawdown (25% weight - lower is better)
    - Trade count (10% weight - more trades = more confidence)
    """
    if not systems:
        return {}
    
    # Find min/max for normalization
    returns = [s['return'] for s in systems.values()]
    win_rates = [s['win_rate'] for s in systems.values()]
    drawdowns = [s['max_dd'] for s in systems.values()]
    trade_counts = [s['trades'] for s in systems.values()]
    
    # Add absolute min/max for consistency
    min_return = min(returns) if returns else 0
    max_return = max(returns) if returns else 100
    min_win_rate = min(win_rates) if win_rates else 0
    max_win_rate = max(win_rates) if win_rates else 100
    min_dd = min(drawdowns) if drawdowns else 0
    max_dd = max(drawdowns) if drawdowns else 10
    max_trades = max(trade_counts) if trade_counts else 1
    
    scores = {}
    for name, data in systems.items():
        # Normalize return (0-100)
        if max_return > min_return:
            return_score = (data['return'] - min_return) / (max_return - min_return) * 100
        else:
            return_score = 50
        
        # Normalize win rate (0-100)
        if max_win_rate > min_win_rate:
            win_rate_score = (data['win_rate'] - min_win_rate) / (max_win_rate - min_win_rate) * 100
        else:
            win_rate_score = 50
        
        # Drawdown score (lower drawdown = higher score)
        if max_dd > min_dd:
            dd_score = (1 - (data['max_dd'] - min_dd) / (max_dd - min_dd)) * 100
        else:
            dd_score = 50
        
        # Trade count score (more trades = higher score, but diminishing returns)
        trade_score = min(100, (data['trades'] / max_trades) * 100) if max_trades > 0 else 50
        
        # Weighted total
        total_score = (return_score * 0.40) + (win_rate_score * 0.25) + (dd_score * 0.25) + (trade_score * 0.10)
        
        scores[name] = {
            'name': data['name'],
            'score': round(total_score, 1),
            'return': round(data['return'], 1),
            'win_rate': round(data['win_rate'], 1),
            'max_dd': round(data['max_dd'], 1),
            'trades': data['trades'],
            'current_holding': data.get('current_holding', 'N/A'),
            'return_raw': data['return'],
            'win_rate_raw': data['win_rate'],
        }
    
    return scores


def load_previous_switch_log() -> Tuple[Optional[str], Optional[datetime]]:
    """Load last switch decision from log"""
    if not SWITCH_LOG_PATH.exists():
        return None, None
    
    try:
        df = pd.read_csv(SWITCH_LOG_PATH)
        if df.empty:
            return None, None
        
        last_row = df.iloc[-1]
        return last_row.get('recommended_system'), pd.to_datetime(last_row.get('date'))
    except Exception:
        return None, None


def save_switch_recommendation(recommendation: str, current_system: str, scores: Dict) -> None:
    """Save switch recommendation to log"""
    new_row = pd.DataFrame([{
        'date': datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        'recommended_system': recommendation,
        'current_leader': current_system,
        'top_score': max(scores.values(), key=lambda x: x['score'])['score'] if scores else 0,
    }])
    
    if SWITCH_LOG_PATH.exists():
        existing = pd.read_csv(SWITCH_LOG_PATH)
        updated = pd.concat([existing, new_row], ignore_index=True)
    else:
        updated = new_row
    
    updated.to_csv(SWITCH_LOG_PATH, index=False)


def get_business_days_since(last_date: datetime) -> int:
    """Count business days (Mon-Fri) between now and last_date"""
    if last_date is None:
        return 999  # Unlimited if never switched
    
    current = datetime.now()
    days = 0
    current_date = last_date.replace(hour=0, minute=0, second=0)
    end_date = current.replace(hour=0, minute=0, second=0)
    
    while current_date < end_date:
        current_date += timedelta(days=1)
        if current_date.weekday() < 5:  # Monday=0, Friday=4
            days += 1
    
    return days


def get_regime() -> str:
    """Determine current market regime"""
    try:
        qqq = yf.Ticker("QQQ")
        hist = qqq.history(period="3mo")
        if len(hist) >= 50:
            current_price = hist['Close'].iloc[-1]
            ma50 = hist['Close'].rolling(50).mean().iloc[-1]
            ma20 = hist['Close'].rolling(20).mean().iloc[-1]
            ma20_slope = ma20 > hist['Close'].rolling(20).mean().shift(1).iloc[-1]
            
            if current_price > ma50 and ma20_slope:
                return "BULL"
        return "CASH"
    except Exception:
        return "UNKNOWN"


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
        return 0.0
    except Exception:
        return 0.0


def fetch_current_price(ticker: str) -> float:
    """Fetch current price"""
    try:
        stock = yf.Ticker(ticker)
        hist = stock.history(period="1d")
        if not hist.empty:
            return float(hist['Close'].iloc[-1])
        return 0.0
    except Exception:
        return 0.0


# ============================================================
# EMAIL
# ============================================================

def send_email(
    scores: Dict[str, dict],
    top_system: str,
    top_score: float,
    second_system: str,
    second_score: float,
    gap: float,
    days_since_switch: int,
    regime: str,
    recommendations: List[str],
    recipients: List[str]
) -> bool:
    """Send decision engine email with full analysis"""
    mail_username = os.environ.get("MAIL_USERNAME")
    mail_password = os.environ.get("MAIL_PASSWORD")
    
    if not mail_username or not mail_password:
        print("❌ Email credentials not found")
        return False
    
    date_str = datetime.now().strftime("%Y-%m-%d %H:%M")
    
    # Build rankings table
    ranking_lines = []
    for i, (name, data) in enumerate(sorted(scores.items(), key=lambda x: x[1]['score'], reverse=True), 1):
        leader_marker = " ← CURRENT LEADER" if name == top_system else ""
        ranking_lines.append(f"  {i}. {data['name']:15} {data['score']:3.0f}{leader_marker}")
    
    ranking_text = "\n".join(ranking_lines)
    
    # Build scores detail
    scores_detail = []
    for name, data in scores.items():
        scores_detail.append(f"  {data['name']:15} Score: {data['score']:.0f}  |  Return: +{data['return']:.0f}%  |  Win: {data['win_rate']:.0f}%")
    scores_text = "\n".join(scores_detail)
    
    # Build recommendation
    rec_text = "\n".join([f"  {r}" for r in recommendations])
    
    body = f"""
═══════════════════════════════════════════════════════════
  ⚠️ FOR TRADING ONLY — NOT INVESTING ⚠️
  These are short-term trading signals. Not investment advice.
═══════════════════════════════════════════════════════════

  DECISION ENGINE - {date_str} ET

═══════════════════════════════════════════════════════════

📊 SYSTEM RANKINGS (0-100 Scale)

{ranking_text}

📈 DETAILED SCORES

{scores_text}

🔄 SWITCH ANALYSIS

  Current leader: {top_system} (Score: {top_score:.0f})
  2nd place: {second_system} (Score: {second_score:.0f})
  Gap: {gap:.0f} points (switch requires 10+ points)
  Days since last switch: {days_since_switch} (requires 5+ business days)

💰 CASH CHECK

  Top score: {top_score:.0f}
  Threshold for trading: 50
  Regime: {regime}
  
  {'✅ STAY ACTIVE - Top score above 50' if top_score >= 50 else '🔴 GO TO CASH - Top score below 50'}

🎯 RECOMMENDATION

{rec_text}

═══════════════════════════════════════════════════════════
  Previous recommendation: {'Hold ' + top_system if top_score >= 50 else 'Go to cash'}
  {'No action needed' if top_score >= 50 else 'Exit all positions to cash'}
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
    
    # Load config
    try:
        config = load_config()
        recipients = config.get('email_recipients', [])
        print(f"✅ Loaded config, {len(recipients)} recipient(s)")
    except Exception as e:
        print(f"⚠️ Could not load config: {e}")
        recipients = ["mrpink970@gmail.com"]
    
    # Get system performance and scores
    systems_raw = get_system_performance()
    scores = calculate_system_scores(systems_raw)
    
    if not scores:
        print("⚠️ No system data available")
        return
    
    # Sort by score
    sorted_scores = sorted(scores.items(), key=lambda x: x[1]['score'], reverse=True)
    top_system, top_data = sorted_scores[0]
    top_score = top_data['score']
    
    second_system, second_data = sorted_scores[1] if len(sorted_scores) > 1 else (top_system, top_data)
    second_score = second_data['score']
    gap = top_score - second_score
    
    # Get regime
    regime = get_regime()
    
    # Load switch history
    last_switch_system, last_switch_date = load_previous_switch_log()
    days_since_switch = get_business_days_since(last_switch_date) if last_switch_date else 999
    
    # Generate recommendations
    recommendations = []
    
    # Cash check
    if top_score < 50:
        recommendations.append("🔴 GO TO CASH - Top system score is below 50")
        recommendations.append("   Exit all positions. Wait for scores to recover.")
        action = "CASH"
    elif regime == "CASH":
        recommendations.append("🔴 GO TO CASH - Market regime is CASH (QQQ < MA50)")
        recommendations.append("   Exit all positions. Re-enter when regime turns BULL.")
        action = "CASH"
    else:
        # Check if switch is warranted
        if gap >= 10 and days_since_switch >= 5:
            recommendations.append(f"🔄 SWITCH: Move from {top_system} to {second_system}")
            recommendations.append(f"   Gap: {gap:.0f} points (10+ threshold met)")
            recommendations.append(f"   Days since last switch: {days_since_switch} (5+ business days)")
            recommendations.append(f"   ACTION: Exit {top_data['current_holding']}, enter {second_data['current_holding']}")
            action = "SWITCH"
            # Save this switch recommendation
            save_switch_recommendation(second_system, top_system, scores)
        else:
            # Hold current leader
            if gap >= 10:
                gap_note = f"Gap is {gap:.0f} points (meets threshold), but only {days_since_switch} business days since last switch. Wait {(5 - days_since_switch)} more days."
            elif days_since_switch < 5:
                gap_note = f"Only {days_since_switch} business days since last switch. Wait {(5 - days_since_switch)} more days before considering switch."
            else:
                gap_note = f"Gap is only {gap:.0f} points (requires 10+ to switch)."
            
            recommendations.append(f"✅ HOLD: {top_system}")
            recommendations.append(f"   Current leader with score {top_score:.0f}")
            recommendations.append(f"   2nd place: {second_system} ({second_score:.0f})")
            recommendations.append(f"   {gap_note}")
            action = "HOLD"
    
    # Print summary
    print(f"\n📊 SYSTEM RANKINGS:")
    for name, data in sorted_scores:
        print(f"   {data['name']}: {data['score']:.0f} (Return: +{data['return']:.0f}%, Win: {data['win_rate']:.0f}%)")
    
    print(f"\n🎯 RECOMMENDATION: {action}")
    print(f"   Top score: {top_score:.0f} | Regime: {regime}")
    print(f"   Gap to 2nd: {gap:.0f} points | Days since switch: {days_since_switch}")
    
    # Send email
    send_email(
        scores=scores,
        top_system=top_data['name'],
        top_score=top_score,
        second_system=second_data['name'],
        second_score=second_score,
        gap=gap,
        days_since_switch=days_since_switch,
        regime=regime,
        recommendations=recommendations,
        recipients=recipients
    )
    
    print("\n" + "=" * 60)
    print("DECISION ENGINE COMPLETE")
    print("=" * 60)


if __name__ == "__main__":
    main()
