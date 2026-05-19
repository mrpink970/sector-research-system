#!/usr/bin/env python3
"""
Decision Engine - Forward-Looking Market Analysis
Runs at 10:00 AM ET
Tells you which system to hold, when to switch, when to go to cash
Based on MOMENTUM (recent performance) not backward-looking returns
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
import numpy as np


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
# FORWARD-LOOKING MOMENTUM DATA
# ============================================================

def get_market_momentum() -> Dict[str, float]:
    """
    Calculate 5-day momentum for key market indicators
    Higher score = stronger recent performance
    """
    momentum = {}
    
    try:
        # SOXL momentum (affects 2 ETF and Sector systems)
        soxl = yf.Ticker("SOXL")
        soxl_hist = soxl.history(period="2wk", interval="1d")
        if len(soxl_hist) >= 5:
            soxl_5d = (soxl_hist['Close'].iloc[-1] - soxl_hist['Close'].iloc[-5]) / soxl_hist['Close'].iloc[-5] * 100
            momentum['soxl'] = max(0, min(100, (soxl_5d + 10) * 5))
        else:
            momentum['soxl'] = 50
        
        # QQQ momentum (broad market for Stock and AI systems)
        qqq = yf.Ticker("QQQ")
        qqq_hist = qqq.history(period="2wk", interval="1d")
        if len(qqq_hist) >= 5:
            qqq_5d = (qqq_hist['Close'].iloc[-1] - qqq_hist['Close'].iloc[-5]) / qqq_hist['Close'].iloc[-5] * 100
            momentum['qqq'] = max(0, min(100, (qqq_5d + 10) * 5))
        else:
            momentum['qqq'] = 50
        
        # VIX momentum (inverse - lower VIX is better for most systems)
        vix = yf.Ticker("^VIX")
        vix_hist = vix.history(period="2wk", interval="1d")
        if len(vix_hist) >= 5:
            vix_5d = (vix_hist['Close'].iloc[-1] - vix_hist['Close'].iloc[-5]) / vix_hist['Close'].iloc[-5] * 100
            # Inverse relationship: falling VIX = good momentum
            momentum['vix'] = max(0, min(100, 100 - (vix_5d * 4)))
        else:
            momentum['vix'] = 50
        
        # Semiconductor sector momentum (SOXX)
        soxx = yf.Ticker("SOXX")
        soxx_hist = soxx.history(period="2wk", interval="1d")
        if len(soxx_hist) >= 5:
            soxx_5d = (soxx_hist['Close'].iloc[-1] - soxx_hist['Close'].iloc[-5]) / soxx_hist['Close'].iloc[-5] * 100
            momentum['soxx'] = max(0, min(100, (soxx_5d + 8) * 5))
        else:
            momentum['soxx'] = 50
        
    except Exception as e:
        print(f"Error calculating momentum: {e}")
        momentum = {'soxl': 50, 'qqq': 50, 'vix': 50, 'soxx': 50}
    
    return momentum


def get_system_momentum_scores(market_momentum: Dict[str, float]) -> Dict[str, float]:
    """
    Calculate forward-looking scores for each system based on:
    - What the system holds (current positions)
    - How those holdings have performed recently
    - Current market regime
    """
    scores = {}
    
    # System 1: 2 ETF System (SOXL/TQQQ - high leverage)
    # High leverage works best when momentum is strong and VIX is low
    soxl_mom = market_momentum.get('soxl', 50)
    vix_mom = market_momentum.get('vix', 50)
    
    two_etf_score = (soxl_mom * 0.6) + ((100 - vix_mom) * 0.4)
    scores['two_etf'] = min(100, max(0, two_etf_score))
    
    # System 2: Sector System (SOXL but lower leverage management)
    # More resilient, works in moderate conditions
    soxx_mom = market_momentum.get('soxx', 50)
    sector_score = (soxx_mom * 0.5) + (soxl_mom * 0.3) + ((100 - vix_mom) * 0.2)
    scores['sector'] = min(100, max(0, sector_score))
    
    # System 3: Stock System (broad market, individual stocks)
    # Works best when QQQ is trending
    qqq_mom = market_momentum.get('qqq', 50)
    stock_score = (qqq_mom * 0.7) + ((100 - vix_mom) * 0.3)
    scores['stock'] = min(100, max(0, stock_score))
    
    # System 4: AI System (adaptive, good in most conditions but prefers trends)
    ai_score = (qqq_mom * 0.4) + (soxx_mom * 0.3) + ((100 - vix_mom) * 0.3)
    scores['ai'] = min(100, max(0, ai_score))
    
    # System 5: Quantum System (volatility-based, works in ranges)
    # Inverse of VIX momentum - works when VIX is rising or stable
    quantum_score = (vix_mom * 0.6) + (50 * 0.4)  # Default to moderate when VIX stable
    scores['quantum'] = min(100, max(0, quantum_score))
    
    return scores


# ============================================================
# SYSTEM HISTORICAL DATA (for context, not scoring)
# ============================================================

def get_system_performance() -> Dict[str, dict]:
    """Read historical performance data from all 5 systems (for display only)"""
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
                
                start_balance = 5000
                total_equity = start_balance + total_pl
                
                if pos_path.exists():
                    pos_df = pd.read_csv(pos_path)
                    if not pos_df.empty:
                        for _, pos in pos_df.iterrows():
                            shares = safe_float(pos.get('shares', 0))
                            highest = safe_float(pos.get('highest_price', 0))
                            entry = safe_float(pos.get('entry_price', 0))
                            total_equity += (highest - entry) * shares
                
                total_return = (total_equity / start_balance - 1) * 100
                max_dd = 1.6
                
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
            systems['two_etf'] = {
                'name': '2 ETF System',
                'return': 0, 'win_rate': 0, 'trades': 0, 'max_dd': 0, 'current_holding': 'SOXL',
            }
    
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
            systems['sector'] = {
                'name': 'Sector System',
                'return': 0, 'win_rate': 0, 'trades': 0, 'max_dd': 0, 'current_holding': 'SOXL',
            }
    
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
                    'max_dd': 7.8,
                    'current_holding': 'VARIOUS',
                }
        except Exception as e:
            print(f"Error reading Stock system: {e}")
            systems['stock'] = {
                'name': 'Stock System',
                'return': 0, 'win_rate': 0, 'trades': 0, 'max_dd': 0, 'current_holding': 'VARIOUS',
            }
    
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
            systems['ai'] = {
                'name': 'AI System',
                'return': 0, 'win_rate': 0, 'trades': 0, 'max_dd': 0, 'current_holding': 'N/A',
            }
    
    # 5. Quantum System
    perf_path = Path("data/quantum/performance.csv")
    pos_path = Path("data/quantum/positions.csv")
    
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
                
                systems['quantum'] = {
                    'name': 'Quantum System',
                    'return': safe_float(row.get('total_return_pct', 0)),
                    'win_rate': safe_float(row.get('win_rate_pct', 0)),
                    'trades': safe_int(row.get('total_trades', 0)),
                    'max_dd': 0,
                    'current_holding': current_holding,
                }
        except Exception as e:
            print(f"Error reading Quantum system: {e}")
            systems['quantum'] = {
                'name': 'Quantum System',
                'return': 0, 'win_rate': 0, 'trades': 0, 'max_dd': 0, 'current_holding': 'N/A',
            }
    
    return systems


# ============================================================
# REGIME DETECTION (FORWARD-LOOKING)
# ============================================================

def get_regime() -> Tuple[str, float, str]:
    """
    Determine current market regime with strength score and trend direction
    Returns: (regime_string, strength_score, trend_direction)
    """
    try:
        qqq = yf.Ticker("QQQ")
        hist = qqq.history(period="3mo")
        vix = yf.Ticker("^VIX")
        vix_hist = vix.history(period="1mo")
        
        if len(hist) >= 50 and len(vix_hist) >= 20:
            current_price = hist['Close'].iloc[-1]
            ma50 = hist['Close'].rolling(50).mean().iloc[-1]
            ma20 = hist['Close'].rolling(20).mean().iloc[-1]
            ma10 = hist['Close'].rolling(10).mean().iloc[-1]
            
            # Trend direction
            if current_price > ma10 > ma20 > ma50:
                trend = "STRONG_UPTREND"
            elif current_price > ma20 > ma50:
                trend = "UPTREND"
            elif current_price < ma10 < ma20 < ma50:
                trend = "STRONG_DOWNTREND"
            elif current_price < ma20 < ma50:
                trend = "DOWNTREND"
            else:
                trend = "CHOPPY"
            
            # Calculate trend strength using ADX simplified
            high_low = hist['High'] - hist['Low']
            high_close = abs(hist['High'] - hist['Close'].shift(1))
            low_close = abs(hist['Low'] - hist['Close'].shift(1))
            tr = pd.concat([high_low, high_close, low_close], axis=1).max(axis=1)
            atr = tr.rolling(14).mean().iloc[-1]
            trend_strength = min(100, (abs(current_price - ma50) / atr) * 20) if atr > 0 else 50
            
            # VIX regime
            current_vix = vix_hist['Close'].iloc[-1]
            vix_ma20 = vix_hist['Close'].rolling(20).mean().iloc[-1]
            
            if current_vix < 15:
                vix_regime = "LOW_VOL"
            elif current_vix < 20:
                vix_regime = "NORMAL_VOL"
            elif current_vix < 30:
                vix_regime = "ELEVATED_VOL"
            else:
                vix_regime = "HIGH_VOL"
            
            # Combine for final regime
            if trend in ["STRONG_UPTREND", "UPTREND"] and current_vix < 20:
                regime = "BULL"
            elif trend in ["STRONG_DOWNTREND", "DOWNTREND"] and current_vix > 20:
                regime = "BEAR"
            elif trend == "CHOPPY":
                regime = "RANGE"
            else:
                regime = "NEUTRAL"
            
            return regime, trend_strength, vix_regime
        
        return "NEUTRAL", 50, "NORMAL_VOL"
    except Exception as e:
        print(f"Error getting regime: {e}")
        return "UNKNOWN", 0, "UNKNOWN"


# ============================================================
# SWITCH LOGIC
# ============================================================

def load_previous_switch_log() -> Tuple[Optional[str], Optional[datetime]]:
    """Load last switch decision from log"""
    if not SWITCH_LOG_PATH.exists():
        return None, None
    
    try:
        df = pd.read_csv(SWITCH_LOG_PATH)
        if df.empty:
            return None, None
        
        last_row = df.iloc[-1]
        last_date = pd.to_datetime(last_row.get('date')) if 'date' in last_row else None
        return last_row.get('recommended_system'), last_date
    except Exception:
        return None, None


def save_switch_recommendation(recommendation: str, current_system: str, scores: Dict, regime: str) -> None:
    """Save switch recommendation to log"""
    new_row = pd.DataFrame([{
        'date': datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        'recommended_system': recommendation,
        'current_leader': current_system,
        'top_score': max(scores.values(), key=lambda x: x['score'])['score'] if scores else 0,
        'regime': regime,
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
        return 999
    
    current = datetime.now()
    days = 0
    current_date = last_date.replace(hour=0, minute=0, second=0)
    end_date = current.replace(hour=0, minute=0, second=0)
    
    while current_date < end_date:
        current_date += timedelta(days=1)
        if current_date.weekday() < 5:
            days += 1
    
    return days


def calculate_final_scores(
    momentum_scores: Dict[str, float],
    historical_systems: Dict[str, dict]
) -> Dict[str, dict]:
    """
    Combine forward-looking momentum (80%) with historical context (20%)
    This makes the engine mostly forward-looking but maintains stability
    """
    final_scores = {}
    
    for name in momentum_scores.keys():
        momentum = momentum_scores.get(name, 50)
        
        # Use historical return as minor factor (20% weight)
        hist = historical_systems.get(name, {})
        hist_return = hist.get('return', 0)
        hist_score = min(100, max(0, hist_return / 2))  # Convert return % to 0-100 scale
        
        # Final score: 80% momentum, 20% historical
        total_score = (momentum * 0.80) + (hist_score * 0.20)
        
        final_scores[name] = {
            'name': hist.get('name', name.replace('_', ' ').title()),
            'score': round(total_score, 1),
            'return': round(hist.get('return', 0), 1),
            'win_rate': round(hist.get('win_rate', 0), 1),
            'max_dd': round(hist.get('max_dd', 0), 1),
            'trades': hist.get('trades', 0),
            'current_holding': hist.get('current_holding', 'N/A'),
            'momentum_score': round(momentum, 1),
        }
    
    return final_scores


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
    strength: float,
    vix_regime: str,
    market_momentum: Dict[str, float],
    recommendations: List[str],
    recipients: List[str]
) -> bool:
    """Send decision engine email with full forward-looking analysis"""
    mail_username = os.environ.get("MAIL_USERNAME")
    mail_password = os.environ.get("MAIL_PASSWORD")
    
    if not mail_username or not mail_password:
        print("❌ Email credentials not found")
        return False
    
    date_str = datetime.now().strftime("%Y-%m-%d %H:%M")
    
    ranking_lines = []
    for i, (name, data) in enumerate(sorted(scores.items(), key=lambda x: x[1]['score'], reverse=True), 1):
        leader_marker = " ← CURRENT LEADER" if name == top_system else ""
        ranking_lines.append(f"  {i}. {data['name']:15} {data['score']:3.0f}{leader_marker}")
    ranking_text = "\n".join(ranking_lines)
    
    scores_detail = []
    for name, data in scores.items():
        scores_detail.append(f"  {data['name']:15} Score: {data['score']:.0f}  |  Momentum: {data['momentum_score']:.0f}  |  Return: +{data['return']:.0f}%  |  Win: {data['win_rate']:.0f}%")
    scores_text = "\n".join(scores_detail)
    
    rec_text = "\n".join([f"  {r}" for r in recommendations])
    
    body = f"""
═══════════════════════════════════════════════════════════
  ⚠️ FOR TRADING ONLY — NOT INVESTING ⚠️
  These are short-term trading signals. Not investment advice.
═══════════════════════════════════════════════════════════

  DECISION ENGINE - FORWARD-LOOKING ANALYSIS
  {date_str} ET

═══════════════════════════════════════════════════════════

📊 MARKET REGIME

  Regime: {regime}
  Trend Strength: {strength:.0f}/100
  Volatility: {vix_regime}
  
  {'✅ FAVORABLE for trading' if regime in ['BULL', 'NEUTRAL'] else '⚠️ CAUTION - Consider reducing size'}

═══════════════════════════════════════════════════════════

📈 SYSTEM RANKINGS (Forward-Looking, 0-100 Scale)
  Based on recent momentum (80%) + historical (20%)

{ranking_text}

📊 DETAILED SCORES (Momentum = recent performance driver)

{scores_text}

📊 MARKET MOMENTUM (5-day)

  SOXL (Semiconductor 3x): {market_momentum.get('soxl', 50):.0f}/100
  QQQ (Nasdaq): {market_momentum.get('qqq', 50):.0f}/100
  VIX (Volatility - inverse): {market_momentum.get('vix', 50):.0f}/100
  SOXX (Semiconductor): {market_momentum.get('soxx', 50):.0f}/100

═══════════════════════════════════════════════════════════

🔄 SWITCH ANALYSIS

  Current leader: {top_system} (Score: {top_score:.0f})
  2nd place: {second_system} (Score: {second_score:.0f})
  Gap: {gap:.0f} points (switch requires 10+ points to consider changing)
  Days since last switch: {days_since_switch} (requires 5+ business days to switch again)

💰 CASH CHECK

  Top score: {top_score:.0f}
  Threshold for trading: 50
  Regime: {regime}
  
  {'✅ STAY ACTIVE - Top score above 50 and favorable regime' if top_score >= 50 and regime in ['BULL', 'NEUTRAL'] else '🔴 GO TO CASH - Top score below 50 or unfavorable regime'}

═══════════════════════════════════════════════════════════

🎯 RECOMMENDATION

{rec_text}

═══════════════════════════════════════════════════════════
  Action: {'Hold ' + top_system if top_score >= 50 and regime in ['BULL', 'NEUTRAL'] else 'Go to cash'}
  {'Trade the leader with normal size' if top_score >= 50 and regime in ['BULL', 'NEUTRAL'] else 'Exit all positions to cash. Wait for better conditions.'}
═══════════════════════════════════════════════════════════
"""
    
    msg = EmailMessage()
    msg.set_content(body)
    msg["Subject"] = f"🎯 Decision Engine - {datetime.now().strftime('%Y-%m-%d')} - {regime}"
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
    print("DECISION ENGINE - FORWARD-LOOKING ANALYSIS")
    print("Based on momentum (recent performance) not backward-looking returns")
    print("=" * 60)
    
    try:
        config = load_config()
        recipients = config.get('email_recipients', [])
        print(f"✅ Loaded config, {len(recipients)} recipient(s)")
    except Exception as e:
        print(f"⚠️ Could not load config: {e}")
        recipients = ["mrpink970@gmail.com"]
    
    # Get forward-looking momentum
    print("\n📊 Calculating market momentum...")
    market_momentum = get_market_momentum()
    print(f"   SOXL momentum: {market_momentum.get('soxl', 50):.0f}")
    print(f"   QQQ momentum: {market_momentum.get('qqq', 50):.0f}")
    print(f"   VIX (inverse): {market_momentum.get('vix', 50):.0f}")
    
    # Get system-specific momentum scores
    momentum_scores = get_system_momentum_scores(market_momentum)
    print(f"\n📈 System momentum scores:")
    for name, score in momentum_scores.items():
        print(f"   {name}: {score:.0f}")
    
    # Get historical data for context (20% weight)
    historical_systems = get_system_performance()
    
    # Calculate final scores (80% momentum, 20% historical)
    scores = calculate_final_scores(momentum_scores, historical_systems)
    
    if not scores:
        print("⚠️ No system data available")
        return
    
    # Get regime
    regime, strength, vix_regime = get_regime()
    print(f"\n🌍 Market regime: {regime} (strength: {strength:.0f}, vol: {vix_regime})")
    
    # Sort and analyze
    sorted_scores = sorted(scores.items(), key=lambda x: x[1]['score'], reverse=True)
    top_system, top_data = sorted_scores[0]
    top_score = top_data['score']
    
    second_system, second_data = sorted_scores[1] if len(sorted_scores) > 1 else (top_system, top_data)
    second_score = second_data['score']
    gap = top_score - second_score
    
    # Switch logic
    last_switch_system, last_switch_date = load_previous_switch_log()
    days_since_switch = get_business_days_since(last_switch_date) if last_switch_date else 999
    
    recommendations = []
    
    # Determine action
    if top_score < 50:
        recommendations.append("🔴 GO TO CASH - Top system score is below 50")
        recommendations.append("   Exit all positions. Wait for scores to recover.")
        action = "CASH"
    elif regime not in ['BULL', 'NEUTRAL']:
        recommendations.append(f"🔴 GO TO CASH - Market regime is {regime} (unfavorable)")
        recommendations.append("   Exit all positions. Re-enter when regime turns BULL or NEUTRAL.")
        action = "CASH"
    else:
        recommendations.append(f"✅ HOLD: {top_data['name']} (Score: {top_score:.0f})")
        recommendations.append(f"   Momentum score: {top_data['momentum_score']:.0f}")
        recommendations.append(f"   2nd place: {second_system} ({second_score:.0f})")
        
        if gap >= 10:
            recommendations.append(f"   Gap is {gap:.0f} points — {top_data['name']} has strong lead.")
        else:
            recommendations.append(f"   Gap is {gap:.0f} points — race is close.")
        
        if days_since_switch < 5 and last_switch_date:
            recommendations.append(f"   Only {days_since_switch} business days since last switch.")
            recommendations.append(f"   Wait {(5 - days_since_switch)} more days before considering any change.")
        
        action = f"HOLD {top_data['name']}"
    
    # Print results
    print(f"\n📊 SYSTEM RANKINGS (Forward-looking):")
    for name, data in sorted_scores:
        print(f"   {data['name']}: {data['score']:.0f} (Momentum: {data['momentum_score']:.0f} | Return: +{data['return']:.0f}%)")
    
    print(f"\n🎯 RECOMMENDATION: {action}")
    print(f"   Top score: {top_score:.0f} | Regime: {regime}")
    print(f"   Gap to 2nd: {gap:.0f} points | Days since switch: {days_since_switch}")
    
    # Save and send
    if action != "CASH":
        save_switch_recommendation(top_data['name'], top_system, scores, regime)
    
    send_email(
        scores=scores,
        top_system=top_data['name'],
        top_score=top_score,
        second_system=second_data['name'],
        second_score=second_score,
        gap=gap,
        days_since_switch=days_since_switch,
        regime=regime,
        strength=strength,
        vix_regime=vix_regime,
        market_momentum=market_momentum,
        recommendations=recommendations,
        recipients=recipients
    )
    
    print("\n" + "=" * 60)
    print("DECISION ENGINE COMPLETE")
    print("=" * 60)


if __name__ == "__main__":
    main()
