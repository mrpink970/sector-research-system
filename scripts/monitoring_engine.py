#!/usr/bin/env python3
"""
Monitoring Engine - Daily email summary of all trading systems
Reads master_config.yaml and sends formatted email to all recipients
"""

import os
import smtplib
from email.message import EmailMessage
from datetime import datetime
from pathlib import Path
from typing import Dict, List

import pandas as pd
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


def safe_read_csv(path: Path) -> pd.DataFrame:
    """Safely read CSV, return empty DataFrame if error"""
    if not path.exists():
        return pd.DataFrame()
    try:
        df = pd.read_csv(path)
        return df if not df.empty else pd.DataFrame()
    except Exception:
        return pd.DataFrame()


def safe_float(value, default=0.0) -> float:
    """Safely convert to float"""
    if value is None or (isinstance(value, float) and pd.isna(value)):
        return default
    try:
        return float(value)
    except (TypeError, ValueError):
        return default


def safe_int(value, default=0) -> int:
    """Safely convert to int"""
    return int(safe_float(value, default))


# ============================================================
# SYSTEM DATA EXTRACTION
# ============================================================

def get_sector_system_data(config: dict) -> dict:
    """Extract performance data for sector system"""
    paths = config['systems']['sector']['data_files']
    start_balance = config['systems']['sector']['starting_balance']
    
    perf = safe_read_csv(Path(paths['performance']))
    positions = safe_read_csv(Path(paths['positions']))
    
    result = {
        'name': config['systems']['sector']['display_name'],
        'start_date': config['systems']['sector']['start_date'],
        'starting_balance': start_balance,
        'dashboard_url': config['systems']['sector']['dashboard_url'],
        'balance': start_balance,
        'total_return_pct': 0.0,
        'win_rate': 0.0,
        'total_trades': 0,
        'open_positions': [],
        'open_count': 0,
    }
    
    if not perf.empty:
        row = perf.iloc[0]
        net_profit = safe_float(row.get('net_profit_dollars', 0))
        result['balance'] = start_balance + net_profit
        result['total_return_pct'] = safe_float(row.get('total_return_pct', 0))
        result['win_rate'] = safe_float(row.get('win_rate_pct', 0))
        result['total_trades'] = safe_int(row.get('total_trades', 0))
    
    if not positions.empty:
        for _, row in positions.iterrows():
            entry_price = safe_float(row.get('entry_price', 0))
            shares = safe_int(row.get('shares', 0))
            result['open_positions'].append({
                'ticker': row.get('ticker', 'N/A'),
                'shares': shares,
                'entry_price': entry_price,
            })
        result['open_count'] = len(result['open_positions'])
    
    return result


def get_two_etf_system_data(config: dict) -> dict:
    """Extract performance data for 2 ETF Bull system"""
    paths = config['systems']['two_etf']['data_files']
    start_balance = config['systems']['two_etf']['starting_balance']
    
    perf = safe_read_csv(Path(paths['performance']))
    positions = safe_read_csv(Path(paths['positions']))
    
    result = {
        'name': config['systems']['two_etf']['display_name'],
        'start_date': config['systems']['two_etf']['start_date'],
        'starting_balance': start_balance,
        'dashboard_url': config['systems']['two_etf']['dashboard_url'],
        'balance': start_balance,
        'total_return_pct': 0.0,
        'win_rate': 0.0,
        'total_trades': 0,
        'open_positions': [],
        'open_count': 0,
    }
    
    if not perf.empty:
        row = perf.iloc[0]
        total_pl = safe_float(row.get('total_gross_pl', 0))
        result['balance'] = start_balance + total_pl
        result['total_return_pct'] = (result['balance'] / start_balance - 1) * 100
        result['win_rate'] = safe_float(row.get('win_rate', 0)) * 100
        result['total_trades'] = safe_int(row.get('total_trades', 0))
    
    if not positions.empty:
        for _, row in positions.iterrows():
            entry_price = safe_float(row.get('entry_price', 0))
            shares = safe_int(row.get('shares', 0))
            result['open_positions'].append({
                'ticker': row.get('ticker', 'N/A'),
                'shares': shares,
                'entry_price': entry_price,
            })
        result['open_count'] = len(result['open_positions'])
    
    return result


def get_stock_system_data(config: dict) -> dict:
    """Extract performance data for stock system (trend + breakout combined)"""
    paths = config['systems']['stock']['data_files']
    start_balance = config['systems']['stock']['starting_balance']
    
    perf = safe_read_csv(Path(paths['performance']))
    positions_trend = safe_read_csv(Path(paths['positions_trend']))
    positions_breakout = safe_read_csv(Path(paths['positions_breakout']))
    
    result = {
        'name': config['systems']['stock']['display_name'],
        'start_date': config['systems']['stock']['start_date'],
        'starting_balance': start_balance,
        'dashboard_url': config['systems']['stock']['dashboard_url'],
        'balance': start_balance,
        'total_return_pct': 0.0,
        'win_rate': 0.0,
        'total_trades': 0,
        'open_positions': [],
        'open_count': 0,
    }
    
    if not perf.empty:
        # Sum balances from trend and breakout rows
        total_balance = perf['balance'].sum() if 'balance' in perf.columns else start_balance
        result['balance'] = total_balance
        result['total_return_pct'] = (total_balance / start_balance - 1) * 100
        
        # Sum total trades
        result['total_trades'] = safe_int(perf['total_trades'].sum()) if 'total_trades' in perf.columns else 0
        
        # Weighted average win rate
        if 'total_trades' in perf.columns and 'win_rate' in perf.columns:
            total_trades = perf['total_trades'].sum()
            if total_trades > 0:
                weighted_win_rate = (perf['total_trades'] * perf['win_rate']).sum() / total_trades
                result['win_rate'] = weighted_win_rate
    
    # Combine positions from both strategies
    all_positions = []
    if not positions_trend.empty:
        for _, row in positions_trend.iterrows():
            all_positions.append({
                'ticker': row.get('ticker', 'N/A'),
                'system': 'Trend',
                'shares': safe_int(row.get('shares', 0)),
                'entry_price': safe_float(row.get('entry_price', 0)),
            })
    if not positions_breakout.empty:
        for _, row in positions_breakout.iterrows():
            all_positions.append({
                'ticker': row.get('ticker', 'N/A'),
                'system': 'Breakout',
                'shares': safe_int(row.get('shares', 0)),
                'entry_price': safe_float(row.get('entry_price', 0)),
            })
    
    result['open_positions'] = all_positions
    result['open_count'] = len(all_positions)
    
    return result


# ============================================================
# EMAIL FORMATTING
# ============================================================
def format_currency(value: float) -> str:
    if value is None or value == 0:
        return "$0.00"
    return f"${value:,.2f}"


def format_percent(value: float) -> str:
    if value is None:
        return "0.0%"
    sign = "+" if value >= 0 else ""
    return f"{sign}{value:.1f}%"


def format_position_summary(positions: List[dict]) -> str:
    if not positions:
        return "None"
    
    lines = []
    for pos in positions[:5]:
        ticker = pos.get('ticker', 'N/A')
        shares = pos.get('shares', 0)
        entry = pos.get('entry_price', 0)
        
        if 'system' in pos:
            lines.append(f"      • {ticker} ({pos['system']}): {shares} shares @ ${entry:.2f}")
        else:
            lines.append(f"      • {ticker}: {shares} shares @ ${entry:.2f}")
    
    if len(positions) > 5:
        lines.append(f"      ... and {len(positions) - 5} more")
    
    return "\n".join(lines)


def build_html_email(data: dict, config: dict) -> str:
    sector = data['sector']
    two_etf = data['two_etf']
    stock = data['stock']
    date_str = datetime.now().strftime("%Y-%m-%d")
    
    html = f"""
<!DOCTYPE html>
<html>
<head>
    <meta charset="UTF-8">
    <title>Trading Systems Daily Summary - {date_str}</title>
    <style>
        body {{ font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, Helvetica, Arial, sans-serif; line-height: 1.6; color: #1a2a3a; background-color: #f0f4f8; margin: 0; padding: 20px; }}
        .container {{ max-width: 800px; margin: 0 auto; background: white; border-radius: 16px; overflow: hidden; box-shadow: 0 2px 8px rgba(0,0,0,0.1); }}
        .header {{ background: linear-gradient(135deg, #1e1e2f 0%, #2d2d4e 100%); color: white; padding: 24px 30px; }}
        .header h1 {{ margin: 0 0 8px 0; font-size: 24px; }}
        .header p {{ margin: 0; opacity: 0.8; font-size: 14px; }}
        .system {{ border-bottom: 1px solid #e2e8f0; padding: 24px 30px; }}
        .system h2 {{ margin: 0 0 16px 0; font-size: 20px; color: #1a4d7a; }}
        .metrics {{ display: flex; flex-wrap: wrap; gap: 16px; margin-bottom: 16px; }}
        .metric {{ flex: 1; min-width: 100px; background: #f8fafc; padding: 12px 16px; border-radius: 12px; }}
        .metric-label {{ font-size: 11px; text-transform: uppercase; color: #6c7e8f; letter-spacing: 0.5px; margin-bottom: 4px; }}
        .metric-value {{ font-size: 20px; font-weight: 700; }}
        .metric-value.positive {{ color: #1e8a4c; }}
        .metric-value.negative {{ color: #c2412c; }}
        .positions {{ background: #f8fafc; padding: 16px; border-radius: 12px; margin-top: 12px; font-family: monospace; font-size: 13px; }}
        .positions-title {{ font-weight: 700; margin-bottom: 8px; color: #4a627a; }}
        .dashboard-link {{ margin-top: 16px; }}
        .dashboard-link a {{ color: #2c7fb8; text-decoration: none; font-weight: 600; }}
        .footer {{ background: #f8fafc; padding: 16px 30px; text-align: center; font-size: 12px; color: #8ba0b0; }}
    </style>
</head>
<body>
<div class="container">
    <div class="header">
        <h1>📊 Trading Systems Daily Summary</h1>
        <p>{date_str} | Market Closed</p>
    </div>
"""
    
    # Sector System
    sector_return_class = "positive" if sector['total_return_pct'] >= 0 else "negative"
    html += f"""
    <div class="system">
        <h2>💰 {sector['name']}</h2>
        <div class="metrics">
            <div class="metric">
                <div class="metric-label">Balance</div>
                <div class="metric-value">{format_currency(sector['balance'])}</div>
                <div style="font-size: 11px; color: #6c7e8f;">Started: {format_currency(sector['starting_balance'])} ({sector['start_date']})</div>
            </div>
            <div class="metric">
                <div class="metric-label">Total Return</div>
                <div class="metric-value {sector_return_class}">{format_percent(sector['total_return_pct'])}</div>
            </div>
            <div class="metric">
                <div class="metric-label">Win Rate</div>
                <div class="metric-value">{sector['win_rate']:.1f}%</div>
                <div style="font-size: 11px; color: #6c7e8f;">{sector['total_trades']} trades</div>
            </div>
        </div>
        <div class="positions">
            <div class="positions-title">📌 Open Positions ({sector['open_count']})</div>
            <pre style="margin: 0; font-family: monospace; font-size: 12px;">{format_position_summary(sector['open_positions'])}</pre>
        </div>
        <div class="dashboard-link">
            <a href="{sector['dashboard_url']}">🔗 View Full Dashboard →</a>
        </div>
    </div>
"""
    
    # 2 ETF System
    two_etf_return_class = "positive" if two_etf['total_return_pct'] >= 0 else "negative"
    html += f"""
    <div class="system">
        <h2>📈 {two_etf['name']}</h2>
        <div class="metrics">
            <div class="metric">
                <div class="metric-label">Balance</div>
                <div class="metric-value">{format_currency(two_etf['balance'])}</div>
                <div style="font-size: 11px; color: #6c7e8f;">Started: {format_currency(two_etf['starting_balance'])} ({two_etf['start_date']})</div>
            </div>
            <div class="metric">
                <div class="metric-label">Total Return</div>
                <div class="metric-value {two_etf_return_class}">{format_percent(two_etf['total_return_pct'])}</div>
            </div>
            <div class="metric">
                <div class="metric-label">Win Rate</div>
                <div class="metric-value">{two_etf['win_rate']:.1f}%</div>
                <div style="font-size: 11px; color: #6c7e8f;">{two_etf['total_trades']} trades</div>
            </div>
        </div>
        <div class="positions">
            <div class="positions-title">📌 Open Positions ({two_etf['open_count']})</div>
            <pre style="margin: 0; font-family: monospace; font-size: 12px;">{format_position_summary(two_etf['open_positions'])}</pre>
        </div>
        <div class="dashboard-link">
            <a href="{two_etf['dashboard_url']}">🔗 View Full Dashboard →</a>
        </div>
    </div>
"""
    
    # Stock System
    stock_return_class = "positive" if stock['total_return_pct'] >= 0 else "negative"
    html += f"""
    <div class="system">
        <h2>📊 {stock['name']}</h2>
        <div class="metrics">
            <div class="metric">
                <div class="metric-label">Balance</div>
                <div class="metric-value">{format_currency(stock['balance'])}</div>
                <div style="font-size: 11px; color: #6c7e8f;">Started: {format_currency(stock['starting_balance'])} ({stock['start_date']})</div>
            </div>
            <div class="metric">
                <div class="metric-label">Total Return</div>
                <div class="metric-value {stock_return_class}">{format_percent(stock['total_return_pct'])}</div>
            </div>
            <div class="metric">
                <div class="metric-label">Win Rate</div>
                <div class="metric-value">{stock['win_rate']:.1f}%</div>
                <div style="font-size: 11px; color: #6c7e8f;">{stock['total_trades']} trades</div>
            </div>
        </div>
        <div class="positions">
            <div class="positions-title">📌 Open Positions ({stock['open_count']})</div>
            <pre style="margin: 0; font-family: monospace; font-size: 12px;">{format_position_summary(stock['open_positions'])}</pre>
        </div>
        <div class="dashboard-link">
            <a href="{stock['dashboard_url']}">🔗 View Full Dashboard →</a>
        </div>
    </div>
"""
    
    # Footer
    recipients = ", ".join(config.get('email_recipients', []))
    html += f"""
    <div class="footer">
        <p>⚡ Automated daily summary | Data as of market close {date_str}</p>
        <p>Sent to: {recipients}</p>
    </div>
</div>
</body>
</html>
"""
    
    return html


def send_email(html_content: str, recipients: List[str]) -> bool:
    mail_username = os.environ.get("MAIL_USERNAME")
    mail_password = os.environ.get("MAIL_PASSWORD")
    
    if not mail_username or not mail_password:
        print("❌ Email credentials not found")
        return False
    
    date_str = datetime.now().strftime("%Y-%m-%d")
    
    msg = EmailMessage()
    msg.set_content("Please enable HTML to view this email.")
    msg.add_alternative(html_content, subtype='html')
    msg["Subject"] = f"📊 Trading Systems Daily Summary - {date_str}"
    msg["From"] = mail_username
    msg["To"] = recipients[0] if recipients else mail_username
    msg["Cc"] = recipients[1:] if len(recipients) > 1 else []
    
    try:
        with smtplib.SMTP_SSL("smtp.gmail.com", 465) as smtp:
            smtp.login(mail_username, mail_password)
            smtp.send_message(msg)
        print(f"✅ Email sent to {len(recipients)} recipient(s)")
        return True
    except Exception as e:
        print(f"❌ Failed to send email: {e}")
        return False


# ============================================================
# MAIN
# ============================================================
def main():
    print("=" * 60)
    print("MONITORING ENGINE")
    print("=" * 60)
    
    try:
        config = load_config()
        print(f"✅ Loaded config from {CONFIG_PATH}")
    except Exception as e:
        print(f"❌ Failed to load config: {e}")
        return
    
    print("\n📊 Collecting system data...")
    
    sector_data = get_sector_system_data(config)
    print(f"   Sector System: Balance ${sector_data['balance']:,.2f} ({sector_data['total_trades']} trades)")
    
    two_etf_data = get_two_etf_system_data(config)
    print(f"   2 ETF System:  Balance ${two_etf_data['balance']:,.2f} ({two_etf_data['total_trades']} trades)")
    
    stock_data = get_stock_system_data(config)
    print(f"   Stock System:  Balance ${stock_data['balance']:,.2f} ({stock_data['total_trades']} trades)")
    
    print("\n📧 Building email...")
    data = {
        'sector': sector_data,
        'two_etf': two_etf_data,
        'stock': stock_data,
    }
    html_content = build_html_email(data, config)
    
    recipients = config.get('email_recipients', [])
    if not recipients:
        print("⚠️ No email recipients found")
        return
    
    print(f"\n📨 Sending to {len(recipients)} recipient(s)...")
    for r in recipients:
        print(f"   • {r}")
    
    send_email(html_content, recipients)
    
    print("\n" + "=" * 60)
    print("MONITORING ENGINE COMPLETE")
    print("=" * 60)


if __name__ == "__main__":
    main()
