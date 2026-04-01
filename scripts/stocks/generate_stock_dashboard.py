#!/usr/bin/env python3
from __future__ import annotations

from datetime import datetime
from pathlib import Path
from typing import Dict, List, Optional

import pandas as pd
import yaml


def load_performance(data_dir: Path) -> Dict:
    """Load performance data for both systems"""
    perf_file = data_dir / "stock_performance.csv"
    if not perf_file.exists():
        return {"trend": {"balance": 1000.0, "win_rate": 0, "total_trades": 0},
                "breakout": {"balance": 1000.0, "win_rate": 0, "total_trades": 0}}
    
    df = pd.read_csv(perf_file)
    result = {}
    for _, row in df.iterrows():
        system = row["system"]
        result[system] = {
            "balance": float(row["balance"]),
            "win_rate": float(row["win_rate"]),
            "total_trades": int(row["total_trades"]),
            "total_return_pct": float(row["total_return_pct"]),
            "max_drawdown_pct": float(row["max_drawdown_pct"]),
            "avg_win_pct": float(row["avg_win_pct"]),
            "avg_loss_pct": float(row["avg_loss_pct"]),
            "profit_factor": float(row["profit_factor"]),
        }
    return result


def load_open_positions(data_dir: Path) -> Dict[str, pd.DataFrame]:
    """Load open positions for both systems with empty file handling"""
    trend_file = data_dir / "trend_open_positions.csv"
    breakout_file = data_dir / "breakout_open_positions.csv"
    
    result = {"trend": pd.DataFrame(), "breakout": pd.DataFrame()}
    
    if trend_file.exists() and trend_file.stat().st_size > 0:
        try:
            df = pd.read_csv(trend_file)
            if not df.empty and 'ticker' in df.columns:
                result["trend"] = df
        except:
            pass
    
    if breakout_file.exists() and breakout_file.stat().st_size > 0:
        try:
            df = pd.read_csv(breakout_file)
            if not df.empty and 'ticker' in df.columns:
                result["breakout"] = df
        except:
            pass
    
    return result


def load_recent_trades(data_dir: Path, limit: int = 10) -> pd.DataFrame:
    """Load recent trades from both systems and combine with empty file handling"""
    trend_file = data_dir / "trend_trade_log.csv"
    breakout_file = data_dir / "breakout_trade_log.csv"
    
    trades = []
    
    if trend_file.exists() and trend_file.stat().st_size > 0:
        try:
            trend_df = pd.read_csv(trend_file)
            if not trend_df.empty:
                trend_df["system_display"] = "Trend"
                trades.append(trend_df)
        except:
            pass
    
    if breakout_file.exists() and breakout_file.stat().st_size > 0:
        try:
            breakout_df = pd.read_csv(breakout_file)
            if not breakout_df.empty:
                breakout_df["system_display"] = "Breakout"
                trades.append(breakout_df)
        except:
            pass
    
    if not trades:
        return pd.DataFrame()
    
    combined = pd.concat(trades, ignore_index=True)
    combined = combined.sort_values("exit_date", ascending=False)
    return combined.head(limit)


def load_candidates(data_dir: Path) -> Dict[str, pd.DataFrame]:
    """Load today's candidates for both systems"""
    trend_file = data_dir / "stock_candidates_history.csv"
    breakout_file = data_dir / "stock_breakout_candidates_history.csv"
    
    result = {"trend": pd.DataFrame(), "breakout": pd.DataFrame()}
    
    if trend_file.exists() and trend_file.stat().st_size > 0:
        try:
            df = pd.read_csv(trend_file)
            if not df.empty:
                latest_date = df["date"].max()
                result["trend"] = df[df["date"] == latest_date].head(5)
        except:
            pass
    
    if breakout_file.exists() and breakout_file.stat().st_size > 0:
        try:
            df = pd.read_csv(breakout_file)
            if not df.empty:
                latest_date = df["date"].max()
                result["breakout"] = df[df["date"] == latest_date].head(5)
        except:
            pass
    
    return result


def load_run_log(data_dir: Path) -> Dict:
    """Load latest run log entry - handles both 5-column and 6-column formats"""
    log_file = data_dir / "stock_system_run_log.csv"
    if not log_file.exists():
        return {"date": "N/A", "stocks_scored": 0, "candidates_found": 0, "breakout_candidates_found": 0}
    
    try:
        df = pd.read_csv(log_file)
        if df.empty:
            return {"date": "N/A", "stocks_scored": 0, "candidates_found": 0, "breakout_candidates_found": 0}
        
        # Get the last row (most recent run)
        latest = df.iloc[-1]
        
        # Handle both 5-column and 6-column formats
        stocks_scored = int(latest.get("stocks_scored", 0))
        candidates_found = int(latest.get("candidates_found", 0))
        breakout_candidates_found = int(latest.get("breakout_candidates_found", 0)) if "breakout_candidates_found" in df.columns else 0
        
        return {
            "date": str(latest.get("date", "N/A")),
            "stocks_scored": stocks_scored,
            "candidates_found": candidates_found,
            "breakout_candidates_found": breakout_candidates_found,
        }
    except Exception as e:
        print(f"Error reading run log: {e}")
        return {"date": "N/A", "stocks_scored": 0, "candidates_found": 0, "breakout_candidates_found": 0}


def generate_html(perf: Dict, positions: Dict, trades: pd.DataFrame, candidates: Dict, run_log: Dict) -> str:
    """Generate the HTML dashboard"""
    
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S ET")
    
    # Determine colors for trend system
    trend_balance = perf.get("trend", {}).get("balance", 1000.0)
    trend_return = trend_balance - 1000.0
    trend_return_pct = (trend_return / 1000.0) * 100
    trend_color = "positive" if trend_return >= 0 else "negative"
    
    # Determine colors for breakout system
    breakout_balance = perf.get("breakout", {}).get("balance", 1000.0)
    breakout_return = breakout_balance - 1000.0
    breakout_return_pct = (breakout_return / 1000.0) * 100
    breakout_color = "positive" if breakout_return >= 0 else "negative"
    
    # Build open positions HTML
    def build_positions_table(df: pd.DataFrame, system_name: str) -> str:
        if df.empty:
            return f'<tr><td colspan="7" style="text-align: center;">No open positions</td></tr>'
        
        rows = []
        for _, row in df.iterrows():
            entry_date = row.get("entry_date", "")
            ticker = row.get("ticker", "")
            entry_price = float(row.get("entry_price", 0))
            shares = int(row.get("shares", 0))
            stop = float(row.get("trailing_stop", 0))
            days_held = (datetime.now() - datetime.strptime(entry_date, "%Y-%m-%d")).days if entry_date else 0
            
            rows.append(f"""
            <tr>
                <td>{system_name}</td>
                <td><strong>{ticker}</strong></td>
                <td>{entry_date}</td>
                <td>${entry_price:.2f}</td>
                <td>{shares}</td>
                <td>${stop:.2f}</td>
                <td>{days_held}</td>
            </tr>
            """)
        return "".join(rows)
    
    # Build candidates table for trend (uses total_score)
    def build_trend_candidates_table(df: pd.DataFrame) -> str:
        if df.empty:
            return '<tr><td colspan="3" style="text-align: center;">No candidates today</td></tr>'
        
        rows = []
        for _, row in df.iterrows():
            ticker = row.get("ticker", "")
            score = int(row.get("total_score", 0))
            close = float(row.get("close", 0))
            
            rows.append(f"""
            <tr>
                <td><strong>{ticker}</strong></td>
                <td>{score}</td>
                <td>${close:.2f}</td>
            </tr>
            """)
        return "".join(rows)
    
    # Build candidates table for breakout (uses breakout_total_score)
    def build_breakout_candidates_table(df: pd.DataFrame) -> str:
        if df.empty:
            return '<tr><td colspan="3" style="text-align: center;">No candidates today</td></tr>'
        
        rows = []
        for _, row in df.iterrows():
            ticker = row.get("ticker", "")
            score = int(row.get("breakout_total_score", 0))
            close = float(row.get("close", 0))
            
            rows.append(f"""
            <tr>
                <td><strong>{ticker}</strong></td>
                <td>{score}</td>
                <td>${close:.2f}</td>
            </tr>
            """)
        return "".join(rows)
    
    # Build trades table
    def build_trades_table(df: pd.DataFrame) -> str:
        if df.empty:
            return '<tr><td colspan="8" style="text-align: center;">No closed trades yet</td></tr>'
        
        rows = []
        for _, row in df.iterrows():
            system = row.get("system_display", row.get("system", ""))
            ticker = row.get("ticker", "")
            entry_date = row.get("entry_date", "")
            exit_date = row.get("exit_date", "")
            entry_price = float(row.get("entry_price", 0))
            exit_price = float(row.get("exit_price", 0))
            return_pct = float(row.get("return_pct", 0))
            pnl = float(row.get("gross_pnl", 0))
            pnl_class = "positive" if pnl >= 0 else "negative"
            return_class = "positive" if return_pct >= 0 else "negative"
            
            rows.append(f"""
            <tr>
                <td>{system}</td>
                <td><strong>{ticker}</strong></td>
                <td>{entry_date}</td>
                <td>{exit_date}</td>
                <td>${entry_price:.2f}</td>
                <td>${exit_price:.2f}</td>
                <td class="{return_class}">{return_pct:+.1f}%</td>
                <td class="{pnl_class}">${pnl:+.2f}</td>
            </tr>
            """)
        return "".join(rows)
    
    html = f"""<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>Stock Discovery & Trading Dashboard</title>
    <style>
        * {{ box-sizing: border-box; margin: 0; padding: 0; }}
        body {{
            font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, Helvetica, Arial, sans-serif;
            background: #f0f2f5;
            color: #1a1a2e;
            padding: 20px;
        }}
        .container {{
            max-width: 1200px;
            margin: 0 auto;
            background: white;
            border-radius: 12px;
            overflow: hidden;
            box-shadow: 0 2px 8px rgba(0,0,0,0.05);
        }}
        .header {{
            background: #1a1a2e;
            color: white;
            padding: 20px 24px;
            border-bottom: 3px solid #00d4ff;
        }}
        .header h1 {{ margin: 0; font-size: 20px; font-weight: 600; }}
        .header p {{ margin: 8px 0 0; font-size: 13px; opacity: 0.75; }}
        .section {{ padding: 20px 24px; border-bottom: 1px solid #e9ecef; }}
        .section-title {{
            font-size: 16px;
            font-weight: 600;
            margin: 0 0 16px 0;
            color: #1a1a2e;
            border-left: 3px solid #00d4ff;
            padding-left: 12px;
        }}
        .stats-grid {{
            display: flex;
            flex-wrap: wrap;
            gap: 16px;
            margin-bottom: 8px;
        }}
        .stat-card {{
            flex: 1;
            min-width: 150px;
            background: #f8f9fa;
            border-radius: 8px;
            padding: 12px 16px;
            text-align: center;
        }}
        .stat-label {{
            font-size: 11px;
            color: #6c757d;
            text-transform: uppercase;
            letter-spacing: 0.5px;
            margin-bottom: 6px;
        }}
        .stat-value {{
            font-size: 24px;
            font-weight: 700;
            color: #1a1a2e;
        }}
        .stat-value.positive {{ color: #10b981; }}
        .stat-value.negative {{ color: #ef4444; }}
        .stat-sub {{
            font-size: 11px;
            color: #6c757d;
            margin-top: 4px;
        }}
        table {{
            width: 100%;
            border-collapse: collapse;
            font-size: 13px;
        }}
        th {{
            text-align: left;
            padding: 10px 8px;
            background: #f8f9fa;
            font-weight: 600;
            color: #495057;
            border-bottom: 1px solid #dee2e6;
        }}
        td {{
            padding: 10px 8px;
            border-bottom: 1px solid #f0f0f0;
        }}
        .badge {{
            display: inline-block;
            padding: 2px 8px;
            border-radius: 20px;
            font-size: 11px;
            font-weight: 600;
        }}
        .badge-trend {{
            background: #d1fae5;
            color: #065f46;
        }}
        .badge-breakout {{
            background: #fef3c7;
            color: #92400e;
        }}
        .positive {{ color: #10b981; font-weight: 600; }}
        .negative {{ color: #ef4444; font-weight: 600; }}
        .footer {{
            background: #f8f9fa;
            padding: 12px 24px;
            font-size: 11px;
            color: #6c757d;
            text-align: center;
        }}
        .two-col {{
            display: flex;
            gap: 20px;
            margin-bottom: 20px;
        }}
        .col {{
            flex: 1;
            background: #f8f9fa;
            border-radius: 8px;
            padding: 12px;
        }}
        .col h4 {{
            font-size: 14px;
            margin-bottom: 12px;
            color: #1a1a2e;
        }}
    </style>
</head>
<body>
<div class="container">
    
    <div class="header">
        <h1>📊 Stock Discovery & Trading Dashboard</h1>
        <p>Generated: {timestamp} | Parallel Paper Trading: Trend vs Breakout</p>
    </div>
    
    <!-- EXECUTIVE SUMMARY -->
    <div class="section">
        <div class="section-title">Executive Summary</div>
        <div class="stats-grid">
            <div class="stat-card">
                <div class="stat-label">Trend System</div>
                <div class="stat-value {trend_color}">${trend_balance:.2f}</div>
                <div class="stat-sub">{trend_return:+.2f} ({trend_return_pct:+.1f}%) | {perf.get('trend', {}).get('win_rate', 0):.1f}% win rate</div>
            </div>
            <div class="stat-card">
                <div class="stat-label">Breakout System</div>
                <div class="stat-value {breakout_color}">${breakout_balance:.2f}</div>
                <div class="stat-sub">{breakout_return:+.2f} ({breakout_return_pct:+.1f}%) | {perf.get('breakout', {}).get('win_rate', 0):.1f}% win rate</div>
            </div>
            <div class="stat-card">
                <div class="stat-label">Total Trades</div>
                <div class="stat-value">{perf.get('trend', {}).get('total_trades', 0) + perf.get('breakout', {}).get('total_trades', 0)}</div>
                <div class="stat-sub">Trend: {perf.get('trend', {}).get('total_trades', 0)} | Breakout: {perf.get('breakout', {}).get('total_trades', 0)}</div>
            </div>
            <div class="stat-card">
                <div class="stat-label">Last Run</div>
                <div class="stat-value">{run_log.get('stocks_scored', 0)}</div>
                <div class="stat-sub">stocks scored | {run_log.get('candidates_found', 0)} trend | {run_log.get('breakout_candidates_found', 0)} breakout</div>
            </div>
        </div>
    </div>
    
    <!-- OPEN POSITIONS -->
    <div class="section">
        <div class="section-title">📌 Open Positions</div>
        <table>
            <thead>
                <tr><th>System</th><th>Ticker</th><th>Entry Date</th><th>Entry Price</th><th>Shares</th><th>Trailing Stop</th><th>Days Held</th></tr>
            </thead>
            <tbody>
                {build_positions_table(positions.get("trend", pd.DataFrame()), "Trend")}
                {build_positions_table(positions.get("breakout", pd.DataFrame()), "Breakout")}
            </tbody>
        </table>
    </div>
    
    <!-- TODAY'S CANDIDATES -->
    <div class="section">
        <div class="section-title">🎯 Today's Top Candidates</div>
        <div class="two-col">
            <div class="col">
                <h4>Trend System (Score ≥ 6) Range: 0–8</h4>
                <table style="width: 100%;">
                    <thead><tr><th>Ticker</th><th>Score</th><th>Price</th></tr></thead>
                    <tbody>
                        {build_trend_candidates_table(candidates.get("trend", pd.DataFrame()))}
                    </tbody>
                </table>
            </div>
            <div class="col">
                <h4>Breakout System (Score ≥ 6) Range: 0–11</h4>
                <table style="width: 100%;">
                    <thead><tr><th>Ticker</th><th>Score</th><th>Price</th></tr></thead>
                    <tbody>
                        {build_breakout_candidates_table(candidates.get("breakout", pd.DataFrame()))}
                    </tbody>
                </table>
            </div>
        </div>
    </div>
    
    <!-- RECENT CLOSED TRADES -->
    <div class="section">
        <div class="section-title">📋 Recent Closed Trades</div>
        <table>
            <thead>
                <tr><th>System</th><th>Ticker</th><th>Entry Date</th><th>Exit Date</th><th>Entry</th><th>Exit</th><th>Return</th><th>PnL</th></tr>
            </thead>
            <tbody>
                {build_trades_table(trades)}
            </tbody>
        </table>
    </div>
    
    <!-- SYSTEM COMPARISON -->
    <div class="section">
        <div class="section-title">⚔️ System Comparison</div>
        <div class="stats-grid">
            <div class="stat-card">
                <div class="stat-label">Trend Win Rate</div>
                <div class="stat-value {('positive' if perf.get('trend', {}).get('win_rate', 0) > 50 else 'negative')}">{perf.get('trend', {}).get('win_rate', 0):.1f}%</div>
                <div class="stat-sub">Profit Factor: {perf.get('trend', {}).get('profit_factor', 0):.2f}</div>
            </div>
            <div class="stat-card">
                <div class="stat-label">Breakout Win Rate</div>
                <div class="stat-value {('positive' if perf.get('breakout', {}).get('win_rate', 0) > 50 else 'negative')}">{perf.get('breakout', {}).get('win_rate', 0):.1f}%</div>
                <div class="stat-sub">Profit Factor: {perf.get('breakout', {}).get('profit_factor', 0):.2f}</div>
            </div>
            <div class="stat-card">
                <div class="stat-label">Trend Avg Win/Loss</div>
                <div class="stat-value positive">+{perf.get('trend', {}).get('avg_win_pct', 0):.1f}%</div>
                <div class="stat-sub negative">-{perf.get('trend', {}).get('avg_loss_pct', 0):.1f}% loss</div>
            </div>
            <div class="stat-card">
                <div class="stat-label">Breakout Avg Win/Loss</div>
                <div class="stat-value positive">+{perf.get('breakout', {}).get('avg_win_pct', 0):.1f}%</div>
                <div class="stat-sub negative">-{perf.get('breakout', {}).get('avg_loss_pct', 0):.1f}% loss</div>
            </div>
        </div>
    </div>
    
    <!-- FOOTER -->
    <div class="footer">
        <div>⚠️ Paper trading simulation — no real money at risk</div>
        <div style="margin-top: 4px;">Strategy: Trend (2-day confirmation, 15% stop) vs Breakout (1-day confirmation, 10% stop)</div>
        <div style="margin-top: 4px;">Dashboard generated by GitHub Actions | Data source: yfinance</div>
    </div>
    
</div>
</body>
</html>
"""
    return html


def main():
    data_dir = Path("data/stocks")
    output_path = Path("stock_dashboard.html")
    
    # Load all data
    perf = load_performance(data_dir)
    positions = load_open_positions(data_dir)
    trades = load_recent_trades(data_dir, limit=10)
    candidates = load_candidates(data_dir)
    run_log = load_run_log(data_dir)
    
    # Generate HTML
    html = generate_html(perf, positions, trades, candidates, run_log)
    
    # Write output
    output_path.write_text(html)
    print(f"Dashboard generated: {output_path.absolute()}")
    print(f"  - Trend balance: ${perf.get('trend', {}).get('balance', 1000):.2f}")
    print(f"  - Breakout balance: ${perf.get('breakout', {}).get('balance', 1000):.2f}")
    print(f"  - Total trades: {perf.get('trend', {}).get('total_trades', 0) + perf.get('breakout', {}).get('total_trades', 0)}")


if __name__ == "__main__":
    main()
