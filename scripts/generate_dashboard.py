from __future__ import annotations

#!/usr/bin/env python3
"""
generate_dashboard.py
Reads trading system CSVs and generates a self-contained dashboard.html.
Run after run_paper_trading.py completes.
"""

import json
import math
import sys
from datetime import datetime, date
from pathlib import Path

import pandas as pd
import yaml

# ---------------------------------------------------------------------------
# Paths
# ---------------------------------------------------------------------------
ROOT       = Path(".")
DATA       = ROOT / "data"
CONFIG     = ROOT / "config" / "paper_trading_parameters.yaml"
OUTPUT     = ROOT / "dashboard.html"

# ---------------------------------------------------------------------------
# Load data
# ---------------------------------------------------------------------------
def load_data():
    def read_csv(path):
        try:
            df = pd.read_csv(path)
            return df if not df.empty else pd.DataFrame()
        except Exception:
            return pd.DataFrame()

    positions    = read_csv(DATA / "paper_positions.csv")
    trade_log    = read_csv(DATA / "paper_trade_log.csv")
    performance  = read_csv(DATA / "paper_performance.csv")
    scores       = pd.read_csv(DATA / "sector_scores.csv")
    indicators   = pd.read_csv(DATA / "indicators.csv")
    market       = pd.read_csv(DATA / "market_data.csv")

    with open(CONFIG) as f:
        params = yaml.safe_load(f)

    return positions, trade_log, performance, scores, indicators, market, params

# ---------------------------------------------------------------------------
# Helper formatters
# ---------------------------------------------------------------------------
def fmt_dollar(v, sign=False):
    if v is None or (isinstance(v, float) and math.isnan(v)):
        return "$0.00"
    prefix = "+" if sign and v > 0 else ""
    return f"{prefix}${v:,.2f}"

def fmt_pct(v, sign=False, decimals=1):
    if v is None or (isinstance(v, float) and math.isnan(v)):
        return "0.0%"
    prefix = "+" if sign and v > 0 else ""
    return f"{prefix}{v:.{decimals}f}%"

def color_val(v, positive_green=True):
    if v is None: return ""
    if v > 0: return "color:#10b981;font-weight:600" if positive_green else "color:#ef4444;font-weight:600"
    if v < 0: return "color:#ef4444;font-weight:600" if positive_green else "color:#10b981;font-weight:600"
    return "color:#6b7280"

def signal_badge(state):
    colors = {
        "Strong Bull": ("background:#065f46;color:#ffffff", "SB"),
        "Bull":        ("background:#d1fae5;color:#065f46", "B"),
        "Neutral":     ("background:#e5e7eb;color:#374151", "N"),
        "Bear":        ("background:#fee2e2;color:#991b1b", "B"),
        "Strong Bear": ("background:#991b1b;color:#ffffff", "SB"),
    }
    style, abbr = colors.get(state, ("background:#e5e7eb;color:#374151", "?"))
    label = state if state else "Unknown"
    return f'<span style="padding:3px 10px;border-radius:99px;font-size:11px;font-weight:700;{style}">{label}</span>'

def arrow(change):
    if change > 0: return f'<span style="color:#10b981">&#9650; +{change}</span>'
    if change < 0: return f'<span style="color:#ef4444">&#9660; {change}</span>'
    return f'<span style="color:#9ca3af">&#9644; 0</span>'

# ---------------------------------------------------------------------------
# Build sections
# ---------------------------------------------------------------------------

def kpi_card(label, value, sub="", value_style=""):
    return f"""
    <div style="background:#fff;border-radius:12px;padding:24px 20px;flex:1;min-width:160px;box-shadow:0 1px 3px rgba(0,0,0,.08)">
      <div style="font-size:11px;font-weight:600;color:#9ca3af;text-transform:uppercase;letter-spacing:.08em;margin-bottom:8px">{label}</div>
      <div style="font-size:28px;font-weight:800;font-family:'DM Mono',monospace;{value_style}">{value}</div>
      {f'<div style="font-size:11px;color:#9ca3af;margin-top:6px">{sub}</div>' if sub else ''}
    </div>"""

def build_kpi(perf, trade_log, account_size, margin_pct, margin_rate):
    if perf.empty or perf['total_trades'].iloc[0] == 0:
        balance  = account_size
        net_prof = 0.0
        win_rate = 0.0
        expect   = 0.0
        n_trades = 0
        max_dd   = 0.0
        interest = 0.0
    else:
        p        = perf.iloc[0]
        n_trades = int(p['total_trades'])
        win_rate = float(p['win_rate_pct'])
        expect   = float(p['expectancy_per_trade_pct'])
        max_dd   = float(p['max_drawdown_pct'])
        net_prof = float(p.get('net_profit_dollars', p.get('net_profit_dollars', 0)))
        interest = float(p.get('total_margin_interest_dollars', 0))
        balance  = account_size + net_prof

    net_pct   = (net_prof / account_size * 100) if account_size else 0
    buying_pw = balance * (1 + margin_pct)

    win_color = ("#10b981" if win_rate >= 50 else "#f59e0b" if win_rate >= 40 else "#ef4444")
    net_color = "#10b981" if net_prof >= 0 else "#ef4444"
    net_arrow = "&#9650;" if net_prof > 0 else "&#9660;" if net_prof < 0 else "&#9644;"

    cards = (
        kpi_card("Account Balance", fmt_dollar(balance),
                 f"Started: {fmt_dollar(account_size)} &nbsp;|&nbsp; Buying power: {fmt_dollar(buying_pw)}") +
        kpi_card("Net Profit",
                 f'{net_arrow} {fmt_dollar(abs(net_prof))} <span style="font-size:18px">({fmt_pct(net_pct, sign=True)})</span>',
                 f"Margin interest paid: {fmt_dollar(interest)}",
                 f"color:{net_color}") +
        kpi_card("Win Rate", fmt_pct(win_rate),
                 f"{n_trades} total trades &nbsp;|&nbsp; Max drawdown: {fmt_pct(max_dd)}",
                 f"color:{win_color}") +
        kpi_card("Expectancy", fmt_pct(expect, sign=True),
                 "Average net return per trade",
                 f"color:{'#10b981' if expect >= 0 else '#ef4444'}")
    )
    return f'<div style="display:flex;gap:16px;flex-wrap:wrap">{cards}</div>'

def build_qqq_banner(scores):
    latest = scores[scores['date'] == scores['date'].max()]
    qqq    = latest[latest['sector'] == 'Nasdaq Growth']
    if qqq.empty:
        state, score = "Unknown", 0
    else:
        state = qqq.iloc[0]['signal_state']
        score = int(qqq.iloc[0]['total_score'])

    bull = state in ("Bull", "Strong Bull")
    bg   = "#d1fae5" if bull else "#fee2e2"
    dot  = "#10b981" if bull else "#ef4444"
    icon = "&#9899;" # circle
    msg  = "NEW ENTRIES ALLOWED" if bull else "NEW ENTRIES BLOCKED"
    sub  = ("Positions will open when strong sector signals appear."
            if bull else
            "Open positions continue running with trailing stops active.")
    return f"""
    <div style="background:{bg};border-radius:12px;padding:18px 24px;display:flex;align-items:center;gap:16px">
      <div style="width:14px;height:14px;border-radius:50%;background:{dot};flex-shrink:0;box-shadow:0 0 0 4px {dot}33"></div>
      <div>
        <div style="font-weight:700;font-size:14px;color:#111">
          QQQ MARKET REGIME: {state.upper()} &nbsp;
          <span style="font-size:12px;color:#6b7280;font-weight:400">Score: {score}</span>
          &nbsp;&mdash;&nbsp;
          <span style="font-weight:800">{msg}</span>
        </div>
        <div style="font-size:12px;color:#6b7280;margin-top:3px">{sub}</div>
      </div>
    </div>"""

def build_positions(positions, market, min_hold_days, today_str):
    if positions.empty or len(positions) == 0:
        return """
        <div style="text-align:center;padding:40px;color:#9ca3af">
          <div style="font-size:32px;margin-bottom:8px">&#128683;</div>
          <div style="font-weight:600;font-size:15px">No open positions</div>
          <div style="font-size:12px;margin-top:4px">System is in cash. Waiting for confirmed Bull signal with QQQ approval.</div>
        </div>"""

    latest_prices = {}
    latest_date   = market['date'].max()
    for _, row in market[market['date'] == latest_date].iterrows():
        latest_prices[row['ticker']] = float(row['close'])

    rows_html = ""
    for _, pos in positions.iterrows():
        ticker      = str(pos['ticker'])
        cur_price   = latest_prices.get(ticker, pos['entry_price'])
        entry_price = float(pos['entry_price'])
        shares      = int(pos['shares'])
        stop        = float(pos['trailing_stop'])
        entry_date  = str(pos['entry_date'])

        gain_pct    = (cur_price - entry_price) / entry_price * 100
        gain_dollar = (cur_price - entry_price) * shares
        stop_dist   = (cur_price - stop) / cur_price * 100

        try:
            days_held = (datetime.strptime(today_str, "%Y-%m-%d") -
                         datetime.strptime(entry_date, "%Y-%m-%d")).days
        except Exception:
            days_held = 0

        hold_pct  = min(100, int(days_held / max(min_hold_days, 1) * 100))
        hold_color = "#10b981" if days_held >= min_hold_days else "#f59e0b"
        g_color   = "#10b981" if gain_dollar >= 0 else "#ef4444"
        g_sign    = "+" if gain_dollar >= 0 else ""

        rows_html += f"""
        <tr style="border-bottom:1px solid #f3f4f6">
          <td style="padding:12px 14px;font-weight:600">{pos['sector']}</td>
          <td style="padding:12px 14px;font-family:'DM Mono',monospace;font-weight:700">{ticker}</td>
          <td style="padding:12px 14px;color:#6b7280">{entry_date}</td>
          <td style="padding:12px 14px">
            <div style="font-size:12px;color:{hold_color};margin-bottom:3px">{days_held} / {min_hold_days} days</div>
            <div style="background:#e5e7eb;border-radius:99px;height:4px;width:80px">
              <div style="background:{hold_color};border-radius:99px;height:4px;width:{hold_pct}%"></div>
            </div>
          </td>
          <td style="padding:12px 14px;font-family:'DM Mono',monospace">${entry_price:.2f}</td>
          <td style="padding:12px 14px;font-family:'DM Mono',monospace">${cur_price:.2f}</td>
          <td style="padding:12px 14px;color:{g_color};font-weight:600;font-family:'DM Mono',monospace">
            {g_sign}{gain_pct:.1f}%<br>
            <span style="font-size:11px">{g_sign}{fmt_dollar(abs(gain_dollar))}</span>
          </td>
          <td style="padding:12px 14px;font-family:'DM Mono',monospace">
            ${stop:.2f}<br>
            <span style="font-size:11px;color:#9ca3af">{stop_dist:.1f}% below</span>
          </td>
        </tr>"""

    return f"""
    <table style="width:100%;border-collapse:collapse;font-size:13px">
      <thead>
        <tr style="background:#f9fafb;border-bottom:2px solid #e5e7eb">
          <th style="padding:10px 14px;text-align:left;font-size:11px;font-weight:700;color:#6b7280;text-transform:uppercase">Sector</th>
          <th style="padding:10px 14px;text-align:left;font-size:11px;font-weight:700;color:#6b7280;text-transform:uppercase">Ticker</th>
          <th style="padding:10px 14px;text-align:left;font-size:11px;font-weight:700;color:#6b7280;text-transform:uppercase">Entry Date</th>
          <th style="padding:10px 14px;text-align:left;font-size:11px;font-weight:700;color:#6b7280;text-transform:uppercase">Days Held</th>
          <th style="padding:10px 14px;text-align:left;font-size:11px;font-weight:700;color:#6b7280;text-transform:uppercase">Entry $</th>
          <th style="padding:10px 14px;text-align:left;font-size:11px;font-weight:700;color:#6b7280;text-transform:uppercase">Current $</th>
          <th style="padding:10px 14px;text-align:left;font-size:11px;font-weight:700;color:#6b7280;text-transform:uppercase">Gain / Loss</th>
          <th style="padding:10px 14px;text-align:left;font-size:11px;font-weight:700;color:#6b7280;text-transform:uppercase">Trailing Stop</th>
        </tr>
      </thead>
      <tbody>{rows_html}</tbody>
    </table>
    <div style="font-size:11px;color:#9ca3af;margin-top:10px;padding:0 4px">
      &#9432; Trailing stops tighten automatically as profits grow. Stop distance shrinks from 18% to 10% on 3x ETFs as gains compound.
    </div>"""

def build_sector_signals(scores, indicators, allowed_sectors):
    latest_date = scores['date'].max()
    latest      = scores[scores['date'] == latest_date]
    latest_ind  = indicators[indicators['date'] == latest_date]

    ind_map = {}
    for _, row in latest_ind.iterrows():
        ind_map[row['sector']] = row.to_dict()

    signal_map = {
        "Strong Bull": ("#065f46", "#d1fae5", 100),
        "Bull":        ("#065f46", "#d1fae5",  70),
        "Neutral":     ("#374151", "#e5e7eb",  50),
        "Bear":        ("#991b1b", "#fee2e2",  30),
        "Strong Bear": ("#7f1d1d", "#fecaca",   0),
    }

    score_etf_map = {
        "Semiconductors":         ("SMH",  "SOXL"),
        "Consumer Discretionary": ("XLY",  "WANT"),
        "Biotechnology":          ("XBI",  "LABU"),
        "Communication Services": ("XLC",  "LTL"),
        "Healthcare":             ("XLV",  "CURE"),
    }

    cards_html = ""
    for sector in allowed_sectors:
        row = latest[latest['sector'] == sector]
        if row.empty:
            continue
        row = row.iloc[0]

        state       = str(row['signal_state'])
        score       = int(row['total_score'])
        change      = int(row['score_change'])
        direction   = str(row['direction'])
        signal_etf, trade_etf = score_etf_map.get(sector, ("--", "--"))
        tradable    = direction == "long"

        text_c, bg_c, bar_pct = signal_map.get(state, ("#374151", "#e5e7eb", 50))
        trade_icon  = '<span style="color:#10b981">&#10003; Tradable</span>' if tradable else '<span style="color:#ef4444">&#10007; Not tradable</span>'
        change_html = arrow(change)

        # Score bar (-7 to +7 mapped to 0-100%)
        bar_fill    = int((score + 7) / 14 * 100)
        bar_color   = "#10b981" if score > 0 else "#ef4444" if score < 0 else "#9ca3af"

        # Tooltip data from indicators
        ind = ind_map.get(sector, {})
        tooltip_rows = ""
        for col, label in [
            ("trend_score", "Trend (MA50)"),
            ("trend_ignition_score", "Trend Ignition"),
            ("momentum_score", "Momentum"),
            ("relative_strength_score", "Rel. Strength"),
            ("rs_persistence_score", "RS Persistence"),
            ("momentum_exhaustion_score", "Exhaustion"),
            ("volatility_score", "Volatility"),
        ]:
            val = ind.get(col, 0)
            vc  = "#10b981" if val > 0 else "#ef4444" if val < 0 else "#9ca3af"
            tooltip_rows += f'<tr><td style="padding:2px 8px 2px 0;color:#d1d5db;font-size:11px">{label}</td><td style="padding:2px 0;color:{vc};font-weight:700;font-size:11px">{val:+d}</td></tr>'

        tooltip = f"""
        <div class="tooltip-content" style="display:none;position:absolute;z-index:99;background:#1f2937;border-radius:8px;padding:12px 14px;min-width:200px;box-shadow:0 8px 24px rgba(0,0,0,.3);top:0;left:105%">
          <div style="font-weight:700;color:#fff;margin-bottom:8px;font-size:12px">{sector} Breakdown</div>
          <table>{tooltip_rows}</table>
          <div style="border-top:1px solid #374151;margin-top:8px;padding-top:8px;font-weight:700;color:#fff;font-size:12px">Total: {score:+d}</div>
        </div>"""

        cards_html += f"""
        <div class="sector-card" style="background:#fff;border-radius:12px;padding:18px;flex:1;min-width:160px;position:relative;cursor:pointer;box-shadow:0 1px 3px rgba(0,0,0,.08);transition:box-shadow .15s"
             onmouseenter="this.querySelector('.tooltip-content').style.display='block'"
             onmouseleave="this.querySelector('.tooltip-content').style.display='none'">
          <div style="font-weight:700;font-size:13px;margin-bottom:10px;color:#111">{sector}</div>
          <div style="background:{bg_c};border-radius:8px;padding:8px 10px;margin-bottom:10px">
            <div style="display:flex;justify-content:space-between;align-items:center;margin-bottom:6px">
              <span style="font-weight:800;font-size:12px;color:{text_c}">{state.upper()}</span>
              <span style="font-size:11px;color:{text_c}">{change_html}</span>
            </div>
            <div style="background:rgba(0,0,0,.1);border-radius:99px;height:5px">
              <div style="background:{bar_color};border-radius:99px;height:5px;width:{bar_fill}%;transition:width .3s"></div>
            </div>
            <div style="font-size:11px;color:{text_c};margin-top:4px;font-family:'DM Mono',monospace">Score: {score:+d}</div>
          </div>
          <div style="display:flex;justify-content:space-between;align-items:center;font-size:11px">
            <span style="color:#9ca3af">Signal: <strong style="color:#111">{signal_etf}</strong> &rarr; <strong style="color:#111">{trade_etf}</strong></span>
            <span>{trade_icon}</span>
          </div>
          {tooltip}
        </div>"""

    return f"""
    <div style="display:flex;gap:12px;flex-wrap:wrap">{cards_html}</div>
    <div style="font-size:11px;color:#9ca3af;margin-top:10px;padding:0 4px">
      &#9432; Score components: Trend (MA50), Trend Ignition (MA20 slope), Momentum (5-day ROC),
      Relative Strength vs VOO, RS Persistence, Momentum Exhaustion, Volatility (ATR).
      Hover any card to see breakdown. Range: -7 to +7.
    </div>"""

def build_charts(trade_log, account_size):
    if trade_log.empty:
        return """
        <div style="display:flex;gap:16px;flex-wrap:wrap">
          <div style="flex:1;min-width:280px;background:#fff;border-radius:12px;padding:24px;text-align:center;color:#9ca3af;box-shadow:0 1px 3px rgba(0,0,0,.08)">
            <div style="font-size:13px;font-weight:600;color:#374151;margin-bottom:16px">Equity Curve</div>
            <div style="font-size:32px;margin-bottom:8px">&#128202;</div>
            <div>No trades yet. Chart will appear after first closed trade.</div>
          </div>
          <div style="flex:1;min-width:280px;background:#fff;border-radius:12px;padding:24px;text-align:center;color:#9ca3af;box-shadow:0 1px 3px rgba(0,0,0,.08)">
            <div style="font-size:13px;font-weight:600;color:#374151;margin-bottom:16px">Drawdown</div>
            <div style="font-size:32px;margin-bottom:8px">&#128201;</div>
            <div>No trades yet. Chart will appear after first closed trade.</div>
          </div>
        </div>"""

    tl = trade_log.sort_values('entry_date').reset_index(drop=True)
    pnl_col = 'net_pnl_dollars' if 'net_pnl_dollars' in tl.columns else 'gross_pnl_dollars'

    equity    = [account_size]
    drawdowns = [0.0]
    labels    = [str(tl.iloc[0]['entry_date'])]
    peak      = account_size

    for _, row in tl.iterrows():
        bal  = equity[-1] + float(row[pnl_col])
        peak = max(peak, bal)
        dd   = (bal - peak) / peak * 100 if peak > 0 else 0
        equity.append(round(bal, 2))
        drawdowns.append(round(dd, 2))
        labels.append(str(row.get('exit_date', row['entry_date'])))

    eq_json  = json.dumps(equity)
    dd_json  = json.dumps(drawdowns)
    lbl_json = json.dumps(labels)

    return f"""
    <div style="display:flex;gap:16px;flex-wrap:wrap">
      <div style="flex:1;min-width:280px;background:#fff;border-radius:12px;padding:20px;box-shadow:0 1px 3px rgba(0,0,0,.08)">
        <div style="font-size:13px;font-weight:700;color:#111;margin-bottom:14px">Equity Curve</div>
        <canvas id="equityChart" height="160"></canvas>
      </div>
      <div style="flex:1;min-width:280px;background:#fff;border-radius:12px;padding:20px;box-shadow:0 1px 3px rgba(0,0,0,.08)">
        <div style="font-size:13px;font-weight:700;color:#111;margin-bottom:14px">Drawdown</div>
        <canvas id="drawdownChart" height="160"></canvas>
      </div>
    </div>
    <script>
    (function() {{
      var labels   = {lbl_json};
      var equity   = {eq_json};
      var drawdown = {dd_json};
      var startBal = {account_size};

      var eCtx = document.getElementById('equityChart').getContext('2d');
      new Chart(eCtx, {{
        type: 'line',
        data: {{
          labels: labels,
          datasets: [{{
            data: equity,
            borderColor: '#10b981',
            borderWidth: 2,
            fill: true,
            backgroundColor: 'rgba(16,185,129,.08)',
            pointRadius: 2,
            tension: .3
          }}, {{
            data: Array(labels.length).fill(startBal),
            borderColor: '#e5e7eb',
            borderWidth: 1,
            borderDash: [4,4],
            pointRadius: 0,
            fill: false
          }}]
        }},
        options: {{
          plugins: {{ legend: {{ display: false }},
            tooltip: {{ callbacks: {{ label: function(c) {{ return '$' + c.raw.toLocaleString('en-US', {{minimumFractionDigits:2}}); }} }} }}
          }},
          scales: {{
            x: {{ ticks: {{ maxTicksLimit: 8, font: {{ size: 10 }} }}, grid: {{ display: false }} }},
            y: {{ ticks: {{ callback: function(v) {{ return '$' + (v/1000).toFixed(1) + 'k'; }}, font: {{ size: 10 }} }} }}
          }}
        }}
      }});

      var dCtx = document.getElementById('drawdownChart').getContext('2d');
      new Chart(dCtx, {{
        type: 'line',
        data: {{
          labels: labels,
          datasets: [{{
            data: drawdown,
            borderColor: '#ef4444',
            borderWidth: 2,
            fill: true,
            backgroundColor: 'rgba(239,68,68,.1)',
            pointRadius: 2,
            tension: .3
          }}]
        }},
        options: {{
          plugins: {{ legend: {{ display: false }},
            tooltip: {{ callbacks: {{ label: function(c) {{ return c.raw.toFixed(1) + '%'; }} }} }}
          }},
          scales: {{
            x: {{ ticks: {{ maxTicksLimit: 8, font: {{ size: 10 }} }}, grid: {{ display: false }} }},
            y: {{ ticks: {{ callback: function(v) {{ return v.toFixed(0) + '%'; }}, font: {{ size: 10 }} }},
                 max: 0 }}
          }}
        }}
      }});
    }})();
    </script>"""

def build_trades_table(trade_log):
    if trade_log.empty:
        return """
        <div style="text-align:center;padding:40px;color:#9ca3af">
          <div style="font-size:32px;margin-bottom:8px">&#128203;</div>
          <div style="font-weight:600">No closed trades yet</div>
          <div style="font-size:12px;margin-top:4px">Trades will appear here after positions close.</div>
        </div>"""

    pnl_col = 'net_pnl_dollars' if 'net_pnl_dollars' in trade_log.columns else 'gross_pnl_dollars'
    recent  = trade_log.sort_values('exit_date', ascending=False).head(20)

    exit_labels = {
        "trailing_stop":    "&#9660; Stop",
        "signal_change":    "&#8635; Signal",
        "direction_change": "&#8646; Direction",
        "ticker_changed":   "&#8644; Ticker",
    }

    rows_html = ""
    for _, t in recent.iterrows():
        ret    = float(t['return_pct'])
        pnl    = float(t[pnl_col])
        r_col  = "#10b981" if ret >= 0 else "#ef4444"
        p_col  = "#10b981" if pnl >= 0 else "#ef4444"
        r_sign = "+" if ret >= 0 else ""
        p_sign = "+" if pnl >= 0 else ""
        exit_t = exit_labels.get(str(t.get('exit_type','')), str(t.get('exit_type','')))

        rows_html += f"""
        <tr style="border-bottom:1px solid #f3f4f6">
          <td style="padding:10px 12px;color:#6b7280;font-size:12px">{t.get('exit_date','')}</td>
          <td style="padding:10px 12px;font-weight:500">{t.get('sector','')}</td>
          <td style="padding:10px 12px;font-family:'DM Mono',monospace;font-weight:700">{t.get('ticker','')}</td>
          <td style="padding:10px 12px;font-family:'DM Mono',monospace">${float(t.get('entry_price',0)):.2f}</td>
          <td style="padding:10px 12px;font-family:'DM Mono',monospace">${float(t.get('exit_price',0)):.2f}</td>
          <td style="padding:10px 12px;color:{r_col};font-weight:700;font-family:'DM Mono',monospace">{r_sign}{ret:.1f}%</td>
          <td style="padding:10px 12px;font-size:12px;color:#6b7280">{exit_t}</td>
          <td style="padding:10px 12px;color:{p_col};font-weight:700;font-family:'DM Mono',monospace">{p_sign}{fmt_dollar(abs(pnl))}</td>
        </tr>"""

    return f"""
    <div style="overflow-x:auto">
    <table style="width:100%;border-collapse:collapse;font-size:13px" id="tradesTable">
      <thead>
        <tr style="background:#f9fafb;border-bottom:2px solid #e5e7eb">
          <th style="padding:9px 12px;text-align:left;font-size:11px;font-weight:700;color:#6b7280;text-transform:uppercase;cursor:pointer" onclick="sortTable(0)">Exit Date &#8597;</th>
          <th style="padding:9px 12px;text-align:left;font-size:11px;font-weight:700;color:#6b7280;text-transform:uppercase;cursor:pointer" onclick="sortTable(1)">Sector &#8597;</th>
          <th style="padding:9px 12px;text-align:left;font-size:11px;font-weight:700;color:#6b7280;text-transform:uppercase">Ticker</th>
          <th style="padding:9px 12px;text-align:left;font-size:11px;font-weight:700;color:#6b7280;text-transform:uppercase">Entry $</th>
          <th style="padding:9px 12px;text-align:left;font-size:11px;font-weight:700;color:#6b7280;text-transform:uppercase">Exit $</th>
          <th style="padding:9px 12px;text-align:left;font-size:11px;font-weight:700;color:#6b7280;text-transform:uppercase;cursor:pointer" onclick="sortTable(5)">Return &#8597;</th>
          <th style="padding:9px 12px;text-align:left;font-size:11px;font-weight:700;color:#6b7280;text-transform:uppercase">Exit Type</th>
          <th style="padding:9px 12px;text-align:left;font-size:11px;font-weight:700;color:#6b7280;text-transform:uppercase;cursor:pointer" onclick="sortTable(7)">Net P&amp;L &#8597;</th>
        </tr>
      </thead>
      <tbody>{rows_html}</tbody>
    </table>
    </div>
    <script>
    function sortTable(col) {{
      var t = document.getElementById('tradesTable');
      var rows = Array.from(t.tBodies[0].rows);
      var asc = t.dataset.lastCol == col && t.dataset.lastDir == 'asc' ? false : true;
      t.dataset.lastCol = col; t.dataset.lastDir = asc ? 'asc' : 'desc';
      rows.sort(function(a, b) {{
        var av = a.cells[col].innerText.replace(/[$+,%]/g,'').trim();
        var bv = b.cells[col].innerText.replace(/[$+,%]/g,'').trim();
        var an = parseFloat(av), bn = parseFloat(bv);
        var cmp = isNaN(an) ? av.localeCompare(bv) : an - bn;
        return asc ? cmp : -cmp;
      }});
      rows.forEach(function(r) {{ t.tBodies[0].appendChild(r); }});
    }}
    </script>"""

def build_perf_metrics(perf, trade_log):
    if perf.empty or perf['total_trades'].iloc[0] == 0:
        metrics = {
            "Avg Win": "--", "Avg Loss": "--",
            "Largest Win": "--", "Largest Loss": "--",
            "Avg Hold (Winners)": "--", "Avg Hold (Losers)": "--",
            "Profit Factor": "--", "Total Interest": "--",
        }
    else:
        p = perf.iloc[0]
        pnl_col = 'net_pnl_dollars' if 'net_pnl_dollars' in trade_log.columns else 'gross_pnl_dollars'
        winners = trade_log[trade_log[pnl_col] > 0] if not trade_log.empty else pd.DataFrame()
        losers  = trade_log[trade_log[pnl_col] < 0] if not trade_log.empty else pd.DataFrame()
        gross_p = float(p.get('gross_profit_dollars', 0))
        gross_l = abs(float(p.get('gross_loss_dollars', 0)))
        pf      = round(gross_p / gross_l, 2) if gross_l else 0

        metrics = {
            "Avg Win":            fmt_pct(float(p['average_gain_pct']), sign=True),
            "Avg Loss":           fmt_pct(-float(p['average_loss_pct'])),
            "Largest Win":        fmt_pct(float(p['largest_gain_pct']), sign=True),
            "Largest Loss":       fmt_pct(float(p['largest_loss_pct'])),
            "Avg Hold (Winners)": f"{winners['trade_duration_days'].mean():.0f}d" if len(winners) else "--",
            "Avg Hold (Losers)":  f"{losers['trade_duration_days'].mean():.0f}d"  if len(losers)  else "--",
            "Profit Factor":      str(pf) if pf else "--",
            "Total Interest":     fmt_dollar(float(p.get('total_margin_interest_dollars', 0))),
        }

    cards = ""
    for label, val in metrics.items():
        val_color = ""
        if "Win" in label and val != "--":
            val_color = "color:#10b981"
        elif "Loss" in label and val != "--":
            val_color = "color:#ef4444"
        cards += f"""
        <div style="background:#fff;border-radius:10px;padding:14px 16px;flex:1;min-width:120px;box-shadow:0 1px 3px rgba(0,0,0,.06)">
          <div style="font-size:10px;font-weight:600;color:#9ca3af;text-transform:uppercase;letter-spacing:.06em;margin-bottom:5px">{label}</div>
          <div style="font-size:18px;font-weight:700;font-family:'DM Mono',monospace;{val_color}">{val}</div>
        </div>"""
    return f'<div style="display:flex;gap:10px;flex-wrap:wrap">{cards}</div>'

def section(title, content, icon=""):
    return f"""
    <div style="background:#fff;border-radius:16px;padding:24px;box-shadow:0 1px 4px rgba(0,0,0,.07);margin-bottom:16px">
      <h2 style="margin:0 0 18px 0;font-size:15px;font-weight:700;color:#111;display:flex;align-items:center;gap:8px">
        {f'<span style="font-size:18px">{icon}</span>' if icon else ''}{title}
      </h2>
      {content}
    </div>"""

# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------
def main():
    positions, trade_log, performance, scores, indicators, market, params = load_data()

    account_size  = float(params['positions']['account_size'])
    margin_pct    = float(params['positions'].get('margin_pct', 0))
    margin_rate   = float(params['positions'].get('margin_annual_rate', 0))
    max_pos       = int(params['positions']['max_concurrent_positions'])
    min_hold      = int(params.get('exit', {}).get('min_hold_days', 0))
    confirm_n     = int(params['confirmation']['required_consecutive_closes'])
    start_date    = str(params.get('paper_trading_start_date', '--'))
    allowed       = params['sectors']['allowed']
    data_date     = scores['date'].max()
    today_str     = date.today().strftime("%Y-%m-%d")
    generated_at  = datetime.now().strftime("%Y-%m-%d %H:%M ET")

    net_profit  = float(performance.iloc[0].get('net_profit_dollars', 0)) if not performance.empty else 0
    balance     = account_size + net_profit
    buying_pw   = balance * (1 + margin_pct)
    net_pct     = net_profit / account_size * 100 if account_size else 0
    n_open      = len(positions) if not positions.empty else 0
    qqq_row     = scores[(scores['date']==data_date) & (scores['sector']=='Nasdaq Growth')]
    qqq_state   = qqq_row.iloc[0]['signal_state'] if not qqq_row.empty else "Unknown"
    qqq_bull    = qqq_state in ("Bull", "Strong Bull")
    win_rate    = float(performance.iloc[0]['win_rate_pct']) if not performance.empty else 0
    total_int   = float(performance.iloc[0].get('total_margin_interest_dollars', 0)) if not performance.empty else 0

    html = f"""<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width,initial-scale=1">
<title>Trading Dashboard &mdash; {today_str}</title>
<link rel="preconnect" href="https://fonts.googleapis.com">
<link href="https://fonts.googleapis.com/css2?family=DM+Sans:wght@400;500;600;700;800&family=DM+Mono:wght@400;500;700&display=swap" rel="stylesheet">
<script src="https://cdn.jsdelivr.net/npm/chart.js@4.4.0/dist/chart.umd.min.js"></script>
<style>
  *{{box-sizing:border-box;margin:0;padding:0}}
  body{{font-family:'DM Sans',sans-serif;background:#f0f2f5;color:#111;min-height:100vh}}
  .container{{max-width:960px;margin:0 auto;padding:24px 16px}}
  @media(max-width:600px){{
    .container{{padding:12px 8px}}
    .kpi-grid{{flex-direction:column!important}}
    .sector-grid{{flex-direction:column!important}}
  }}
</style>
</head>
<body>

<!-- Header -->
<div style="background:linear-gradient(135deg,#1e1e2f 0%,#2d2d4e 100%);padding:20px 0;margin-bottom:0">
  <div style="max-width:960px;margin:0 auto;padding:0 16px;display:flex;justify-content:space-between;align-items:center;flex-wrap:wrap;gap:12px">
    <div>
      <div style="font-size:11px;font-weight:600;color:#818cf8;text-transform:uppercase;letter-spacing:.12em;margin-bottom:4px">Automated Sector Rotation</div>
      <h1 style="font-size:22px;font-weight:800;color:#fff;font-family:'DM Sans',sans-serif">Trading Dashboard</h1>
      <div style="font-size:12px;color:#94a3b8;margin-top:3px">{today_str} &nbsp;&bull;&nbsp; EXP06a &nbsp;&bull;&nbsp; Paper Trading</div>
    </div>
    <div style="text-align:right">
      <div style="font-size:28px;font-weight:800;color:#fff;font-family:'DM Mono',monospace">{fmt_dollar(balance)}</div>
      <div style="font-size:13px;color:{'#10b981' if net_profit >= 0 else '#ef4444'};font-weight:600">
        {'&#9650;' if net_profit > 0 else '&#9660;'} {fmt_dollar(abs(net_profit))} ({fmt_pct(net_pct, sign=True)})
      </div>
    </div>
  </div>
</div>

<div class="container">

<!-- QQQ Banner -->
<div style="margin-bottom:16px">
{build_qqq_banner(scores)}
</div>

<!-- KPI Cards -->
{section("Performance Overview", build_kpi(performance, trade_log, account_size, margin_pct, margin_rate), "&#127942;")}

<!-- Charts -->
{section("Equity Curve &amp; Drawdown", build_charts(trade_log, account_size), "&#128202;")}

<!-- Open Positions -->
{section("Open Positions", build_positions(positions, market, min_hold, today_str), "&#128202;")}

<!-- Sector Signals -->
{section("Sector Signals", build_sector_signals(scores, indicators, allowed), "&#128268;")}

<!-- Recent Trades -->
{section("Recent Closed Trades", build_trades_table(trade_log), "&#128203;")}

<!-- Performance Metrics -->
{section("Performance Breakdown", build_perf_metrics(performance, trade_log), "&#128200;")}

<!-- Footer -->
<div style="background:#fff;border-radius:16px;padding:20px 24px;box-shadow:0 1px 4px rgba(0,0,0,.07)">
  <div style="font-size:12px;color:#9ca3af;line-height:1.8">
    <div style="margin-bottom:6px">
      <strong style="color:#374151">System Config:</strong>
      Max positions: {max_pos} &nbsp;|&nbsp;
      Min hold: {min_hold} days &nbsp;|&nbsp;
      Entry confirmation: {confirm_n} closes &nbsp;|&nbsp;
      Margin: {margin_pct:.0%} @ {margin_rate:.3%}/yr &nbsp;|&nbsp;
      QQQ filter: {'On' if params['filters']['require_qqq_bull'] else 'Off'} &nbsp;|&nbsp;
      Live since: {start_date}
    </div>
    <div style="margin-bottom:6px">
      <strong style="color:#374151">Data:</strong>
      Last market data: {data_date} &nbsp;|&nbsp;
      Dashboard generated: {generated_at} &nbsp;|&nbsp;
      Source: Yahoo Finance via yfinance
    </div>
    <div style="background:#fef9c3;border-radius:8px;padding:10px 14px;margin-top:10px;color:#854d0e">
      &#9888; This is a <strong>paper trading simulation</strong>. No real money is at risk.
      Strategy: EXP06a | QQQ Filter | Top 5 Sectors | 10-Day Min Hold | Compounding + Margin Interest
    </div>
  </div>
</div>

</div><!-- /container -->
</body>
</html>"""

    OUTPUT.write_text(html, encoding="utf-8")
    print(f"Dashboard written to {OUTPUT}")
    print(f"Balance: {fmt_dollar(balance)} | Net: {fmt_dollar(net_profit)} | Open positions: {n_open} | QQQ: {qqq_state}")

if __name__ == "__main__":
    main()
