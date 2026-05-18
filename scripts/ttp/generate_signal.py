#!/usr/bin/env python3
"""
Trade The Pool - SOXX Signal Generator
Determines Green Day status and trading signal
"""

import pandas as pd
import yaml
from pathlib import Path
from datetime import datetime
import sys

# Paths
CONFIG_PATH = Path("config/ttp_config.yaml")
DATA_DIR = Path("data/ttp")
SIGNALS_PATH = DATA_DIR / "signals.csv"


def load_config():
    with open(CONFIG_PATH, 'r') as f:
        return yaml.safe_load(f)


def load_latest_data():
    """Load the most recent price data"""
    log_path = DATA_DIR / "price_log.csv"
    if not log_path.exists():
        return None
    
    df = pd.read_csv(log_path)
    if df.empty:
        return None
    
    return df.iloc[-1].to_dict()


def check_green_day(data: dict, config: dict) -> tuple[bool, list]:
    """Check if conditions meet Green Day criteria"""
    conditions = []
    all_met = True
    
    # Condition 1: 1-hour return > 0.5%
    min_return = config['entry_conditions']['min_1h_return']
    if data['return_1h_pct'] >= min_return:
        conditions.append(f"✅ 1h return: {data['return_1h_pct']:.2f}% >= {min_return}%")
    else:
        conditions.append(f"❌ 1h return: {data['return_1h_pct']:.2f}% < {min_return}%")
        all_met = False
    
    # Condition 2: Above MA20
    if config['entry_conditions']['above_ma20']:
        if data['above_ma20']:
            conditions.append(f"✅ Above MA20 (${data['ma20']:.2f})")
        else:
            conditions.append(f"❌ Below MA20 (${data['ma20']:.2f})")
            all_met = False
    
    # Condition 3: Volume ratio > 1.0
    min_volume = config['entry_conditions']['min_volume_ratio']
    if data['volume_ratio'] >= min_volume:
        conditions.append(f"✅ Volume ratio: {data['volume_ratio']:.2f} >= {min_volume}")
    else:
        conditions.append(f"❌ Volume ratio: {data['volume_ratio']:.2f} < {min_volume}")
        all_met = False
    
    # Condition 4: RSI > 50
    rsi_min = config['entry_conditions']['rsi_min']
    if data['rsi'] >= rsi_min:
        conditions.append(f"✅ RSI: {data['rsi']:.1f} >= {rsi_min}")
    else:
        conditions.append(f"❌ RSI: {data['rsi']:.1f} < {rsi_min}")
        all_met = False
    
    return all_met, conditions


def calculate_positions(data: dict, config: dict) -> dict:
    """Calculate entry, stop, and target prices"""
    price = data['price']
    shares = config['trade_management']['shares_per_trade']
    stop_pct = config['exit_rules']['stop_loss_pct']
    target_pct = config['exit_rules']['take_profit_pct']
    
    stop_price = round(price * (1 - stop_pct / 100), 2)
    target_price = round(price * (1 + target_pct / 100), 2)
    
    # Calculate potential profit
    profit_per_share = target_price - price
    total_profit = profit_per_share * shares
    
    # Commission
    commission_per_share = config['commission']['per_share']
    min_commission = config['commission']['min_per_order']
    commission = max(min_commission, shares * commission_per_share) * 2  # entry + exit
    
    net_profit = total_profit - commission
    
    return {
        'entry_price': price,
        'stop_price': stop_price,
        'target_price': target_price,
        'shares': shares,
        'gross_profit': round(total_profit, 2),
        'commission': round(commission, 2),
        'net_profit': round(net_profit, 2),
        'stop_loss_pct': stop_pct,
        'target_pct': target_pct
    }


def save_signal(data: dict, is_green: bool, conditions: list, positions: dict):
    """Save signal to CSV"""
    new_row = pd.DataFrame([{
        'timestamp': datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        'session': data['session'],
        'signal': 'GREEN' if is_green else 'RED',
        'price': data['price'],
        'stop_price': positions['stop_price'],
        'target_price': positions['target_price'],
        'shares': positions['shares'],
        'net_profit': positions['net_profit'],
        'conditions': ' | '.join(conditions)
    }])
    
    if SIGNALS_PATH.exists():
        existing = pd.read_csv(SIGNALS_PATH)
        updated = pd.concat([existing, new_row], ignore_index=True)
    else:
        updated = new_row
    
    updated.to_csv(SIGNALS_PATH, index=False)


def main():
    print("=" * 50)
    print("TTP SOXX Signal Generator")
    print("=" * 50)
    
    config = load_config()
    data = load_latest_data()
    
    if not data:
        print("❌ No data available. Run collect_data.py first.")
        return
    
    print(f"Processing {data['session']} data from {data['timestamp']}")
    print(f"Current SOXX price: ${data['price']:.2f}")
    
    # Check for Green Day
    is_green, conditions = check_green_day(data, config)
    
    print("\n📊 Conditions Check:")
    for c in conditions:
        print(f"   {c}")
    
    if is_green:
        print("\n🟢 SIGNAL: GREEN DAY - READY TO BUY")
        positions = calculate_positions(data, config)
        
        print(f"\n📈 Trade Plan:")
        print(f"   Buy: {positions['shares']} shares @ ${positions['entry_price']:.2f}")
        print(f"   Stop Loss: ${positions['stop_price']:.2f} (-{positions['stop_loss_pct']}%)")
        print(f"   Take Profit: ${positions['target_price']:.2f} (+{positions['target_pct']}%)")
        print(f"   Gross Profit: ${positions['gross_profit']:.2f}")
        print(f"   Commission: ${positions['commission']:.2f}")
        print(f"   Net Profit: ${positions['net_profit']:.2f}")
    else:
        print("\n🔴 SIGNAL: RED DAY - WAIT")
        positions = {
            'entry_price': data['price'],
            'stop_price': 0,
            'target_price': 0,
            'shares': 2,
            'net_profit': 0
        }
    
    save_signal(data, is_green, conditions, positions)
    
    print("\n✅ Signal saved")


if __name__ == "__main__":
    main()
