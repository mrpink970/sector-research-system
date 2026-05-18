#!/usr/bin/env python3
"""
Trade The Pool - Main System Runner
Runs data collection and signal generation
"""

import subprocess
import sys
from pathlib import Path

def main():
    print("=" * 60)
    print("TTP SOXX GREEN DAY SYSTEM")
    print("=" * 60)
    
    # Step 1: Collect data
    print("\n📊 Step 1: Collecting SOXX data...")
    result = subprocess.run([sys.executable, "scripts/ttp/collect_data.py"])
    if result.returncode != 0:
        print("❌ Data collection failed")
        return
    
    # Step 2: Generate signal
    print("\n📈 Step 2: Generating signal...")
    result = subprocess.run([sys.executable, "scripts/ttp/generate_signal.py"])
    if result.returncode != 0:
        print("❌ Signal generation failed")
        return
    
    print("\n✅ TTP System complete")
    print("📊 Dashboard: https://mrpink970.github.io/sector-research-system/docs/ttp/ttp_dashboard.html")

if __name__ == "__main__":
    main()
