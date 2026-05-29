#!/usr/bin/env python3
"""
Trade The Pool - Main System Runner
Runs data collection and signal generation
Includes git pull to avoid push conflicts
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
    
    # Step 3: Commit and push with pull first
    print("\n📤 Step 3: Committing and pushing updates...")
    
    # Pull latest changes first to avoid conflicts
    subprocess.run(["git", "pull", "origin", "main", "--rebase"], capture_output=True)
    
    # Add and commit
    subprocess.run(["git", "config", "user.name", "github-actions[bot]"])
    subprocess.run(["git", "config", "user.email", "github-actions[bot]@users.noreply.github.com"])
    subprocess.run(["git", "add", "data/ttp/"])
    subprocess.run(["git", "add", "docs/ttp/"])
    
    # Check if there are changes to commit
    result = subprocess.run(["git", "diff", "--staged", "--quiet"])
    if result.returncode != 0:
        subprocess.run(["git", "commit", "-m", f"TTP system update {__import__('datetime').datetime.now().strftime('%Y-%m-%d %H:%M')}"])
        subprocess.run(["git", "push", "origin", "main"])
        print("✅ Changes committed and pushed")
    else:
        print("✅ No changes to commit")
    
    print("\n✅ TTP System complete")
    print("📊 Dashboard: https://mrpink970.github.io/sector-research-system/docs/ttp/ttp_dashboard.html")

if __name__ == "__main__":
    main()
