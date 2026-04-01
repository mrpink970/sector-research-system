name: Exit Efficiency Analysis

on:
  schedule:
    - cron: '0 9 1 * *'   # 9:00 AM UTC on the 1st of each month
  workflow_dispatch:

jobs:
  run-analysis:
    runs-on: ubuntu-latest

    steps:
      - name: Checkout repo
        uses: actions/checkout@v4

      - name: Set up Python
        uses: actions/setup-python@v5
        with:
          python-version: '3.11'

      - name: Install dependencies
        run: |
          pip install pandas pyyaml

      - name: Run analysis script
        run: |
          python scripts/analyze_exit_efficiency.py

      - name: Commit results
        run: |
          git config user.name "github-actions"
          git config user.email "actions@github.com"
          git add data/exit_efficiency_audit.csv data/top_givebacks.csv data/exit_efficiency_summary.csv
          git commit -m "Exit efficiency analysis $(date '+%Y-%m-%d')" || echo "No changes to commit"
          git push
