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

      - name: Upload results as artifact
        uses: actions/upload-artifact@v4
        with:
          name: exit-analysis-results
          path: data/
