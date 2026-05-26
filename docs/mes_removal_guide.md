# MES System Removal Guide

This document lists all files to delete to completely remove the MES (Micro E-mini S&P 500) futures trading system from your GitHub repository.

## Purpose
The MES system was designed to automatically trade MES futures based on EMA crossovers. It was never fully functional due to Yahoo Finance's inability to provide reliable futures data. The system is being removed to clean up the repository.

---

## Files to Delete

### 1. Workflow Files (GitHub Actions)

| File Path | Purpose |
|-----------|---------|
| `.github/workflows/mes_paper.yml` | Automated workflow that ran the MES system hourly |

### 2. Configuration Files

| File Path | Purpose |
|-----------|---------|
| `config/mes_config.yaml` | Configuration file for MES system parameters (contracts, stop loss, profit target, etc.) |

### 3. Script Files

| File Path | Purpose |
|-----------|---------|
| `scripts/mes_paper_trade/system.py` | Main MES trading system script |
| `scripts/mes_paper_trade/__init__.py` | Python package initializer (may be empty) |

### 4. Data Directory (Entire Folder)

| Folder Path | Contents |
|-------------|----------|
| `data/mes_paper/` | All MES paper trading data including: |
|             | - `positions.csv` (open positions) |
|             | - `trades.csv` (trade history) |
|             | - `dashboard_data.json` (dashboard data) |
|             | - `daily_log.csv` (daily P&L log) |
|             | - `progress.json` (evaluation progress) |

### 5. Documentation (if any)

| File Path | Purpose |
|-----------|---------|
| `docs/mes/` | Any MES-related documentation (check if this folder exists) |

---

## Deletion Commands (Run from repository root)

### Option A: Delete via Git commands

```bash
# Delete workflow file
git rm .github/workflows/mes_paper.yml

# Delete config file
git rm config/mes_config.yaml

# Delete script folder and all contents
git rm -r scripts/mes_paper_trade/

# Delete data folder and all contents
git rm -r data/mes_paper/

# Delete docs folder if it exists and only contains MES files
git rm -r docs/mes/ 2>/dev/null || echo "docs/mes folder not found"

# Commit the deletions
git commit -m "Remove MES futures trading system - not functional due to data limitations"

# Push to GitHub
git push
