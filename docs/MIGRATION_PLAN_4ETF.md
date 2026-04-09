
---

## Step-by-Step Migration

### Phase 1: Create Folders (via GitHub.com)

Create these empty files to auto-create folders:

1. `config/4_etf/.gitkeep`
2. `scripts/4_etf/.gitkeep`
3. `data/4_etf/.gitkeep`
4. `docs/4_etf/.gitkeep`

### Phase 2: Copy Files (No Path Changes Yet)

Copy each file from 4_ETF repo to main repo:

| Source (4_ETF repo) | Destination (Main repo) |
|---------------------|-------------------------|
| `run_etf_paper_trading.py` | `scripts/4_etf/run_4etf_system.py` |
| `update_4etf_daily_data.py` | `scripts/4_etf/update_4etf_data.py` |
| `collect_options_data.py` | `scripts/4_etf/collect_4etf_options.py` |
| `paper_trading_parameters.yaml` | `config/4_etf/4_etf_parameters.yaml` |
| `Dashboard.html` | `docs/4_etf/4etf_dashboard.html` |
| `.github/workflows/etf_paper_trading.yml` | `.github/workflows/4etf_system.yml` |
| `.github/workflows/collect_options.yml` | `.github/workflows/4etf_options.yml` |
| `4_ETF_Trading_Workbook_Template.xlsx` | `data/4_etf/4_ETF_Workbook.xlsx` |
| All CSV files | `data/4_etf/` (keep names) |

### Phase 3: Update Paths in Copied Files

When ready, replace each file with the path-corrected version.

**Path changes needed:**

| Old Path | New Path |
|----------|----------|
| `4_ETF_Trading_Workbook_Template.xlsx` | `data/4_etf/4_ETF_Workbook.xlsx` |
| `etf_paper_positions.csv` | `data/4_etf/positions.csv` |
| `etf_paper_trade_log.csv` | `data/4_etf/trade_log.csv` |
| `etf_paper_performance.csv` | `data/4_etf/performance.csv` |
| `account_balance.csv` | `data/4_etf/account_balance.csv` |
| `options_data.csv` | `data/4_etf/options_data.csv` |
| `paper_trading_parameters.yaml` | `config/4_etf/4_etf_parameters.yaml` |

### Phase 4: Test

1. Run `scripts/4_etf/update_4etf_data.py` manually
2. Run `scripts/4_etf/run_4etf_system.py` manually
3. Check that data appears in `data/4_etf/`
4. Verify dashboard works with new CSV paths

### Phase 5: Deploy

1. Push all changes to main repo
2. Trigger GitHub Actions workflows manually
3. Verify emails are still sending
4. Confirm all systems running

### Phase 6: Cleanup

1. Archive or delete the old `4-etf-trading-plan` repo
2. Update any bookmarks or references

---

## Current Progress

- [ ] Phase 1: Folders created
- [ ] Phase 2: Files copied
- [ ] Phase 3: Paths updated
- [ ] Phase 4: Tested
- [ ] Phase 5: Deployed
- [ ] Phase 6: Cleanup

---

## Notes

- The 4_ETF system continues running from its own repo during migration
- No rush — do one step at a time
- If something breaks, the old repo still works

---

## Last Updated

YYYY-MM-DD
