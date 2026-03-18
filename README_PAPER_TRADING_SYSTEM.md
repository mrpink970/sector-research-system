
# Paper Trading System v1

This package adds a fully automated paper-trading engine to the existing sector research repo.

## What it reads
- `data/sector_scores.csv`
- `data/market_data.csv`
- `config/sector_map.yaml`

## What it writes
- `data/paper_positions.csv`
- `data/paper_trade_log.csv`
- `data/paper_performance.csv`

## Locked v1 behaviors
- Max concurrent positions: 2
- Position size: 100 shares
- Confirmation: 2 consecutive closes in the same direction
- Entry universe: confirmed non-neutral signals only
- Direction mapping:
  - Bull / Strong Bull -> bull ETF
  - Bear / Strong Bear -> inverse ETF
- Cash is valid when there are fewer than 2 tradable candidates
- Protective trailing stops:
  - 1x ETF / stock: 10%
  - 2x ETF: 14%
  - 3x ETF: 18%
- Managed exit:
  - Bull-side position exits after 2 confirmed bearish closes
  - Bear-side position exits after 2 confirmed bullish closes
- Replacement exit:
  - Only when a clearly stronger confirmed candidate exists

## Important implementation clarification
The sector research system ranks sectors by raw `total_score`, which naturally favors bullish sectors over bearish sectors.
To make the paper system direction-agnostic, this engine computes its own **tradable rank** using instrument-adjusted strength:

- bull candidate strength = `total_score`
- bear candidate strength = `-total_score`

Candidates are then ranked by this positive directional strength.
This is what allows a strong bearish setup to outrank a weak bullish setup.

## Integration
Merge these files into the same repo as the sector research system, then enable:
- `.github/workflows/paper_trading_system.yml`

That workflow is configured to run automatically after the `Sector Research System` workflow completes successfully.

## Notes
- Sectors with missing or blank directional mapping are skipped for that direction.
- If `sector_map.yaml` notes contain the word `placeholder`, the affected direction is skipped and written to the logs.
- Outputs are rebuilt deterministically from the available historical data each run.


## Missing inverse ETF handling (locked)
If a sector produces a confirmed bearish signal but `bear_etf` is blank or missing in `config/sector_map.yaml`, the paper system:

- keeps that sector in the research ranking
- skips it for execution
- moves to the next eligible tradable candidate

It never substitutes the signal ETF and never guesses an inverse instrument.
