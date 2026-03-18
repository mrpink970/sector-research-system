
# PAPER TRADING SYSTEM — DESIGN GOALS v1 (LOCKED)

## 1. Purpose
The Paper Trading System is a fully automated execution simulation engine designed to:
- evaluate real-world performance of signal-driven strategies
- execute trades using strict, rule-based logic
- measure performance, risk, and expectancy
- deploy capital only when a confirmed directional edge exists

This system:
- executes simulated trades
- tracks full trade lifecycle
- does not execute real trades
- does not modify upstream signals

## 2. System Role
Inputs:
- Primary: `sector_scores.csv`
- Must support future signal systems

Outputs:
- Simulated trades
- Performance metrics

## 3. Core Principles
1. Fully automated
2. Deterministic
3. Signal-driven
4. Separation of concerns
5. Direction-agnostic
6. Designed for high return with controlled downside

## 4. Position Model
### 4.1 Position Limit
- Max concurrent positions: 2

### 4.2 Position Size
- Position size: 100 shares (fixed)

## 5. Entry Logic
- Select from highest-ranked eligible signals
- Signal must be confirmed by 2 closes
- Signal must be non-neutral
- Bullish / Strong Bullish -> bull ETF
- Bearish / Strong Bearish -> inverse ETF
- Cash is valid if fewer than 2 qualifying signals exist

## 6. Exit System
### 6.1 Protective Exit
Trailing stop based on leverage class:
- 1x ETF / stock: 10%
- 2x ETF: 14%
- 3x ETF: 18%

### 6.2 Managed Exit
Primary exit:
- Bull-side position exits after 2 confirmed bearish closes
- Bear-side position exits after 2 confirmed bullish closes

### 6.3 Replacement Exit
Replace only if all conditions are true:
- current position is no longer the strongest directionally confirmed candidate
- replacement candidate is ranked #1 by tradable strength
- replacement candidate strength is at least 2 points higher
- replacement candidate is confirmed for 2 closes

## 7. Daily Execution Flow
1. Load latest research signals
2. Map signals to tradeable instruments
3. Update open positions and trailing stops
4. Evaluate exits
5. Execute exits
6. Evaluate new entries
7. Open positions
8. Record outputs

## 8. Output Files
### `paper_positions.csv`
- Active positions
- Entry price
- Highest price
- Trailing stop
- Current close
- Unrealized P/L

### `paper_trade_log.csv`
- Ticker
- Entry date / exit date
- Entry price / exit price
- Shares
- Entry signal / exit signal
- Gross P/L ($)
- Return (%)
- Trade duration
- Exit type

### `paper_performance.csv`
- Total trades
- Win rate
- Loss rate
- Average gain
- Average loss
- Largest gain
- Largest loss
- Total return
- Max drawdown
- Expectancy per trade

Expectancy formula:
`Expectancy = (Win Rate × Avg Gain) − (Loss Rate × Avg Loss)`

## 9. System Character
Direction-agnostic trend-following with controlled downside and selective rotation.

## 10. Boundaries
Does not include:
- real trading
- portfolio optimization
- signal generation


## Missing inverse ETF handling
If a sector has a confirmed bearish signal and no valid inverse ETF exists, that sector is skipped for execution but remains in the research ranking. The system then evaluates the next eligible tradable candidate.
