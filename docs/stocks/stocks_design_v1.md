# stocks_design_v1.md

## System
Stock Discovery System v1

## Type
Long-only momentum discovery system

## Objective
Identify stocks with sustained bullish momentum and institutional-style accumulation.

## Universes

### Universe A — Quality
- Price >= 10
- Market Cap >= 1B
- Average Daily Volume >= 1M

### Universe B — Emerging
- Price >= 3
- Market Cap >= 300M
- Average Daily Volume >= 250K

## Exclusions
- OTC
- ETFs
- SPAC remnants
- warrants
- rights
- non-operating listings

## Score language
- >= 6 → Strong Bullish
- 3 to 5 → Bullish
- -2 to 2 → Neutral

For this system:
- only Strong Bullish becomes a tradable candidate
- Bullish remains informational
- Neutral and lower are ignored for execution

## Indicators
- Relative strength vs market
- Multi-timeframe trend
- Trend structure
- Volume confirmation
- Volatility control
- Extension filter

## Trade use later
- Long only
- Require 2-day confirmation later in the execution layer
- Cash if no qualifying candidates

## Status
Version: v1
State: LOCKED
