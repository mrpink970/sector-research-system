# Paper Trading Experiment Log

## Purpose
This document tracks all changes made to the paper trading system, including:
- Strategy adjustments
- Entry/exit rule modifications
- Structural changes (long-only, filters, etc.)
- Observed results and conclusions

The goal is to:
- Avoid repeating failed ideas
- Identify what actually improves performance
- Build toward a stable, data-driven system

---

# Version 1 — Baseline System

### Description
- Long + Short system
- Entry based on sector signals
- Exit on:
  - Neutral signal
  - Trailing stop

### Result
- Mixed performance
- High noise from short trades
- Inconsistent outcomes

### Conclusion
- System functional, but unclear edge
- Needed isolation of variables

---

# Version 2 — Stricter Entry Filter

### Change
- Increased entry requirements (stronger signals only)

### Expected
- Higher quality trades
- Fewer losses

### Result
- Significantly fewer trades
- Sample size too small for evaluation

### Conclusion
- Not useful at this stage
- Revisit later after system stabilizes

---

# Version 3 — Neutral Exit (2-Day Confirmation)

### Change
- Required 2 consecutive neutral signals before exit

### Expected
- Avoid premature exits
- Capture longer trends

### Result
- Trades stayed open too long
- Losses increased significantly
- System performance degraded

### Conclusion
- Too slow for leveraged ETFs
- Rejected

---

# Version 4 — Bear-Only Exit

### Change
- Removed neutral exit
- Exit only on:
  - Bear signal
  - Trailing stop

### Expected
- Allow trades more room to develop
- Reduce premature exits

### Result (2-Year Backtest)
- Total trades: 89
- Win rate: ~47%
- Avg win: ~+9.9%
- Avg loss: ~-5.5%
- Total return: +160%

### Conclusion
- First clearly profitable structure
- Strong improvement over prior versions
- Valid baseline going forward

---

# Version 5 — Long-Only System

### Change
- Disabled all short trades
- Only take Bull / Strong Bull signals

### Expected
- Remove noise from weak bear trades
- Focus on strongest edge

### Result
- Cleaner trade behavior
- Bull trades showed strong upside
- Bear trades previously identified as low-impact

### Conclusion
- Long side is the core edge
- Likely superior to long/short system
- Use as foundation going forward

---

# Key Findings (So Far)

### 1. Bull Trades Are the Edge
- Larger gains
- More consistent trends
- Better expectancy

### 2. Bear Trades Add Noise
- Smaller moves
- Lower impact on performance
- Not necessary at this stage

### 3. Exit Logic Matters Most
- Neutral exits were too aggressive or too slow (depending on version)
- Bear-based exits performed better
- Trailing stops act as protection, not primary profit driver

### 4. Sample Size Matters
- Small datasets gave misleading conclusions
- 2-year backfill revealed true system behavior

---

# Current Direction

The system will move forward with:

- Long-only structure
- Bear-based exits
- Trailing stops always active
- No additional complexity until more data is analyzed

---

# Next Steps

- Add detailed trade logging (entry + daily + exit context)
- Run additional backtests with larger datasets
- Analyze:
  - Signal transitions (Strong Bull → Bull → etc.)
  - Trade duration patterns
  - Drawdowns before exits

---

# Notes

All future changes should be logged here before and after testing.

Each modification should include:
- What changed
- Why it changed
- Expected outcome
- Actual result
- Final conclusion

This ensures the system evolves based on data, not assumptions.
