# BTCUSDT 1D Surveyor SR Pine Parity Golden Zones

Generated at: `2026-05-04T15:54:20.393466Z`
JSON artifact: `artifacts/pine_sr_parity/btcusdt_1d_golden_zones.json`

These fixtures are point-in-time outputs from the canonical LiquidSniper Surveyor V3 daily-major SR path, using MarketArbiter's imported Binance USD-M futures daily candles.

## Data-source caveat

Golden zones are generated from MarketArbiter's local imported Binance Public Data USD-M futures candles. TradingView candle history/session/vendor rounding can differ; Pine parity checks should separate code-port bugs from data-source mismatch.

## Config

- `daily_cluster_eps`: `1.1`
- `daily_max_zones`: `8`
- `daily_min_meaningful_touches`: `5`
- `daily_min_strength`: `70.0`
- `daily_min_zone_separation_bps`: `250.0`
- `daily_reaction_atr_min`: `0.6`
- `daily_require_first_retest_quality`: `True`
- `min_history_bars`: `365`
- `use_operator_core_bounds`: `True`

## Snapshots

| As-of date | Candle count | Last close | Candidates S/B/R/M | Selected | Zone ranks |
|---|---:|---:|---:|---:|---|
| 2023-01-01 | 672 | 16610.30 | 13/0/73/35 | 6 | #1 resistance core 31442.38-31481.62 score 127.26 families reaction<br>#2 resistance core 41393.38-41432.62 score 123.69 families reaction<br>#3 resistance core 36603.38-36642.62 score 122.16 families reaction<br>#4 resistance core 24388.52-25021.28 score 119.31 families reaction,structure<br>#5 resistance core 46820.90-48200.00 score 118.54 families reaction,structure<br>#6 resistance core 63019.10-64986.11 score 116.93 families reaction,structure |
| 2024-01-01 | 1037 | 44230.20 | 20/13/19/23 | 5 | #1 resistance core 20026.00-21473.70 score 132.71 families base,reaction,structure<br>#2 resistance core 29050.00-29686.70 score 127.04 families base,reaction<br>#3 resistance core 43392.36-44396.68 score 123.36 families reaction,structure<br>#4 resistance core 63019.10-64986.11 score 122.24 families reaction,structure<br>#5 resistance core 46820.90-48200.00 score 122.22 families reaction,structure |
| 2025-01-01 | 1403 | 94580.90 | 26/39/11/24 | 4 | #1 support core 50512.70-52307.89 score 135.66 families base,reaction,structure<br>#2 resistance core 29070.00-29686.70 score 132.17 families base,reaction,structure<br>#3 resistance core 20387.40-21473.70 score 131.11 families base,reaction,structure<br>#4 resistance core 64920.13-64986.11 score 122.59 families reaction,structure |
| 2026-03-31 | 1857 | 68241.50 | 31/30/20/33 | 6 | #1 resistance core 20387.40-21473.70 score 133.72 families base,reaction,structure<br>#2 support core 50512.70-51316.90 score 133.06 families base,reaction,structure<br>#3 support core 28076.00-30374.60 score 130.77 families base,reaction,structure<br>#4 support core 107200.00-109732.30 score 127.33 families base,reaction<br>#5 support core 85570.80-85600.00 score 125.35 families base,reaction<br>#6 resistance core 64920.13-64986.11 score 122.59 families reaction,structure |

## Pine parity use

Compare Pine-selected daily-major operator-core bounds and selector ranks against the JSON `operator_core_bounds`/`selector_rank` values for each fixed as-of date. Full macro bounds are present under `full_zone_bounds` for debugging divergence.
