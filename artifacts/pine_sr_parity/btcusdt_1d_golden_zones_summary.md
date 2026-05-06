# BTCUSDT 1D Surveyor SR Pine Parity Golden Zones

Generated at: `2026-05-06T02:23:52.553470Z`
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
| 2023-01-01 | 672 | 16610.30 | 13/0/73/35 | 7 | #1 resistance core 31430.54-31493.46 score 127.26 families reaction<br>#2 resistance core 41377.32-41448.68 score 123.69 families reaction<br>#3 resistance core 36587.32-36658.68 score 122.16 families reaction<br>#4 resistance core 56380.34-56451.70 score 117.44 families reaction<br>#5 support core 28669.38-28726.78 score 112.09 families reaction<br>#6 support core 46330.92-46423.67 score 100.25 families reaction<br>#7 support core 15865.33-15924.27 score 99.50 families reaction |
| 2024-01-01 | 1037 | 44230.20 | 20/13/19/23 | 3 | #1 resistance core 20026.00-21473.70 score 132.71 families base,reaction,structure<br>#2 resistance core 29050.00-29686.70 score 127.04 families base,reaction<br>#3 mixed core 59421.87-59903.12 score 100.54 families reaction |
| 2025-01-01 | 1403 | 94580.90 | 26/39/11/24 | 4 | #1 support core 50512.70-52307.89 score 135.66 families base,reaction,structure<br>#2 resistance core 28990.04-29766.66 score 132.17 families base,reaction,structure<br>#3 resistance core 20387.40-21473.70 score 131.11 families base,reaction,structure<br>#4 support core 17414.89-18191.51 score 121.70 families base,structure |
| 2026-03-31 | 1857 | 68241.50 | 31/30/20/33 | 6 | #1 resistance core 20387.40-21473.70 score 133.72 families base,reaction,structure<br>#2 support core 50512.70-51316.90 score 133.06 families base,reaction,structure<br>#3 support core 28076.00-30374.60 score 130.77 families base,reaction,structure<br>#4 support core 107200.00-109732.30 score 127.33 families base,reaction<br>#5 support core 85314.58-85856.22 score 125.35 families base,reaction<br>#6 resistance core 57727.93-59279.97 score 115.72 families base |

## Pine parity use

Compare Pine-selected daily-major operator-core bounds and selector ranks against the JSON `operator_core_bounds`/`selector_rank` values for each fixed as-of date. Full macro bounds are present under `full_zone_bounds` for debugging divergence.
