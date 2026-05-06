# BTCUSDT 1D SR Selector Decomposition

Generated at: `2026-05-06T01:52:52.914871Z`
JSON artifact: `artifacts/sr_selector_decomposition/btcusdt_1d_20260504_decomposition.json`

## Data-source caveat

Requested 2026-05-04 currently resolves to the last local MarketArbiter candle on 2026-03-31. This decomposition reflects the local SQLite state, not a newer exchange candle. TradingView/vendor history differences should still be separated from selector logic.

Requested as-of date: `2026-05-04`
Resolved source candle timestamp: `2026-03-31T00:00:00Z`

## Counts

- Structure candidates: `31`
- Base candidates: `30`
- Reaction candidates: `20`
- Merged candidates: `33`
- Confirmed weighted candidates: `33`
- Touch-floor candidates: `18`
- Prefilter candidates: `16`
- Selected daily majors: `6`

## Selected Daily Majors

| Rank | Zone | Families | Core bounds | Full bounds | Touches | Strength | Selection | Reason |
|---|---|---|---|---|---:|---:|---:|---|
| 1 | `BTCUSDT:1D:base:611:resistance` | `base,reaction,structure` | 20387.40-21473.70 | 16800.00-23000.00 | 6 | 100.00 | 133.72 | kept: daily macro anchor after pocket consolidation |
| 2 | `BTCUSDT:1D:base:1087:support` | `base,reaction,structure` | 50512.70-51316.90 | 48888.00-58144.50 | 4 | 100.00 | 133.06 | kept: daily macro anchor after pocket consolidation |
| 3 | `BTCUSDT:1D:base:961:support` | `base,reaction,structure` | 28076.00-30374.60 | 24581.00-42882.54 | 5 | 100.00 | 130.77 | kept: daily macro anchor after pocket consolidation |
| 4 | `BTCUSDT:1D:base:1586:support` | `base,reaction` | 107200.00-109732.30 | 102978.10-111160.00 | 6 | 100.00 | 127.33 | kept: daily macro anchor after pocket consolidation |
| 5 | `BTCUSDT:1D:base:1507:support` | `base,reaction` | 85314.58-85856.22 | 83063.90-88651.20 | 4 | 100.00 | 125.35 | kept: daily macro anchor after pocket consolidation |
| 6 | `BTCUSDT:1D:base:1278:resistance` | `base` | 57727.93-59279.97 | 57093.00-59914.90 | 5 | 100.00 | 115.72 | kept: daily current-regime coverage anchor |

## Focus Bands

Last close: `68241.50`

### 60k `58000-62000`

- Overlapping scored candidates: `3`
- Selected in band: `2`

| Zone | Families | Full bounds | Core bounds | Touches | Strength | Selection | Bucket | Reason |
|---|---|---|---|---:|---:|---:|---|---|
| `BTCUSDT:1D:base:1087:support` | `base,reaction,structure` | 48888.00-58144.50 | 50512.70-51316.90 | 4 | 100.00 | 133.06 | `selected` | kept: daily macro anchor after pocket consolidation |
| `BTCUSDT:1D:base:1278:resistance` | `base` | 57093.00-59914.90 | 57727.93-59279.97 | 5 | 100.00 | 115.72 | `selected` | kept: daily current-regime coverage anchor |
| `BTCUSDT:1D:structure:bos_anchor:225:228:support` | `reaction,structure` | 53750.00-60062.80 | 54048.93-60062.80 | 1 | 100.00 | 122.05 | `below_min_touches` | rejected by daily-major touch floor: meaningful_touch_count=1 < 3 |

### 65k `63000-67000`

- Overlapping scored candidates: `1`
- Selected in band: `0`

| Zone | Families | Full bounds | Core bounds | Touches | Strength | Selection | Bucket | Reason |
|---|---|---|---|---:|---:|---:|---|---|
| `BTCUSDT:1D:structure:flip_anchor:44:55:resistance` | `reaction,structure` | 63019.10-69198.70 | 63019.10-68450.75 | 1 | 100.00 | 122.59 | `below_min_touches` | rejected by daily-major touch floor: meaningful_touch_count=1 < 3 |

### 74k `70000-78000`

- Overlapping scored candidates: `2`
- Selected in band: `0`

| Zone | Families | Full bounds | Core bounds | Touches | Strength | Selection | Bucket | Reason |
|---|---|---|---|---:|---:|---:|---|---|
| `BTCUSDT:1D:structure:flip_anchor:1498:1543:support` | `reaction,structure` | 73881.40-81148.10 | 73992.29-80762.87 | 1 | 100.00 | 121.30 | `below_min_touches` | rejected by daily-major touch floor: meaningful_touch_count=1 < 3 |
| `BTCUSDT:1D:10:70523.54` | `reaction` | 68994.02-72053.06 | 68994.02-72053.06 | 96 | 53.71 | 65.20 | `below_min_strength` | rejected by min-strength prefilter: strength_score=53.71 < 70.00 |

### 85k `83000-88000`

- Overlapping scored candidates: `1`
- Selected in band: `1`

| Zone | Families | Full bounds | Core bounds | Touches | Strength | Selection | Bucket | Reason |
|---|---|---|---|---:|---:|---:|---|---|
| `BTCUSDT:1D:base:1507:support` | `base,reaction` | 83063.90-88651.20 | 85314.58-85856.22 | 4 | 100.00 | 125.35 | `selected` | kept: daily macro anchor after pocket consolidation |

### 108k `102000-112000`

- Overlapping scored candidates: `3`
- Selected in band: `1`

| Zone | Families | Full bounds | Core bounds | Touches | Strength | Selection | Bucket | Reason |
|---|---|---|---|---:|---:|---:|---|---|
| `BTCUSDT:1D:base:1586:support` | `base,reaction` | 102978.10-111160.00 | 107200.00-109732.30 | 6 | 100.00 | 127.33 | `selected` | kept: daily macro anchor after pocket consolidation |
| `BTCUSDT:1D:structure:bos_anchor:1574:1592:support` | `structure` | 90200.00-102079.80 | 93583.51-100948.99 | 1 | 100.00 | 119.87 | `below_min_touches` | rejected by daily-major touch floor: meaningful_touch_count=1 < 3 |
| `BTCUSDT:1D:structure:flip_anchor:1421:1457:resistance` | `structure` | 101311.50-110000.00 | 102270.46-109041.04 | 1 | 100.00 | 117.95 | `below_min_touches` | rejected by daily-major touch floor: meaningful_touch_count=1 < 3 |

## Parity Check

- Decomposition matches canonical `select_daily_majors`: `True`

