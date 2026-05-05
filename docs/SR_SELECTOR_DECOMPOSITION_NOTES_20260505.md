# SR Selector Decomposition Notes 2026-05-05

Scope:
- additive diagnostics plus the doctrine fix from `liquidsniper/core/zone_selectors.py`
- daily-major selector now enforces a selected-zone floor of `meaningful_touch_count >= 3`
- local DB request date `2026-05-04` currently resolves to source candle `2026-03-31T00:00:00Z`

Artifacts:
- JSON: `artifacts/sr_selector_decomposition/btcusdt_1d_20260504_decomposition.json`
- Markdown: `artifacts/sr_selector_decomposition/btcusdt_1d_20260504_decomposition.md`

## Selected zones after 3-touch floor

The decomposition path matches `select_daily_majors` exactly after the touch-floor fix:

| Rank | Zone id | Families | Full bounds | Core bounds | Touches | Selection |
|---|---|---|---|---|---:|---:|
| 1 | `BTCUSDT:1D:base:611:resistance` | `base,reaction,structure` | `16800.00-23000.00` | `20387.40-21473.70` | 6 | 133.72 |
| 2 | `BTCUSDT:1D:base:1087:support` | `base,reaction,structure` | `48888.00-58144.50` | `50512.70-51316.90` | 4 | 133.06 |
| 3 | `BTCUSDT:1D:base:961:support` | `base,reaction,structure` | `24581.00-42882.54` | `28076.00-30374.60` | 5 | 130.77 |
| 4 | `BTCUSDT:1D:base:1586:support` | `base,reaction` | `102978.10-111160.00` | `107200.00-109732.30` | 6 | 127.33 |
| 5 | `BTCUSDT:1D:base:1507:support` | `base,reaction` | `83063.90-88651.20` | `85570.80-85600.00` | 4 | 125.35 |
| 6 | `BTCUSDT:1D:base:1278:resistance` | `base` | `57093.00-59914.90` | `57727.93-59279.97` | 5 | 115.72 |

## Focus bands

### 60k band `58000-62000`

- The old broad 50k support still clips the bottom of this band.
- A base resistance candidate at `57093.00-59914.90` with 5 touches now becomes the current-regime coverage anchor.
- The 1-touch `53750.00-60062.80` structure+reaction candidate is rejected by the daily-major touch floor.

### 65k band `63000-67000`

- No 65k zone is selected after the fix.
- `BTCUSDT:1D:structure:flip_anchor:44:55:resistance` still exists as a scored candidate, but it has only 1 meaningful touch and is rejected by the daily-major touch floor.
- This directly addresses the flaw Redact called out: current-regime coverage can no longer rescue a one-touch zone into selected daily majors.

### 74k band `70000-78000`

- No zone in this band is selected.
- The `73881.40-81148.10` structure+reaction candidate is also rejected by the 3-touch selected-zone floor because it has only 1 meaningful touch.
- The `70523.54` reaction zone still has 96 meaningful touches but low strength (`53.71`), so it remains rejected by the min-strength prefilter.

### 85k band `83000-88000`

- The selected zone remains `BTCUSDT:1D:base:1507:support`.
- Full bounds are `83063.90-88651.20`, core bounds are `85570.80-85600.00`, and it carries 4 meaningful touches.
- This remains valid under the selected-zone floor.

### 107k-110k band `102000-112000`

- The selected zone remains `BTCUSDT:1D:base:1586:support`.
- Full bounds are `102978.10-111160.00`, core bounds are `107200.00-109732.30`, with 6 meaningful touches.
- The nearby structure-only 1-touch candidates are now explicitly rejected by the touch floor.

## Observed doctrine outcome

- The selected daily-major set now has no zone below 3 meaningful touches.
- The suspicious `64.9k/65k` zone is removed.
- The 107k-110k zone is preserved, which matches the earlier qualitative review.
- The replacement around 60k is a 5-touch base resistance coverage anchor, not another one-touch structure rescue.

## Proposal-only next checks

- Decide whether the selected-zone floor should stay hardcoded at 3 or become a caller-visible selector parameter.
- Keep monitoring whether pure base-only replacements like `57093.00-59914.90` are doctrinally acceptable as daily current-regime coverage anchors.
- Surface the selected-zone touch floor in any operator-facing SR selector docs so this invariant is explicit rather than remembered in chat.
