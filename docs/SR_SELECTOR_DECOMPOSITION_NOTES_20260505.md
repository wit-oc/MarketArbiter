# SR Selector Decomposition Notes 2026-05-05

Scope:
- additive diagnostics only
- no selector doctrine change
- local DB request date `2026-05-04` currently resolves to source candle `2026-03-31T00:00:00Z`

Artifacts:
- JSON: `artifacts/sr_selector_decomposition/btcusdt_1d_20260504_decomposition.json`
- Markdown: `artifacts/sr_selector_decomposition/btcusdt_1d_20260504_decomposition.md`

## Selected zones

The canonical daily-major selector still resolves to six majors, and the decomposition path matches `select_daily_majors` exactly:

| Rank | Zone id | Families | Full bounds | Core bounds | Touches | Selection |
|---|---|---|---|---|---:|---:|
| 1 | `BTCUSDT:1D:base:611:resistance` | `base,reaction,structure` | `16800.00-23000.00` | `20387.40-21473.70` | 6 | 133.72 |
| 2 | `BTCUSDT:1D:base:1087:support` | `base,reaction,structure` | `48888.00-58144.50` | `50512.70-51316.90` | 4 | 133.06 |
| 3 | `BTCUSDT:1D:base:961:support` | `base,reaction,structure` | `24581.00-42882.54` | `28076.00-30374.60` | 5 | 130.77 |
| 4 | `BTCUSDT:1D:base:1586:support` | `base,reaction` | `102978.10-111160.00` | `107200.00-109732.30` | 6 | 127.33 |
| 5 | `BTCUSDT:1D:base:1507:support` | `base,reaction` | `83063.90-88651.20` | `85570.80-85600.00` | 4 | 125.35 |
| 6 | `BTCUSDT:1D:structure:flip_anchor:44:55:resistance` | `reaction,structure` | `63019.10-69198.70` | `64920.13-64986.11` | 1 | 122.59 |

## Focus bands

### 60k band `58000-62000`

- No exact `60k` major is selected.
- The selected nearby major is broader support `48888.00-58144.50`, which only clips the bottom of this band.
- A `reaction,structure` support candidate at `53750.00-60062.80` with 1 meaningful touch survived to distance collapse, then got demoted as too close to a stronger representative.
- A pure base resistance candidate at `57093.00-59914.90` had 5 touches but lost in the local-band representative stage.

### 65k band `63000-67000`

- The selected zone is `BTCUSDT:1D:structure:flip_anchor:44:55:resistance`.
- Full bounds are `63019.10-69198.70`, but the operator core is the narrow `64920.13-64986.11` pocket Redact called out.
- It has only 1 meaningful touch, but it carries `reaction,structure` confluence and enters the final set through the current-regime coverage anchor step.

### 74k band `70000-78000`

- No zone in this band is selected.
- A `reaction,structure` support candidate at `73881.40-81148.10` exists with selection score `121.30`, but it is removed by distance collapse as too close to a stronger representative.
- A pure reaction zone centered around `70523.54` has 96 meaningful touches, but its strength score is only `53.71`, so it fails the `min_strength >= 70` prefilter.

### 85k band `83000-88000`

- The selected zone is `BTCUSDT:1D:base:1507:support`.
- Full bounds are `83063.90-88651.20`, core bounds are `85570.80-85600.00`, and it carries 4 meaningful touches.
- This zone looks consistent with current doctrine: confirmed, high-scoring, and retained through pocket consolidation.

### 107k-110k band `102000-112000`

- The selected zone is `BTCUSDT:1D:base:1586:support`.
- Full bounds are `102978.10-111160.00`, core bounds are `107200.00-109732.30`, with 6 meaningful touches.
- Two structure-only candidates also overlap this area, but both lose in the local-band representative stage.
- This band looks broadly reasonable under the current selector.

## Observed doctrine smells

- The `64.9k/65k` selected zone is a narrow core inside a much wider `63019.10-69198.70` envelope and only has 1 meaningful touch. It survives because structure+reaction confluence plus current-regime coverage logic can outrank local touch count.
- The `74k` area has a structure+reaction candidate with a very strong selection score, but it still disappears at distance collapse because a stronger nearby representative occupies the same neighborhood.
- The `70.5k` reaction candidate shows the opposite smell: very high touch count, but low strength. That implies the current reaction scoring is discounting those touches heavily because the interaction quality/body-respect/retest profile is poor.
- The `85k` zone is selected with only 4 touches, which is lower than the old daily minimum-touch intuition but consistent with the current merged-family scoring path.
- The `107k-110k` selection looks less suspicious than the mid-60k area because it combines broad support geography with 6 touches and no current-regime coverage rescue.

## Proposal-only next changes

- Add explicit stage-demotion provenance to the canonical selector outputs, not just to this debug script, so band loss vs distance collapse vs coverage replacement are visible without a second pass.
- Review whether the current-regime coverage rescue should have an extra floor for low-touch candidates, especially when the final selected core is much narrower than the macro envelope.
- Review whether distance collapse is too aggressive around adjacent high-scoring candidates in the `60k-75k` region.
- Review why the `70523.54` reaction zone can accumulate 96 meaningful touches yet still score only `53.71`; if that is doctrinally correct, surface the penalty components more explicitly.
- Keep all of the above as proposal-only until a separate doctrine pass decides whether the current behavior is wrong or simply unintuitive.
