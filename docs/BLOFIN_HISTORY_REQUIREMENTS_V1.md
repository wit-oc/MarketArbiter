# BloFin History Requirements V1

Date: 2026-04-21  
Status: proposed historical hydration contract  
Intent: freeze the per-timeframe history windows needed to seed and maintain the BloFin-first Surveyor stack

---

## 1) Why this exists

MarketArbiter should not treat all history as one flat bucket.
Different timeframes serve different purposes and need different hydration windows.

The system should therefore define:
- what historical context each timeframe must hold,
- what gets hydrated from REST,
- what remains hot operational state,
- and what must be rebuilt after reseed.

---

## 2) Core rule

Historical hydration should be **timeframe-specific**, not derived entirely from `5m`.

That means:
- `1W` history is pulled directly as weekly data
- `1D` history is pulled directly as daily data
- `4H` history is pulled directly as `4H` data
- `5m` is only the hot operational intraday layer

This keeps storage bounded and avoids forcing deep history through the noisiest timeframe.

---

## 3) Canonical history windows

## A. `1W`
### Purpose
- long-horizon market structure context
- macro SR context
- long-range bias anchoring

### Hydration contract
- hydrate the full bounded weekly history window available/required for the product

### First-pass recommendation
- keep all weekly candles that fit the product boundary
- or use a bounded cap such as **520 weeks** if we need an explicit number

---

## B. `1D`
### Purpose
- daily structure and SR context
- medium/long trend framing
- fib and regime context at daily scale

### Hydration contract
- hydrate up to **1440 daily candles**

### Rationale
- deep enough for strong daily context
- still operationally cheap to store

---

## C. `4H`
### Purpose
- operational higher-timeframe context for intraday decisions
- primary higher-timeframe structure / SR / fib context

### Hydration contract
- operational/live minimum: hydrate **180 days** of `4H`
- research/backtest minimum: hydrate at least **3 years** of `4H`
- preferred BTC/top-100 research target: **5 years** of `4H` where provider history supports it

### Approximate size
- 6 candles/day
- about **1,080 candles** for 180 days
- about **6,570 candles** for 3 years
- about **10,950 candles** for 5 years

---

## D. `5m`
### Purpose
- live operational intraday state
- short-horizon continuity and recovery
- feed freshness and near-term market-state inspection

### Hydration contract
- hydrate only the hot operational window

### First-pass recommendation
- **rolling 5D** `5m`

### Approximate size
- 288 candles/day
- **1440 candles** over 5 days

This aligns neatly with the current BloFin REST page limit and the preferred recovery horizon.

---

## 4) Why `5m` should stay small

`5m` is the most expensive and least stable history layer to hoard.

If we try to make it our forever archive, we get:
- more storage churn
- more recovery complexity
- more risk of subtle defects in long-span repair logic

But if we constrain it to a hot rolling window, we get:
- simpler startup behavior
- simpler repair rules
- and enough operational state for live ingestion and recent continuity

---

## 5) Relationship to derived aggregates

There are two distinct things here:

### Timeframe-specific historical hydration
- direct REST pulls for `1W`, `1D`, `4H`, and `5m`

### Derived aggregate materialization
- higher-timeframe candles created from canonical `5m` during live operation for continuity and local consistency

These can coexist.

### Practical interpretation
- deep history at `1W`, `1D`, `4H` comes directly from REST
- live incremental updates after startup may derive `4H`, `1D`, `1W` from canonical `5m`
- if trust is broken, refresh history directly again from REST according to each timeframe’s contract

That gives us both bounded live operation and authoritative reseed paths.

---

## 6) Bootstrap contract

At initial symbol hydration:

1. load weekly history according to `1W` contract
2. load daily history according to `1D` contract
3. load `4H` history according to `4H` contract
4. load hot `5m` history according to `5m` contract
5. validate each timeframe independently
6. then begin live WS `5m` consumption

### Important rule
Bootstrap should finish before normal live consumption becomes authoritative.

---

## 7) Recovery contract

### If outage gap <= `5m` hot window
- reload the missing/affected `5m` hot window from REST
- rebuild overlapping derived windows
- preserve longer timeframe historical stores unless they are directly invalidated

### If outage gap > `5m` hot window
- full reseed using the full timeframe-specific history contracts:
  - `1W`
  - `1D`
  - `4H`
  - `5m`

This is the clean split between repair and reseed.

---

## 8) Data volume implications

For the first top-100 symbol target:

### `5m`
- 1440 candles per symbol over 5D
- top 100 = about **144,000 `5m` candles**

### `4H`
- operational/live minimum: about 1,080 candles per symbol over 180D
- research minimum: about 6,570 candles per symbol over 3Y
- preferred research target: about 10,950 candles per symbol over 5Y
- top 100 at 5Y = about **1,095,000 `4H` candles**

### `1D`
- 1440 candles per symbol
- top 100 = about **144,000 `1D` candles**

### `1W`
- capped weekly store is still comparatively cheap

That is very manageable.

---

## 9) Dataset-family dependency posture

### `5m`
Used for:
- feed continuity
- recent operational context
- live close detection
- intraday short-horizon sanity checks

### `4H`
Used for:
- primary higher-timeframe structure
- SR context
- dynamic levels and fib context for current strategy framing

### `1D`
Used for:
- daily structure and regime framing
- stronger SR anchoring
- longer fib context

### `1W`
Used for:
- macro structure / bias context
- major long-range levels

This supports the current stated expectation that most heavy recompute should occur at `4H`, `1D`, and `1W`, not constantly at `5m`.

---

## 10) Retention policy

### Recommended first-pass retention
- `5m`: **rolling 5D**
- `4H`: **180D**
- `1D`: **1440 candles**
- `1W`: full bounded weekly history cap

### Daily maintenance
Run daily retention/compaction that:
- removes expired `5m`
- trims expired raw WS staging
- keeps `4H/1D/1W` within contract bounds
- records a compaction audit

---

## 11) Historical-depth caveat

The BloFin-first plan is workable, but the exchange’s actual historical depth still needs empirical validation for the symbols we care about.

### What must be tested
- whether BloFin REST reliably serves the required `1W`, `1D`, `4H`, and `5m` windows
- whether pagination behavior is stable enough for reseed
- whether OKX still needs to remain a deep-history fallback for some dataset families

Until validated, the safest posture is:
- BloFin for live + normal bootstrap
- keep open the possibility of OKX as a historical fallback if required by deeper SR/structure needs

---

## 12) Rate-limit safety note

These history windows describe *what* must be loaded, not permission to load them recklessly.

### Required posture
- paginate conservatively
- route all BloFin REST calls through the shared governor
- prefer demo endpoints during development/testing
- if production is used later, keep recovery/hydration orchestration governor-aware by default

## 13) Decision

The canonical historical-hydration posture for the BloFin-first Surveyor stack is:
- **timeframe-specific hydration**
- **rolling 5D `5m` operational state**
- **180D `4H`**
- **1440 `1D` candles**
- **full bounded `1W` context**
- and **full timeframe-specific reseed when the hot intraday window is no longer trustworthy**

That is the history contract the recovery workflow and recompute scheduler should assume.
