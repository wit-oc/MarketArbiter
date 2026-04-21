# Surveyor / Arbiter Repo Refocus Plan

Date: 2026-04-19  
Status: proposed cleanup plan based on current repository shape  
Intent: narrow the repo to the actual product direction, without losing the historical work we may still need for reference

---

## Bottom line

Yes, we should refocus the repo.

My recommendation is:
1. **freeze the new product boundary first**,
2. **archive non-core legacy surfaces inside the repo before deleting anything**,
3. **rename the repo only after the boundary is real**.

If we rename too early, we just smear the current ambiguity onto a new name.
If we archive first, the rename becomes honest.

---

## 1) What the repo is trying to become

The active product is now much narrower than historical LiquidSniper.

The repo should primarily exist to support:
- **Surveyor**: descriptive market-state assembly
- **Arbiter**: interpretation / decision contract
- canonical feed ingestion and storage
- packetized multi-timeframe analysis
- replay / simulation / backtesting built on that packet
- operator-facing inspection UI

That means the repo is no longer primarily about:
- Telegram/Mobchart ingestion
- paper-trading runtime behavior
- execution-policy plumbing
- TradingView automation as a major first-class surface
- legacy multi-lane strategy sweeps as a product identity

Those may remain useful as references, but they should stop defining the top-level shape.

---

## 2) Recommended product boundary

## Keep as first-class

### A. Surveyor core
- `liquidsniper/core/surveyor_snapshot.py`
- `liquidsniper/ops/surveyor_feed_refresh.py`
- `IntradayTrading/engine/surveyor_packet.py`
- `IntradayTrading/engine/phase1_contract.py`
- `IntradayTrading/engine/fib_anchors.py`
- `IntradayTrading/engine/fib_context.py`
- `IntradayTrading/engine/dynamic_levels.py`
- the upstream structure engine dependencies these rely on

### B. Canonical storage and feed-quality layer
- `liquidsniper/core/db.py`
- `liquidsniper/core/market_data.py`
- `liquidsniper/core/market_scheduler.py`
- migrations and feed-health tables

### C. Operator UI
- `liquidsniper/web/app.py`
- only the tabs/views that support Surveyor inspection and later Arbiter review

### D. Backtesting / replay foundation
Keep only the parts that are still useful for:
- deterministic replay
- simulation
- evidence capture
- packet-based backtesting

But move them under the Surveyor/Arbiter framing, not the old paper-runtime framing.

### E. Architecture and doctrine docs
Keep and elevate docs that explain:
- Surveyor / Arbiter architecture
- canonical feed posture
- structure / Fib / S&R contract boundaries
- backtesting design

---

## 3) Strong archive candidates

These are the surfaces I would treat as legacy-first unless proven otherwise.

### A. Telegram / Mobchart ingestion surfaces
Likely archive candidates:
- `liquidsniper/ingestor/`
- legacy ingestion docs that assume Telegram is a core boundary

Reason:
- current architecture is centered on canonical feed + packet assembly, not message ingestion as the product identity.

### B. Paper-trading runtime and execution-policy surfaces
Likely archive or split-out candidates:
- paper daemon / execution-control docs
- runtime control logic anchored to old paper-trading behavior
- policy/execution boundary code that is not needed for Surveyor or first-pass Arbiter

Reason:
- useful history, but it muddies the repo’s current purpose.

### C. TradingView-heavy surfaces
Likely archive candidates:
- `tradingview/`
- TradingView artifact and automation flows that are not needed for Surveyor packet correctness
- related docs that present TV as a primary product lane

Reason:
- can remain as optional context tooling, but should not dominate the repo shape.

### D. Legacy sweep / experiment tooling
Likely archive candidates:
- `tools/strategy_sweep/`
- old research sweep outputs and ad hoc evaluation helpers
- stale artifacts under `IntradayTrading/artifacts/` that were tied to earlier lanes

Reason:
- these are historical evidence, not the current product surface.

### E. Old phase docs that describe a superseded product identity
Likely archive candidates:
- old phase packets that center the repo on LiquidSniper-as-paper-bot
- docs whose assumptions conflict with Surveyor descriptive-only and Arbiter-first interpretation boundaries

Reason:
- they create doctrine collisions.

---

## 4) Proposed archive shape

Do **not** delete first.

Instead, create a clear archive boundary such as:

- `archive/legacy-ingestion/`
- `archive/legacy-paper-runtime/`
- `archive/legacy-tradingview/`
- `archive/legacy-sweeps/`
- `archive/legacy-docs/`

For code that may still be importable or reusable later, prefer:
- `legacy/<domain>/...`

For docs/artifacts that are purely historical, prefer:
- `docs/archive/...`
- `artifacts/archive/...`

The key is to make top-level navigation honest.

---

## 5) Top-level repo shape I would aim for

A cleaner future top-level could look roughly like:

- `surveyor/` or `market_state/`
- `arbiter/`
- `core/` or `shared/`
- `ui/`
- `docs/`
- `tests/`
- `archive/`

If we do not want a larger package rewrite immediately, we can keep the current package names for one transition phase while simplifying the repo surface around them.

---

## 6) Rename recommendation

I think a rename is probably right, but **not as the first step**.

### My recommendation
Do this in order:
1. archive and refocus the repo structure
2. freeze the new product boundary in docs + README + top-level layout
3. then rename the repo to match the actual thing

### Why
Right now `LiquidSniper` still carries:
- old ingestion identity
- old paper-runtime identity
- old tradingview-heavy identity
- and the new Surveyor/Arbiter identity

A rename before cleanup would make the name cleaner, but the repo still confusing.

### Candidate rename directions
Depending on how product-y vs internal you want it to sound:

1. **surveyor-arbiter**
   - clearest and most literal
2. **intraday-revisit**
   - matches the current initiative language, but less product-like
3. **market-state-engine**
   - accurate, but bland
4. **surveyor**
   - clean, but underplays Arbiter

My recommendation right now: **surveyor-arbiter** if the split remains the lasting architecture.

---

## 7) Safe execution order

### Phase 1: boundary freeze
- add a top-level refocus doc
- update README / architecture docs to say the repo is now Surveyor + Arbiter centered
- explicitly mark legacy subsystems as non-core

### Phase 2: archive pass
- move non-core docs into archive buckets
- move legacy tooling into `archive/` or `legacy/`
- keep imports/build green
- do not break working Surveyor UI/feed paths

### Phase 3: package and path cleanup
- reduce awkward split between `liquidsniper/` and `IntradayTrading/`
- decide whether `IntradayTrading` becomes an internal engine package or gets renamed under the new boundary
- flatten or rename top-level dirs where the old identity leaks hardest

### Phase 4: rename repo
- rename GitHub repo
- update README, package metadata, docs links, CI references, and local runbooks

---

## 8) Biggest cleanup risks

1. **Deleting historical evidence too early**  
   Some old docs and artifacts still explain why current code exists.

2. **Breaking hidden imports**  
   There is still real coupling between `liquidsniper` and `IntradayTrading`.

3. **Confusing archive with product code**  
   If archived code still looks top-level and active, the refocus will fail socially.

4. **Renaming before doctrine is frozen**  
   That produces a cleaner label but not a cleaner system.

---

## 9) Recommended immediate next move

If we want to start now, I would do this first:

1. create a small set of archive buckets
2. write a repo-level refocus memo / README update
3. inventory top-level dirs/files into three lists:
   - keep
   - archive
   - uncertain
4. do one non-destructive archive PR first

That first PR should be mostly:
- docs moves
- artifact moves
- explicit legacy labels
- no risky package renames yet

---

## 10) My blunt read

The repo is overdue for this cleanup.

The current architecture is coherent enough now that the old mixed identity is actively costing us clarity.
So yes, I think we should make the repo tell the truth:

- Surveyor first
- Arbiter next
- legacy paper/ingestion/tradingview surfaces demoted or archived
- rename after the boundary is real
