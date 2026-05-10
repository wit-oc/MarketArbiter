# Surveyor Feed Onboarding Contract V1

Date: 2026-04-25  
Status: proposed operator contract for intraday rollout  
Intent: freeze the manifest shape, CLI flow, automated check-ins, and anti-blacklist guardrails for staged Surveyor feed expansion

Related closeout target:
- `docs/SURVEYOR_FEED_TOP100_READY_TARGET_V1.md` is the controlling end-state for this thread. This contract is an implementation slice under that target.

---

## 1) Why this exists

The current repo already has the core runner surfaces:
- `market_arbiter/ops/surveyor_feed_runner.py`
- `market_arbiter/ops/blofin_ws_candle5m_consumer.py`
- `configs/surveyor_feed_workset.default.json`
- `market_arbiter/core/market_scheduler.py`

But onboarding is still mostly implicit.
For the intraday rollout we need one explicit operator contract that says:
- how a symbol is admitted,
- how stage promotion is decided,
- what cron/check-ins run during soak,
- and what breaker posture prevents provider/IP abuse.

This contract is deliberately config-first.
Adding a pair must remain a manifest change, not a Python edit.

---

## 2) Design rules

1. **No code edit per symbol**
2. **Control thread owns promotion and phase advancement**
3. **Check-ins analyze local output first; they do not create extra provider load**
4. **REST recovery traffic always goes through the shared BloFin governor path**
5. **No broad cohort expansion until the prior stage has a PASS / CONDITIONAL PASS review**
6. **Backtests and replays remain point-in-time and must not leak future-zone knowledge**

---

## 3) Operator artifacts

Two manifest layers are required.

### A. Active workset manifest (already implemented)
This is the runner-consumable workset used by `surveyor_feed_runner.py`.
Current contract in repo:
- `surveyor_feed_workset_manifest_v1`

Current file example:
- `configs/surveyor_feed_workset.default.json`

### B. Onboarding control manifest (new operator contract)
This is the source of truth for staged rollout and promotion state.
It is not the raw runner input.
Instead, it drives which symbols are rendered into the active workset.

### Contract name
- `surveyor_symbol_onboarding_manifest_v1`

### Recommended file
- `configs/surveyor_symbol_onboarding.intraday.json`

### Contract shape

```json
{
  "contract": "surveyor_symbol_onboarding_manifest_v1",
  "environment": "demo",
  "db_path": "data/market_arbiter.sqlite",
  "workset_output_path": "configs/generated/surveyor_feed_workset.intraday.json",
  "control": {
    "owner": "intraday-control-thread",
    "stage": "single_pair_soak",
    "approval_required": true,
    "promotion_policy": "manual_with_auto_recommendation"
  },
  "provider_policy": {
    "provider": "blofin_ws_plus_rest",
    "rest_requests_per_minute_cap": 90,
    "rest_hard_ceiling_per_minute": 120,
    "ws_connects_per_second_cap": 1,
    "rate_limit_cooldown_seconds": 60,
    "firewall_ban_cooldown_seconds": 300,
    "rapid_failure_threshold": 3,
    "rapid_failure_window_seconds": 600,
    "rapid_failure_cooldown_seconds": 900,
    "allow_prod": false
  },
  "stages": [
    {
      "id": "single_pair_soak",
      "label": "Stage 1 - single pair",
      "target_symbols": 1,
      "min_soak_hours": 72,
      "max_soak_hours": 168,
      "promotion_requires": [
        "ws_canary_pass",
        "runner_canary_pass",
        "no_firewall_ban_events",
        "no_open_recovery_blockers",
        "daily_boundary_observed"
      ]
    },
    {
      "id": "small_cohort_soak",
      "label": "Stage 2 - 5 to 10 pairs",
      "target_symbols": 5,
      "min_soak_hours": 72,
      "max_soak_hours": 120,
      "promotion_requires": [
        "status_green_majority",
        "no_firewall_ban_events",
        "recompute_backlog_clear",
        "operator_pass"
      ]
    },
    {
      "id": "medium_cohort_soak",
      "label": "Stage 3 - 25 pairs",
      "target_symbols": 25,
      "min_soak_hours": 72,
      "max_soak_hours": 120,
      "promotion_requires": [
        "shard_health_visible",
        "bounded_recompute_pressure",
        "operator_pass"
      ]
    }
  ],
  "symbols": [
    {
      "symbol": "BTC-USDT",
      "enabled": true,
      "stage_state": "promoted",
      "cohort": "core",
      "priority": 1,
      "shard_hint": "ws-a",
      "last_review_status": "pass",
      "notes": "initial live canary pair"
    },
    {
      "symbol": "ETH-USDT",
      "enabled": false,
      "stage_state": "proposed",
      "cohort": "candidate",
      "priority": 2,
      "shard_hint": "ws-a",
      "last_review_status": "pending",
      "notes": "held until Stage 2 approval"
    }
  ]
}
```

---

## 4) Symbol states

Each symbol moves through these explicit states:
- `proposed`
- `validated`
- `hydrating`
- `canary`
- `soaking`
- `promoted`
- `paused`
- `rejected`

### State meanings

- `proposed`: listed but not admitted to the active workset
- `validated`: symbol format + venue path + history assumptions passed
- `hydrating`: bounded historical seed/recovery is in progress
- `canary`: bounded WS + runner checks are being executed
- `soaking`: admitted to the current stage cohort, but not yet approved for the next stage
- `promoted`: active for the current approved stage
- `paused`: temporarily removed from active workset without deleting data
- `rejected`: operator decided not to admit the symbol

`paused` is operationally important.
If a symbol misbehaves, we pause it instead of touching code or widening blast radius.

---

## 5) Active workset rendering rule

The active feed workset is derived from the onboarding manifest.

### Rendering rule
Only symbols with:
- `enabled = true`
- `stage_state in ["soaking", "promoted"]`

may be rendered into `surveyor_feed_workset_manifest_v1` for the live runner.

That keeps the current runner contract stable while making onboarding richer.

---

## 6) CLI contract

Two layers exist:

### A. Existing repo commands (already real)

#### WS consumer
```bash
python -m market_arbiter.ops.blofin_ws_candle5m_consumer \
  --mode run|status|confirm-canary \
  --db-path data/market_arbiter.sqlite \
  --symbols BTC-USDT \
  --environment demo \
  --requests-per-minute 120
```

#### Feed runner
```bash
python -m market_arbiter.ops.surveyor_feed_runner \
  --mode once|loop|status|canary \
  --manifest-path configs/generated/surveyor_feed_workset.intraday.json
```

These remain the underlying execution surfaces.

### B. New thin onboarding CLI (to add)
Recommended path:
- `python -m market_arbiter.ops.surveyor_symbol_onboarding ...`

This wrapper should orchestrate the existing runner/consumer paths instead of re-implementing feed logic.

### Commands

#### 1. Validate symbol
```bash
python -m market_arbiter.ops.surveyor_symbol_onboarding \
  validate \
  --manifest configs/surveyor_symbol_onboarding.intraday.json \
  --symbol ETH-USDT
```

Checks:
- symbol format valid
- WS subscription symbol valid
- REST history path valid
- enough history exists for `5m -> 4h -> 1d -> 1w`
- symbol is not already active in another shard unexpectedly

#### 2. Hydrate symbol
```bash
python -m market_arbiter.ops.surveyor_symbol_onboarding \
  hydrate \
  --manifest configs/surveyor_symbol_onboarding.intraday.json \
  --symbol ETH-USDT
```

Effects:
- bounded canonical `5m` hydration
- enough upstream seed for derived `4h`, `1d`, `1w`
- writes a local hydration report
- does **not** auto-promote

#### 3. Run WS confirm canary
```bash
python -m market_arbiter.ops.surveyor_symbol_onboarding \
  ws-canary \
  --manifest configs/surveyor_symbol_onboarding.intraday.json \
  --symbol ETH-USDT
```

Underlying call:
- `blofin_ws_candle5m_consumer.py --mode confirm-canary`

Pass means:
- confirmed closed `5m` messages observed
- checkpoint advanced
- no unresolved reconnect explosion

#### 4. Run feed-runner canary
```bash
python -m market_arbiter.ops.surveyor_symbol_onboarding \
  runner-canary \
  --manifest configs/surveyor_symbol_onboarding.intraday.json \
  --symbol ETH-USDT
```

Underlying call:
- render temporary single-symbol workset
- run `surveyor_feed_runner.py --mode canary`

Pass means:
- canonical ingest succeeds
- close manifest emits
- derived aggregates materialize
- recompute completes
- continuity/status remains sane

#### 5. Admit to current stage cohort
```bash
python -m market_arbiter.ops.surveyor_symbol_onboarding \
  admit \
  --manifest configs/surveyor_symbol_onboarding.intraday.json \
  --symbol ETH-USDT \
  --stage small_cohort_soak
```

Effects:
- updates symbol state to `soaking`
- renders the active workset
- does not itself claim promotion to the next stage

#### 6. Generate stage report
```bash
python -m market_arbiter.ops.surveyor_symbol_onboarding \
  report \
  --manifest configs/surveyor_symbol_onboarding.intraday.json
```

Outputs:
- current stage summary
- symbol health rollup
- checkpoint freshness rollup
- canary history
- breaker/rate-limit incidents
- recommendation: `hold`, `promote`, or `pause`

#### 7. Stage advancement check
```bash
python -m market_arbiter.ops.surveyor_symbol_onboarding \
  advance-check \
  --manifest configs/surveyor_symbol_onboarding.intraday.json
```

Behavior:
- analyzes local outputs only
- never widens the cohort automatically
- emits `recommendation = hold|ready_for_review|pause_required`
- control thread still decides

#### 8. Pause symbol
```bash
python -m market_arbiter.ops.surveyor_symbol_onboarding \
  pause \
  --manifest configs/surveyor_symbol_onboarding.intraday.json \
  --symbol ETH-USDT \
  --reason REST_FIREWALL_BAN
```

Effects:
- symbol becomes `paused`
- active workset is re-rendered without that symbol
- existing data is preserved for diagnosis

---

## 7) Rollout flow by stage

### Stage 1 — single pair soak
Target pair:
- `BTC-USDT`

Flow:
1. validate
2. hydrate
3. `ws-canary`
4. `runner-canary`
5. `admit --stage single_pair_soak`
6. run live soak for `72 to 168` hours
7. daily report + advance check
8. control thread decides PASS / CONDITIONAL PASS / FAIL

### Stage 2 — small cohort soak
Target:
- `5 to 10` pairs

Entry requirement:
- Stage 1 passed
- no unresolved provider/IP safety incidents

Flow:
1. validate each proposed pair
2. hydrate in bounded batches
3. canary per symbol before cohort admission
4. admit only approved symbols
5. soak `72 to 120` hours
6. analyze recompute pressure and reconnect behavior

### Stage 3 — medium cohort soak
Target:
- `25` pairs

Entry requirement:
- Stage 2 passed
- shard posture defined
- status/reporting still understandable

### Stage 4 — architecture checkpoint
Before `50+` pairs:
- shard model must be explicit
- per-shard health must exist
- bounded recompute worker pool must exist
- pause/disable flow must be proven

No jump to broad sweep before this checkpoint passes.

---

## 8) Automated check-ins

The automation should be mostly **local-state analysis**, not extra exchange probing.
That is the safest way to get useful operator prompts without burning IP budget.

### Rule
Scheduled check-ins may read:
- local state JSON
- SQLite checkpoints
- feed health events
- runner status output
- canary result artifacts

Scheduled check-ins should **not** repeatedly hit BloFin just to ask whether things are okay.

### Recommended jobs

#### A. 30-minute status pulse during active soak
Purpose:
- inspect local continuity posture
- detect stale/degraded/tripped states quickly
- produce a compact report artifact

Recommended command:
```bash
python -m market_arbiter.ops.surveyor_symbol_onboarding report \
  --manifest configs/surveyor_symbol_onboarding.intraday.json
```

Result:
- write `artifacts/feed_rollout/latest_report.json`
- if recommendation is `pause_required`, alert the control thread

#### B. Daily promotion check
Purpose:
- prompt stage review when minimum soak window has elapsed
- analyze whether the stage is boring enough to widen

Recommended command:
```bash
python -m market_arbiter.ops.surveyor_symbol_onboarding advance-check \
  --manifest configs/surveyor_symbol_onboarding.intraday.json
```

Recommended timing:
- once daily after the exchange daily close has definitely settled
- do not hard-code the wrong day boundary; derive from the provider candle boundary used in the DB

#### C. Weekly heavy-boundary review
Purpose:
- explicitly analyze the highest normal pressure point
- check reconnect storms, recovery bursts, and recompute backlog

Recommended behavior:
- run one post-weekly-boundary report
- compare weekly-boundary window against ordinary-day baseline
- never auto-promote on this alone; it is review evidence

#### D. Canary-on-admission only
When a new symbol is admitted:
- run one bounded WS canary
- run one bounded runner canary
- stop there

Do **not** run frequent synthetic canaries against production all day.
That is noise, not safety.

### Promotion recommendation rules

`advance-check` may emit:
- `hold`
- `ready_for_review`
- `pause_required`

It should emit `ready_for_review` only if all are true:
- minimum soak window elapsed
- no `REST_FIREWALL_BAN` events during the window
- no open `resync_required` or blocked recompute states
- continuity state stayed acceptable for the stage
- status artifacts remain explainable by a human

Even then, the next stage is still manual approval.

---

## 9) Report contract

The rollout report should be a local JSON artifact.

### Contract name
- `surveyor_rollout_report_v1`

### Minimum fields

```json
{
  "contract": "surveyor_rollout_report_v1",
  "as_of_ms": 1777104000000,
  "manifest_path": "configs/surveyor_symbol_onboarding.intraday.json",
  "stage": "single_pair_soak",
  "recommendation": "hold",
  "summary": {
    "symbols_total": 1,
    "symbols_green": 1,
    "symbols_degraded": 0,
    "symbols_tripped": 0,
    "symbols_paused": 0,
    "continuity_state": "live_continuous"
  },
  "provider_safety": {
    "rest_429_events": 0,
    "rest_403_events": 0,
    "rapid_failure_cooldowns": 0,
    "max_backoff_seconds": 0
  },
  "recompute": {
    "blocked_manifests": 0,
    "pending_manifests": 0,
    "weekly_boundary_seen": true
  },
  "notes": [
    "single-pair soak still within minimum window"
  ]
}
```

This is the artifact the cron/check-ins should write and the control thread should review.

---

## 10) Anti-blacklist / circuit-breaker policy

This is the non-negotiable part.

### 10.1 One governor path
All BloFin REST traffic for:
- hydrate,
- repair,
- backfill,
- and canary support

must go through the shared governor path already reflected in `market_arbiter/feed/blofin.py`.

Current grounded constants/code posture:
- documented BloFin REST limit: `500/min per IP`
- current safe repo default: `120/min`
- `429` -> rate-limit handling
- `403` -> firewall restriction handling with `300s` ban backoff

### 10.2 Rollout cap
For staged onboarding, set the operator cap below the repo ceiling.

Recommended rollout cap:
- `rest_requests_per_minute_cap = 90`
- hard ceiling remains `120`

Reason:
- leaves headroom for unexpected repair bursts
- avoids treating the safe ceiling as a target
- lowers the chance that operator actions plus automation combine into a noisy minute

### 10.3 WS connection discipline
Respect the documented setup posture:
- max `1` WS connect attempt per second per IP

Operational rule:
- do not let multiple shards or scripts reconnect in a tight loop
- reconnect storms are an IP-risk event, not just a nuisance

### 10.4 Cooldown rules
Use these minimum floors:
- after `429`: cooldown floor `60s`
- after `403`: cooldown floor `300s`
- after `3` rapid failures within `600s`: cooldown floor `900s`

These numbers match the current WS reconnect policy and BloFin ban guidance.

### 10.5 Breaker scopes
Treat breaker decisions at three scopes.

#### Provider/IP scope
Trigger:
- any `403`, or
- repeated `429` bursts across symbols/shards

Action:
- freeze all non-essential REST recovery jobs
- freeze new symbol admissions
- freeze promotion checks that would widen scope
- require cooldown before the next repair attempt

#### Shard scope
Trigger:
- reconnect storm
- repeated WS disconnects
- repeated recovery-blocked state on one shard

Action:
- pause that shard cohort
- do not tear down unaffected shards automatically

#### Symbol scope
Trigger:
- repeated `resync_required`
- repeated canary failure
- repeated blocked recompute for one symbol

Action:
- set symbol `paused`
- exclude from rendered workset
- preserve evidence for diagnosis

### 10.6 Promotion guardrail
A stage may **not** be recommended for promotion if the soak window contains:
- any `REST_FIREWALL_BAN`,
- unresolved `resync_required`,
- unresolved blocked recompute,
- or repeated rate-limit degradation that required cooldown.

That is intentionally conservative.
If the system got close enough to provider protection to trigger these, the answer is not “scale now.”

### 10.7 No ad hoc scripts
Do not let one-off scripts talk to BloFin outside the governor path during rollout.
One careless sweep can invalidate the entire safety posture.

---

## 11) Practical cron posture

The point of cron here is:
- remind us when evidence is ready,
- summarize output data,
- and surface pause-worthy incidents.

The point is **not** to create a chatty automated test harness against BloFin.

### Good cron work
- local report generation
- local stage recommendation
- stale/degraded/tripped detection
- boundary-window analysis

### Bad cron work
- repeated live canaries every few minutes
- brute-force production backfills on a timer
- parallel repair sweeps across the whole candidate set

---

## 12) Immediate implementation order

1. add the onboarding manifest file
2. add the thin onboarding CLI wrapper
3. render active workset from onboarding state
4. add report + advance-check artifact generation
5. wire cron only for local report/check decisions
6. keep stage advancement manual in the control thread

---

## 13) Bottom line

The right contract is:
- **onboarding manifest controls rollout state**
- **existing runner/consumer remain execution engines**
- **cron analyzes local evidence and prompts review**
- **promotion stays manual**
- **provider/IP safety beats speed every time**

That gets us a real staged rollout path without turning the feed into a blacklist experiment.

This contract closes the onboarding/control-plane slice, but it does **not** by itself close the full feed thread. Full thread closure now requires the top-100-ready target state: shard-aware WS ingestion, per-shard health, bounded recompute concurrency, staged soak evidence, and a final readiness packet.
