# Feed Provider Bakeoff Plan V1

Date: 2026-04-29  
Status: Phase A smoke harness implemented; quick 1m connectivity smoke passed on 2026-04-30; 5m Phase A can run from the CLI  
Intent: choose the canonical top-100 Surveyor feed provider with evidence instead of assuming BloFin scales.

---

## 1) Decision context

The feed decision is not closed.

Current posture:

- BloFin has useful implementation work and remains a valid canary / venue-context candidate.
- BloFin should **not** be presumed to be the canonical top-100 Surveyor feed until it wins an evidence-backed bakeoff.
- Surveyor needs a canonical provider for broad market-state coverage; Arbiter/Sentinel later need that feed to be stable enough for replayable decision records.
- The closeout target remains **top-100-ready**, not merely single-pair-operable.

This bakeoff decides which provider becomes the default canonical Surveyor feed for top-100 staging.

---

## 2) Candidate set

Initial free/public-feed candidates:

1. **BloFin**
   - role going in: incumbent implementation / venue-context canary
   - concern: scale and confirmed-close behavior are not yet proven

2. **Bybit linear market data**
   - role going in: serious canonical-feed candidate for perpetuals coverage
   - concern: adapter effort and exact symbol/timeframe semantics need discovery

3. **OKX market data**
   - role going in: current extraction-era fallback / historical seed candidate
   - concern: whether it is the best live top-100 canonical source versus a history/bootstrap source

4. **Binance market data**
   - role going in: broad public-feed comparator if regional/access constraints are acceptable
   - concern: availability, policy, and regional constraints must be treated as first-class risks

Optional paid/normalized comparator only if free feeds fail the acceptance gate:

- Massive/Polygon-style normalized market data, or another low-friction paid provider.
- Higher-friction institutional vendors should not enter V1 unless the free-feed bakeoff shows a real blocker that normalized paid data uniquely solves.

---

## 3) What we are testing

The bakeoff tests provider fitness for Surveyor, not exchange execution.

Primary questions:

1. Can the provider deliver reliable closed-candle events for all in-scope symbols?
2. Can we detect closed bars deterministically without guessing?
3. Is close latency acceptable and stable?
4. Does reconnect behavior stay sane under small/medium cohort load?
5. Can REST bootstrap/repair fill gaps without provider/IP risk?
6. Is historical depth sufficient for SR, structure, Fib, and replay seed windows?
7. Is the provider operationally simple enough to become canonical?

---

## 4) Bakeoff phases

### Phase A — adapter discovery and smoke

Target duration:

- 1 to 2 hours per provider, enough to prove the adapter can observe multiple candle closes.

Candidate cohort:

- `BTC-USDT`
- `ETH-USDT`
- `SOL-USDT`

Pass criteria:

- connect without credentials for public market data,
- map symbols deterministically,
- observe at least three closed candles per symbol on the selected live timeframe,
- write local event logs using the common bakeoff schema,
- no provider/IP safety incidents.

Output:

- `artifacts/feed_bakeoff/<run_id>/<provider>/phase_a_smoke.json`
- `artifacts/feed_bakeoff/<run_id>/<provider>/close_events.jsonl`
- `artifacts/feed_bakeoff/<run_id>/<provider>/raw_messages.jsonl`
- `artifacts/feed_bakeoff/<run_id>/phase_a_smoke_combined.json`
- `artifacts/feed_bakeoff/<run_id>/phase_a_smoke_summary.md`

Current runner:

```bash
python3 -m market_arbiter.ops.feed_bakeoff_smoke \
  --config configs/feed_provider_bakeoff.v1.json \
  --providers blofin,bybit,okx \
  --symbols BTC-USDT,ETH-USDT,SOL-USDT \
  --timeframe 5m \
  --duration-seconds 1200 \
  --target-closes-per-symbol 3 \
  --allow-live-provider-calls
```

### Phase B — 24h side-by-side live bakeoff

Target duration:

- 24 hours minimum.

Candidate cohort:

- 10 symbols from the top-100 target universe, weighted toward high-liquidity majors and a few lower-liquidity names.

Recommended first 10:

- `BTC-USDT`
- `ETH-USDT`
- `SOL-USDT`
- `XRP-USDT`
- `DOGE-USDT`
- `BNB-USDT`
- `ADA-USDT`
- `LINK-USDT`
- `AVAX-USDT`
- `TON-USDT`

Timeframes:

- Live base: `5m` unless a provider makes `15m` materially more reliable.
- Derived checks: `4H`, `1D`, `1W` generated locally from base bars where enough data exists.

Pass criteria:

- at least 99.5% expected closed candles observed or repaired,
- no unrecovered symbol outage longer than one close interval plus repair window,
- no provider/IP safety incidents,
- reconnects do not create duplicate/conflicting canonical bars,
- REST repair can backfill missing intervals without tripping guardrails,
- report artifacts are complete enough to support a decision.

Output:

- `artifacts/feed_bakeoff/<run_id>/phase_b_live_summary.json`
- `artifacts/feed_bakeoff/<run_id>/phase_b_live_summary.md`

### Phase C — historical depth and bootstrap audit

Target duration:

- bounded by provider rate limits; run conservatively.

Questions:

- How far back can we reliably page each provider for `5m`, `4H`, `1D`, and `1W` equivalent data?
- Are gaps or truncation visible and deterministic?
- Does the provider return enough lookback to seed SR/structure/Fib without another source?
- If not, can this provider still be the live canonical feed while OKX or another source seeds history?

Pass criteria:

- historical lookback meets the minimum Surveyor bootstrap windows, or
- the final recommendation explicitly separates live canonical feed from historical seed source.

Output:

- `artifacts/feed_bakeoff/<run_id>/phase_c_history_depth.json`
- `artifacts/feed_bakeoff/<run_id>/phase_c_history_depth.md`

### Phase D — decision packet

Output:

- `artifacts/feed_bakeoff/<run_id>/feed_provider_bakeoff_decision_v1.json`
- `artifacts/feed_bakeoff/<run_id>/feed_provider_bakeoff_decision_v1.md`

Decision values:

- `promote_provider`
- `conditional_promote_provider`
- `keep_blofin_canary_only`
- `split_live_and_history_sources`
- `escalate_to_paid_provider`
- `fail_no_provider_ready`

---

## 5) Common event schema

Every provider adapter should write a normalized event stream before touching canonical Surveyor storage.

Minimum close event:

```json
{
  "contract": "feed_bakeoff_close_event_v1",
  "run_id": "feed-bakeoff-YYYYMMDD-HHMM",
  "provider": "blofin|bybit|okx|binance|paid_candidate",
  "symbol": "BTC-USDT",
  "provider_symbol": "provider-native-symbol",
  "timeframe": "5m",
  "ts_open_ms": 1777515000000,
  "ts_close_ms": 1777515300000,
  "event_kind": "closed_candle|working_candle|repair_candle|gap|duplicate|conflict|reconnect|error",
  "provider_event_ts_ms": 1777515300123,
  "received_ts_ms": 1777515300456,
  "close_latency_ms": 456,
  "open": "0",
  "high": "0",
  "low": "0",
  "close": "0",
  "volume_base": "0",
  "volume_quote": "0",
  "raw_ref": "path-or-offset"
}
```

---

## 6) Metrics

### Reliability

- expected closes
- observed closes
- repaired closes
- missing closes
- duplicate closes
- conflicting closes
- longest unrecovered gap
- stale intervals per symbol

### Latency

- close latency p50 / p95 / max
- provider event timestamp availability
- local receive timestamp availability
- close ordering consistency

### Provider safety

- connect failures
- reconnects/hour
- 429 or equivalent rate-limit events
- 403/firewall/ban-like events
- cooldown escalations
- REST requests/minute observed
- WS connection attempts/minute observed

### Coverage

- top-100 symbol match rate
- symbol mapping ambiguity count
- missing high-priority symbols
- stable instrument identifiers

### Historical depth

- maximum reliable lookback by timeframe
- pagination consistency
- gap density in historical responses
- repair/backfill cost estimate

### Implementation complexity

- adapter complexity
- schema weirdness
- special-case burden
- canonical normalization risk
- operational guardrail burden

---

## 7) Scoring model

Use hard gates first, then score.

### Hard gates

A provider cannot become canonical if it fails any of these:

- cannot observe closed candles deterministically,
- cannot cover the top-100 target universe well enough for the staged rollout,
- creates provider/IP safety risk under 10-symbol bakeoff load,
- produces ambiguous or conflicting candle identity,
- cannot be repaired from REST or equivalent history path.

### Weighted score after gates

- 30% live close reliability
- 20% top-100 coverage and symbol semantics
- 15% REST repair / historical bootstrap
- 15% provider/IP safety posture
- 10% operational complexity
- 10% latency and ordering quality

The winner should be the highest scoring provider that passes hard gates. If the best live provider is not the best historical source, the decision packet may recommend a split source posture.

---

## 8) Acceptance thresholds

### Promote as canonical Surveyor feed

All true:

- hard gates pass,
- 24h side-by-side score is best or statistically tied for best,
- no unresolved provider/IP safety incident,
- at least 99.5% expected closes observed or repaired,
- top-100 symbol coverage is sufficient for staged rollout,
- adapter complexity is acceptable.

### Conditional promote

Allowed when:

- hard gates pass,
- live reliability is strong,
- one bounded follow-up remains, such as historical depth split-source decision or one missing lower-priority symbol mapping class.

### Reject / canary-only

Use when:

- hard gates fail,
- confirmed-close semantics remain ambiguous,
- provider safety risk is too high,
- top-100 coverage is weak,
- or REST repair/history cannot be made safe.

---

## 9) Implementation work order

1. Freeze this bakeoff plan and config.
2. Add provider-neutral bakeoff event schema and artifact directory contract.
3. Implement provider adapter smoke runner with no canonical DB writes.
4. Implement per-provider adapters in this order:
   - BloFin incumbent adapter wrapper,
   - Bybit adapter,
   - OKX adapter,
   - Binance adapter if access constraints are acceptable.
5. Run Phase A smoke per provider.
6. Run Phase B 24h live bakeoff for passing providers.
7. Run Phase C historical depth audit.
8. Produce decision packet and update the top-100-ready target doc with the selected provider posture.

Do not start large-scale Surveyor feed soak until the bakeoff has either selected a provider or produced an explicit conditional path.

---

## 10) Final decision artifact shape

```json
{
  "contract": "feed_provider_bakeoff_decision_v1",
  "run_id": "feed-bakeoff-YYYYMMDD-HHMM",
  "as_of_ms": 1777516200000,
  "decision": "promote_provider|conditional_promote_provider|split_live_and_history_sources|escalate_to_paid_provider|fail_no_provider_ready",
  "canonical_live_provider": "bybit|okx|binance|blofin|paid_candidate|null",
  "historical_seed_provider": "okx|bybit|binance|blofin|paid_candidate|null",
  "blofin_role": "canonical|canary_only|venue_context|rejected",
  "providers_tested": [],
  "hard_gate_results": {},
  "weighted_scores": {},
  "evidence": [],
  "remaining_work": []
}
```

---

## 11) Bottom line

The feed thread should now move from “BloFin implementation” to “canonical provider selection.”

BloFin remains useful evidence. It is not the default answer until it survives the bakeoff.
