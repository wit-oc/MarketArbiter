# BloFin Market Data Contract V1

Date: 2026-04-21  
Updated: 2026-04-29  
Status: proposed BloFin adapter/canary contract; not a final canonical-provider decision  
Intent: define a practical first-pass data contract that avoids 1m sprawl while preserving enough fidelity for Surveyor, SR logic, and later backtesting

---

## 1) Recommended first-pass posture

For the initial BloFin buildout, the recommended posture is:

- **live canonical base timeframe: `5m`**
- derive **`4h`**, **`1d`**, and **`1w`** locally from canonical closed `5m` bars
- use **REST** for:
  - cold bootstrap
  - gap repair
  - periodic reconciliation
- keep raw storage intentionally bounded

This is the compromise between:
- not overbuilding a `1m` warehouse too early,
- still reducing live subscription count and rate pressure,
- and preserving enough structure fidelity for the current body of work.

Important provider-selection boundary: this document defines the BloFin adapter/canary shape. It does **not** decide that BloFin is the canonical top-100 Surveyor feed. Canonical provider selection now belongs to `docs/FEED_PROVIDER_BAKEOFF_PLAN_V1.md`.

### Why not `1m` first

`1m` is still the cleaner long-term canonical base if we eventually need:
- tighter replay fidelity,
- intra-5m path analysis,
- or more exact reconstruction of partial-bar behavior.

But for now, `5m` is a more practical first production boundary if:
- the main Surveyor and SR work is built around `5m` and higher,
- storage budget matters,
- and we want to keep ingestion simpler.

---

## 2) Timeframe roles

### A. Live canonical raw timeframe
- `5m`

### B. Derived aggregate timeframes
- `4h` = 48 closed `5m` bars
- `1d` = 288 closed `5m` bars
- `1w` = 2016 closed `5m` bars

### C. Not in first-pass canonical scope
- `1m`
- tick/trade-by-trade reconstruction

Those can be added later if the strategy or replay layer proves they are needed.

---

## 3) WebSocket vs REST responsibilities

## WebSocket responsibility
WebSocket should be treated as the **live update path**.

For BloFin candlestick channels, the system should subscribe to:
- `candle5m` per instrument

WebSocket records should be treated as:
- **current working candle updates** while `confirm = 0`
- **final close event** when `confirm = 1`

Only `confirm = 1` rows should be promoted into canonical closed-candle storage.

## REST responsibility
REST should be treated as the **history and repair path**.

Use REST for:
- initial backfill before live start
- repair after disconnects or subscription failures
- reconciliation against canonical store
- deep lookback needed for SR, fib, and market structure seed windows

## Endpoint safety and rate-limit posture

As of 2026-04-21, the BloFin docs explicitly provide **demo trading** REST and WS endpoints in addition to production endpoints. The implementation should therefore default to **demo** for development/testing unless Redact explicitly chooses production.

### Demo endpoints
- REST: `https://demo-trading-openapi.blofin.com`
- Public WS: `wss://demo-trading-openapi.blofin.com/ws/public`
- Private WS: `wss://demo-trading-openapi.blofin.com/ws/private`

### Production endpoints
- REST: `https://openapi.blofin.com`
- Public WS: `wss://openapi.blofin.com/ws/public`
- Private WS: `wss://openapi.blofin.com/ws/private`

### Documented rate-limit posture
- REST: **500 requests/minute per IP**
- WS connection setup: **1 request/second per IP**
- the documented rate-limit response is HTTP **`429`**
- the docs also state HTTP **`403`** indicates a firewall-rule violation, and in some cases the restriction may last **5 minutes**

### Required implementation posture
- test against **demo** first
- keep REST pagination conservative instead of pushing the documented maximum rate
- use WS for live ingestion and REST only for bootstrap/repair
- do not use production endpoints as a noisy test harness
- route **all BloFin REST traffic through one governor path**
- treat HTTP `429` as rate limiting / cooldown
- treat HTTP `403` as a stronger protective restriction and back off accordingly instead of hammering retries
- do not let ad hoc scripts bypass the governor logic when talking to BloFin

---

## 4) Raw live candle contract

This contract represents the exchange payload as received, with minimal normalization.
It is useful for short-lived staging, debugging, and reconciliation.

### Contract name
- `blofin_ws_candle_5m_raw_v1`

### Required fields

```json
{
  "contract": "blofin_ws_candle_5m_raw_v1",
  "venue": "blofin",
  "inst_id": "BTC-USDT",
  "channel": "candle5m",
  "timeframe": "5m",
  "ts_open_ms": 1776807600000,
  "ts_close_ms": 1776807900000,
  "open": "85000.1",
  "high": "85020.4",
  "low": "84980.0",
  "close": "85010.2",
  "vol_contract": "12345",
  "vol_base": "12.345",
  "vol_quote": "1049500.12",
  "confirm": "0|1",
  "exchange_event_ts_ms": 1776807889000,
  "received_ts_ms": 1776807889123,
  "ingest_trace_id": "..."
}
```

### Rules
- `confirm = 0` means the bar is still forming
- `confirm = 1` means the bar is closed and can be promoted to canonical storage
- store raw payloads only briefly unless debugging/replay requires otherwise

---

## 5) Canonical closed 5m candle contract

This is the durable primary candle contract for the first pass.

### Contract name
- `canonical_candle_5m_v1`

### Required fields

```json
{
  "contract": "canonical_candle_5m_v1",
  "venue": "blofin",
  "provider": "blofin_ws",
  "symbol": "BTC-USDT",
  "timeframe": "5m",
  "ts_open_ms": 1776807600000,
  "ts_close_ms": 1776807900000,
  "open": "85000.1",
  "high": "85020.4",
  "low": "84980.0",
  "close": "85010.2",
  "volume_contract": "12345",
  "volume_base": "12.345",
  "volume_quote": "1049500.12",
  "bar_status": "closed",
  "source_confirm": "1",
  "source_contract": "blofin_ws_candle_5m_raw_v1",
  "dataset_version": "blofin_5m_v1",
  "ingest_ts_ms": 1776807889123,
  "trace_id": "..."
}
```

### Rules
- one row per `(venue, symbol, timeframe, ts_open_ms)`
- only closed bars belong here
- duplicates must be idempotent
- conflicting duplicates should fail validation

---

## 6) Derived aggregate candle contract

This contract represents bars derived from canonical closed `5m` candles.

### Contract name
- `derived_candle_v1`

### Required fields

```json
{
  "contract": "derived_candle_v1",
  "venue": "blofin",
  "provider": "local_aggregate",
  "symbol": "BTC-USDT",
  "timeframe": "4h|1d|1w",
  "derived_from_timeframe": "5m",
  "ts_open_ms": 1776796800000,
  "ts_close_ms": 1776811200000,
  "open": "84850.0",
  "high": "85200.0",
  "low": "84620.5",
  "close": "85010.2",
  "volume_contract": "456789",
  "volume_base": "456.789",
  "volume_quote": "38880000.44",
  "source_bar_count_expected": 48,
  "source_bar_count_observed": 48,
  "aggregation_status": "complete",
  "aggregation_run_ts_ms": 1776811201000,
  "trace_id": "..."
}
```

### Aggregation rules
- open = first source open
- high = max source high
- low = min source low
- close = last source close
- volumes = sum of source volumes

### Validation rules
- `source_bar_count_observed` must equal expected count for a complete bar
- if not equal, do not promote as canonical complete aggregate
- partial aggregates may exist in staging for diagnostics, but should not be treated as canonical closed higher-timeframe bars

---

## 7) Surveyor feed-family contract expectations

The Surveyor `feed_state` family should expose, per timeframe:
- freshness state
- continuity state
- source contract
- whether the bar set is live, repaired, or replayed
- whether the timeframe is canonical raw or locally derived

Suggested additions:

```json
{
  "timeframe_role": "canonical_raw|derived_aggregate",
  "derived_from_timeframe": "5m|null",
  "retention_tier": "hot|warm",
  "repair_state": "clean|repair_pending|recently_repaired"
}
```

---

## 8) Retention policy

The key point is to distinguish **hot operational storage** from **longer-lived derived context**.

## Recommended hot retention

### Canonical `5m`
- retain **14 days** hot by default
- minimum acceptable for first pass: **7 days**
- recommended default: **14 days**

### Derived `4h`
- retain **180 days**

### Derived `1d`
- retain **2 years**

### Derived `1w`
- retain **full project lifetime** unless storage becomes material

### Raw websocket staging
- retain **24 to 72 hours** only
- longer only when debugging or doing ingestion audits

## Why this is reasonable

For the current strategy posture:
- `5m` is the expensive, high-churn layer
- `4h/1d/1w` are cheap to store and carry more of the medium/long context we need for SR and market structure

This means we do **not** need to keep months of `5m` in the hot store to preserve higher-timeframe context.
We preserve that context by storing the derived higher bars directly.

## Daily maintenance job

Run a daily compaction task that:
1. verifies aggregate bars have been built and validated
2. deletes `5m` rows older than the hot-retention threshold
3. deletes raw websocket staging older than staging retention
4. records a compaction audit summary

### Important caution
If later replay/backtesting needs long-span `5m` history, prefer:
- archive export to parquet/object storage
- or a separate cold historical store

Do **not** force the hot operational SQLite store to be both the live cache and the forever archive.

---

## 9) Storage implications

Rough order of magnitude for `5m` hot storage:
- 14 days = 4032 bars per symbol
- 500 symbols = about 2.0 million `5m` rows

That is materially larger than a toy dataset, but still operationally reasonable for a bounded hot store if indexing stays disciplined.
The expensive mistake would be keeping every raw layer forever in the same operational database.

---

## 10) What the BloFin WebSocket actually gives us

Based on the current BloFin docs:
- the candlestick WebSocket pushes live candlestick updates
- fastest push frequency is documented as **1 second**
- bars include `confirm`, where:
  - `0` = incomplete current candle
  - `1` = completed candle

The docs do **not** present the candlestick WebSocket as an arbitrary historical replay mechanism.
So the correct assumption is:
- WebSocket = live current-bar updates from the moment you subscribe
- REST = historical backfill and repair

That means yes, we should expect to need REST history for:
- initial SR level seed windows
- market structure lookback
- restart/bootstrap after downtime
- gap repair after dropped subscriptions

---

## 11) Historical depth concern

This is the real caveat with BloFin.

BloFin REST candlesticks currently document:
- `GET /api/v1/market/candles`
- pagination using `after` / `before`
- max `limit = 1440`

That is enough to backfill in chunks, but the docs do **not** make as explicit a claim as OKX’s history-candles endpoint about recent-years historical coverage.

So the right current posture is:
- treat BloFin as a viable live `5m` candidate / canary until the bakeoff says otherwise
- **validate historical depth empirically before making it the sole long-lookback source**
- keep open the possibility that OKX or another provider remains the better long-history seed path if deeper SR/structure windows are required than BloFin reliably exposes
- do not promote BloFin to canonical top-100 Surveyor feed without the bakeoff decision packet

---

## 12) Initial recommendation

### Recommended BloFin adapter V1
- subscribe to BloFin `candle5m` as a candidate/canary adapter
- store only closed `5m` bars in bakeoff/canary output unless BloFin wins canonical promotion
- derive and persist `4h`, `1d`, `1w` in the candidate pipeline when testing canonical fitness
- use REST for bootstrap and repair
- keep `5m` hot retention at **14 days** if promoted
- retain higher aggregates much longer if promoted

### Recommended immediate next follow-up
Freeze a second document for:
- bootstrap lookback windows needed per dataset family
- exact retention thresholds per family
- compaction rules and failure handling
- empirical BloFin historical-depth test results versus OKX / Bybit / Binance as part of the provider bakeoff
