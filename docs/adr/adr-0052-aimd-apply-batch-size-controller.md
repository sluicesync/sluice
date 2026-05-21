# ADR-0052 — AIMD apply-batch-size controller

**Status:** **Accepted (2026-05-21).** All four decision points
signed off by the owner via AskUserQuestion dialogue (recorded inline
below). Closes GitHub issue #18 step 3 (the AIMD controller),
following the v0.45.0 Phase 1 (batch-latency DEBUG telemetry) and
Phase 2 (planetscale cross-region WARN safety-rail) that already
shipped. Concurrency-class (touches the applier hot path + the IR
contract); push-first / CI-Integration-green-before-tag discipline
applies per CLAUDE.md.

**Dialogue resolution (2026-05-21):**
- DP-1: **(b) Opt-out** — controller on by default; bare
  `--apply-batch-size=N` makes N a cap with the controller active;
  `--no-auto-tune` opts back to static behaviour.
- DP-2: **(c) Engine-default + operator override** —
  `planetscale=5s`, `mysql=10s`, `postgres=10s` defaults;
  `--apply-tune-target-latency=DURATION` to override.
- DP-3: **(c) INFO on significant events + DEBUG per-batch +
  Prometheus gauges** — wires into the existing `--metrics-listen`
  endpoint (`MetricsServer` in `internal/pipeline/metrics.go`).
- DP-4: **(a) + (b)** — latency p95 + retriable-error rate as control
  inputs, plus an advisory INFO log when ADR-0028's byte-cap is the
  binding constraint (not a control input — operator hint only).

## Context

### The problem

`--apply-batch-size` is sluice's primary CDC throughput tuning knob
(ADR-0017). The right value is a function of three things the
operator doesn't always know up front and which change over the
lifetime of a stream:

1. **Target-side commit latency.** Cross-region targets (a PS-MySQL
   target in `us-east` from a SoCal-resident sluice talking to a
   `us-west` source) have 70–90ms per-batch RTT each way. Larger
   batches amortize that cost — but only up to the target's
   transaction-time limit. PlanetScale-MySQL's vttablet kills any
   single tx that exceeds **20 seconds**; cross-region 100-row batches
   were observed hitting the timeout and tripping the ADR-0038
   retry path (GH #18 finding #2).
2. **Source CDC event rate.** Under sustained ~175 ops/sec the
   PlanetScale validation rig observed Local MySQL keeping pace at
   `batch=100` but PS-Postgres falling behind at the same setting.
3. **Target write throughput.** PG `COPY` and MySQL
   `LOAD DATA INFILE` reward large batches; PlanetScale-MySQL via
   vtgate has a sweet spot well below local-disk ceilings.

Today operators tune this by hand. Getting it wrong on the high side
produces an audit-style failure mode (every batch hits target
tx-timeout, ADR-0038 retry fires 8× with exponential backoff, exits
with an error) that is expensive to debug. Phase 2's safety-rail WARN
(planetscale + batch>50, `streamer.go::maybeWarnApplyBatchSizeRisky`)
mitigates the specific cross-region foot-gun but doesn't generalize.

### Why now

Phase 1+2 shipped in v0.45.0 — the telemetry and the safety rail. The
empirical evidence from the PlanetScale validation rig is concrete
(GH #18's table). The retry classifier ADR-0038 provides the
retriable-error signal AIMD needs. The pieces a controller would
consume already exist.

The chunk has waited for "real operator demand" — the validation rig
is the operator (and the PlanetScale validation track is the
make-or-break audience per the [[planetscale-validation-track]] memory).
This isn't theoretical demand; it's the rig's recurring footgun.

## Decision (proposed; some elements require owner sign-off below)

A bounded AIMD (Additive-Increase / Multiplicative-Decrease) controller
governs `--apply-batch-size` on a per-stream basis when opted into. The
controller's two signals mirror the GH #18 issue body:

- **Latency per batch.** Wall-clock from "batch start" to "tx commit
  returned" — exactly the value already measured by the v0.45.0
  `applier: batch latency` DEBUG line. If the rolling p95 over the
  last N batches stays below a configurable **target latency** (default
  5s — 4× headroom under Vitess's 20s tx-killer), additive-increase
  by a small step (default +5 rows). If p95 climbs above the target,
  multiplicative-decrease (halve).
- **Retriable errors per minute.** Detected via ADR-0038's
  `ir.RetriableError` interface — walking the applier-returned error
  chain. If retriable errors fire > **R per minute** (default 3),
  halve immediately and enter a cool-off period. Zero retries for K
  successful batches (default 20) re-enables additive increase.

The controller respects:

- **Floor = 1** (ADR-0017 conservative-default; always usable).
- **Ceiling = operator-supplied or engine-default.** Per the proposed
  Stage 1 defaults: 1000 for mysql/postgres, 100 for planetscale (the
  GH #18 validation-rig figures).
- **Cool-off after a multiplicative decrease** to avoid oscillation.
  The controller will not additively increase for K successful batches
  (default 20) following any decrease event.
- **Sliding window N batches** (default 50) for p95 calculation;
  bounded memory, no time-based eviction.

### Flag surface (RESOLVED — opt-out semantics per DP-1)

| Form | Behaviour |
|---|---|
| `--apply-batch-size=N` (default today's flag) | **Controller on; N becomes a CAP.** Floor = 1; target latency = engine-default per DP-2 unless overridden. This is the behaviour change from today's pure-static semantics. |
| `--apply-batch-size=auto` | Controller on; ceiling = engine-default (1000 mysql/postgres, 100 planetscale). |
| `--apply-batch-size=N --no-auto-tune` | **Strict static** — N is the fixed value, controller off. Operators with hand-tuned values use this form to preserve today's semantics. |
| `--apply-tune-target-latency=DURATION` | Overrides the engine-default p95 target (DP-2). Engaged only when the controller is active (any of the first two rows). |

This is the **opt-out** shape per DP-1 (b): controller on by default
when a batch size is configured at all. Operators who have hand-tuned
`--apply-batch-size=N` for a specific workload and want the static
semantics preserved must add `--no-auto-tune`. The behaviour change is
deliberate and documented in the v0.72.0 release notes' Compatibility
section.

### Where the controller lives

**New `internal/applier_control` package** owning the Controller type
(sliding-window p95, additive/multiplicative state machine, cool-off
timer, per-stream telemetry). Engine-neutral; both engine appliers
consult it via two new optional IR surfaces:

```go
// BatchSizeProvider is the optional surface a [ChangeApplier] can
// implement to consult an external controller for each batch's
// target size. When unset, the applier uses the static value passed
// to ApplyBatch. AIMD controller wiring (ADR-0052).
type BatchSizeProvider interface {
    NextBatchSize() int
}

// BatchObserver is the optional surface a [ChangeApplier] can
// implement to report per-batch outcomes back to an external
// controller (latency, row count, retriable-error count). Mirrors
// the v0.45.0 `applier: batch latency` DEBUG telemetry, but as a
// programmatic signal a controller can consume. ADR-0052.
type BatchObserver interface {
    ObserveBatch(latency time.Duration, rows int, err error)
}
```

Same shape as the existing `ir.RedactorSetter` / `ir.StreamIDSetter` /
`ir.ExecTimeoutSetter` optional surfaces. The Streamer constructs the
controller (when `--apply-batch-size=auto` or `--auto-tune` is set),
threads it onto both appliers via the optional surfaces, and the
applier wraps each internal batch with `provider.NextBatchSize()` +
`observer.ObserveBatch(...)` calls.

### Interaction with ADR-0038 retry classifier

The controller detects retriable errors via the same `ir.RetriableError`
interface ADR-0038 uses — walking the error chain with
`errors.As(err, &re)`. A single retriable error is a soft signal; the
counter trips on > R/min (default 3) over a rolling 1-minute window.
This dampens the controller's response to transient blips (single
deadlock, single connection drop) while still reacting decisively to
sustained pain (the cross-region tx-killer pattern).

ADR-0038's existing exponential backoff inside the retry policy is
unchanged; the AIMD controller observes the OUTCOME (retriable error
fired) not the retry machinery itself.

### Interaction with ADR-0017 conservative-default

ADR-0017's conservative-default of `--apply-batch-size=1` is
**preserved**. The controller's floor is 1 — it never goes below
ADR-0017's safety value. The controller is opt-in (DP-1 below);
default behaviour is unchanged.

## Decision points (RESOLVED 2026-05-21)

All four genuine owner decisions, resolved via AskUserQuestion
dialogue. The narrative below records the dialogue for future
reviewers; the resolutions are the load-bearing record.

### DP-1 — Default behaviour: opt-in vs opt-out

**RESOLVED 2026-05-21: (b) Opt-out — controller on by default.**

Owner chose the more ergonomic default after weighing the trade. New
operators (the make-or-break audience) get auto-tuning out of the box;
existing operators with hand-tuned values must add `--no-auto-tune`
to preserve the static semantics. The behaviour change is deliberate
and surfaces in v0.72.0's release-notes Compatibility section. The
forcing function (validation-rig cross-region pain) was the read
weighting toward (b) — operators don't always know what's optimal,
and the controller's floor=1 + ADR-0017 conservative-default mean the
worst case stays safe.

The dialogue's recommendation was **(a) opt-in**; owner overrode in
favour of better ergonomic defaults for new adoption. The override is
preserved verbatim here because the trade-off may resurface if a
future operator workload is broken by the default flip.

### DP-2 — Target latency: fixed / engine-dependent / operator-tunable

**RESOLVED 2026-05-21: (c) Engine-default + operator override.**

Defaults: `planetscale=5s` (Vitess 20s tx-killer + 4× headroom),
`mysql=10s`, `postgres=10s`. Override via
`--apply-tune-target-latency=DURATION`. Engine-default lookup follows
the same shape as the existing per-engine `Capabilities` declaration
pattern.

### DP-3 — Controller telemetry shape

**RESOLVED 2026-05-21: (c) INFO on significant events + DEBUG
per-batch + Prometheus gauges.**

Implementation:
- **INFO log** on multiplicative-decrease events, cool-off
  enter/exit, ceiling caps, and the byte-cap-dominant advisory
  (per DP-4 (b) below). Operators at default INFO see "what
  happened" without per-batch noise.
- **DEBUG log** per batch with current size + observed latency + p95
  + decision reason. Cycle-test runs at `--log-level=debug` get the
  trace.
- **Prometheus gauges** wired into the existing `--metrics-listen`
  exporter (`internal/pipeline/metrics.go`):
  - `sluice_apply_batch_size_current{stream_id}` — current target
    batch size after the controller's latest decision.
  - `sluice_apply_batch_size_p95_seconds{stream_id}` — rolling p95
    latency over the controller's sliding window.
  - `sluice_apply_batch_size_decreases_total{stream_id}` — counter
    incremented on each multiplicative-decrease event (lets operators
    alert on persistent oscillation).
  - `sluice_apply_batch_size_cooloff{stream_id}` — gauge 0/1 for
    "currently in cool-off period."

  The exporter reads these at scrape time without touching the apply
  path's hot loop (the controller's state is updated on-batch; the
  exporter snapshots it). Mirrors the existing metrics.go discipline
  of "no instrumentation of the apply path" — the controller's
  apply-path work is for control, not for metrics; metrics piggyback.

### DP-4 — Scope: stage 1 only, or full envelope?

**RESOLVED 2026-05-21: (a) + (b) — latency p95 + retriable-error rate
as control inputs, plus an advisory INFO log when ADR-0028's byte-cap
is the binding constraint.**

Control inputs (drive AI/MD):
- Latency p95 over the rolling N-batch window (default N=50).
- Retriable-error rate over the rolling 1-minute window (default R=3).

Advisory only (no control influence):
- When the byte-cap (ADR-0028's `maxBufferBytes`) consistently fires
  before the row cap, the controller logs `level=INFO msg="applier:
  byte-cap dominant" hint="consider raising --max-buffer-bytes"` once
  per cool-off period. The controller does NOT change behaviour
  based on this signal — it just hints to the operator that
  additive-increase on rows can't help because the binding constraint
  is bytes. Same rate-limited shape as the v0.45.0 Phase 2 WARN.

Cross-stream coordination (the dialogue's (c)) is explicitly out of
scope for v1; tracked as a follow-up if operator workload surfaces
the case.

## Implementation pre-resolutions (not separate DPs)

These are pre-decided based on classical AIMD literature + codebase
precedent; called out explicitly so a reviewer can override at the
dialogue if any look wrong:

- **Additive step = +5 rows per batch** (GH #18 issue body). Classical
  AIMD uses an additive constant. At small batch sizes (size=10) the
  +5 is aggressive (+50%); at large (size=500) it's slow (+1%). The
  multiplicative decrease handles the upper end; the slow large-end
  convergence is acceptable because operators with large-size needs
  set their own ceiling.
- **Multiplicative factor = 0.5 (halve)** (GH #18 issue body, matches
  TCP). Halve from oversized states is the proven-fast escape from
  the danger zone.
- **Sliding window = 50 batches** for p95 calculation. Count-based,
  bounded memory, no time-based eviction needed. The window matches
  the apply-loop's natural cadence; at 1 batch/sec the window covers
  ~50s of history.
- **Retriable-error threshold = 3 per 1-minute rolling window**. The
  ADR-0038 retry policy's default of 8 attempts means a single
  failure-and-eventual-recovery shows up as 1–7 retries, all within a
  short span; the 3/min threshold catches sustained pain (multiple
  batches in a row needing retries) without firing on a single
  recovered episode.
- **Cool-off after MD = 20 successful batches**. Count-based, mirrors
  the sliding-window pattern. Avoids ping-pong; the controller
  doesn't try to climb back until 20 consecutive clean batches at the
  new (smaller) size prove it's stable.
- **Per-stream state, not per-process.** Each `ChangeApplier`
  instance owns its controller (or none, if the optional surfaces
  aren't wired). Multiple streams in one process have independent
  controllers.
- **Cross-engine identical behaviour.** Both MySQL and PG appliers
  wire the same two interfaces. Tested symmetrically.

## Consequences

### Positive

- The cross-region PS-MySQL foot-gun the Phase 2 WARN partially
  mitigates becomes a self-correcting one when operators opt in. No
  hand-tuning required.
- The v0.45.0 telemetry investment compounds: existing DEBUG signals
  feed a programmatic controller.
- Bounded blast radius: opt-in (per DP-1 recommendation), per-stream
  state, hard floor at ADR-0017's safe value.

### Negative / load-bearing

- The applier's hot path gains two interface calls per batch
  (`NextBatchSize` + `ObserveBatch`). On a no-controller default the
  optional surfaces aren't wired and the calls don't exist — zero
  overhead. With controller wired, the cost is two cheap calls per
  batch; trivial vs the batch's existing tx-commit cost.
- The IR contract grows by two optional interfaces. Established
  pattern (RedactorSetter, StreamIDSetter, ExecTimeoutSetter), but
  every addition is interface-surface debt. The new interfaces are
  scoped narrowly to the controller's needs.
- Engine-side wiring touches `applyOneBatch` in both engines —
  consult provider before opening tx, observe after commit. The Phase
  1 `applier: batch latency` instrumentation already measures the
  exact window; the new observer call sits next to that DEBUG line.
- Misconfiguration risk: operator runs `--apply-batch-size=auto` on a
  workload where the controller's defaults are wrong (target latency
  too low → constant decreases; too high → never reaches the right
  size). The `--apply-tune-target-latency` override (DP-2 (c))
  addresses this; documentation surfaces the override prominently.

### Test matrix

**Unit tests** (`internal/applier_control`):
- p95 calculation over a known fixture window
- Additive increase fires when p95 < target AND zero retries in window
- Multiplicative decrease fires when p95 > target
- MD fires when retriable errors > 3/min
- Cool-off prevents AI for 20 batches after MD
- Floor enforcement (never below 1)
- Ceiling enforcement (never above configured cap)
- Per-stream isolation (two controllers in the same process don't
  interfere)

**Engine unit tests** (mysql, postgres):
- Applier with no BatchSizeProvider falls back to static `maxBatchSize`
  argument (regression guard — existing operators don't see behaviour
  change)
- Applier with provider consults `NextBatchSize()` before each batch
- Applier with observer calls `ObserveBatch(latency, rows, err)` after
  each commit (success path)
- Applier with observer calls `ObserveBatch(latency, rows, err)` after
  each rollback (failure path; err is the wrapped retriable error)

**Integration tests** (cross-engine, like Phase 1's matrix):
- PG → MySQL with controller engaged: workload at sustained rate
  drives p95 to target; batch size converges to a stable value within
  N batches.
- Cross-region simulation (artificial latency injection via `tc` or a
  Go time-of-flight wrapper): controller decreases on observed
  latency, recovers when latency drops.

**Telemetry-output tests**:
- INFO-level log fires on MD event; doesn't fire on AI events
  (per DP-3 (b) recommendation).
- DEBUG-level log fires per batch with current size + p95 + decision
  reason.

## References

- GitHub issue #18 (proposal + empirical evidence table)
- [ADR-0017](adr-0017-batched-cdc-apply.md) — conservative-default
  rationale and the floor=1 invariant
- [ADR-0028](adr-0028-memory-bounded-streaming.md) — `maxBufferBytes`
  byte-cap (DP-4 (b) scope-creep option)
- [ADR-0038](adr-0038-applier-retry-on-transient-errors.md) — the
  `ir.RetriableError` interface and the retry policy whose firings
  the AIMD controller observes
- `internal/engines/{mysql,postgres}/change_applier_batch.go::applyOneBatch`
  — the existing batch loop the controller wires into
- `internal/pipeline/streamer.go::maybeWarnApplyBatchSizeRisky` —
  GH #18 Phase 2 cross-region safety rail (stays as-is; complements
  the controller for operators who don't opt in)
