# ADR-0110: Coordinated cold-copy grow-window pause

## Status

**Accepted (2026-06-21).** The *proactive* deepening of the v0.99.92–v0.99.99
reactive storage-grow arc (ADR-0108 reparent-retry, ADR-0109 source-read-resume,
and the classifier widenings). Roadmap item 37. `-race`-before-tag concurrency chunk.

Implemented as specified. The engine-neutral `ir.GrowGate` (+ `ir.GrowGateSetter`)
seam lives in `internal/ir/grow_gate.go`; the concrete `pipeline.growGate`
coordinator in `internal/pipeline/grow_gate.go`; the hot-path `Await`/`Trip` wiring
in `internal/engines/mysql/row_writer_reparent_retry.go` (write lanes) and
`internal/pipeline/copy_source_read_retry.go` (source-read lane); the
telemetry-driven trip + recovery probe in `internal/pipeline/streamer_telemetry.go`.
One deviation from the design sketch, for race-safety (recorded in Consequences
below): the owner goroutine does NOT reopen *mid-window* to let lanes probe and then
re-close on a re-trip (that would need a second owner / a probe-vs-trip race).
Instead a quiet backoff cycle ENDS the window — the reopen lets the lanes resume and
probe; if the target is still bad the first lane to re-hit the transient opens a
FRESH window. This is observably equivalent (coordinated quiesce, coalesced
concurrent trips, exponential hold matching the retry envelope, bounded by max-hold)
and is single-owner / single-teardown, so it has no double-close / dangling-owner
race. **The `-race` integration gate has NOT been run locally (CGO=0 box); the main
session must land it through CI's `-race` Integration job before any tag** — this is
a concurrency chunk.

**Wiring fix (v0.99.102).** v0.99.100 wired the gate onto the writer only in the
migrate keyset-chunked path (`openOneChunkConn` → `applyGrowGate`). The **sync
cold-start path** (incl. the native-concurrent W×D cold-copy that Track-D / every
PlanetScale CDC migration uses) opens ONE top-level writer that the fan-out reuses
across all D workers, and that writer never had `SetGrowGate` called — so the gate was
**inert** there (the source-read retry got it via `deps.growGate`, but the write path
did not). The v0.99.101 PS-320-v11 live run proved it: the gate tripped **zero** times
while the writers rode **74** real grow-window retries independently. Fix:
`runBulkCopyPhases` now calls `applyGrowGate(rw, parallel.growGate)` centrally on the
top-level writer, so every cold-copy path (sync parallel, native-concurrent,
migrate-nonchunked) engages the coordination; the chunked path keeps its per-chunk
wiring. Pinned by `TestRunBulkCopyPhases_WiresGrowGateOntoTopLevelWriter` (+ a nil-gate
no-op pin). The v0.99.100 regression cycle missed this because its no-op Focus tests
used the migrate `--bulk-parallelism` path (where the wiring existed) and could not
trigger a live grow to observe the sync path never tripping.

**Real wiring + wall-clock bound (v0.99.103).** v0.99.102's wiring was *still* on the
wrong function: `runBulkCopyPhases` is used by the ADR-0079 shareable-snapshot fast
path and `migrate`, but the native-MySQL concurrent cold-copy that Track-D / every
PlanetScale MySQL→MySQL `sync` uses goes through a DIFFERENT path —
`coldStartRunCopy` → `runBulkCopyWithOpts` → `runConcurrentTableCopy(rw)` — which
never constructed or wired a gate at all. The PS-320-v10/11/12 live runs tripped the
gate zero times. v0.99.103 constructs + wires the gate in `coldStartRunCopy` (and the
multi-DB twin in `streamer_multidb.go`), where the Streamer's `TargetTelemetry` is in
scope; `runConcurrentTableCopy` reuses the single `rw` across all W×D workers, so
`SetGrowGate(rw)` engages the coordination for the whole fan-out.

v0.99.103 ALSO changes the cold-copy retry bound from **attempt-count to WALL-CLOCK**
(`flushWithReparentRetry` + `copyTableWithSourceReadRetry`, ~30 min). The gate's fast
probe cycles consume attempts far faster than wall-clock, so the old 24-attempt cap
exhausted on a *single* batch mid-grow (PS-320-v11/v12 died on `documents`/`bool_tiny`
during the initial 12→39 grow); a wall-clock deadline rides a prolonged multi-step
grow regardless of probe cadence — the robust "don't get stuck on a storage threshold"
guarantee — while still surfacing a genuinely-wedged target loudly after ~30 min. The
attempt count remains only as a high runaway backstop. Two misfires (v0.99.101/102
wired adjacent functions, not Track-D's actual path) are the lesson: trace the exact
runtime path from ground truth before fixing.

**Reopen-semantics refinement (v0.99.105, the v14 finding).** The first live run with
telemetry ON (v0.99.104 PS-320-v14) exposed that the original "a proactive pause holds
until `recovered()` or max-hold" rule made the telemetry-enabled gate *worse* than the
reactive one: the `volume_*` gauges `recovered()` reads swing wildly and transiently
**disappear** across a reparent (observed live: 62 GB@85.9% → 1.66 TB@~0%, series
absent), so `recovered()` could not confirm "grow finished" and the gate rode the full
~20-min max-hold — a flat zero-progress stall on every reparent, strictly worse than the
reactive path's incremental ~30 s cycling. Fix: the **quiet-cycle reopen now applies
ALWAYS** (telemetry or not) — a backoff cycle with no re-trip reopens so the lanes
resume and probe, the proven reactive behaviour. `recovered()` is demoted to an
*early-reopen accelerator* (reopen sooner when the signal is trustworthy), and max-hold
stays the backstop. So a proactive (telemetry) trip is now a BRIEF anticipatory pause
that hands off to the reactive cycling, not a hold for the whole grow. (Smarter proactive
behaviour — a rate-of-change trigger and reparent-robust recovery detection — is the
Phase-3 metrics follow-up, roadmap item 32/§Phase 3.)

**Escalation is per WINDOW, not per trip (item 138, 2026-08-05 — the second field report).** The shipped coordinator advanced its exponential ladder once per `Trip`, via an `extend` signal the owner goroutine consumed as fast as it arrived. The ladder was meant to express *temporal* persistence — "the target is still bad after we waited" — but what actually fed it was *fan-out width*. Every lane parks in `Await` while the gate is closed, so the only trips that can arrive during a window come from the lanes that were mid-flush when it closed: all of them reporting the SAME target event, carrying no information the first trip did not.

Measured on the reporter's log, not inferred: 246 close/reopen pairs, and the hold matched `backoff(tripCount)` to the millisecond — 0.100 s at 1 trip, 0.81 s at 4, 12.87 s at 8, 30.0 s at ≥14. With the default fan-out (W×D = 16 lanes on the sync cold-start native-concurrent path; up to 32 on `migrate`) one connection drop produced 13–16 trips within ~4 ms and burned the ladder to its 30 s cap in those 4 ms. **The 30 s was never a design choice — it is the cap of a ladder consumed by concurrency.** The run spent 4369.3 s closed against 1015.8 s open: **81.1 % of a 91-minute copy quiesced**, four chunks in the last hour, on a target that was merely dropping connections (244 of the 246 windows were tripped by `vtgate connection error: no endpoints`, a routing/transport availability error carrying no reparent evidence at all).

Two further defects surfaced with it, both of the "written invariant nobody checks" class:

- The code claimed "the exponential backoff grows across windows". It did not. `finishWindow` cleared all window state and every new window restarted at rung 1, so *all* the growth happened inside a single window — which is exactly why it had to be driven by trip count.
- `GrowGateMaxHold` (20 min) reads like a bound on how long the gate may stall a run. It bounds **one window**. No window in the field run came close to it, so the backstop never fired while the gate accumulated 73 minutes of quiesce. A gate whose scope is narrower than its name.

The fix, therefore: a window is one hold of `backoff(cycle)` and nothing can lengthen it (the `extend` channel is **deleted**, which makes the defect inexpressible rather than merely fixed); `cycle` is EPISODE-scoped and advances exactly one rung per window while trouble persists, resetting after `GrowGateEpisodeIdle` of healthy open time; and a new RUN-level ceiling — at most `GrowGateMaxQuiesceShare` of any trailing `GrowGateQuiesceWindow` — bounds the thing max-hold never could, after which the gate declines to close and the per-lane budgets carry the run.

Sizing rationale, since it is not arbitrary: the ladder deliberately tracks the *per-lane* reparent-retry ladder (same 100 ms → 30 s shape, now also advancing once per lane attempt), so the gate's hold OVERLAPS the backoff each lane would have taken anyway. The gate buys **alignment**, not extra waiting. That is also why declining to close is safe rather than a return to thrashing: ADR-0110's cost case assumes lanes "independently hammer-retry", and they do not — the same field log shows 1061 of 2195 lane retries already sitting at their own 30 s cap.

Pinned by five gates in `internal/pipeline/migcore/grow_gate_duty_test.go`, stated as a PAIR because a gate proving only "the gate closes" would have passed the broken behaviour: a sustained storm must leave the copy most of the wall clock, AND sustained trouble must still escalate to a meaningful quiesce. Mutation-verified in both directions, including the "just shorten the quiesce" non-fix, which the anti-vacuity half catches.

## Context

A non-Metal PlanetScale MySQL volume auto-grows in steps (12 → 39 → 62 → 214 GB).
Each grow step opens a **serving-transition / reparent window** during which the
target rejects writes through a rotating set of faces — `not serving`, `code =
Canceled QueryList.TerminateAll`, errno-28 / `ER_DISK_FULL` 1021 / `ER_RECORD_FILE_FULL`
1114, and 1205 lock-wait-timeout. v0.99.92–v0.99.99 made every one of those faces
**retriable** and widened the budget to a ~15–20 min envelope, so the cold-copy now
**rides through** a grow REACTIVELY.

### What the live diagnostic proved (2026-06-21, Track-D PS-320 v6–v9)

Three runs on a *growing* 12 GB volume (v6/v7/v8) all froze at the **same** byte
point — 10.34 GB ≈ 86 % of the 12 GB volume, i.e. exactly the auto-grow trigger
threshold — and exhausted the retry budget. A fourth run (v9) on a volume that had
**already** grown to 62 GB rode straight through that point, copying the big
`documents` (MEDIUMTEXT) table clean past its full row count with **zero** transient
faces. Two hypotheses were ruled out by ground truth:

- **Concurrency is not the cause.** A 1-lane run stalled identically to a 16-lane run.
- **Data is not pathological.** `documents` copies fine on a pre-grown volume; it only
  looked like the culprit because it is the big table being hammered when the
  threshold trips.

The cause is precisely **being mid-write into the volume during its grow/reparent
transition**. The resize itself is fast; the *serving-transition window* it triggers
is where writes are rejected.

### Why reactive alone is not enough

Reactive ride-through *works* but is **inefficient and self-prolonging**. During the
multi-minute window, all ~16 cold-copy lanes (W tables × D fan-out) independently
hammer-retry the struggling target. That contention slows the grow + recovery and
breeds secondary errors (the 1205 lock-wait-timeouts were a *consequence* of the
hammering, not an independent fault). The target could complete the transition faster
if left alone. We want to **quiesce the lanes together** for the window instead of
letting each burn its own budget pounding the target.

### The trip signal is an INFERENCE, and that argument does not cover most of what it fires on (item 143, 2026-08-06)

Everything above is an argument about a **serving transition**. The gate does not trip on serving transitions; it trips on whatever the engine's classifier calls retriable, which is one flat verdict spanning genuine grow faces (`not serving`, ER_DISK_FULL 1021/1114, read-only 1290, PG 53100 / `cluster is read-only`) and pure transport losses (`vtgate connection error`, `connection reset by peer`, `unexpected EOF`, errno 2006/2013). Two independent datasets say the second population dominates overwhelmingly: **244 of the 246 windows** in the 2026-08-05 field log were opened by `Error 1105 (HY000): unavailable: vtgate connection error: no endpoints`, with **zero** opened by any reparent-evidenced face; and across **~870 absorbed drops over six A/B runs on real PlanetScale**, **zero** carried reparent evidence. So the mechanism's own WARN — "quiescing all cold-copy lanes for a coordinated target storage-grow / reparent window", and the engine lanes' "likely a primary reparent / 'not serving'" — was naming a cause that had almost never occurred, and sent readers hunting for it.

**The trip set is UNCHANGED, and that is a decision rather than an omission.** A real storage-grow reparent is not reliably separable from a plain transport drop *at the trip point*: a grow's most common face on an in-flight write is the connection dying, which is byte-identical on the wire, so absence of grow evidence is not evidence of absence. The corroborating observations the finding proposed are not available where the decision is made — the tier probe (`ProbeTargetConnectionBudget`) is a one-shot preflight read of `@@max_connections` / `@@innodb_buffer_pool_size` and observes no storage quantity at all; `--min-storage` is a PlanetScale provisioning knob and **not a sluice flag** (it does not exist in this codebase); and the telemetry sidecar is opt-in behind `--planetscale-org`, absent entirely on `migrate` and `restore`, lagging by up to a 60 s poll plus a 180 s freshness window, and documented in this very ADR (the v0.99.105 v14 finding) as transiently **disappearing** across the reparent it would corroborate. Narrowing the trip on that basis would trade a bounded, loud-safe false positive for the possibility of missing the event the gate exists for — the wrong direction on this project's first tenet.

**What changed instead is the CLAIM, and it is now derived rather than asserted.** `ir.GrowEvidence` is a per-trip verdict — `target-grow-face`, `no-grow-evidence`, `telemetry-headroom` — computed by each engine from the *same predicates its retry classifier uses* (`internal/engines/{mysql,postgres}/grow_evidence.go`), carried through `GrowGate.Trip`, and printed as `evidence=` on the gate's CLOSED/reopened lines and on both engines' retry WARNs. Under-claiming is the rule: a face that a grow merely *causes* — a killed query (`QueryList.TerminateAll`), a lock-wait timeout, a throttler rejection, a dropped connection — stays `no-grow-evidence`, because a busy healthy target produces all of them. The zero value is `no-grow-evidence`, so a caller that forgets to classify under-claims rather than lies.

**And the justification for quiescing on a transport drop is now written down, because it is not the one above.** It buys (a) *alignment* for the lane that tripped — its hold overlaps the backoff it would have taken anyway, giving the target contiguous quiet instead of a smear — and (b) a probabilistic *shield* for the four bulk-write lanes that have no replay, which do not issue a doomed write while the gate is closed. Note the scope correction the old wording obscured: "the gate buys alignment, not extra waiting" is true of the tripping lane and **false of a sibling that was copying successfully**, since `Await` runs before every attempt, so a healthy lane's hold is added latency. The cost, post-item-138 and measured rather than assumed, converges on `GrowGateMaxQuiesceShare` — half the wall clock — and item 138's real-PlanetScale validation observed that ceiling engaging (150.1 s per 300 s window, in both runs). That is a live tension, not a settled question; what it is not is a silent-loss risk, since the gate never swallows an error and each lane's bounded retry remains the loud-on-exhaustion floor.

The gates: the per-engine face matrices with anti-vacuity floors in both directions (`grow_evidence_test.go` in each engine package), the AST sweep requiring every trip to route through the deriving helper so a new lane cannot claim its own verdict, the partition gate on `vtgateTransientSubstrings`' two halves, and — the one that keeps this decision honest — `TestGrowGate_EvidenceIsDescriptiveOnlyAndNeverChangesTheHold`, which fails if the verdict is ever wired into the gate's timing without this reasoning being revisited. *(That last gate did its job: item 154 below reverses part of this decision, and the test is what forced the reversal to be argued rather than slipped in. It has been replaced by `TestGrowGate_EvidenceGovernsTheDeepEscalationAndNotTheEarlyHolds`, which pins the narrower property that survived.)*

**The instrument this leaves behind is the point.** Item 143 could not answer "how often does the gate fire on a real grow?" because every trip reason was raw error text and the answer had to be re-derived by hand from 246 log lines. `evidence=` is greppable and countable, so the next field report answers it in one pass — and if that count says the grow faces are as rare as they now look, narrowing the trip becomes an *evidenced* change rather than a bet.

### The evidence becomes a TIMING input, for the deep escalation only (item 154, 2026-08-07)

The bet came due. A user migrating a 2.64 GB / 16 M-row MariaDB to PlanetScale reported that `migrate` copied the whole database in 45 minutes while `sync` failed five times, always in the cold-start bulk copy, always on `audits` (1.7 M rows, ~890 MB, ~520-byte rows). Their third run: killed after 91 minutes, **2,687 vtgate drops absorbed, zero unhandled**, max retry attempt 36 — and **four chunks copied in the last sixty minutes**. Drops per bucket were flat across the whole run. The retry machinery worked perfectly and the copy went nowhere.

**What the ground truth said.** Re-measured from the field log rather than from the report: the gate was **CLOSED 81.1% of the run** (4,369 s of 5,385 s across 246 windows), and closed **92–97%** of every ten-minute bucket after 14:00 — the period in which those four chunks landed. The correlation is direct: the one bucket at 42% duty completed 62 tables; the buckets at 92–97% completed three. Mean hold was pinned at ~30 s, the backoff cap, because `GrowGateEpisodeIdle` is 60 s and drops arriving every ~2 s mean an episode never ends. Every one of the 2,687 drops classified `no-grow-evidence` (`vtgate connection error` is in the transport half of the partition, by construction). The discriminator item 143 built was correct, computed, and ignored.

**Two confounds, named rather than assumed.** First, the comparison is not clean: `migrate` ran against a **PS-160 production branch** and every failing `sync` ran against a **dev branch**, which the log confirms (`innodb_buffer_pool_size=33554432` — 32 MiB, the dev-branch signature). A dev branch is the far more likely source of a chronic drop storm, so "why does `migrate` survive what `sync` does not" is answered mostly by *the target*, not by the code path — both entry points construct the same gate with the same envelope (`migrate_phases.go` vs the three `streamer_coldstart*.go` sites; the only difference is migrate's `recovered == nil`, which affects early release, not hold length). Second, the user's binary was **v0.111.1, which predates item 138's run-level share ceiling** (v0.113.0); on current `main` that ceiling alone bounds this at 50%. Neither confound rescues the design: 50% of the wall clock is still a 2× tax on lanes that were copying fine, and — the part a fraction hides — it is spent in **30-second units**, so a lane that finishes a chunk one millisecond after the gate closes waits a full cap-length hold before starting the next. Throughput is quantised to one chunk per hold regardless of what the duty cycle averages.

**The decision, and exactly how much of item 143 it reverses.** `GrowGateEvidenceFreeHoldCap` (250 ms) bounds a single window's hold during an episode in which **no trip has yet carried grow evidence**; an episode that has seen evidence keeps the full 30 s ladder, and the flag accumulates per episode (one evidenced trip restores the deep ladder for the rest of it) and resets with the ladder.

Three things this deliberately does **not** do, because they are the parts of item 143's refusal that still hold:

- **The trip set is still unchanged.** Every classified transient still trips the gate and still quiesces the fleet. This is not the "narrow the trip on absence of evidence" change item 143 declined — absence of evidence is still not evidence of absence, and a trip that produces no evidence is still honoured.
- **The early rungs are still evidence-independent.** Rungs below the cap are bit-identical for every evidence value, so the *first* hold — the one the shield actually rides on — is unchanged. Item 143's argument is sound about the trip point and is preserved there in full.
- **What is rejected is only the DEEP escalation on an episode that has never produced evidence.** By the tenth consecutive re-trip the gate has armed, held, reopened and been re-tripped repeatedly with no evidence anywhere; "this is a bounded serving transition that needs quiet" is no longer an unfalsified hypothesis but one the gate's own measurements have refuted, because a grow window *resolves* and that is what makes it a window.

**Why the discriminator has to be the evidence.** The tempting evidence-free fix — de-escalate when a window is re-tripped immediately, since that hold demonstrably bought nothing — is wrong in the dangerous direction: a real multi-minute grow reparent produces immediate re-trips too, so de-escalating on them collapses the hold to the probe interval during precisely the window this ADR exists for. Prompt re-trip is ambiguous between the two populations; the evidence is the only available signal that is not.

**Why this does not reintroduce the hammering.** The same argument `GrowGateMaxQuiesceShare` already makes: the retrying lanes are rate-limited by their own 100 ms → 30 s backoff, not by the gate — 1,061 of the field log's 2,195 lane retries were already sitting at the 30 s cap. The gate's marginal value on an evidence-free drop is *alignment*, and 250 ms still buys it, because it still parks every lane on a common boundary; 250 ms is chosen to cover the tripping lane's own first two backoff rungs, which is exactly the overlap "alignment" names. The four no-replay lanes keep a shield of that length instead of a cap-length one — a narrowing of a probabilistic shield, not the removal of a guarantee, and their failure remains loud.

**Expected duty cycle.** Against the field's ~30 drops/minute (one per ~2 s), worst-case cost before coalescing is `H/(H+I)`: **~11%** at a 250 ms cap, against **81.1% measured** and the **~50%** the share ceiling alone still permits. A first cut of this fix used a 1 s cap and the throughput gate caught it at 49.7% — 1 s against a 2 s drop interval is 50%, i.e. exactly what the ceiling already gave.

**The gates**, stated in the user's units rather than in duty cycle, because "four chunks in the last sixty minutes" is the defect: `TestGrowGate_EvidenceFreeDropStormLeavesTheCopyMostOfItsThroughput` scores chunks landed against **the same lanes against the same target with no gate at all**, measured in the same run — an independent expected value that no change to the gate's self-reporting can move. It requires ≥50% of ungated throughput (pre-fix: 9.1%) and carries an anti-vacuity half requiring an *evidenced* storm to still be quiesced to ≤25%, so the cap cannot be widened onto the evidenced path without failing. `TestGrowGate_EvidenceGovernsTheDeepEscalationAndNotTheEarlyHolds` pins both halves of the scope (early rungs identical, deep rungs diverging), and `TestGrowGate_EvidenceAccumulatesPerEpisodeAndResetsWithTheLadder` pins stickiness and reset — the latter after a mutation run showed the obvious one-window version could not see a deleted reset, because the ladder resets to rung 1 independently and the cap is not observable there.

## Decision

Introduce **one engine-neutral coordinated-pause primitive** shared across every
cold-copy write lane in a run, tripped from **two sources**:

1. **Signal-driven (baseline, no external dependency).** The FIRST classified
   grow-transient on any write lane trips the shared gate; all sibling lanes quiesce
   together for a coordinated window, then resume. This works for **any** target with
   storage-auto-grow / transient-reparent behaviour — **non-PlanetScale included** —
   because the trigger is the classified transient itself, not a PS-specific metric.

2. **Telemetry-driven (precision enhancement, PlanetScale-only when configured).**
   The Item-32 storage-headroom sidecar (`streamer_telemetry.go`) trips the SAME gate
   **proactively** — before lanes start hitting transients — when
   `storage_available_bytes` heads toward the grow boundary. This avoids burning any
   retry attempts and avoids the source-read backpressure-EOF cascade entirely. It is
   **advisory**: a no-metrics run still rides through via source (1), just less
   efficiently.

Both sources drive **one** mechanism. This is the layering item 37 should have: the
signal-driven pause is the universal floor; telemetry just fires the same pause
*earlier*.

### The primitive — `ir.GrowGate`

A small interface in `internal/ir` (the shared contract both `pipeline` and the engine
packages already import, mirroring how `ir.TargetTelemetry` reaches the apply path):

```go
// GrowGate coordinates a cold-copy quiesce during a target storage-grow /
// reparent window. A nil GrowGate ⇒ pre-ADR-0110 behaviour, byte-for-byte:
// Await returns immediately, Trip is a no-op. (Construct via the typed-nil
// guard so a nil concrete value never becomes a non-nil interface.)
type GrowGate interface {
    // Await blocks while the gate is CLOSED (a pause is in effect) and
    // returns nil the instant it reopens. It returns ctx.Err() promptly on
    // cancel — this is the load-bearing no-deadlock contract. When the gate
    // is OPEN (the common case) it is a cheap near-lock-free return.
    Await(ctx context.Context) error

    // Trip closes the gate (or extends an open pause) and records reason for
    // the structured log. Idempotent and concurrency-safe: concurrent trips
    // from many lanes + the telemetry sidecar coalesce into ONE pause window.
    Trip(reason string)
}
```

Hot-path placement: each write lane calls `gate.Await(ctx)` **at the top of every
batch-flush attempt** (inside `flushWithReparentRetry`, before the exec) and the
source-read retry calls it before each (re)attempt. When open this is a couple of
atomic reads; it adds no measurable cost to an untroubled copy.

### The coordinator — `pipeline.growGate` (concrete impl)

Lives in `internal/pipeline` (the per-run orchestration owner), constructed once per
cold-copy run and threaded to (a) every MySQL `RowWriter` via its config, (b) the
pipeline source-read retry, and (c) the telemetry sidecar. Behaviour:

- **State** = `open` / `closed`, guarded by a mutex + a `chan struct{}` "reopen"
  broadcast (closed-channel broadcast pattern, re-created on each close→open). `Await`
  fast-paths an `open` read; when closed it selects on `{reopenCh, ctx.Done()}`.
- **On `Trip`:** if already closed, just extend the deadline (coalesce). If open,
  close the gate and start (or hand off to) the single **owner goroutine**.
- **Owner goroutine** runs the quiesce cycle: hold closed for a backoff interval
  (exponential, same 100 ms→30 s shape as ADR-0108/0109, so the pause envelope matches
  the retry envelope), then **reopen** to let lanes probe the target. If a lane
  immediately re-trips (still in the window), the owner closes again and extends —
  bounded by a max-hold so a genuinely-dead target still surfaces (the lane's own
  `flushWithReparentRetry` budget remains the authoritative loud-on-exhaustion floor;
  the gate NEVER swallows a terminal error, it only changes *how the wait is spent* —
  coordinated-and-calm vs independent-and-hammering).
- **Telemetry trip release:** a proactive trip (no lane error yet) reopens on the
  earlier of (max-hold timer | the sidecar observing storage headroom recovered).

### Why this composes safely (the gotchas, answered)

1. **No deadlock with the errgroup + AIMD.** `Await` is the only new blocking point and
   it always selects on `ctx.Done()`; when any lane exhausts its bounded retry and
   returns terminal, the errgroup cancels the group ctx → every parked `Await` returns
   `ctx.Err()` → clean unwind. The gate holds no lock across the block. The per-lane
   AIMD is untouched — the gate gates *whether a lane attempts now*, not *how big its
   batch is*.
2. **Bounded + loud.** The gate has a max-hold; the lane retry budgets are unchanged
   and remain the terminal floor. A dead target still fails loudly, just after a calmer
   wait. No new correctness contract — same dup-free / loss-free guarantees as
   ADR-0108/0109 (the gate only delays attempts; it never marks a table complete or
   advances a position).
3. **Telemetry stays optional + advisory.** nil provider ⇒ the gate is only ever
   tripped by source (1); nil gate ⇒ pre-ADR-0110 behaviour. Both degrade cleanly.
4. **Zero-value-safe.** The gate is an interface reached via a typed-nil guard (the
   `telemetryHintOrNil` pattern); there is no `EnableX`-defaulting-true config bool.
   The default for a non-PlanetScale / no-config run is "signal-driven gate active"
   because it is constructed unconditionally for the cold-copy run — but with no trip
   source firing, it is inert. (If we choose to make it CLI-gated, the flag is
   opt-*out* — `--no-coordinated-grow-pause` — never an opt-in bool that the zero value
   silently disables.)

## Consequences

- **Win:** faster + calmer ride-through of a storage-grow window — less target
  thrashing, fewer secondary 1205s, fewer burned retry attempts, no source-read EOF
  cascade when telemetry is wired. Measured against the same PS-320 storage-grow
  scenario the reactive arc used (the v6–v9 Track-D rig).
- **Cost:** one new engine-neutral interface + one concrete coordinator + threading
  through the RowWriter config and the source-read retry. A new `Await` call on the
  flush hot path (cheap when open).
- **Not changed:** the correctness contract, the resume format, the retry budgets
  (they remain the loud terminal floor), any untroubled-copy behaviour.

### Impl notes (deviations from the design sketch)

- **Window lifecycle = single owner, quiet-cycle teardown (not mid-window
  reopen/re-close).** The decision sketched a per-cycle "reopen to let lanes probe;
  if a lane immediately re-trips, close + extend." Implemented differently for
  race-safety: ONE owner goroutine per window holds the gate closed across
  exponential-backoff cycles and reopens exactly once, via a SINGLE teardown
  (`finishWindow`), when the FIRST of {a quiet cycle with no re-trip | recovered() |
  max-hold | ctx-cancel} fires. Lanes "probe" by the window ending (reopen → they
  resume); a still-bad target's first re-trip opens a fresh window. A mid-window
  reopen-then-re-close would need either a second owner or a probe-vs-Trip race on
  the open/closed flag; the single-owner shape sidesteps both (`Trip` coalesces onto
  the live owner via the `extend` channel while `g.extend != nil`, and only spawns a
  new owner once the window has fully torn down). Observably equivalent: coordinated
  quiesce, concurrent-trip coalescing into one window, exponential hold matching the
  ADR-0108/0109 retry envelope, bounded by max-hold, proactive early-release on
  recovery.
- **Construction is unconditional + zero-value-safe; no CLI flag added.** The gate is
  built once per cold-copy run (signal-driven on `migrate`; signal + telemetry-recovery
  on the sync cold-start, which has the `TargetTelemetry` seam) and is inert until a
  trip fires. No `--no-coordinated-grow-pause` flag was added (deferred until there's
  a reason to disable it); if one is ever added it must be opt-*out* per the design.
- **Telemetry trip layered on the existing WARN edge, not woven into it.** The
  storage-headroom tick (`evalStorageHeadroomTick`) keeps its exact WARN edge-trigger
  semantics; `evalStorageHeadroomTickWithGate` wraps it and trips the gate on the same
  false→true latch transition. A cold-copy-phase headroom watch (gated) runs alongside
  the unchanged apply-phase watch (gate=nil, WARN-only).

## Validation

- Unit: gate FSM (`internal/pipeline/grow_gate_test.go`) — coalescing concurrent
  trips into one window (owner-count seam), reopen broadcast wakes all N parked
  `Await`ers, prompt ctx-cancel unwind of N parked `Await`ers (the ADR-0099
  shutdown-hang lesson: park-then-cancel proves no hang / no leak), owner-exit +
  reopen on run-ctx cancel, max-hold bound on a forever-re-tripping target,
  telemetry-recovery early release, backoff shape, nil-gate pre-ADR no-op. Writer-seam
  pins (`internal/engines/mysql/row_writer_grow_gate_test.go`): Await before each
  flush attempt, Trip on classified transient, no Trip on terminal, nil-gate inert,
  ctx-cancel halts. Source-read seam + telemetry probe pins in the pipeline package.
  **NOTE: `-race` is CI-only on this box (CGO=0); the unit tests are deterministic but
  the race gate must run in CI before tag.**
- Integration (no Docker): `TestSourceReadRetryE2E_GrowGate_QuiescesAndConverges`
  drives the full migrate per-table copy with a real `growGate` wired in; a mid-chunk
  source drop trips the gate and each retry Awaits it, and the copy still converges
  byte-identically (zero dup / zero drop). The cross-lane "siblings issue no new
  flushes while closed" property is pinned mechanically by the FSM unit tests
  (reopen-broadcast + coalescing + ctx-cancel-unwind) rather than a multi-container
  fan-out, which would need testcontainers.
- Live (main session): re-run the Track-D PS-320 growing-volume scenario and confirm
  the copy rides the grow with fewer total retry attempts + no 1205 storm vs the
  v0.99.99 reactive baseline (the win is efficiency, not new correctness).
