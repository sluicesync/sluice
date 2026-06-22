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
