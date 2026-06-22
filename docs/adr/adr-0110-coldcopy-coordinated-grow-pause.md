# ADR-0110: Coordinated cold-copy grow-window pause

## Status

**Proposed (2026-06-21).** The *proactive* deepening of the v0.99.92–v0.99.99
reactive storage-grow arc (ADR-0108 reparent-retry, ADR-0109 source-read-resume,
and the classifier widenings). Roadmap item 37. `-race`-before-tag concurrency chunk.

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

## Validation

- Unit: gate FSM under `-race` — coalescing concurrent trips, reopen broadcast,
  prompt ctx-cancel unwind of N parked `Await`ers, max-hold bound, telemetry-recovery
  release. A deterministic "all lanes parked then ctx-cancel" test (the ADR-0099
  shutdown-hang lesson) proving no goroutine leak / no hang.
- Integration: a fan-out cold-copy against a fake writer that injects a classified
  grow-transient on lane 0 and asserts sibling lanes quiesce (don't issue new flushes)
  until reopen, then converge byte-identically to the serial copy.
- Live: re-run the Track-D PS-320 growing-volume scenario and confirm the copy rides
  the grow with fewer total retry attempts + no 1205 storm vs the v0.99.99 reactive
  baseline (the win is efficiency, not new correctness).
