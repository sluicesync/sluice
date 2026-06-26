# ADR-0121: Delayed-replica CDC apply (`--apply-delay`)

## Status

**Accepted (2026-06-26).** Roadmap item 46. A new steady-state CDC apply mode:
`--apply-delay DURATION` holds each change until its source commit timestamp plus
the configured delay has elapsed before applying it — the MySQL `MASTER_DELAY`
"oops window" disaster-recovery pattern. Off by default (0 = no delay = today's
behaviour, zero-value-safe). Engine-neutral, lives in the shared apply path; the
cold-start / bulk-copy phase is unaffected.

**Touches the position/resume contract.** A delayed change is an un-applied
change, and the load-bearing invariant is that the persisted resume position
NEVER advances past a change that has not actually been applied — so a crash with
N changes still held in the delay window re-reads them on resume (exactly-once is
preserved by the ADR-0010 idempotent-replay contract). This is a
concurrency-adjacent change (an extra goroutine on the apply pipeline + wall-clock
timing); the CI `-race` integration job must pass before any tag.

## Context

CDC apply is as-fast-as-it-can today. The "oops window" DR pattern wants the
target deliberately held N minutes/hours behind the source so an operator can
**stop sluice before an accidental `DROP TABLE` / bad migration / runaway
`DELETE` replicates**, then recover from the still-intact target. MySQL's
`CHANGE MASTER TO ... MASTER_DELAY=N` does exactly this: the IO thread reads the
binlog at full speed into the relay log, and only the SQL (apply) thread is held
back. sluice has no relay log, so the design question is *where* the un-applied
changes live while they wait, and how resume stays exactly-once across a crash.

Item 45 (v0.99.12x) added `ir.Change.SourceCommitTime()` — the source-side commit
instant, already populated by every CDC reader (PG pgoutput commit time, MySQL
binlog/VStream event time). That is precisely the timestamp this feature needs;
the delay gate is `SourceCommitTime() + delay <= now`.

The apply pipeline is a chain of single-goroutine pass-through interceptors over
an `<-chan ir.Change` (the live-add table filter → ADR-0054/0058 schema-snapshot
intercepts → the item-45 sync-lag observer), terminating at the
`ChangeApplier` / `BatchedChangeApplier`. The applier — and ONLY the applier —
advances the durable resume position, writing it in the SAME target transaction
as the row data (ADR-0007 position-and-data atomicity). Everything upstream of the
applier is, by construction, un-applied.

## Decision

### 1. Hold-at-apply gate (`delayChanges` interceptor)

A new pass-through interceptor, `delayChanges`, is wired into the apply intercept
chain (`phaseWireInterceptChain`) when `Streamer.ApplyDelay > 0`. For each change
it reads from its input channel:

- if `SourceCommitTime()` is non-zero and `commitTs + delay` is in the future, it
  **sleeps** (ctx-cancellable) until that release instant, then forwards the
  change downstream;
- if the timestamp is zero (a source/path that supplies none, or a non-row
  `SchemaSnapshot`) or the release instant is already past, it forwards
  immediately.

The interceptor holds **at most one** change at a time — the one it is currently
sleeping on. It does not accumulate a queue. Because it blocks on the sleep
*before* reading the next change, the upstream channels back-pressure all the way
to the CDC reader, which stops pulling from the source. This is the
"gate at the read/apply boundary" the roadmap prefers over an in-heap buffer.

### 2. Resume-safety invariant (the load-bearing argument)

The persisted resume position advances ONLY when the applier durably commits a
change (ADR-0007). `delayChanges` sits strictly UPSTREAM of the applier in the
channel pipeline, so a change it is holding has not been forwarded → the applier
has never seen it → the position has not advanced to or past it.

If the process crashes (or the context cancels) while N changes are held in the
delay window, none of those N were applied; the persisted position is still at the
last change the applier durably committed, which is strictly *before* the held
window. On restart, `StreamChanges(persistedPos)` re-reads from there, re-
delivering every held-but-unapplied change. **The delay buffer is therefore never
the sole home of an un-applied change — the source is.** A held change whose
position was already persisted would be silent loss on crash; that cannot happen
because the position is never persisted for an un-applied change.

On ctx cancellation while sleeping, the interceptor returns WITHOUT forwarding the
held change (it closes its output and exits) — so a graceful stop mid-delay-window
behaves identically to a crash: nothing applied, nothing past the position. The
boundary change that may have been mid-flight in the applier when the crash hit
re-applies idempotently (ADR-0010), so exactly-once holds for keyed tables (and
at-least-once for keyless, per the ADR-0089 baseline — unchanged).

#### 2a. Load-bearing prerequisite — the Postgres slot ack must follow APPLIED, not DECODED, LSN (a pre-existing latent bug this feature exposed)

The sluice-level invariant above (persisted position never advances past a held
change) is necessary but, on Postgres, NOT sufficient on its own: the replication
**slot** carries its own ack (`confirmed_flush_lsn`), set by the reader's keepalive,
and on reconnect PG fast-forwards `START_REPLICATION` to `max(requested_lsn,
confirmed_flush_lsn)`. If the slot ack advances past an un-applied change, that
change is silently dropped on resume regardless of where sluice's persisted
position sits.

ADR-0020 (Bug 15) introduced exactly the right mechanism — the applier feeds its
committed LSN to an `lsnTracker`, the reader's keepalive acks that (not the decoded
"streamed" LSN) — so the slot ack trails the applier. The streamer wires it via the
engine-neutral `lsnTrackerProvider.LSNTracker() any` → `lsnTrackerAttacher.AttachLSNTracker(any)`
interface pair. **But the Postgres reader's method was declared
`AttachLSNTracker(t *lsnTracker)` (concrete type), which does NOT satisfy
`AttachLSNTracker(any)`** — so the streamer's type-assertion failed silently and the
tracker was NEVER attached on the cold-start / warm-resume paths. The keepalive
therefore fell back to acking the **decoded** LSN. With lockstep apply
(unbuffered channel chain ⇒ streamed ≈ applied) the gap was ~0 and crash-resume
re-read the tiny in-flight window, so the bug stayed latent and untested at the
streamer level (the `confirmed_flush_invariant` test wires the tracker manually,
bypassing the broken streamer path).

`--apply-delay` is the first thing that runs the reader far ahead of the applier
(the whole delay window), so a keepalive reliably fires while the reader has decoded
changes the applier has not yet applied — turning the latent bug into **active
silent loss** on a crash mid-delay-window. Ground truth from the integration test:
applied/persisted = row 6, but `confirmed_flush_lsn` had advanced ~5 rows past it, so
warm resume lost rows 7–10.

**Fix:** the Postgres reader's `AttachLSNTracker` now takes `any` and type-asserts
internally (the documented contract — "the matching CDC reader type-asserts
internally"), so the streamer actually attaches the tracker and the slot ack trails
the applied position. Both attach sites call it BEFORE `StreamChanges` starts the
pump goroutine, so the field write happens-before the pump's reads — no data race
(but this is concurrency-adjacent and MUST clear the CI `-race` integration gate).
A compile-time assertion (`var _ interface{ AttachLSNTracker(any) } = (*CDCReader)(nil)`)
pins the contract so the signature can't silently drift back. This also finally
engages the ADR-0020 slot-ack-after-apply behaviour on the streamer path for every
Postgres sync (strictly safer; the slot retains slightly more WAL — up to the
in-flight/delay window — which is correct and intended).

### 3. Bounded memory

The interceptor's own footprint is one in-flight change. The reachable in-flight
set is bounded by the existing channel-hop buffers plus whatever the source CDC
reader buffers internally (governed by `--max-buffer-bytes` and the reader's
windowing) — NOT by `delay × write-rate`. The cost of the bound is throughput: a
delayed replica reads the source no faster than it applies (one delay-window
behind), which is the intended DR semantics. We deliberately do NOT read-ahead
into a large in-heap queue (the roadmap's explicit warning) — backpressure keeps
memory bounded and makes resume trivial.

**Tradeoff — replication idle timeout (named, documented).** Because the PG CDC
reader sends slot keepalives (`SendStandbyStatusUpdate`) and forwards changes on
the *same* goroutine, holding the downstream for longer than the source's
replication idle timeout (PG `wal_sender_timeout`, default 60s; MySQL
`net_write_timeout` / `slave_net_timeout`) can cause the source to reap the
replication connection while a change is held. sluice then surfaces this as a
retriable source error, reconnects from the (un-advanced) persisted position, and
replays — **correct via the resume invariant + idempotent replay, but churny**.
For delays approaching or exceeding those server timeouts (the common minutes-to-
hours "oops window"), operators should raise `wal_sender_timeout` (PG) /
`net_write_timeout`+`slave_net_timeout` (MySQL) on the source accordingly. This is
a known limitation of the backpressure design, accepted in favour of bounded
memory + trivial resume-safety; a read-ahead-with-disk-spill relay-log analog is a
possible future enhancement but out of scope here (more concurrency surface for a
throughput-only gain the DR use case does not need).

### 4. Transaction atomicity

The gate keys on each change's `SourceCommitTime()`, and every row event in a
source transaction carries that transaction's commit timestamp (PG pgoutput
stamps the commit time on all rows; MySQL binlog row events in a transaction share
the statement/commit header timestamp). So gating each change at `commitTs + delay`
releases the **whole transaction at one instant** — the interceptor forwards
`TxBegin → rows → TxCommit` in order, all eligible at the same release time, and
the `BatchedChangeApplier` groups them between the boundary events exactly as it
does undelayed. A transaction is never split across the delay. (MySQL binlog
commit time is second-granular, so the effective delay precision is ~1s on that
source — immaterial for a minutes-scale DR window; documented.)

### 5. Interaction with the item-45 sync-lag metric

The intentional delay must NOT be reported as "falling behind." `delayChanges` is
wired BEFORE the item-45 sync-lag observer (`observeSyncLagChanges`), so the
observer sees each change at `now ≈ commitTs + delay`. The `syncLagTracker` is
constructed with the configured `ApplyDelay` and subtracts it in `observe`
(`lag = now − commitTs − applyDelay`, floored at 0). A correctly-running delayed
replica therefore reads `sluice_sync_lag_seconds ≈ 0`, not `delay` seconds, and the
sync-lag threshold alert measures genuine apply backlog ON TOP of the configured
delay. With `ApplyDelay = 0` the subtraction is a no-op (byte-identical to the
pre-item-46 metric).

### 6. Clock assumption

The gate compares the source commit timestamp against the local wall clock. It
assumes the source and sluice clocks are roughly aligned; a large skew simply
shifts the *effective* delay (a sluice clock N seconds ahead of the source applies
N seconds later than configured, and vice-versa). For a DR "oops window" this is
immaterial — the operator wants "at least N minutes of reaction time," and modest
skew preserves that. Documented; not corrected for (no clock-sync dependency
introduced).

## Consequences

- A single new opt-in flag (`sync start --apply-delay`) + one `Streamer` field;
  zero-value-off, so every non-CLI construction (tests, broker, future callers)
  gets today's behaviour with no zero-value trap.
- One extra goroutine + channel hop on the apply pipeline ONLY when the delay is
  set; the default path is byte-identical.
- Resume stays exactly-once across a crash mid-delay-window by the position
  invariant above — pinned by a unit test (ctx-cancel mid-hold drops the held
  change) and an integration test (cancel before release → target unchanged →
  restart → target converges to the exact row count, no dupes).
- The replication-idle-timeout tradeoff (§3) is the one rough edge; surfaced in
  the flag help and operator docs.

## Alternatives considered

- **Read-ahead bounded in-heap buffer (relay-log analog).** Keep consuming the
  reader (keepalives flow) and buffer changes with release timestamps up to
  `--max-buffer-bytes`, backpressuring only at the cap. Keeps keepalives alive for
  the common case and would smooth the §3 tradeoff. Rejected for this chunk: it
  adds a timer + byte-accounting + a second goroutine (more concurrency surface to
  get right under `-race`) for a throughput optimization the DR use case does not
  need; the roadmap explicitly steers to the simple gate. Left as a documented
  future enhancement.
- **Delay at the position/applier layer (apply, then hold the position write).**
  Rejected outright: the position write is atomic with the data write (ADR-0007);
  "apply but don't advance" would mean applied-but-replayable data on the target,
  defeating the entire "target is N behind" guarantee.
- **Per-transaction explicit hold at `TxBegin`.** Functionally equivalent to the
  per-change gate (shared commit timestamp), but more code and a special-case for
  sources that omit boundary events. The uniform per-change gate subsumes it.
