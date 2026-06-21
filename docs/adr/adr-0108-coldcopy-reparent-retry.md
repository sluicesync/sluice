# ADR-0108: Cold-copy reparent-retry

## Status

**Accepted (2026-06-21).** The copy-phase analog of ADR-0038's
apply-phase retry. Scoped to the MySQL cold-copy write path (the
demonstrated Track-D failure). Roadmap item 33.

## Context

A bulk cold-copy of a large table runs for minutes. During that window a
**transient target primary reparent** can occur — most concretely, a
PlanetScale **non-Metal** branch hitting a storage **auto-grow** at the
~39 GB boundary, which triggers a primary reparent of the underlying
Vitess shard. The in-flight `INSERT` connection dies and the bulk write
returns the raw driver error.

The CDC **apply** path already rides this out: ADR-0038's classifier
(`internal/engines/mysql/applier_errors.go` `classifyApplierError`) wraps
the transient shapes (Error 1105 `code = Unavailable` — how "tablet not
serving" surfaces — plus `driver.ErrBadConn` / `gomysql.ErrInvalidConn` /
`io.EOF` / `context.DeadlineExceeded` / "connection reset by peer" /
"broken pipe" / "i/o timeout") as `ir.RetriableError`, and the
pipeline's streamer retry loop (`internal/pipeline/streamer_retry.go`)
retries with bounded ADR-0038 backoff.

The **cold-copy** path did **not**. The MySQL `RowWriter` batch flush
(`row_writer_batch.go` for the idempotent UPSERT path,
`row_writer.go`/`writeBatchedConn` for the plain INSERT path, and their
`WriteRowsParallel` / `WriteRowsIdempotentParallel` fan-out callers)
returned the raw driver error unwrapped, with no retry. Confirmed live on
Track-D: on the reparent the cold-copy process **EXITED** and the
watchdog **crash-looped (9 relaunches)** — each relaunch immediately
re-hit the still-in-progress reparent. The supervisor could not make
progress; the copy itself had to ride it out.

## Decision

Add a **bounded, observable reparent-retry around the cold-copy batch
flush** in the MySQL RowWriter — the copy-phase analog of ADR-0038.

### Where the retry lives

Unlike ADR-0038 (which lives on the pipeline side because the pipeline
owns the apply batch loop), the cold-copy retry lives **in the engine**,
in one helper — `flushWithReparentRetry`
(`row_writer_reparent_retry.go`) — wrapped around the per-batch flush
that both the plain and idempotent flush closures call. Reasons:

- The pipeline's ADR-0038 loop is apply-phase only; the cold-copy bulk
  write is engine-internal (the orchestrator hands the writer a row
  channel and waits). There is no pipeline-side batch loop to wrap.
- The load-bearing recovery step — **re-acquire a FRESH connection** —
  is inherently engine-local (`w.db` is the engine's pool).

The helper deliberately does **not** import `internal/pipeline` (an
engine package must not). The backoff shape is re-derived in a small
self-contained function (`coldCopyReparentBackoff`), mirroring
`pipeline.computeRetryBackoff` minus the `RetryHint` plumbing (cold-copy
has no hint source).

### Classification reuse

A flush error is routed through the SAME `classifyApplierError` the CDC
apply path uses. The loop retries **only** when the classified error
satisfies `ir.RetriableError`; otherwise it returns the error unchanged
(terminal, exactly as before this ADR). No new retry class is introduced
for the copy path beyond what the apply path already trusts.

### The re-acquire-fresh-conn requirement (load-bearing)

The cold-copy connection is **pinned** for the whole table write (so the
post-flush session-scoped `SHOW WARNINGS` Vector-B probe reads on the
same session that ran the INSERT). **After a reparent the pinned conn is
DEAD.** A retry MUST re-acquire a FRESH connection from `w.db` — the pool
reconnects to the new primary on the next `db.Conn()` — and re-run BOTH
the flush exec AND the warning check on it. It must **never** reuse the
dead conn. `flushWithReparentRetry` enforces this structurally: the first
attempt runs on the caller's pinned conn; every retry calls
`w.db.Conn(ctx)` for a fresh one, runs the attempt, and closes it.

### Bounds

Package constants (vars, so the tests can shrink them — production never
mutates them, so there is no zero-value-default trap; they are baked at
package init, not config fields):

| Bound | Value |
|---|---|
| attempts | 12 |
| base backoff | 100ms |
| per-attempt cap | 30s |

Exponential doubling from base, capped: `100ms → 200ms → … → 25.6s → 30s
→ 30s → …`. 12 × max(30s) ≈ up to ~4 min tolerated before a **LOUD
terminal error** — long enough to ride a 30–60s (up to ~2–3 min)
reparent / storage-grow, short enough that a genuinely-wedged target
surfaces rather than hiding for hours. Stretched slightly past ADR-0038's
8×30s because a storage-auto-grow reparent can run longer than a
tx-killer blip.

Each retry logs a loud `WARN` (table, attempt, max, backoff, err). On
exhaustion the helper returns a loud terminal error naming the table +
row count + attempt count and wrapping the most recent transient (`%w`,
so the underlying `*MySQLError` stays reachable). The terminal error does
**not** implement `ir.RetriableError` — once the budget is spent, the
copy aborts. The backoff honors `ctx.Done()` for prompt cancellation.
**Never infinite, never silent.**

### Classifier extension (belt-and-suspenders)

`classifyApplierError`'s text fallback is extended to also match the
substrings **"not serving"** and **"reparent"** (case-insensitive,
pinned in `reparentRetriableSubstrings` with a change-detector test in
the same discipline as `vitessRetriableSubstrings`). The vttablet-framed
shape (Error 1105 `code = Unavailable`) is already caught, but a
PlanetScale/vtgate reparent can surface **without** that framing; this
fallback catches it. It also benefits the CDC apply path (ADR-0038) for
free.

### Plain-path 1062-on-retry tolerance (the wart)

A plain cold-copy batch is a SINGLE atomic multi-row `INSERT`. On a
classified transient the prior attempt either:

1. **fully rolled back** → the retry re-applies cleanly; or
2. **committed but the ack was lost** (the reparent dropped the
   connection between the server's commit and the client's
   acknowledgement) → the retry re-applies the BYTE-IDENTICAL batch and
   collides with the rows it already landed → **Error 1062** (duplicate
   key).

Because the batch is byte-identical and **cold-copy is the SOLE writer
onto a fresh target**, a 1062 **on the retry of the same batch** PROVES
those exact rows are already durable. So the plain path **tolerates** it
(treats the batch as done, continues — no silent loss; the data is
there). This is implemented as a named, commented wart in
`writeBatchedConn` ("tolerate-1062-on-retry"), with a loud WARN when it
fires.

**The tolerance is scoped to `isRetry` ONLY.** A FIRST-attempt 1062 stays
**terminal** — a real non-PK uniqueness violation or a dirty target must
fail loudly (unchanged ADR-0038 policy: 1062 is non-retriable). The
`flushWithReparentRetry` helper threads an `isRetry bool` into the
attempt closure precisely so the plain path can make this distinction;
the helper itself never tolerates anything (it only retries classified
transients).

The **idempotent path needs no such wart** — its `ON DUPLICATE KEY
UPDATE` absorbs the ambiguous-commit replay natively.

### Fan-out composition

The retry is local to a worker/batch. Under `WriteRowsParallel` /
`WriteRowsIdempotentParallel`, a transient on one table/worker now
retries **locally** (re-acquiring its own fresh conn) instead of aborting
its siblings. The existing errgroup behavior — first **terminal** error
cancels the shared child ctx and unwinds peers — is preserved: only an
**exhausted** or **non-retriable** flush returns terminal and aborts
siblings. The loud-on-genuine-error abort is unchanged.

## Consequences

- A storage-auto-grow / planned-reparent during a large MySQL cold-copy
  is ridden out in-process instead of crash-looping the supervisor.
- The recovery is bounded and observable (≤ ~4 min, WARN per retry, loud
  terminal on exhaustion).
- The plain-path 1062 wart is the one subtlety; it is provably safe
  (atomic single-statement batch + sole writer onto a fresh target) and
  scoped to retry-after-classified-transient only.

## Scope / non-goals

- **MySQL cold-copy only** (the demonstrated Track-D path). The PG COPY
  writer has the analogous gap (a reparent/failover mid-COPY would abort
  the copy); it is **NOT** addressed here — noted as a follow-up. PG's
  cold-copy uses the COPY protocol, a different recovery shape, and was
  not the live failure.
- No new CLI flag or config field — the bounds are baked constants (the
  envelope is sized for the managed-Vitess reparent window; the apply
  path's `--apply-retry-*` knobs remain apply-phase only). If a future
  operator needs to tune the copy envelope, promote the constants to
  config with zero-value-safe defaults at that time.

## Concurrency note

This touches the cold-copy write path including the parallel-worker
fan-out (`WriteRowsParallel` / `WriteRowsIdempotentParallel`) — a
`-race` integration gate is required before any tag (the per-worker
retry, fresh-conn re-acquire, and shared `workerCtx` cancel are the
concurrency-sensitive surface).

## See also

- ADR-0038 — applier retry on transient errors (the apply-phase analog;
  the classifier this reuses)
- ADR-0102 — native-MySQL per-table write fan-out (`WriteRowsParallel`,
  the plain-INSERT fan-out caller)
- ADR-0097 — write-side fan-out (`WriteRowsIdempotentParallel`)
- ADR-0007 — position persistence (the whole-table join is the durability
  guarantee; the retry is invisible to it)
- roadmap item 30 — the PS-10 non-Metal storage-resize resilience runs
  that surfaced this
