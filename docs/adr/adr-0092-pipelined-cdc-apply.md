# ADR-0092: Pipelined CDC apply (collapse N per-batch round trips into one)

## Status

Accepted. Extends — does not supersede — [ADR-0017](adr-0017-batched-cdc-apply.md)
(batched apply), [ADR-0052](adr-0052-aimd-apply-batch-size-controller.md) (AIMD
batch-size controller), [ADR-0081](adr-0081-applier-control-plane-extraction.md) (the shared
`appliershared.RunBatchLoop` seam), and [ADR-0089](adr-0089-default-adaptive-apply-batch-size.md)
(adaptive batch size by default). Those decided *how many* changes share a transaction;
this one decides *how those changes are put on the wire*. The keyless guard, AIMD
sizing, byte cap, tx-boundary alignment, and the ADR-0007 position-then-commit ordering
are all preserved verbatim.

## Context

The shared batch loop ([ADR-0081]) applies a batch of N changes as **N serial
`tx.ExecContext` round trips inside one transaction**, then one position write, then one
commit:

```
BEGIN
  Exec(change₁)   ← round trip 1
  Exec(change₂)   ← round trip 2
  …
  Exec(changeₙ)   ← round trip N
  Exec(position upsert)   ← round trip N+1
COMMIT            ← round trip N+2 (+ fsync)
```

Batching ([ADR-0017]/[ADR-0089]) amortizes the **commit fsync** over N changes — which is
why `=auto` beat `=1` by >10× on the Bug-141 soak (240 → ~6,500 rows/s). But it does
**nothing** for the N data round trips: they remain serial, so steady-state apply
throughput is bounded by

```
throughput ≈ 1 / per_row_exec_latency
```

and `per_row_exec_latency` is dominated by the **network round-trip time (RTT)** between
the sluice process and the target. On a low-latency link (sub-millisecond execs, e.g. a
co-located target) the serial cost is small and batching alone reaches thousands of
rows/s. On a non-trivial-latency link the serial RTT dominates and no amount of batching,
and no target-resource increase, lifts the ceiling.

### Measured on the live PlanetScale soak (2026-06-15)

A high-rate load test on the v0.99.48 soak (loadgen ~750–800 rows/s) found the target
apply pinned at **~75–106 rows/s** regardless of the source rate, with these ground-truth
measurements:

- **Warm-connection RTT, sluice host → target PG: ~7 ms** (`us-east-2.pg.psdb.cloud`,
  AWS Ohio; the soak runs sluice on a Vultr VM — a cross-provider hop). Measured with
  `psql … \timing on` over six `SELECT 1`s on one session.
- **Per-row apply cost: ~11 ms** (~7 ms RTT + ~4 ms server-side upsert).
- **AIMD signature: p95 latency scales linearly with batch size** (~11–12 ms × batch at
  batch 161/221/271/311) — the unmistakable tell of a *fixed per-row cost*, not a
  target-resource limit.
- **Upsizing both databases PS-10 → PS-80 changed nothing** — apply stayed ~100 rows/s.
  That ruled out "target is resource-starved" and isolated the cause to the apply path's
  round-trip structure.

So `1 / 0.011 ≈ 90 rows/s`, exactly the observed ceiling. This reconciles with
[ADR-0089]'s ~6,500 rows/s figure: that link had sub-ms execs (≈0.15 ms/row → ~6,500/s
at batch 1000); a 7 ms-RTT link is ~70× slower for the identical sluice code path. The
ceiling is the wire, not the database and not sluice's apply *logic*.

A secondary observation from the same run: the ADR-0019(a) "alive but NO change events
for 5 s — the source may be throttled" WARN **mis-fired ~46 times during the flood**, not
because the *source* throttled but because sluice's own apply backpressure starved the
read loop for >5 s. Tracked separately (see *Follow-ups*); not in scope here.

## Decision

Add a **pipelined apply path** for engines whose driver supports request pipelining, and
adopt it for **Postgres** via `pgx`'s batch protocol. Instead of N serial round trips, the
batch's data statements **and** the position upsert are queued onto a single `pgx.Batch`
and sent in **one** network flush; the server executes them in order and the results are
read back together, then the transaction commits:

```
BEGIN
  SendBatch[ Exec(change₁), …, Exec(changeₙ), Exec(position upsert) ]   ← ONE flush
  read N+1 results
COMMIT
```

Round trips per batch drop from **N+2** to **~3** (begin, the single batched flush,
commit), independent of N. Throughput becomes bounded by the *server's* execution rate
rather than `N × RTT`. The win scales with RTT: ~70× on the soak's 7 ms link, smaller but
still real (collapses the serial-exec span to one flush) on a local link.

### Mechanism — generalize the [ADR-0081] seam's transaction handle

The shared loop touches the transaction concretely in only four ways: `BeginTx` creates
it, `Dispatch` writes to it, `commitBatch` runs `WritePosition`+`Commit` on it, and the
error paths call `tx.Rollback()`. The loop needs nothing from the handle but
`Rollback() error`. So the seam's `*sql.Tx` is generalized to a minimal interface:

```go
// appliershared
type BatchTx interface{ Rollback() error }
```

`*sql.Tx` already satisfies it (MySQL and the PG non-pipelined fall-back keep using
`*sql.Tx` unchanged — type-assert inside their closures). The PG **pipelined** path
returns a `*pgxBatchTx` handle that:

- escapes `database/sql` to the native driver exactly as the raw-COPY path already does
  (`*sql.Conn` → `Raw` → `*stdlib.Conn.Conn()` → `*pgx.Conn`; see `raw_copy.go`), pinning
  one backend for the batch;
- begins a native `pgx.Tx` and issues `SET LOCAL synchronous_commit = on` (the [ADR-0007]
  F7 durability pin, preserved);
- on each `Dispatch`, **queues** the built `(sql, args)` onto an in-handle `pgx.Batch`
  instead of executing it — the existing `buildInsertSQL` / `buildUpdateSQL` /
  `buildDeleteSQL` builders and `prepareValue` codec path are reused **byte-for-byte**, so
  per-statement parameter binding and per-OID codec fidelity are identical to today
  (this is the load-bearing correctness invariant — pipelining changes *when* statements
  are sent, never *how a value is encoded*);
- on `Commit`, appends the position upsert to the same `pgx.Batch`, `SendBatch`es it, reads
  every result (surfacing the first per-statement error with its change's context), then
  commits the `pgx.Tx`.

`Dispatch` becomes a type-switch on the `BatchTx`: a `*pgxBatchTx` → queue; a `*sql.Tx`
(the per-change `Apply` path and any non-pipelined fall-back) → execute as before. Every
other invariant — AIMD consult/observe timing (item 18), the keyless-table flush boundary
([ADR-0089]), byte-cap flush ([ADR-0028]), source-tx-boundary alignment ([ADR-0027]),
schema-event handling ([ADR-0049]), and the position-then-commit ordering — is untouched,
because they all operate above the exec/queue seam.

### Error semantics

Today a `Dispatch` failure rolls back immediately and the loop reports it per change. Under
pipelining a *build* error (SQL/codec) still surfaces synchronously at queue time; an
*execution* error surfaces when `SendBatch`'s results are read at commit. Both still roll
the whole batch back atomically (the batch was never committed), and both still flow
through `classifyApplierError`, so the retriable/fatal classification ([ADR-0038]) and the
`runWithRetry` re-open loop are unchanged. The only observable difference is *when* an
exec error is detected (commit time vs mid-accumulation) — the batch outcome (full
rollback + reclassify + retry) is identical.

### Scope

- **Postgres only** in this ADR (the soak's measured target; pgx has first-class batch
  pipelining). The seam generalization is engine-neutral so MySQL can adopt a pipelined
  path later (the `mysql` driver's pipelining story is different and out of scope here).
- The pipelined path is the default for the PG **batch** apply path (`--apply-batch-size`
  > 1, i.e. the `=auto` default). The per-change `Apply` path (`--apply-batch-size=1`)
  keeps the `*sql.Tx` exec path verbatim — a batch of one has nothing to pipeline.
- No new flag. If the raw-conn escape ever fails (a non-pgx driver, a wrapped conn), the
  path falls back to serial `*sql.Tx` exec with a one-time WARN — loud, never silent, no
  throughput claim made.

## Consequences

- **Throughput on latency-bound links rises ~RTT-fold** (≈70× on the 7 ms soak link;
  e.g. ~90 → thousands of rows/s, until the server's execution rate or the source rate
  becomes the new bound). On a local link the gain is the collapse of the serial-exec span
  into one flush — smaller, still positive, never a regression.
- **The AIMD controller's p95 stops tracking `batch × RTT`** and starts tracking real
  server execution latency, so it can grow the batch into the headroom pipelining opens
  instead of being held down by accumulated RTT.
- **One backend pinned per batch** (via `*sql.Conn`) for the batch's lifetime — same
  resource shape as the existing raw-COPY path; released at commit/rollback.
- **A second code path** through the apply hot path (queue vs exec). Mitigated by reusing
  the identical builders/codecs, keeping the fall-back, and pinning both paths against the
  same value-fidelity and atomicity oracles (see *Testing*).
- **Durability and atomicity are unchanged**: position upsert rides the same tx as the data
  (now in the same batch flush), `synchronous_commit = on` is still pinned, and a crash
  before the single commit rolls back both — the [ADR-0007] contract holds.

## Testing

- **Value fidelity (Bug-74 corollary):** the pipelined path must be pinned across the full
  type-family × shape matrix, src==dst ground-truthed, because pipelining is exactly the
  class of change (a codec/exec-path change) where a per-representative pin is insufficient.
  Re-run the value-fidelity matrix through the batch path.
- **Atomicity / crash-replay:** a mid-batch failure rolls back data *and* position; replay
  from the prior boundary reproduces the batch idempotently (PK/unique) and the keyless
  guard still clamps class-3 to batch-1 blast radius.
- **Equivalence:** for a given change stream, the pipelined path and the serial `*sql.Tx`
  path must produce byte-identical target state (a differential pin).
- **`-race` integration before tag** — this touches the apply hot path and the
  conn-escape/tx lifecycle (concurrency-adjacent), so the `-race` integration gate runs
  before the tag is cut (per the project's `-race`-before-tag rule).

## Alternatives considered

- **Multi-statement via the simple query protocol** (semicolon-joined statements, one
  `Exec`): one round trip, but the simple protocol forces literal-inlined values, which
  discards `prepareValue` + the per-OID binary codecs and reintroduces escaping / type-
  fidelity risk. Unacceptable for a value-fidelity-critical tool. **Rejected.**
- **Multi-row `INSERT … VALUES (…),(…)` collapse:** helps insert-heavy runs but not mixed
  I/U/D streams, complicates `ON CONFLICT` arbitration, and still needs a separate path for
  UPDATE/DELETE. `pgx.Batch` pipelines *all* statement shapes uniformly. **Rejected** as the
  primary mechanism (could be a later additive optimization).
- **Switch the PG applier wholesale from `database/sql` to `pgxpool`:** cleaner native
  access, but a large blast radius across position writes, the control table, and every
  helper — disproportionate to the win. The `Raw`-escape (already proven by the COPY path)
  gets the same pipelining with a contained change. **Rejected** for now.
- **Parallelize execs across K connections within a batch:** breaks the [ADR-0007]
  "data + position in one tx" atomicity (a batch can't span connections). **Rejected.**
- **Do nothing / document the ceiling:** leaves a ~70× throughput ceiling on every
  non-co-located target — the common cloud topology (sluice and target in different
  regions/providers). The reliable, performant path should not require co-location.
  **Rejected.**
