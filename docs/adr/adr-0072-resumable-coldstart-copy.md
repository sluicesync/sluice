# ADR-0072: Resilient, resumable VStream cold-start COPY

## Status

Accepted — Phases A, B, and C all implemented. Builds on [ADR-0071](adr-0071-vstream-snapshot-bounded-memory.md) (bounded-memory snapshot streaming), [ADR-0038](adr-0038-source-reader-retry-classification.md) (source/applier retry classification), and [ADR-0010](adr-0010-idempotent-cdc-applier.md) (idempotent apply). The **Gap 1** classifier change below shipped ahead of this ADR (it is a small, self-contained correctness fix); **Gap 2** is the design this ADR exists to scope.

### Implementation notes (what landed)

- **Phase A** — `shardGtid` gained an additive `TablePKs []encodedTablePK` field (`table_p_ks`, JSON-omitempty). Each entry round-trips one `binlogdata.TableLastPK` via deterministic proto marshalling (`MarshalVT`) base64-encoded into the JSON token, so vtgate's `WHERE pk > lastpk` resume clause gets byte-identical bytes. `vgtidToShardGtidSlice` captures the cursor from the VGTID events vtgate emits after each LASTPK; `toProtoShardGtids` / `buildVStreamRequest` replay it. Old tokens (no `table_p_ks`) decode to a nil cursor — "COPY from the beginning" — pinned by `TestVStreamPos_OldTokenDecodesWithoutTablePKs`. `sameVgtid` now compares `(keyspace, shard, gtid)` field-by-field (the slice field made `shardGtid` non-comparable; TablePKs are a transient cursor irrelevant to anchor identity).
- **Phase B** — the COPY pump checkpoints `currentVgtid` (with its TablePKs) to the control table on a bounded N-rows-or-T-seconds cadence (defaults 50k rows / 10s), via a new optional `ir.CopyCheckpointer` surface the pipeline wires onto the snapshot reader (`applyCopyCheckpoint` → `applier.WritePosition`). The DB write happens OUTSIDE the dispatch lock (the pump snapshots the position under `mu`, then writes unlocked). A checkpoint-write failure is non-fatal (log-and-continue; COPY_COMPLETED still persists the final position).
- **Phase C** — a retriable mid-COPY `Recv` error reconnects the VStream IN PLACE from `currentVgtid` (bounded budget + exponential backoff), reusing the gRPC conn and keeping the bulk-copy goroutines warm, instead of unwinding to `runWithRetry`. The outer loop stays the backstop for budget exhaustion / non-retriable shapes.

**Warm-resume-from-a-mid-COPY-checkpoint is correct (the subtlety that makes Phase B safe).** A Phase-B checkpoint writes a *mid-COPY* position to the control table — so a full process crash makes `runOnce` take the warm-resume branch, which opens the **standalone** `vstreamCDCReader` from that position. Because Phase A now replays `TablePKs` into `buildVStreamRequest`, vtgate **resumes the COPY** for the still-copying tables: the remaining COPY rows arrive as `ir.Insert` events and flow through the idempotent applier (the Bug-125 interlock absorbs any at-cursor overlap), then the stream transitions to CDC. This converges with zero loss even on the crash path — it does NOT skip the unfinished COPY. Phase C keeps the common transient-blip case on the fast bulk-copy path; the warm-resume path is the crash-recovery fallback.

## Context

A field report: during a large-table PlanetScale cold-start (`sync start`), the VStream connection dropped mid-COPY with `Unavailable: connector reset by peer`. Instead of recovering, the sync **failed terminally**, and because no resumable position had been persisted, the only recovery was to **restart the whole cold-start from row 0**. The operator's expectation — reasonably — was that sluice already tolerates transient network faults on long-running large-table copies. It does, but only on two paths that don't cover this one.

### What resilience already exists

- **Steady-state CDC tailing** emits an `ir.Position` on every change; the applier persists it as it applies. A reset mid-tail is classified retriable (ADR-0038) and `runWithRetry` reconnects from the last persisted GTID. Efficient resume.
- **PG→PG copy-pool** has the AIMD slot-exhaustion backoff (the connection-resilience Phase 1/2/2b arc).

Neither covers the **VStream cold-start COPY phase** — which is exactly where a large MySQL/Vitess migration spends its wall-clock time.

### Three gaps, one dependency chain

Tracing the failure end-to-end (`cdc_vstream_snapshot.go` pump → `reader_errors.go` classifier → `streamer.go` `runWithRetry`/`runOnce`) surfaced three distinct defects that compound:

**Gap 1 — the transient is not classified retriable (FIXED, shipped ahead of this ADR).**
The VStream stream's `Recv()` returns a **native gRPC `status` error** (`code = Unavailable`), not a MySQL `1105` wrapper. `classifyApplierError` only recognized Vitess transients as `1105 (HY000)` payloads or a handful of raw-text substrings (`connection reset by peer`, …). The gRPC `desc` wording varies across the transport stack (`transport is closing`, `connector reset by peer`, `error reading from server: EOF`, `the connection is draining`), so text-matching is fragile and let a real transient fall through as **terminal**. `classifyReaderError` now honors the gRPC status code directly (`Unavailable` / `Aborted` / `Unknown` / `ResourceExhausted` → retriable; all others terminal), via `status.FromError` so a `%w`-wrapped `recv:` error still resolves. Both snapshot pumps (`cdc_vstream_snapshot.go:350` COPY, `:681` CDC) and the CDC-tailing pump (`cdc_vstream.go:578`) route through `classifyReaderError`, so the fix reaches the cold-start path. Pinned by `TestClassifyReaderError_GRPCStatusCodes` (the full code class, wrapped exactly as the pump wraps it).

**Gap 1.5 — the retry re-copies into a non-empty target and dies on a duplicate key (idempotency prerequisite).**
`runWithRetry` (`streamer.go:853`) retries `runOnce`, which re-opens the snapshot stream and re-runs the cold-start COPY. But `streamer.go:884` clears `ResetTargetData` after the first attempt ("the reset happens at most once per Run"), so the retry re-copies into a target that **still holds the partial rows** from the failed attempt. With the current non-idempotent COPY writer, the first re-copied batch collides on the already-present rows → **MySQL `1062` duplicate-key**, which ADR-0038 classifies **non-retriable** → terminal. So Gap 1's retry, on its own, would trade one terminal failure for another. The **Bug 125 idempotent COPY writer** (`INSERT … ON DUPLICATE KEY UPDATE` on the real unique key, [ADR-0010] semantics extended to the COPY path) is the prerequisite that makes a re-copy into a partial target **safe** — the re-copy upserts the already-present rows harmlessly and converges. This ADR does not re-specify that writer; it records the dependency: **Gap 1 + the Bug 125 idempotency fix must ship together**, or cold-start retry is inert.

**Gap 2 — no resumable COPY checkpoint, so every retry re-copies from row 0 (this ADR's core).**
Even with Gaps 1 and 1.5 closed, the retry re-copies the **entire** table from the beginning. The cold-start persists a resumable position **only at `COPY_COMPLETED`** (`finishCopy`, `cdc_vstream_snapshot.go:378`), and sluice's position primitive drops the cursor Vitess needs to resume a partial COPY:

```go
type shardGtid struct { Keyspace, Shard, Gtid string }  // no TablePKs
```

Vitess's `VGtid.ShardGtids[i]` carries a **`TablePKs`** field — the per-table last-copied primary key — which is precisely the COPY-resume cursor. `vgtidToShardGtidSlice` discards it. So sluice cannot ask vtgate to resume a COPY from where it left off; it can only restart it. For a 19M-row table over a flaky link, re-copying from row 0 may itself fault before completing → retry → re-copy → **never converges**. That is the operator's "restart completely from the beginning" loop.

## Decision

Make the cold-start COPY **resumable from the last-copied PK**, so a transient fault costs the in-flight chunk, not the whole table.

### Phase A — carry the COPY cursor in the position

Extend the VStream position primitive to carry Vitess's per-shard `TablePKs` alongside the GTID:

- Add a `TablePKs` field to `shardGtid` (encoded form), populated from `VGtid.ShardGtids[i].TablePKs` in `vgtidToShardGtidSlice`, and round-tripped through `encodeVStreamPos` / `decodeVStreamPos`. The encoded JSON stays Debezium-adjacent; `TablePKs` is an additive field, so older tokens decode (absent → "no mid-COPY cursor", i.e. start COPY from the beginning — the current behavior).
- The resume request (`fromBeginningVStreamPos` and the snapshot stream open) passes the decoded `TablePKs` back to vtgate so the COPY resumes from the cursor rather than restarting.

### Phase B — checkpoint the COPY cursor periodically

During the COPY pump, persist the current `currentVgtid` (now including `TablePKs`) to the control table on a **bounded cadence** — every N rows or T seconds, whichever first — not only at `COPY_COMPLETED`. The cadence is a tunable with a safe default; the write is idempotent (overwrites the single in-progress position row). This is the durable checkpoint a post-fault resume reads.

### Phase C — reconnect-and-resume in the snapshot pump

On a **retriable** `Recv` error during COPY (now correctly classified by Gap 1), the pump reconnects the VStream **in place** — re-opening from the last checkpointed `TablePKs` — instead of `failCopy`-ing and unwinding to `runWithRetry`. In-place reconnect avoids re-running schema-apply / pre-flight and keeps the bulk-copy goroutines warm. `runWithRetry` remains the outer backstop for faults the in-place reconnect cannot absorb (budget exhaustion, non-retriable shapes).

Phases A+B are the minimum that makes a cold-start *converge* under sustained flakiness (the outer `runWithRetry` loop already exists and, with A+B, resumes from the checkpoint instead of row 0). Phase C is the efficiency win (no full pipeline teardown per blip) and can land after A+B prove out.

### Load-bearing correctness note — the warm-resume idempotency depends on the **CDC applier**, not only the COPY writer

The warm-resume / process-restart path routes the *remaining* COPY rows through the **CDC applier** (`ChangeApplier.Apply` → `buildInsertSQL`), **not** the Bug-125 idempotent COPY *writer*. The checkpoint cadence (Phase B) lags the writer's flushes, so on resume vtgate re-sends rows the target already holds (`id > lastpk` that were flushed past the last checkpoint). For a **no-PK table** (the Bug-125 shape: no PK + a UNIQUE key, e.g. a `connections` table), the MySQL CDC applier originally emitted a **plain INSERT** when the PK list was empty — so those re-sends hit **MySQL 1062** and the resume failed terminally. The Bug-125 fix made the COPY *writer* idempotent; it did **not** make the CDC *applier* idempotent for no-PK tables.

The fix (this ADR's no-PK applier-idempotency requirement): `buildInsertSQL` now emits `ON DUPLICATE KEY UPDATE` even when the PK list is empty, with a **full-row SET-list** (`col = new.col` for every column). MySQL fires `ON DUPLICATE KEY UPDATE` on a conflict against **any** unique index, so a no-PK table *with* a unique key now upserts idempotently on resume; a *truly keyless* table (no PK and no unique index) never collides, so the clause is inert and behavior stays effective-plain-INSERT — and such tables are refused at cold-start by the Bug-125 keyless guard, so they never reach resume. This is the load-bearing dependency for no-PK warm-resume: without the applier-side no-PK upsert, Phases A+B+C still 1062 the resume on the exact Bug-125 table class.

**Postgres cannot reach this no-PK resume path.** The cross-engine cold-start guard (`copyTableColdStartIdempotent` in `internal/pipeline/migrate.go`) refuses a no-PK VStream→target copy unless the target writer advertises `ir.IdempotentCopyWriter.HandlesNoPKIdempotentCopy() == true`. Only the MySQL `RowWriter` does; the Postgres writer does not implement the surface, so a no-PK VStream→PG cold-start is **refused loudly** before the COPY pump runs — it never checkpoints and therefore never warm-resumes. PG's `buildBatchUpsert` plain-INSERTs no-PK rows and is intentionally left unchanged; the guard, not an applier change, is what keeps PG safe here.

## Consequences

- **Large-table cold-start survives transient network faults** without re-copying the whole table — the operator's expectation.
- **Position format gains a field.** Additive and backward-compatible; a pin must assert old tokens (no `TablePKs`) still decode to a clean "COPY from beginning".
- **Interlock with Bug 125 — two distinct idempotency surfaces.** Resume-into-a-partial-target relies on idempotency on *both* paths: the idempotent COPY **writer** (for the in-attempt re-copy, Gap 1.5) **and** the CDC **applier**'s no-PK upsert (for the warm-resume catch-up rows — see the load-bearing correctness note above). The Bug-125 fix delivered only the former; the latter is this ADR's no-PK `buildInsertSQL` change. Without *both*, the resume 1062s on the no-PK Bug-125 table class. All three (COPY-writer idempotency, applier no-PK upsert, resumable cursor) should be validated together.
- **Concurrency surface.** The checkpoint write happens from the pump goroutine while `ReadRows` drains; Phase C adds in-place stream re-open. Per the project rule, the integration **`-race`** gate must pass **before** the tag is cut.
- **New pins.** (1) position round-trips `TablePKs`; old tokens decode safely. (2) a mid-COPY fault-injection integration test: kill the stream after K rows, assert the resume reads the checkpoint and the final target row count equals the source (no loss, no full re-copy) — ground-truthed on a real Vitess target. (3) the Gap 1.5 interlock: a retry into a partial target with the idempotent writer lands zero `1062`.

## Alternatives considered

- **Auto-retry-from-scratch only (Gap 1 + idempotency, skip Gap 2).** Correct and safe, but does not converge for large tables under sustained flakiness — the exact reported failure. Acceptable as the interim state once Gap 1 + Bug 125 land; insufficient as the end state.
- **Persist the cursor every row.** Simplest checkpoint, but a control-table write per COPY row is a throughput regression on the hot path. The bounded N-rows/T-seconds cadence bounds both data-loss-on-fault (≤ one interval) and write amplification.
- **Disk-spill the in-flight buffer for resume (reuse ADR-0071 Phase 3).** Solves a different problem (multi-table interleave memory), not network-fault resume. Orthogonal.
