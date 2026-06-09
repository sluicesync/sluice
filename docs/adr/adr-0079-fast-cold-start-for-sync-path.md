# ADR-0079: Fast cold-start for the `sync` path (PG-source, capability-gated)

## Status

Accepted — design. Roadmap item 3d. Implementation notes filled in as the chunk lands.

### Implementation notes (what landed)

_(to be completed when the chunk merges)_

## Context

The three cold-start copy speedups — the cross-table worker pool (ADR-0076), index-build overlap (ADR-0077), and the PG→PG raw-copy identity passthrough (ADR-0078) — are **`migrate`-only**. `sluice sync start`'s initial cold-start calls the serial `runBulkCopyWithOpts` (`internal/pipeline/migrate.go:1116`), so the **copy-then-continuously-follow** workflow — the one-command equivalent of pgcopydb's `--follow` — does its initial copy on the slow path. A user who wants *both* a fast initial copy *and* continuous CDC currently cannot get the fast copy: `migrate` is fast but one-shot; `sync start` follows but copies serially. pgcopydb gives fast-copy + follow together; sluice should too.

The sync cold-start is serial "by design" (`migrate.go:1159-1166`) for three coupled reasons, each verified in code:

1. **Durable-write watermark** (v0.99.9, `migrate.go:1141-1158`): on the resumable VStream cold-start the reader's `CopyDurableProgressSink` is coupled to the writer's `CopyDurableProgressReporter` so the COPY checkpoint never advances past durably-committed rows. The VStream pump advances its `TablePKs` cursor as rows are *received* into the in-flight buffer, ahead of the consumer — a hard crash resuming past un-written rows is the silent-loss class this prevents.
2. **Idempotent COPY** (Bug 125, `migrate.go:1137-1140`): the VStream/PlanetScale snapshot reader re-emits COPY-phase rows out of PK order; `ir.IdempotentCopyReader.CopyNeedsIdempotentWriter()` routes to the upsert path.
3. **Single ordered snapshot stream**: sync cold-start consumes ONE `ir.RowReader` (`stream.Rows`) emitting all tables in one sequence (for VStream, literally one gRPC stream tied to the CDC start position). `migrate` instead opens INDEPENDENT per-table / per-PK-chunk readers — which is exactly why migrate parallelizes and sync doesn't.

The crucial asymmetry is the **source's snapshot model**. Postgres exports a snapshot (`OpenSnapshotStream` → `consistent_point` + `snapshotName`) that can be SHARED across many connections via `SET TRANSACTION SNAPSHOT '<name>'` — pgcopydb's exact parallel-worker mechanism. MySQL's `WITH CONSISTENT SNAPSHOT` is per-session and not shareable; the Vitess VStream snapshot is a single ordered stream with the durable-watermark + idempotent coupling above.

Two facts discovered during design framing change the calculus:

- **`ir.SnapshotImporter` / `SnapshotImporterOpener` already exist** (`internal/ir/interfaces.go`) with a working PG implementation (`internal/engines/postgres/snapshot_importer.go`) that does `BEGIN REPEATABLE READ; SET TRANSACTION SNAPSHOT` — but have **zero production callers**. Even `migrate` doesn't use them (its parallel chunks open independent `OpenRowReader`s, accepting the documented ADR-0019 v1 inconsistency window). This chunk finally wires the latent surface in.
- **`migrate` persists no CDC position and creates no replication slot** — it opens `OpenRowReader`, never `OpenSnapshotStream`. So a `migrate`→`sync` position handoff (shape B) would require net-new slot-creation + position-persistence + keeping the slot alive across the inter-command gap + a new gap-free boundary proof. That is a bigger lift than the roadmap assumed, for a worse two-command UX.

## Decision

Implement **shape (A): bring the fast machinery to the sync cold-start, PG-source first, behind a source-capability gate.** Fast where provably safe; the existing serial path stays the loud, correct fallback everywhere else.

**The capability gate.** A source qualifies for the parallel/overlap/raw cold-start iff its `ir.SnapshotStream` carries a non-empty shareable exported-snapshot name (new additive field `SnapshotName`) AND the source engine implements `ir.SnapshotImporterOpener`. Postgres qualifies; MySQL and VStream/PlanetScale do not, so they fall through to today's `runBulkCopyWithOpts` with an INFO log naming the reason. The gate is interface-/field-presence-driven, never an engine-name check (the IR-first tenet).

**The key risk reduction:** the durable-watermark + idempotent-COPY coupling (constraints 1+2) lives ONLY on the VStream path, which stays serial. The raw byte-pipe (PG-only) and the watermark (VStream-only) therefore **never coexist** — this chunk never has to make them compatible.

**Scope of the fast path (when the gate holds — PG→PG, fresh cold-start, no transform):**
- Parallel cross-table pool + within-table chunks, readers minted via `ir.SnapshotImporter` so all N are pinned to the ONE exported snapshot (gap-free, the slot's `consistent_point` is the handoff boundary).
- Index-build overlap (PG writer implements `ir.IncrementalIndexBuilder`).
- Raw-copy passthrough, engaging only on a fresh / `--restart-from-scratch` cold-start (degrading to IR on the resumable path, mirroring the migrate `useFastLoader` gate), behind the SAME value-fidelity gate as migrate.

**Shared value-fidelity gate.** `rawCopyGate` is refactored off `*Migrator` onto a small transform-config struct that both `Migrator` and `Streamer` populate, so the no-transform guarantee (no redaction / type-override / expr-override / shard-injection) is byte-identical on both paths, with the same negative-fallback pins.

**Connection budget.** The sync cold-start resolves a real copy/index budget (mirroring migrate) instead of the current `requested=1`, with ONE extra reservation migrate doesn't need: 1 slot held for the CDC connection that goes live immediately after bulk-copy, so the pool can't starve it. The product `tableP × withinP + indexBudget + 1(CDC) ≤ CopyBudget` holds at the single chokepoint (ADR-0076 discipline).

**Resume stays serial.** The resumable cold-start path (ADR-0072, the VStream-cursor + durable-watermark path) is excluded from the gate and keeps `runBulkCopyWithOpts` unchanged.

## Consequences

- A PG→PG `sluice sync start` cold-start now copies at `migrate` speed and then follows — the one-command pgcopydb-`--follow`-at-full-speed equivalent. Every other source (MySQL, VStream/PlanetScale) is byte-identical to before: serial cold-start, loudly logged.
- The latent `ir.SnapshotImporter` surface gets its first production caller; PG cold-start parallel reads are now snapshot-CONSISTENT (all readers pinned to one exported snapshot), strictly better than migrate's current per-connection-snapshot v1 window.
- One auditable value-fidelity gate now governs both migrate and sync; the raw lane can never silently skip a transform on either path.
- `-race` concurrency chunk (new pool on the critical sync path + snapshot-importer readers across goroutines + the CDC-slot reservation). The `-race` Integration gate must pass before any release tag (CLAUDE.md release discipline).

## Alternatives considered

- **Shape (B): `migrate`→`sync` position handoff.** Rejected for v1 — migrate persists no position and holds no slot today, so (B) needs slot-creation + position-persistence + a slot-kept-alive-across-the-gap + a brand-new gap-free boundary proof (the v0.99.16 silent-loss class), and lands a two-command UX. (A) reuses the already-proven `OpenSnapshotStream` boundary. (B) remains a worthwhile *separate* primitive for a "migrate once, follow much later" workflow; deferred.
- **Parallelize the VStream/MySQL cold-start too.** Rejected — MySQL per-session snapshots aren't shareable (N independent `WITH CONSISTENT SNAPSHOT` txns = N inconsistent views, unsafe on a live source); the VStream single-stream + durable-watermark + idempotent path is the delicate coupling this ADR deliberately leaves untouched. The capability gate exists precisely to leave them serial.
- **Make the raw byte-pipe checkpoint-compatible with the durable watermark.** Unnecessary — they never coexist (raw is PG-fresh-cold-start-only; the watermark is VStream-only). Avoiding this is the central risk reduction.
- **Multi-database / multi-schema PG sync cold-start parallel in v1.** Deferred to a fast-follow (v1.1) — the spanning DB-wide snapshot is also shareable so the same trick applies, but single-database PG validates first.
