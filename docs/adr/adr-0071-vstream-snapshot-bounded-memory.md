# ADR-0071: Bounded-memory VStream snapshot streaming

## Status

Proposed. Extends [ADR-0028](adr-0028-memory-bounded-streaming.md) (memory-bounded streaming via `--max-buffer-bytes`), whose audit covered the SQL `RowReader` (one row in flight) and the writer/applier accumulators but **never reached the PlanetScale VStream snapshot reader**.

## Context

The PlanetScale/Vitess cold-start snapshot reader (`internal/engines/mysql/cdc_vstream_snapshot.go`) buffers the **entire** COPY phase in RAM before a single row reaches the target:

- `openVStreamSnapshotStream` calls `drainCopyPhase(ctx)` **synchronously** (line 130) and only returns the `ir.SnapshotStream` after the *global* `COPY_COMPLETED` event.
- `drainCopyPhase` (loop, lines 240–261) appends every COPY-phase row into `rowBuffer map[string][]ir.Row` (`dispatchCopyEvent`, line ~385).
- Only then does the orchestrator begin bulk-copy; `vstreamSnapshotRows.ReadRows` (line 655) serves each table's rows from `rowBuffer` and frees the slice.

The struct's own lifecycle doc (lines 157–166) states this explicitly. **The buffer is unbounded.** A field report: a ~13 GB / ~19M-row PlanetScale table drove the `sluice` process to 28 → 38 → ~41 GB RSS on a 32 GB host, into swap, until the OOM killer reaped it — with **zero target writes** during the entire cold-start. This is a silent unbounded-memory (OOM) loss class on the single most common cold-start path: one large table.

### Why the buffer exists (invariants any fix must preserve)

1. **Order decoupling** — VStream emits COPY rows in its own order; the orchestrator consumes table-by-table in its own order. Buffering lets the two orders differ.
2. **Multi-shard fan-in** — rows for one logical table arrive interleaved from N shards; keying by unqualified table name merges them.
3. **Dedup** — Vitess re-emits behind-the-scan PKs during COPY; the `copyDedupTracker` drops them inline as events arrive (GitHub #14).
4. **Snapshot position** — `currentVgtid` is only snapshot-consistent at the global `COPY_COMPLETED`; the CDC handoff needs that final position.

### Position-timing is not a blocker

`stream.Position` is read immediately after `OpenSnapshotStream` only on the **add-table live-mode LSN invariant** (`add_table.go:487`), which is **PG-only** (the publication-add path); the MySQL filter-flip path that uses this VStream reader has **no equivalent invariant** (`add_table.go:482`). The main cold-start path consumes `Position` *after* bulk-copy, at the CDC handoff. So finalizing the position at `COPY_COMPLETED` (rather than at `OpenSnapshotStream` return) is safe for every real consumer of this reader.

## Decision

Extend the ADR-0028 bounded-memory contract to this reader, in phases shipped together as one solution.

### Phase 1 — bounded buffer, loud refusal (correctness floor)

Account `rowBuffer` growth in bytes (reuse ADR-0028's `MaxBufferBytes` accounting) inside `dispatchCopyEvent`. When the snapshot buffer would exceed the cap, **refuse loudly** with an actionable message (name the table, the cap, and the `--max-buffer-bytes` / streaming guidance) instead of growing into swap. This converts a *silent* OOM-loss class into a bounded, diagnosable failure — the single most important change, and a safe floor even if Phase 2 regresses.

### Phase 2 — concurrent bounded drain + streaming `ReadRows` (the real fix)

Stop draining to completion before returning. After capturing field metadata and the initial VGTID, return the `SnapshotStream` and pump the gRPC stream from a background goroutine that appends to `rowBuffer` **under the byte cap**. `ReadRows(table)` emits rows onto its channel **as they arrive**; a slow target backpressures the channel, which backpressures the pump's `Recv`, which backpressures Vitess — constant memory, and target writes begin immediately. For the dominant single-large-table case this is the entire win.

Preserved invariants: dedup stays inline in the pump; multi-shard fan-in still merges by unqualified name; `Position` finalizes at `COPY_COMPLETED` (before `StreamChanges`, which the orchestrator only calls after bulk-copy).

The multi-table interleaving edge — rows for a not-yet-consumed table accumulating while another is being read — is bounded by the Phase 1 cap: if the not-yet-drained tables exceed it, refuse loudly (Phase 1 message) rather than OOM. The cap is the safety net; Phase 3 removes the ceiling if field demand appears.

### Phase 3 — deferred

Disk-spill the not-yet-consumed-table buffer (per-table temp files of encoded `ir.Row`) so genuinely interleaved large *multi-table* snapshots bound RAM without the Phase 1 refusal. Deferred until a real workload needs it; the Phase 1 cap makes the failure loud and actionable in the meantime.

## Consequences

- **Bounded memory** on VStream cold-start (single-table: constant; multi-table: capped with a loud refuse).
- **Lower cold-start latency** — target writes start immediately instead of after the whole snapshot buffers.
- **Concurrency change** — the background pump + the streaming channel handoff must be race-clean. Per the project's `-race`-before-tag rule, the integration `-race` gate **must** pass before the release tag is cut.
- **New pins** — a large-table bounded-memory integration test (assert RSS / buffered-bytes stays under the cap while target row count climbs), plus a multi-shard fan-in regression and a dedup regression under the streaming path.

## Alternatives considered

- **Spill-to-disk only (skip streaming).** Bounds RAM but keeps the "no target writes until `COPY_COMPLETED`" latency wart and adds temp-file lifecycle. Rejected as the primary fix; retained as Phase 3 for the interleaved-multi-table tail.
- **Align orchestrator consumption to VStream's emission order.** Would let each table stream directly with no cross-table buffer, but couples the engine-neutral orchestrator to VStream specifics — violates the IR-first / engine-neutral-orchestrator tenet. Rejected.
