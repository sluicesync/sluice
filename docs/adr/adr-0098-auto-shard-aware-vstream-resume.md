# ADR-0098: Auto-shard-aware VStream cold-copy resume (bounded-memory resume of a large multi-table keyspace)

## Status

Accepted. Fixes the resume gap [ADR-0095](adr-0095-vstream-auto-shard-by-table-copy.md) deferred as "a v1 limitation, not a correctness gap" — a judgment proven wrong live (see Context). Builds on [ADR-0095](adr-0095-vstream-auto-shard-by-table-copy.md) (the per-table auto-shard COPY pump + the per-shard GTID-set-min stitch this reuses verbatim), [ADR-0072](adr-0072-resumable-coldstart-copy.md) (the resumable COPY cursor — Vitess's per-shard `TablePKs` — this resume seeds from), [ADR-0071](adr-0071-vstream-snapshot-bounded-memory.md) (the Phase-1 loud-refusal floor whose crash-loop on resume is the bug), [ADR-0010](adr-0010-idempotent-applier.md) (idempotent apply — the upsert that makes the re-copy of already-finished tables harmless), and [ADR-0007](adr-0007-position-persistence.md) (position-then-data ordering — never advance CDC past an incompletely-copied table). Composes with [ADR-0097](adr-0097-parallel-writer-fanout-vstream-snapshot-copy.md) (the write-side fan-out, which already runs against the one active table at a time). Does not touch the binlog (vanilla MySQL) or Postgres cold-copy paths.

## Context

ADR-0095 made the VStream cold-copy **auto-shard by table** — one single-table VStream COPY at a time, constant memory, no interleave — so a full multi-table Vitess/PlanetScale keyspace cold-copies in one command at bounded memory. But it engaged that path **only on a fresh cold-start**. The eligibility gate was:

```go
autoShard := start == nil && len(tables) > 1 && !singleStream   // start == nil ⇒ fresh only
```

On a process-restart **resume** (`OpenSnapshotStreamFromPosition`, a position carrying a per-table `TablePKs` cursor — ADR-0072), `start != nil`, so `autoShard` was false and the engine fell back to the **legacy single keyspace-wide VStream** (`Filter: [{Match:"/.*/"}]` for every in-scope table on one stream). That is exactly the path that interleaves every table and buffers the not-yet-active ones under the ADR-0071 cap. ADR-0095 documented this as benign: "Resume is still correct (the idempotent COPY writer absorbs any overlap)… resume-with-auto-shard is a clean follow-on."

**It is not benign.** A resumed copy of a 28-table / 302 GB PlanetScale keyspace (large-scale test program, Track B2) crash-looped with:

```
mysql/vstream: snapshot: table "binary_blobs" would buffer 67111375 bytes,
exceeding the --max-buffer-bytes cap of 67108864 while table "audit_trail" is
being copied; this multi-table interleaving case is not yet disk-spilled
(ADR-0071 Phase 3)
```

It errors **loudly** (the ADR-0071 floor did its job — no silent loss, no OOM), but a resume of a large keyspace can **never make progress** on the interleaved path: the second table inevitably exceeds any host-fittable `--max-buffer-bytes` while the first drains. The failure is loud-but-fatal — it converts a recoverable interruption of exactly the large-keyspace case auto-shard exists for into an unrecoverable one. The fresh cold-start works; the resume of the same copy does not. That asymmetry is the bug.

### What is persisted at a mid-COPY crash under auto-shard (the ground truth)

The auto-shard pump copies one table at a time, each a **single-table** VStream (`Filter:[{Match:<table>}]`). The bounded-cadence checkpoint (ADR-0072 Phase B, durable-watermark-gated per v0.99.9) persists `currentVgtid` — the position of the **one in-flight table**. Because the stream is scoped to a single table, that position carries:

- the per-shard GTID of that table's COPY scan, and
- a `TablePKs` cursor naming **exactly that one table** (vtgate only emits a `TablePKs` entry for a table whose COPY is in flight on the stream).

Between tables the pump resets `currentVgtid = nil` and clears the breadcrumbs, so the persisted position at any moment reflects **only the in-progress table**. It does **not** enumerate which earlier tables already finished. The rows of finished tables are durably on the target; the position carries no list of them.

**Re-derivation, not new state.** The in-scope table list (`tables`, the filtered schema order) is passed to the resume in the same order the auto-shard loop iterated. The persisted cursor names the in-progress table `t_k`. Therefore the completion state is fully re-derivable: tables `[0..k)` finished, `t_k` was in flight (resume it from its cursor), tables `(k..n)` had not started. No new persisted field is required.

## Decision

Make the resume path **auto-shard-aware**: drive the SAME per-table auto-shard pump on a multi-table resume, seeding the in-progress table from its cursor.

1. **Gate change.** `autoShard := len(tables) > 1 && !singleStream` — drop the `start == nil` clause. Both a fresh start and a resume engage auto-shard when the scope has more than one table (and the operator hasn't opted out via `vstream_copy_single_stream`).

2. **Resume placement** (`resolveResumeAutoShard`). Decode the persisted cursor's `TablePKs`. It must name **exactly one** in-scope table — the in-progress table `t_k`. The pump then, in the unchanged `copyTablesSeq` (= filtered schema) order:
   - **re-copies tables `[0..k)` from-beginning** (idempotent upsert absorbs the full re-copy — the same Bug-125 overlap path every cold-start→CDC handoff already relies on), so each contributes a fresh per-table snapshot to the stitch;
   - **resumes `t_k` from its persisted cursor** (`reopenForTableSeeded` — vtgate continues the scan from the last-copied PK, no row-0 restart);
   - **copies tables `(k..n)` fresh** from-beginning.

3. **Stitch unchanged.** After the last table the pump computes the per-shard GTID-set **minimum** of all captured per-table snapshots (`stitchSnapshotMin`, ADR-0095) — the gapless, overlap-safe CDC-resume position. Because every table (re-copied, resumed, or fresh) contributes a captured `P_i`, the stitch's correctness argument is **identical to a fresh cold-start**.

### State-shape decision: re-derive + re-copy, no new token field

The chosen design adds **no field to the persisted position** and **no new state to persist mid-copy**. The two candidate approaches and why this one wins:

- **(A — chosen) Re-derive completion from the cursor + table order; re-copy already-finished tables idempotently.** The cursor names `t_k`; everything before it in the known sequence is finished, everything after is unstarted. Re-copying the finished tables `[0..k)` is wasted bytes, but it is *trivially correct*: every table gets a fresh snapshot, the stitch is the cold-start stitch, the idempotent writer (ADR-0010, already declared `CopyNeedsIdempotentWriter() = true` on this path) absorbs the re-copy. Zero new state, zero GTID-interval arithmetic.

- **(B — rejected) Persist a per-table "completed floor" GTID in the token and SKIP re-copying finished tables.** Skipping is the throughput-optimal choice, but to stay gapless the CDC stitch must resume from a position `≤` every *finished* table's snapshot — and those snapshots are not in the persisted position. Recovering them requires either persisting a running per-shard "completed-tables min GTID" floor (a new token field + interval-min arithmetic over GTID *sets*) or trusting the in-progress cursor's GTID as a floor — but the in-progress cursor was captured *after* the finished tables completed, so it is an **upper** bound on them, not a lower bound. Using it would skip `(P_finished, P_start]` for every finished table during the copy/downtime window — **silent loss**. The correct floor needs hand-rolled GTID-set interval arithmetic, exactly the subtle-codec class the Bug-74 "pin the class, not the representative" lesson warns against. Rejected for v1: the correctness risk of (B) dwarfs the re-copy cost of (A), and (A) is provably correct by reduction to the already-reviewed cold-start path.

The re-copy cost is bounded by the bytes of the already-finished tables in the interrupted run — paid once per resume, at bounded memory and full throughput. The skip-optimization (B, done safely with a persisted completed-floor) is a clean, demand-gated follow-on if a real workload shows resume-re-copy dominating; it does not change the consistency model, only the start position of the finished-table prefix.

### Backward compatibility with positions written by released code

A position written by the **current released auto-shard code** (v0.99.62+) is exactly the single-in-progress-table cursor this resume expects — it round-trips unchanged. The resume refuses **loudly** (never silently re-copies the whole keyspace from row 0, never silently skips a table) when the persisted cursor cannot be placed:

- **A legacy single-stream / interleaved-resume token** (one written by `vstream_copy_single_stream=true`, or a pre-ADR-0095 token) can carry `TablePKs` for **more than one** table. `resolveResumeAutoShard` refuses it and tells the operator to resume on the legacy path (`vstream_copy_single_stream=true`) or restart the cold-start.
- **A cursor naming a table no longer in scope** (an `--include-table` change since the checkpoint, or a stale token) is refused, naming the offending table and the current scope.
- **A cursor-less position** never reaches here (the resume entry gates on `anyTablePKsPresent`); if it somehow does, it is refused rather than full-re-copied.

### Exactly-once + bounded-memory argument (the correctness surface for review)

- **Bounded memory.** The resume drives the per-table pump: exactly one single-table COPY is ever in flight, so `rowBuffer` holds one table's rows under `--max-buffer-bytes`, and the ADR-0071 not-yet-consumed-table loud refusal is **structurally unreachable** (there is no interleave). This is the whole fix — the resume path no longer interleaves.
- **No gap (no silent loss).** Every in-scope table contributes a captured per-table snapshot `P_i` (re-copied, resumed, or fresh), and CDC resumes from `P_start = ⋂_i P_i` (the set-min). `P_start ⊆` every `P_i`, so CDC replays the full window `(P_start, P_i]` and everything after for **every** table — no committed change after any table's snapshot can be skipped. The resumed table `t_k` is no different: its captured `P_k` is its *completion* point, and `P_start ≤ P_k`. The CDC position is recorded only at `finishCopyAutoShard`, after the **last** table completes, so it never advances past an incompletely-copied table (ADR-0007).
- **Overlap is absorbed (no duplicate-loss).** Three overlap sources, all idempotent: (1) the re-copied finished tables `[0..k)` re-send rows already on the target — absorbed by the upsert; (2) `t_k`'s resumed scan may re-send rows around its cursor (checkpoint-lag) — absorbed by the upsert (the Bug-125 no-PK path included); (3) the `(P_start, P_i]` CDC catch-up window — the same at-least-once seam every cold-start→CDC handoff already relies on (ADR-0010).

### Concurrency

The resume uses the same sequential per-table pump as the cold-start auto-shard: exactly one `Recv` loop alive at a time, the same liveness watchdog / in-place reconnect / checkpoint machinery. No new concurrent-`Recv` hazard. Per the project rule, the integration **`-race`** gate MUST pass **before** the release tag is cut (push-first, tag-after, or `scripts/race-integration.ps1`).

## Alternatives considered

- **Persist a completed-tables GTID floor and skip re-copy (option B above).** Rejected for v1 — needs new token state + GTID-set interval arithmetic (Bug-74 risk) to stay gapless; re-copy (A) is provably correct by reduction to the cold-start path. Clean follow-on if resume-re-copy ever dominates.
- **Keep resume on the single-stream interleave and raise `--max-buffer-bytes` to fit Σ(tables).** This is the status-quo crash-loop dressed as an operator knob; the 290 GB requirement fits no host. Rejected (same reasoning as ADR-0095).
- **Refuse a multi-table resume and force a full restart.** "Correct" (no data risk) but throws away the entire interrupted copy and re-copies *every* table from row 0 — strictly worse than option A, which resumes the in-progress table from its cursor. Rejected.
- **Reorder `copyTablesSeq` to start at the in-progress table.** Would skip re-copying the finished prefix, but the orchestrator reads tables in schema order and the per-table `ReadRows`↔pump coupling requires `copyTablesSeq` to match that order (exactly one table in flight). Reordering would desynchronize the consumer from the producer. Rejected; kept the schema order and re-copy the prefix.

## Consequences

- **A resume of a large multi-table Vitess/PlanetScale keyspace now completes at bounded memory** — the live HIGH bug is closed. The resume is memory-equivalent to the fresh cold-start auto-shard (one table in flight), not the interleaved crash-loop.
- **A resume re-copies the already-finished tables of the interrupted run** (idempotently). Bounded, one-time, full-throughput cost; the skip-optimization is a demand-gated follow-on.
- **Loud refusal on an unplaceable cursor** (legacy multi-table token, out-of-scope table) — the operator gets a named, actionable error instead of a silent re-copy or skip.
- **New pins.** Unit: `resolveResumeAutoShard` (single in-scope cursor → table; multi-table / out-of-scope / empty → loud refuse); the resume routing engages the auto-shard pump and opens the in-progress table SEEDED from the cursor while re-copying the prefix and copying the tail fresh (incl. the in-progress-table-at-index-0 edge the constructor's pre-opened table[0] complicates); the stitched handoff position is the per-shard set-min. Integration (`vstream` / `vitesscluster`): cold-start an auto-shard copy of a multi-table keyspace where Σ(tables) far exceeds a tiny `--max-buffer-bytes`, INTERRUPT mid-copy after ≥1 table completes while another is in flight, then `--resume` — assert NO ADR-0071 buffer-cap error, completion, and `target COUNT(*) + checksum == source` for every table (no gap, no dup) with a clean CDC handoff. This is the regression pin for the exact crash-loop.
- **ADR-0095's "resume stays single-stream (v1 limitation)" note is corrected** to point here; the `start == nil` gate is replaced by `len(tables) > 1`.
- **`-race`-before-tag** (concurrency-adjacent stream-lifecycle change).
