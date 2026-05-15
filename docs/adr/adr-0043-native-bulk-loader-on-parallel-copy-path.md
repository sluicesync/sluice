# ADR-0043: native bulk loader on the cold-start parallel-copy path

## Status

**Accepted (2026-05-15).** Design signed off; implementation
pending. Implements ADR-0042 Phase C Option 1 (the only Phase C
option targeting the measured dominant cost). Design point A
resolved at sign-off: **whole-chunk `WriteRows` with coarser
per-chunk checkpoint** (option (b)) — the configuration Phase
A/B showed the single-reader path winning with; checkpoint
coarsening is sound because the fast path is cold-only and a
crashed cold chunk replays idempotently on the resume run.

## Context

ADR-0042 Phase B established the ground truth: the parallel-copy
path (`internal/pipeline/migrate_parallel.go::copyChunk`) routes
*every* chunk through `ir.IdempotentRowWriter.WriteRowsIdempotent`
— a generic `database/sql` batched upsert (`INSERT … ON CONFLICT
DO UPDATE` / `ON DUPLICATE KEY UPDATE`). **Neither MySQL `LOAD
DATA` nor PostgreSQL `COPY` is on the parallel hot path.** Both
engines' pprof profiles under parallel copy are dominated by the
same generic batched-`ExecContext` cost; H5 (column-codec) was
falsified and H3 re-scoped — the gap is the writer *path*, not
driver encoding.

The fast native loaders already exist and are *automatically*
selected — but only on the **single-reader cold-start** path
(`migrate.go::copyTable` → `rw.WriteRows` → PG `writeViaCopy`
(pgx `CopyFrom`) / MySQL `writeLoadData` (`LOAD DATA LOCAL
INFILE`), each with an automatic capability/`local_infile`
fallback to batched INSERT). The parallel path gave up that
speed for a crash-resume correctness property.

**Why the parallel path is idempotent today.** `copyChunk`
checkpoints a per-chunk PK cursor (`TableChunkProgress.LastPK`,
`RowsCopied`, `State`) to the migration state after each batch.
On resume, a chunk re-enters at its last *checkpointed* cursor.
If a crash lands after a batch's write commits but before its
cursor checkpoint is persisted, the resume run re-reads and
re-writes that batch. `WriteRows` (COPY / LOAD DATA / batched
INSERT) is **not** upsert — re-writing collides on the primary
key. `WriteRowsIdempotent` absorbs the overlap. The idempotent
writer is also load-bearing for two *non-resume* situations:
`--force-cold-start` into a populated target (Bug 9 preflight is
skipped, PK collisions expected) and mid-stream live-add
(`copyTableIdempotent`, CDC events may already have populated
rows — ADR-0036).

So the speed/correctness trade was deliberate but **over-broad**:
it forced the slow path on the *common, safe* case (a fresh
cold-start parallel copy into an empty target) to protect the
*uncommon* case (resume / forced-populated / live-add).

## Decision

Make the parallel chunk writer **situation-driven and automatic**
(no new CLI surface — symmetric with how the single-reader path
already auto-selects the loader from engine capability):

> A chunk uses the native fast loader (`RowWriter.WriteRows`) **iff
> all** of: (1) the migration is **not** in resume mode; (2) the
> chunk has **zero recorded prior progress** (`LastPK == nil` &&
> `RowsCopied == 0` && `State != Complete`); (3) cold-start
> pre-flight was **not** bypassed via `--force-cold-start` (target
> proven empty); (4) this is **not** the mid-stream live-add path.
> Otherwise it uses `WriteRowsIdempotent` exactly as today.

Rationale for each gate (all four are correctness-forced, not
tunable):

- **(1)+(2) — resume safety.** The fast path is used only on a
  chunk that has never committed a row. The instant a chunk has
  any persisted progress, *or* the run is a resume, the whole
  remaining chunk uses the idempotent path (we cannot cheaply
  identify which specific rows in a resumed chunk are duplicates,
  so upsert the remainder — simpler and correct, per the
  clean-code tenet). A crash during a first-pass fast chunk is
  safe because the *next* invocation is a resume run: gate (1)
  fails, the chunk replays idempotently. The fast path is thus
  only ever used where a PK collision is impossible.
- **(3) — `--force-cold-start`.** Bypassing the Bug 9 preflight
  means the target may be populated; non-upsert `WriteRows` would
  collide. Force-cold-start ⇒ idempotent.
- **(4) — live-add.** `runBulkCopyForAddTable`/`copyTableIdempotent`
  is a separate (single-table) path today; the gate is stated so
  the invariant survives if parallel is ever wired in there.

Net operator-visible effect: fresh large parallel migrations into
an empty target (the overwhelmingly common case — and the one
Phase A/B measured) get the native loader automatically; resume,
forced-populated, and live-add are byte-for-byte unchanged. No
flag. The only pre-existing knob still in play is `local_infile`
(already documented for the single-reader path; the MySQL writer's
automatic `LOAD DATA → batched INSERT` fallback is unchanged and
still faster than batched *upsert* on an empty target).

### Interface / code shape

- `copyChunk` today *requires* `ir.IdempotentRowWriter` and calls
  only `WriteRowsIdempotent`. Relax to: require `ir.RowWriter`
  (always present); require `ir.IdempotentRowWriter` **only when
  the situation selects the idempotent branch** (both engines
  implement both, so no real capability loss — the requirement
  becomes a checked precondition on the idempotent branch with a
  loud error if ever violated).
- The per-batch loop in `copyChunk` keeps its structure; only the
  single write call becomes a branch on a `useFastLoader bool`
  computed once at chunk entry from the four gates. `resuming` is
  already threaded into `resolveChunks`/`runChunks`; thread it
  (and the force-cold-start + live-add flags) into `copyChunk`.
- `WriteRows`'s contract is whole-stream, but `copyChunk` calls
  the writer **per batch** (limit rows) inside its cursor loop.
  Calling `WriteRows` per batch is semantically fine (each call
  is an independent COPY/LOAD DATA of that batch's rows) — but
  per-batch COPY/LOAD DATA setup has fixed overhead. **Open
  design point (A):** either (a) keep the per-batch call shape
  (simplest; still a large win vs upsert), or (b) for a fast-path
  cold chunk, stream the whole chunk through one `WriteRows` call
  (best throughput; requires the chunk's reader to expose a
  bounded full-chunk stream and defers the per-batch cursor
  checkpoint to a coarser per-chunk checkpoint — acceptable
  because a fast cold chunk that crashes replays from chunk start
  under gate (1) anyway). Recommendation: **(b)** — it is the
  configuration Phase A/B actually showed the single-reader path
  winning with, and the checkpoint coarsening is sound precisely
  because the fast path is cold-only.

  **RESOLVED at sign-off: option (b).** The fast-path cold chunk
  drains its PK-bounded reader stream through ONE `WriteRows`
  call (one COPY / LOAD DATA for the whole chunk range) and
  writes a single terminal per-chunk checkpoint (`State =
  Complete`) on success — no per-batch checkpoint on the fast
  path. A crash before that terminal checkpoint leaves the chunk
  with zero recorded progress, so the resume run replays the
  whole chunk under the idempotent branch (gate (1)). The
  idempotent branch keeps its existing per-batch cursor
  checkpointing unchanged.

### What does not change

- Single-reader cold-start (`copyTable`) — already fast.
- Single-reader resume (`copyTableWithCursor`) — correctly
  idempotent; out of scope.
- Resume / `--force-cold-start` / live-add parallel behaviour —
  identical to today.
- No engine API change, no IR schema change, no CLI flag, no
  migration-state format change (gate (2) reads existing
  `TableChunkProgress` fields).

## Gotchas

- **The crash-window argument is the load-bearing correctness
  claim.** It must be pinned by a test that kills a fast-path
  cold chunk mid-flight and asserts the resume run switches that
  chunk to the idempotent branch and produces a correct,
  collision-free target. This is the ADR-0036-style "permanent
  proof-of-falsification" test.
- **Per-chunk checkpoint coarsening (design point b)** trades
  finer resume granularity for throughput on cold chunks only. A
  cold chunk that crashes restarts from its lower bound (not a
  mid-chunk cursor) on the resume run — correct, but re-reads up
  to one chunk's worth of source rows. Acceptable: chunks are
  bounded (~table_rows / parallelism) and the resume run is
  idempotent anyway. Document in the migrate resume docs.
- **`WriteRows` MySQL fallback.** On `local_infile=OFF` the fast
  path is batched INSERT (non-upsert). Still safe on a proven-empty
  cold chunk and still faster than batched upsert. No special
  handling.
- **`gofumpt`/lint:** the new branch must not introduce a blank
  line after `{`; prefer `errors.New` for the precondition error.

## Testing

- Unit: the four-gate selection function — table-driven over
  (resuming, prior-progress, force-cold-start, live-add) → expect
  fast vs idempotent.
- Integration (both engines, testcontainers): (1) fresh parallel
  cold-start into empty target uses the fast loader and the
  `adr0042:` per-chunk rate rises toward the single-reader rate;
  (2) **crash-mid-fast-chunk → resume** produces a correct target
  with no PK-collision error (the load-bearing test); (3)
  `--force-cold-start` into a populated target still succeeds via
  the idempotent branch; (4) resume of a partially-done parallel
  copy unchanged.
- Rig e2e: medium fixture MySQL→MySQL — expect a material drop in
  wall time toward the PG single-reader band; record in
  `local-rig/baselines.md`. Re-run the ADR-0042 Phase A/B
  instrumented comparison to quantify the closed gap.

## Sizing

~250–400 LOC impl (gate function + `copyChunk` branch + threading
the three flags + the precondition error) + ~300–400 LOC tests
(the crash/resume integration test is the bulk). One focused
release. No new ADR dependencies; supersedes ADR-0042 Phase C
Option 1's "proposed, not implemented" state.

## References

- ADR-0042 — Phase A/B findings (this is its Phase C Option 1).
- ADR-0019 — parallel within-table bulk copy (the chunk model).
- ADR-0028 — memory-bounded streaming (per-chunk byte cap).
- ADR-0036 — mid-stream live-add idempotent absorb (gate 4).
- Bug 9 — populated-target cold-start preflight (gate 3).
