# ADR-0150: Byte-targeted ~1 MiB INSERT batching for the MySQL bulk-load path

Status: Accepted (2026-07-04)

## Context

The MySQL row writer's batched-INSERT bulk path (`internal/engines/mysql/row_writer.go` `writeBatchedConn`, and its upsert mirror `writeBatchedIdempotentConn` in `row_writer_batch.go`) flushed on a fixed 500-row cap (`defaultMaxRowsPerBatch`), with the ADR-0028 `--max-buffer-bytes` accumulation cap (default 64 MiB) as a secondary trigger that in practice never fired first. Vanilla MySQL normally takes LOAD DATA (ADR-0026), so the fixed cap was mostly invisible there — but **Vitess/PlanetScale has no LOAD DATA, so the batched-INSERT path IS the PlanetScale bulk-load path**, and it is also the path every MySQL-flavor idempotent write takes (resume, chunked copy, VStream cold-start COPY — Bug 125 forces idempotent there).

The 2026-07 performance research (`docs/research/perf-gap-analysis-2026-07.md` §"PlanetScale-MySQL: the honest lever list") concluded the structural PS write levers are exhausted — no LOAD DATA (verified), writes are tier-CPU-bound not connection-bound (ADR-0116 ground truth: a PS-10 pins at 100% CPU under a 2-wide copy), `workload=olap` is read-side only, and the ~20 s transaction killer / 30 s DML timeout bound everything — **except one**: round-trip amortization. Narrow rows under the 500-row cap produce 50–100 KB statements; the pscale-cli dumper (the battle-tested reference implementation against PlanetScale, cited in CLAUDE.md §External references) batches by **~1 MB of statement body**, i.e. 10–20× more payload per WAN round trip. The CDC applier already adopted exactly this size for its ADR-0139/0140 coalescing (`maxCoalescedStatementBytes = 1 MiB`); the bulk-copy path had not.

## Decision

Make an **estimated ~1 MiB statement byte target the primary flush trigger** for both batched-INSERT bulk loops, demote the row cap to a safety ceiling, and bound placeholders. Implemented as a small shared batch composer (`insertBatcher`, `internal/engines/mysql/row_writer_bytebatch.go`) used by the plain and idempotent conn loops (and therefore by their ADR-0097/ADR-0102 parallel fan-out callers, which share those loops):

- **`defaultStatementByteTarget = 1 MiB`** — the pscale-dumper statement size, deliberately equal to the CDC applier's `maxCoalescedStatementBytes` (ADR-0139) so the two MySQL round-trip-amortization paths share one number. Accumulation uses `ir.ApproximateRowBytes` — O(value-length), no double-encoding; the estimate is approximate by design (target ~1 MiB, hard-verify far under `max_allowed_packet`: MySQL 8.0 default 64 MiB, PlanetScale 16 MiB+).
- **`defaultMaxRowsPerBatch` 500 → 10,000** — retained as a safety ceiling on heap and statement parse cost, no longer the primary trigger (it now binds only for rows so narrow that 10k of them sit under 1 MiB). The `maxRowsPerBatch` test-override seam is unchanged.
- **`maxBulkInsertPlaceholders = 60,000`** — rows × non-generated columns per statement never exceeds it (MySQL's prepared-statement parameter count is a 16-bit field, hard limit 65,535; same bound and headroom as ADR-0139's `maxCoalescedPlaceholders`). Necessary now that row counts can exceed 500: `ExecContext` with args prepares server-side, so the placeholder count is a real protocol limit.
- **Never split, never refuse:** a single row whose estimate alone exceeds the target ships as a one-row statement; the server's `max_allowed_packet` remains the loud upper bound for a genuinely oversized row, exactly as before.
- **`--max-buffer-bytes` clamps down only:** an operator value below 1 MiB lowers the effective per-statement trigger (preserving ADR-0028's "bound the accumulation" intent); a larger value leaves the target in charge — the ~1 MiB size is a round-trip-amortization choice, not a memory bound, and it already sits far under ADR-0028's 64 MiB default. (Previously a large cap was moot anyway: the 500-row cap bound first.) The applier-side ADR-0028 byte caps (`change_applier_batch.go`, `change_applier_concurrent.go`) are untouched.
- **Value fidelity untouched:** every value still binds to a `?` through the same `prepareValue` codec — the wire encoding of each value is byte-identical, there are only more placeholder groups per statement (the same Bug-74 safety argument ADR-0139 made).
- **Transaction budget unchanged:** both loops execute each flush as one autocommit `ExecContext` — one statement = one transaction, no statement grouping — so a ~1 MiB INSERT stays far inside Vitess's ~20 s transaction killer (ADR-0052 DP-2; `TransactionKiller` capability). Nothing needed flushing on a time budget because nothing groups statements per transaction.
- **AIMD / concurrency ownership unchanged:** this changes statement composition only. The AIMD controllers, connection budgets (ADR-0116), grow-gate (ADR-0110), reparent-retry (ADR-0108, including the tolerate-1062-on-retry wart — still a single atomic multi-row INSERT, just bigger), and Vector-B warning sampling are all untouched.

**Companion operator hint.** When the bulk-write path engages against a **hosted-PlanetScale-flavor** target, the writer emits one INFO per writer (one per migrate run, which shares a single RowWriter): writes are tier-CPU-bound, not connection-bound (ADR-0116), so copy parallelism beyond the auto budget will not scale linearly — a larger tier or Metal is the lever. Self-hosted `vitess` is deliberately excluded (operator hardware, no PS tier ceiling). The gate field (`tierCPUBoundTarget`) is zero-value-safe: false everywhere except the flavor check in `OpenRowWriter`.

## Engine × mode coverage (perf-parity working agreement)

The change lives in the two shared conn loops, so it reaches every mode that flows through the MySQL `RowWriter`'s batched paths — verified against the call sites, not the roadmap:

- **MIG × MySQL-target** — `WriteRows` plain path (PlanetScale/Vitess always; vanilla on the per-call LOAD DATA fallback: `local_infile=OFF` or geometry), `WriteRowsIdempotent` (resume, >threshold chunked copy, add-table), `WriteRowsParallel` / `WriteRowsIdempotentParallel` (ADR-0102/0097 fan-out) — `migrate_copytable.go`, `migrate_parallel.go`, `migrate_bulk.go`, `copy_fanout.go`. ✅
- **CS × MySQL-target** — sync cold-start COPY is `WriteRowsIdempotent(Parallel)` (Bug 125 forces idempotent on VStream sources; binlog cold-start same writer). ✅
- **RST / CR (full-restore leg) / BRK (cold-start reset-leg via ChainRestore) × MySQL-target** — `restore.go` `writeFn = rw.WriteRows` / `iw.WriteRowsIdempotent`. ✅
- **CR/BRK incremental replay and CDC steady-state apply** — ChangeApplier, **deliberately untouched**: already byte-targeted via ADR-0139/0140 coalescing (same 1 MiB constant).
- **BK** — n/a (backup writes JSONL chunks, no RowWriter).
- **LOAD DATA path (vanilla MySQL primary bulk path)** — untouched, already streaming.
- **Other engines** — deliberately untouched: PG uses binary COPY (byte-stream, no statement composition); SQLite has its own 900-param-capped multi-row INSERT sized to its bind limits; D1-as-target is stage-local (ADR-0145).

`docs/dev/perf-parity-matrix.md` row 3 updated in the same change.

## Consequences

- Narrow-row corpora compose ~10–20× fewer statements (pinned: `TestInsertBatcher_StatementCountRegression`, ≥10× vs the 500-row baseline), which is the round-trip amortization win on WAN PlanetScale imports; on LAN targets the difference is modest but strictly non-negative (fewer exec round trips, same bytes).
- Statements can now carry thousands of rows, so the placeholder bound is load-bearing — without it a 100k-row flush on a 2-column table would exceed the 65,535 prepared-statement parameter limit. Pinned by `TestInsertBatcher_PlaceholderBoundClampsWideTables`.
- A failed batch now retries (ADR-0108) with more rows in flight; the retry semantics are unchanged because the batch was always a single atomic multi-row INSERT — the 1062-on-retry proof ("byte-identical batch already landed") is size-independent.
- Per-batch heap grows from ~500 rows to at most min(10k rows, ~1 MiB + one row of values) — bounded and smaller than ADR-0028's 64 MiB allowance.
- The Vector-B `SHOW WARNINGS` sampling schedule now covers more rows per checked flush (fewer, larger flushes); the sampling rationale (repo-audit M3.5) is unaffected — under strict sql_mode the INSERT itself errors, and the relaxed-mode advisory remains once-per-table.

## Alternatives considered

- **Keep the fixed 500-row cap.** Rejected: it is exactly the gap the research quantified — 50–100 KB statements on the one engine flavor that cannot use LOAD DATA, leaving 10–20× WAN round-trip amortization unused. There is no correctness upside to the smaller size; `max_allowed_packet` headroom at 1 MiB remains ≥16×.
- **Telemetry-adaptive statement sizing** (the ADR-0107/0115 CPU-headroom clamp pattern applied to statement bytes). Deferred as future work, per the research: a static 1 MiB captures the bulk of the win with zero new credentials/telemetry surface; adaptive sizing is worth revisiting only with evidence that tier CPU responds to statement size rather than total write volume (ADR-0116 suggests it does not).
- **Raise the byte target above 1 MiB** (e.g. 4–8 MiB). Rejected for now: pscale-dumper's 1 MB is the battle-tested reference; larger statements grow server-side parse/exec time per statement toward the 20 s killer with diminishing amortization returns, and shrink retry granularity under ADR-0108.
- **Group multiple statements per transaction** (explicit BEGIN/COMMIT around N flushes) to amortize further. Rejected: it would put the 20 s transaction killer back in play for slow tiers, change the durability granularity the durable-progress watermark (v0.99.9) relies on, and complicate the 1062-on-retry proof. Autocommit-per-statement is load-bearing.
- **Client-side interpolation (`interpolateParams=true`) to escape the placeholder limit.** Rejected: binding through `?` placeholders is the Bug-74/ADR-0139 value-fidelity safety property; interpolation would move value encoding into SQL-text escaping — a whole new silent-corruption surface for zero measured need.
