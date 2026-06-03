# ADR-0042: MySQL bulk-copy throughput investigation

## Status

**Accepted as a discovery doc** (2026-05-15). **Phase A executed
2026-05-15** (after PII Phase 4 / ADR-0041 shipped as v0.63.0, per
the sequencing below) — see "Phase A findings". Phase A added
DEBUG-gated per-chunk/per-batch wall-time instrumentation (no
behaviour change) and produced decisive ground truth: H1 + H2
falsified, H3/H5 confirmed as the primary MySQL-side hypotheses,
and two new findings (N1: PG parallel-copy silently disabled on a
freshly-loaded source due to stale `reltuples`; N2: PG → PG is
not bulk-copy-bound on this fixture). The original "PG uses
8-chunk parallel COPY" baseline framing is corrected. **Phase B
executed 2026-05-15** (fixture variant + dual-engine pprof) — see
"Phase B findings": **H5 falsified** (no gain from removing
JSON/TINYINT), and a structural finding that **both engines'
parallel-copy path uses the same generic `database/sql`
batched-upsert writer** — neither LOAD DATA nor pgx `CopyFrom` is
on the bulk-copy hot path, which reframes H3 and the ADR's
"PG uses COPY-binary" premise. Phase C (route the chunk writer
onto the native bulk loader) is proposed but **not implemented**;
it is a separate gated decision. The N1 decision remains open.

Recommendation #3 (re-run the medium baseline against v0.62.0 and
record the local-rig baseline) is **satisfied**: the v0.62.0
post-release validation cycle re-ran the medium fixture on the
local-local rig at ~85.7k–89.4k rows/sec under default config
(vs the v0.61.0 default-config ~33k baseline), recorded the
baseline, and additionally
captured the per-table `information_schema.tables.table_rows`
estimate vs actual `COUNT(*)` delta (0.10–2.6% undershoot, and
*noisy* — a ~2.7% swing across `ANALYZE` runs on identical data),
which directly feeds the H-list in this ADR. See the v0.62.0
regression cycle.

## Context

The v0.62.0 local-local rig baselines surfaced a concrete
~2.3× throughput gap between MySQL → MySQL and PG → PG on the
same machine, same fixture, same sluice version:

| Direction | Configuration | Rows/sec | Wall (2.5M rows) |
|---|---|---|---|
| PG → PG | defaults | ~125,000 | 20s |
| MySQL → MySQL | `local_infile=ON`, threshold=50k | ~54,000 | 46s |
| MySQL → MySQL | `local_infile=ON`, threshold=100k (v0.61.0 default) | ~33,000 | 75s |
| MySQL → MySQL | `local_infile=OFF`, threshold=100k | ~28,000 | 88s |

The threshold-default change shipped as v0.62.0 closes the largest
gap operators see by default (the ~95-99k row tables that fell to
single-reader). The remaining ~2.3× gap is real and reader/writer
architectural, not a config-tuning issue.

This ADR scopes the investigation: what's slow, what isn't, and
which optimizations have the best signal-to-effort ratio. **No
code is proposed yet** — the next step after this ADR is accepted
should be a small instrumentation pass to localize the bottleneck
before implementing any fix.

## What's already true

- **PG writer uses COPY-binary protocol** (the canonical fast
  path). Per-row encoding cost is real but well-understood; rows
  flow through a streaming `pgx.Conn.CopyFrom` call.
- **PG reader uses chunked parallel COPY-out** for tables above
  the `--bulk-parallel-min-rows` threshold. ⚠️ **Corrected by
  Phase A (finding N1): this did NOT hold on the medium fixture.**
  PG's parallel-eligibility row estimate reads `pg_class.reltuples`,
  which is ≈0 until `ANALYZE` runs, so a freshly-loaded source
  silently took the single-reader path on every table. The
  "8 chunks per table" annotation in `local-rig/baselines.md` was
  inferred, not observed; the v0.62.0 PG baseline was single-reader.
  See "Phase A findings" below.
- **MySQL writer prefers LOAD DATA INFILE** when the server has
  `local_infile=ON`, falling back to batched INSERT VALUES
  otherwise. LOAD DATA is faster than INSERT by ~5-10× in
  isolation per the WARN-log estimate, but the v0.62.0 baseline
  only saw a ~17% gain on the `local_infile=OFF` → `ON` flip
  (28k → 33k rows/sec at the same threshold).
- **MySQL reader does parallel chunked reads** for tables above
  threshold, same as PG. Architecturally symmetric on the read
  side.

## Hypotheses for the gap

Ordered by my prior probability they're the dominant factor:

### H1: The MySQL writer's LOAD DATA path serializes per-table

Even when readers run N=8 chunks in parallel, the writer may
serialize them through a single `LOAD DATA INFILE` invocation
per chunk + the chunk-finalize handshake. The v0.62.0 baseline
shows the WARN-log "fallback to batched INSERT" is gone, but
the rate per chunk is ~30-40k rows/sec — well below the rates
LOAD DATA achieves in isolation in mysqldump-style tests
(~200-500k rows/sec on the same hardware).

**Test**: instrument the writer to log per-chunk wall time and
identify whether the chunks run truly concurrently or serialize
on a shared mutex / connection.

### H2: Per-chunk overhead dominates at 12.5k rows/chunk

The medium fixture's 100k-row tables split into 8 chunks of
~12,500 rows each. If LOAD DATA's per-call overhead (auth
handshake, server-side staging, commit) is ~50-100ms regardless
of payload, then 8 chunks × 100ms = 800ms per table just in
overhead — meaningful at 25 tables × 800ms = 20s of pure overhead
on a 46-second wall time.

**Test**: re-run with `--bulk-parallelism=2` (large chunks) vs
`--bulk-parallelism=16` (small chunks) and see which is faster.
If the curve favors fewer-larger chunks, H2 is confirmed.

### H3: The Go MySQL driver's protocol encoding is the bottleneck

go-sql-driver/mysql encodes each value individually via
reflection. For 100k rows × ~10 columns = 1M individual encode
calls per table. PG's pgx uses a tighter
`(*pgtype.Map).Encode(... binaryFormat ...)` path that's been
heavily optimized.

**Test**: profile a single-table bulk copy via `pprof` and see
which call stacks dominate. The MySQL path is `go-sql-driver`
internals; the PG path is `pgx` internals. Compare flame graph
distributions.

### H4: The local-rig environment is misleading

Rancher Desktop's lima VM, Windows host file system, Docker
volume drivers — any of these could introduce per-syscall
overhead that disproportionately affects MySQL's protocol shape
(more round-trips than PG's COPY).

**Test**: run the same fixture against a native Linux MySQL
install (no Docker) and compare. If the gap closes, the local-rig
is a measurement artifact, not a sluice issue.

### H5: TINYINT/JSON column encoding has hidden cost

The medium fixture uses `JSON` (audit_log.details, events.payload)
and `TINYINT(1)` (products.active). MySQL JSON column encoding via
LOAD DATA requires server-side parse-from-text; PG's JSONB COPY
takes the binary form. If JSON columns dominate, MySQL is doing
real work the PG path skips.

**Test**: re-run with a fixture that has NO JSON columns and
NO TINYINT(1). Compare to the original.

## Investigation plan

A focused three-phase investigation, each phase ~half a day:

### Phase A: instrumentation (no behaviour change)

1. Add per-chunk DEBUG-level wall-time logging on both MySQL +
   PG writers: chunk-id, row count, bytes, start time, end time.
2. Re-run the medium fixture on both engines.
3. Examine the log to confirm: are chunks running truly in
   parallel (overlapping start/end timestamps)?
4. Identify per-chunk overhead (chunk wall time vs payload rate
   per chunk) and total overhead vs payload share.
5. **Output**: a baseline measurement of "where is the time going."

### Phase B: hypothesis-driven testing

Pick the most likely hypothesis from Phase A's data and target it:

- If H1 (writer serialization): audit the MySQL writer's locking
  model. Look for shared mutex, single connection per table, or
  similar.
- If H2 (per-chunk overhead): empirical sweep `--bulk-parallelism`
  values and chart the curve. Pick the new default + document.
- If H3 (driver encoding): pprof + flame graph. If the
  go-sql-driver hot path is identifiable as fixable, file an
  upstream issue or land a sluice-side encoding shim.
- If H4 (environment): native Linux benchmark.
- If H5 (column types): fixture variant.

### Phase C: implementation

If Phase B identified a clear win, implement + ship. If multiple
small wins exist, bundle into a single release.

## Phase A findings (2026-05-15 — executed)

Phase A was executed end-to-end on the local-local rig (Win11 +
Rancher Desktop, medium fixture: 25 tables × 100k rows = 2.5M
total, both engines, default config, `--log-level=debug`).
Instrumentation: per-chunk + per-batch wall-time DEBUG logging
at the engine-neutral pipeline chunk path
(`internal/pipeline/migrate_parallel.go::copyChunk`, log key
`adr0042:`). Retained as a permanent diagnostic artifact gated
behind `--log-level=debug` (same disposition as the ADR-0033/0036
verify probes), so Phase B can re-run the same measurement.

**Measured aggregates:**

| Run | Wall | Rows | Path actually taken |
|---|---|---|---|
| MySQL → MySQL | 29.4s | 2.49M | 8-way parallel on all 25 tables |
| PG → PG (fresh source) | 13.0s | 2.50M | **single-reader on all 25 tables** |
| PG → PG (post-`ANALYZE` source) | 13.3s | 2.50M | 8-way parallel on all 25 tables |

Per-chunk rate under *identical* 8-way parallelism: **MySQL
~15.4–17.5k rows/sec/chunk vs PG ~30–34.5k** (~2× per-stream).

**Hypothesis verdicts:**

- **H1 (MySQL writer serializes per-table) — FALSIFIED.** All 8
  chunk goroutines share an identical `t_start` and their
  `[t_start, t_end]` intervals fully overlap (~96 ms spread on a
  ~0.8 s table). No shared-mutex / single-connection serialization.
- **H2 (per-chunk fixed overhead dominates) — FALSIFIED.**
  `non_batch_wall` (rowcount kickoff + checkpoint writes) is only
  6–13% of `chunk_wall`; per-batch cost scales with row count, not
  a fixed ~50–100 ms per-call penalty.
- **H3 (driver per-value protocol encoding) / H5 (JSON/TINYINT
  column codec) — LIVE; now the primary hypotheses.** Under
  identical orchestration, same host, same fixture shape, the gap
  is entirely in the per-row read+write data path. This is the
  Phase B pprof + fixture-variant target.
- **H4 (local-rig artifact) — unlikely dominant.** Same host,
  same Docker, same rig; PG is 2× faster per stream → the cost is
  MySQL-protocol-specific, not a uniform host tax. A native-Linux
  cross-check remains a Phase B nicety, not a blocker.

**New findings (not in the original hypothesis list):**

- **N1 — PG parallel-copy is gated on stale planner stats.** The
  PG parallel-eligibility row estimate reads `pg_class.reltuples`
  (≈0 until `ANALYZE`/autovacuum populates it). On a freshly
  loaded/restored source — *exactly the migrate cold-start case* —
  every PG table reports `~0 rows; below --bulk-parallel-min-rows`
  and silently takes the single-reader path. MySQL's analogous
  `information_schema.tables.table_rows` is also an estimate but
  InnoDB populates it on load, so MySQL does not exhibit this. This
  is an asymmetric, silent correctness/perf bug independent of the
  throughput question, and it invalidates this ADR's original
  "PG uses 8-chunk parallel COPY" baseline framing.
- **N2 — PG total wall is insensitive to parallel-copy on this
  fixture.** PG single-reader (13.0s) ≈ PG 8-way parallel (13.3s).
  PG → PG is bound by the non-bulk-copy phases (schema read, DDL,
  index/constraint creation per table), not copy throughput.
  MySQL → MySQL *is* bulk-copy-bound (parallel 29.4s vs the
  historical v0.61.0 single-reader 75s). The two engines have
  different bottleneck profiles; the headline "2.3× gap" conflates
  them.

**Phase B is now precisely scoped:** pprof a single-table MySQL
bulk copy, compare the go-sql-driver write hot path against pgx's
`CopyFrom`; run the no-JSON / no-TINYINT fixture variant to
isolate H5 from H3. N1 is a separate decision (fix the PG estimate
vs. document the `ANALYZE`-first operator step) tracked outside
the Phase B perf work.

## Phase B findings (2026-05-15 — executed)

Phase B was executed on the local-local rig (Win11 + Rancher
Desktop), with a **from-source build** of `sluice` (`go build
./cmd/sluice` at the v0.63.1 tree — not a released binary) so the
permanent `adr0042:` DEBUG instrumentation is present. Two
experiments: (1) the H5-vs-H3 fixture variant, (2) a CPU profile of
each engine's bulk-copy path via the existing `--pprof-listen`
endpoint (`net/http/pprof`; no throwaway code, nothing to clean up).

### Experiment 1 — fixture variant (H5 vs H3)

A faithful `medium-25t-100k-noenc.sql` variant was authored: every
`JSON` column → `VARCHAR(255)`, every `TINYINT(1)` → `INT`, all row
counts/shapes identical, and the changed columns populated with
JSON-shaped text of comparable byte width (`CONCAT('{"idx": ', n,
', "kind": "', …, '"}')`) so payload bytes are held ~constant —
otherwise H5 would be confounded with "less data". MySQL → MySQL,
default config, `--log-level=debug`, two runs each.

Per-chunk `adr0042: chunk done` rows/sec (200 chunks/run, 25 tables
× 8 chunks):

| Run | mean | median | p10 | p90 |
|---|---|---|---|---|
| standard #1 | 18,209 | 17,821 | 16,061 | 21,614 |
| standard #2 | 16,283 | 15,937 | 13,822 | 19,365 |
| noenc #1 | 17,048 | 17,077 | 15,042 | 19,283 |
| noenc #2 | 16,973 | 16,770 | 14,939 | 19,439 |

The two standard runs **straddle** the two noenc runs; the noenc
mean is within the standard run-to-run noise band (±~12%). Drilling
into the directly-changed tables specifically (`audit_log`,
`events` = JSON→VARCHAR; `products`, `feature_flags`, `webhooks` =
TINYINT(1)→INT): no consistent improvement — `audit_log` was
*faster* under standard on one comparison and slower on the next;
`products`/`feature_flags`/`webhooks` moved within ±15% with no
directional signal. Total wall: standard 29s, noenc 30–31s.

**Verdict: H5 (JSON/TINYINT column codec) FALSIFIED as a dominant
factor.** Removing the JSON and `TINYINT(1)` types produced **zero
throughput gain**. The cost is in the **general per-value /
per-batch data path (H3 territory)**, not a column-type-specific
codec.

### Experiment 2 — CPU profile, both engines

CPU profiles captured during a full medium-fixture migrate (all 25
tables exercise the identical per-row read+write path; Experiment 1
proved the JSON/TINYINT codec immaterial, so the whole-fixture run
is representative and gives a clean steady-state sample window — a
single 1M-row table copies in ~7 s, too short to sample cleanly).

**Headline structural finding (reframes the ADR): on the
parallel-copy hot path, *both engines use the same generic
`database/sql` batched-upsert writer* — neither LOAD DATA nor pgx
`CopyFrom` is on the bulk-copy path.** The parallel-copy
orchestrator (`copyChunk`) writes through the `ir.IdempotentRowWriter`
surface (`iw.WriteRowsIdempotent`). On MySQL that is
`(*RowWriter).writeBatchedIdempotent`; on PG it is
`(*RowWriter).writeViaBatchIdempotent`. Both build a multi-row
`INSERT … ON CONFLICT/ON DUPLICATE KEY UPDATE` via `buildBatchUpsert`
+ `flattenArgs` and ship it through `database/sql.(*DB).ExecContext`.
The ADR's "What's already true → PG writer uses COPY-binary
protocol" assumption **does not hold for the medium-fixture
bulk-copy path** — `CopyFrom` exists in the codebase but the
resumable parallel-copy path never calls it. The two engines'
write hot paths are structurally identical, which is exactly why
H5 showed no signal and why the per-stream gap is *not* a
go-sql-driver-vs-pgx codec story.

MySQL profile (`cpu-mysql.pprof`, 18 s sample, top flat frames):

| flat% | frame |
|---|---|
| 11.1% | `runtime.semasleep` |
| 8.1% | `runtime.runqgrab.osyield.func1` |
| 6.6% | `runtime.cgocall` |
| 4.0% | `runtime.selectgo` |
| 3.5% | `runtime.semawakeup` |

PG profile (`cpu-pg.pprof`, 8 s sample, top flat frames):

| flat% | frame |
|---|---|
| 8.8% | `runtime.semasleep` |
| 5.3% | `runtime.cgocall` |
| 4.8% | `runtime.runqgrab.osyield.func1` |
| 3.4% | `runtime.unlock2` (cum), `runtime.selectgo` 2.4% |
| 2.7% | `runtime.semawakeup` |

Both profiles are dominated by the **same** costs: Go scheduler
churn (`semasleep`/`semawakeup`/`runqgrab.osyield`/`stealWork`),
`runtime.cgocall` (Windows + Docker syscall boundary — the net I/O
to the containers), channel `selectgo`, and `runtime.lock2` /
`database/sql.withLock` (the shared `*sql.DB` pool mutex). The
per-chunk writer `-list` confirms the time inside the writer splits
across the streaming-channel `select` (MySQL 1.02 s / PG 0.51 s
cum), `ir.ApproximateRowBytes` (0.24 s / 0.20 s — the byte-cap
accounting on every row), and the `flush()` → `ExecContext`
round-trip (1.86 s / 2.81 s). **No JSON/type codec frame appears in
either top-40.** `go-sql-driver/mysql.(*mysqlStmt).writeExecutePacket`
is present but small (~0.48 s cum, ~3%); pgx's binary-format
encoders do not appear at all because `CopyFrom` is not on this
path.

### H3-vs-H5 verdict

- **H5 — FALSIFIED.** Fixture variant: no gain from removing
  JSON/TINYINT. Profile: no column-codec frame in either engine's
  hot set.
- **H3 — partially confirmed but re-scoped.** The bottleneck *is*
  the general per-value/per-batch data path, not a column codec —
  but it is **not** "go-sql-driver encodes each value via
  reflection while pgx uses a tighter binary path", because the PG
  bulk-copy path here is *also* `database/sql` batched-upsert, not
  `CopyFrom`. The dominant cost on both engines is the synchronous
  read-channel → batch → `ExecContext` round-trip plus the
  scheduler/cgo/pool-mutex tax around it. The per-stream MySQL-vs-PG
  rate gap observed in Phase A is therefore *server-side / wire
  round-trip shape* (MySQL's multi-row `INSERT … ON DUPLICATE KEY
  UPDATE` parse+apply vs PG's `INSERT … ON CONFLICT`), **not**
  driver encoding and **not** column types.

This is a cleaner result than either original hypothesis: the
single biggest lever is **getting off the generic idempotent-batch
`ExecContext` path onto the engine-native bulk loader (LOAD DATA /
`CopyFrom`) for the parallel-copy path**, which today is bypassed
entirely.

### Proposed Phase C options (NOT implemented — data-supported only)

Ranked by signal-to-effort from the Phase B data:

1. **Route the parallel-copy chunk writer through the native bulk
   loader instead of `WriteRowsIdempotent`.** Highest signal. The
   resumable parallel path uses the idempotent batched-upsert
   writer for crash-resume idempotency, but pays the generic
   `ExecContext` tax on every batch and never touches LOAD DATA /
   `CopyFrom`. Option: use the native loader for the first pass of
   each chunk and fall back to idempotent upsert only on
   resume/retry. Medium effort (touches the writer-selection logic
   in `copyChunk` + both engines' writers); largest expected win
   because it changes the actual hot frame.
2. **Reduce per-row overhead in the batch loop.**
   `ir.ApproximateRowBytes` is ~1–2% flat on *every* row purely for
   the ADR-0028 byte-cap; the streaming-channel `select` is another
   3–6% cum. Low effort, modest win (amortise the byte estimate, or
   widen the channel/batch to cut select frequency). Opportunistic.
3. **Investigate the cgo/syscall tax.** `runtime.cgocall` is
   5–7% flat on *both* engines — a uniform host tax (Windows +
   Rancher Docker net path), consistent with Phase A's H4
   "unlikely dominant but present". A native-Linux re-profile would
   quantify how much of the absolute number is rig artifact vs
   real. Low effort (one Linux run), diagnostic only — doesn't
   change the relative MySQL-vs-PG story.
4. **Re-baseline the ADR's premise.** The "PG writer uses
   COPY-binary" claim in *What's already true* is wrong for the
   bulk-copy path and should be corrected; the headline "2.3× gap"
   conflates copy-bound MySQL with not-copy-bound PG (Phase A N2)
   *and* assumed a fast PG path that isn't taken. Whether the gap
   is worth closing should be re-evaluated against option 1's
   expected ceiling. Doc-only, no code.

Recommendation for the Phase C gate: option 1 is the only one that
targets the measured dominant cost; options 2–4 are
opportunistic/diagnostic. No fix is proposed here — Phase C is a
separate, gated decision.

## What's out of scope

- **Asynchronous writer**. The current model is synchronous
  reader → writer per chunk. Going async (writer accumulates a
  ring buffer, applies on its own goroutine) is a larger change
  and out of scope for this ADR. If Phase B surfaces evidence
  that read/write back-pressure is the bottleneck, that's a
  separate ADR.
- **Server-side tuning** (innodb_buffer_pool_size etc.). Operators
  control their server; sluice should be fast on default
  configurations.
- **Cross-region / cross-host throughput**. The local-rig is
  same-host. Cross-region is dominated by network latency, not
  protocol efficiency, and is the PlanetScale rig's domain.

## Open questions

1. **Is 2.3× a real problem?** PG → PG at 125k rows/sec is plenty
   for most workloads — operators running 10M-row migrations see
   it complete in ~80s. MySQL → MySQL at 54k rows/sec finishes
   the same workload in ~185s, still acceptable. Whether the gap
   is worth closing depends on operator urgency. Recommendation:
   the v0.62.0 default-change closed the most painful gap
   (default → default); further work is opportunistic, not blocking.

2. **Could PG → PG be even faster?** The 125k rows/sec number isn't
   a ceiling — pgcopydb reports 300-500k rows/sec on similar
   hardware. If the PG path also has headroom, the gap question
   becomes "can we lift both to similar absolute numbers?" rather
   than "can we close the gap?".

3. **Cross-engine MySQL → PG / PG → MySQL?** Cross-engine paths
   add a translator layer + per-row IR encoding. They're
   inherently slower than same-engine; not a priority. Same-engine
   numbers are the load-bearing measurements for sluice's "is it
   fast enough" pitch.

## Recommendation

1. **Accept this ADR as a discovery doc**, not an implementation
   commitment. The investigation should happen on operator
   demand or when other work surfaces evidence of the
   bottleneck.
2. **Defer Phase A** (instrumentation) until the next time
   throughput is the priority — likely after PII Phase 4
   (keyset) and any other near-term roadmap items land.
3. **Re-run the medium baseline against v0.62.0** to confirm
   the new default produces the expected ~50-55k rows/sec on
   the MySQL side; record in `local-rig/baselines.md`. (This is
   a 5-minute task; can happen anytime.)
4. **Don't change behaviour for v0.62.x patches** based on this
   ADR. The 80k default already absorbed the operator-visible
   pain. Any further work goes in a v0.63.0+ release with its
   own focused ADR.

## References

- Local benchmarking-rig baselines — the empirical baselines
  cited above.
- v0.62.0 CHANGELOG entry — default-threshold change.
- ADR-0019 — Parallel within-table bulk copy (the architectural
  foundation; defines the reader/chunker; doesn't dictate the
  writer's concurrency model).
- ADR-0028 — Memory-bounded streaming (defines per-chunk byte
  cap; relevant to H2 chunk-size investigation).
- pgcopydb (PlanetScale's PG-native bulk-copy tool) — comparison
  point for "how fast can PG → PG go?".
