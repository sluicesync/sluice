# ADR-0042: MySQL bulk-copy throughput investigation

## Status

**Draft**, not yet accepted. No code shipped. Captures the
problem space + investigation paths so a future implementation
session has a clear starting point.

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
  the `--bulk-parallel-min-rows` threshold. The medium fixture's
  log shows 8 chunks per table running concurrently.
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

- `sluice-testing/local-rig/baselines.md` — empirical baselines
  cited above.
- v0.62.0 CHANGELOG entry — default-threshold change.
- ADR-0019 — Parallel within-table bulk copy (the architectural
  foundation; defines the reader/chunker; doesn't dictate the
  writer's concurrency model).
- ADR-0028 — Memory-bounded streaming (defines per-chunk byte
  cap; relevant to H2 chunk-size investigation).
- pgcopydb (PlanetScale's PG-native bulk-copy tool) — comparison
  point for "how fast can PG → PG go?".
