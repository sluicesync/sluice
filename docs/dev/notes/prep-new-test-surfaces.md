# Prep — new test surfaces (DDL fixture + local-local MySQL rig)

Two test-coverage expansions the operator proposed 2026-05-14 after
reading a MySQL Enterprise blog post + observing the latency /
table-limit friction of the PlanetScale validation rig:

1. **DDL test fixture** seeded from `sqllogictest/test/ddl/createtable/createtable1.test` (Dolt fork of the SQLite consortium's logic-test corpus) — many `CREATE TABLE` statements with `statement ok` markers; lets sluice's schema-handling get exercised against a wider variety of real-world DDL shapes than the current synthetic test corpus.
2. **Local-local MySQL → sluice → MySQL rig** — two local MySQL containers (or one container with two databases) connected via sluice on the same machine. Eliminates PlanetScale latency / 2048-table-limit / SSL-handshake costs and lets us measure raw throughput.

Both warrant planning before implementation. This doc is the
planning artefact.

## Idea 1 — DDL test fixture from sqllogictest

### What

`createtable1.test` (and its siblings) is a SQLite-format
logic-test file: pairs of `statement ok` / `statement error`
markers followed by SQL DDL. For sluice's purposes, we ignore
`statement error` cases and harvest the `statement ok` CREATE
TABLE statements as a corpus. Each one is a real-world-ish CREATE
TABLE shape (column types, defaults, constraints, indexes,
multi-column PKs, etc.).

### Revised understanding (post-fetch)

The actual file (~13,725 lines, ~150-200 distinct CREATE TABLE
statements) is heavily MySQL-flavored — full of MySQL-specific
table options like `KEY_BLOCK_SIZE`, `INSERT_METHOD`,
`ROW_FORMAT`, `TABLE_CHECKSUM`, `UNION`, `DELAY_KEY_WRITE`, plus
liberal use of `COMMENT 'text...'` clauses. The "MySQL ∩ PG
portable subset" filter I'd originally proposed would drop the
majority of the corpus.

The realistic harness shape is:

1. Filter the corpus to MySQL-supported syntax (drop none, just
   normalize quoting and split statements).
2. Boot a testcontainers `mysql:8.0` and apply each `statement
   ok` DDL.
3. Read the resulting schema via sluice's MySQL schema reader.
4. For each table, assert structural invariants:
   - Table read without error
   - Column count matches the DDL's column count
   - Each column's IR type isn't `ir.Unknown`
   - PRIMARY KEY (if declared) round-trips via the IR
5. (Optional Phase 2) Emit MySQL DDL via sluice's MySQL writer
   and apply it to a fresh DB; confirm a second read yields the
   same IR shape.
6. (Optional Phase 3) Cross-engine MySQL→IR→PG-emit; assert PG
   accepts the translated DDL or refuses with a documented
   translator-gap message.

This is bigger than the original ~430 LOC estimate (more like
~700-900 LOC including the testcontainer dance + fixture
harness). Build tag: `integration ddlfixture` so the cost is
opt-in and doesn't impact default CI.

### Why this is valuable

### Why this is valuable

- **Surfaces gaps in sluice's DDL parsing / IR translation that
  synthetic tests miss.** sluice's current test corpus reflects
  what the maintainers have *thought of*; the sqllogictest corpus
  reflects what the SQL standard + various engine quirks have
  actually produced over decades.
- **Fast feedback loop.** A handful of `statement ok` failures
  surfaces specific missing features (e.g., `DEFAULT CURRENT_DATE`
  syntax, multi-column unique constraints, fractional-second
  TIMESTAMP precision, etc.) without needing a full validation
  cycle.
- **Reusable across engines.** Same corpus can be applied to MySQL
  source → MySQL target, MySQL → PG, PG → PG, etc. — each direction
  catches a different translation gap.
- **Provenance.** The corpus is upstream; sluice doesn't have to
  invent and maintain shapes. Updates come from Dolt's tracking.

### Risks / caveats

- **Dialect specificity.** The createtable1.test file is SQLite-
  flavoured. Many of its CREATE TABLE shapes WILL fail on MySQL or
  Postgres because they use SQLite-specific syntax (e.g., `INTEGER
  PRIMARY KEY AUTOINCREMENT`). We need to either (a) preprocess
  the file with a dialect filter, (b) accept that ~40-60% of
  `statement ok` cases will fail on the target and treat that as
  the expected baseline, or (c) limit ourselves to the
  dialect-portable subset.
  
  Recommendation: option (c). Filter for shapes that parse on
  both MySQL 8 and Postgres 16; the remainder is out of scope.
  Document the filter rules in the test file.

- **Licensing.** sqllogictest is MIT (compatible with sluice's
  Apache 2.0 per typical Apache-MIT compatibility). Vendor the
  fixture file under `internal/translate/testdata/sqllogictest/`
  with a NOTICE file pointing at the upstream. Don't fork — keep
  the file unchanged and apply the dialect filter at test-run
  time.

- **Volume.** createtable1.test is ~600 lines but only ~100-150
  distinct CREATE TABLE statements. Manageable for a focused
  test pass; not huge enough to bloat CI.

### Implementation sketch

1. New build-tag `ddlfixture` so the test only runs when an
   operator opts in (parser overhead isn't free for default CI).
   Roadmap entry says when to flip to default.
2. New test file `internal/translate/ddl_fixture_test.go`
   (build-tagged): loads the createtable1.test file via
   `embed.FS`, parses out the `statement ok` CREATE TABLE
   statements, applies the dialect filter, then feeds each
   surviving statement to:
   - MySQL `CREATE TABLE` parser → IR
   - IR → MySQL `CREATE TABLE` emit
   - Compare emitted DDL against source (modulo
     dialect-invariant normalisation)
3. Same harness for Postgres: PG parser → IR → PG emit.
4. Same harness for cross-engine: MySQL parser → IR → PG emit
   (and reverse). Flags translator-catalog gaps without needing a
   running database.
5. Refuses that surface a missing translator rule should fail the
   test loudly so the operator knows what to add.

### Sizing

- Fixture vendoring + NOTICE: ~50 LOC
- Dialect filter (regex-based first pass; can grow): ~80 LOC
- Test harness (3 directions): ~300 LOC
- **Total**: ~430 LOC + the embedded fixture file (~50 KB)

### Sequencing

Could land as v0.55.0 alongside the remaining Phase 1.5 items
(schema-preview annotation + backup-stream redaction) since it's
test-only and doesn't touch operator-facing surfaces.

## Idea 2 — Local-local MySQL throughput rig

### What

A docker-compose (or two `docker run` invocations) that boots two
local MySQL 8 containers on the same machine. A sluice process
between them streams from one to the other. A traffic_gen-like
writer drives the source at maximum sustainable rate; we measure:

- **Rows/sec applied** at the target
- **Bytes/sec applied** (rows × avg row size)
- **End-to-end latency** (source-INSERT timestamp → target visible)
- **CPU profile** of sluice during sustained load (pprof CPU
  profile captured during a 5-minute window)

### Why this is valuable

- **Eliminates network noise** that PS rig measurements include.
  Localhost UNIX-socket or short-loopback TCP latency is ~50µs vs
  PS's ~10-50ms cross-region. Latency floor lifts by 200×.
- **No 2048-table limit** so we can exercise high-table-count
  shapes (multi-tenant SaaS schemas with 5000+ tables).
- **No SSL/TLS handshake cost** — both connections are plain TCP
  inside Docker's bridge network or a Unix socket on the host.
- **Free.** No cloud cost. Operator-friendly for quick iteration.
- **Pinpoints sluice's own bottlenecks** vs. network/disk-bound
  bottlenecks. If sluice can sustain X rows/sec locally but only
  X/100 on PS, the gap is network — we know where to look.

### Risks / caveats

- **Local MySQL 8.0 in Docker has its own quirks** (Rancher Desktop
  ryuk-disabled env var, port conflicts, etc.). These are already
  documented in CLAUDE.md so onboarding cost is small.
- **Disk speed dominates at very high throughput.** On a laptop
  with a slow SSD the bottleneck shifts from sluice to fsync.
  Document this and offer a `--sync-disable` knob (MySQL's
  `sync_binlog=0` + `innodb_flush_log_at_trx_commit=0`) for the
  pure-throughput run.
- **No representativeness of production.** A local rig optimises
  for sluice's CPU/memory shape, not the cross-region production
  pattern. The PS rig stays the load-bearing fidelity test;
  local rig is the throughput/regression benchmark.

### Implementation sketch

1. New directory `sluice-testing/local-rig/` (or `sluice-validation/local-rig/`):
   - `docker-compose.yml` with two `mysql:8.0` services + a
     traffic_gen container
   - `bootstrap.ps1` / `bootstrap.sh` to start, create databases,
     seed schemas
   - `run-throughput.ps1` to launch sluice + traffic_gen + record
     metrics
   - `report.md` template the operator fills in after each run

2. Schema fixtures: small (10-table, 100k-row) + medium (100-table,
   1M-row) + large (1000-table, 10M-row). Each lets a different
   sluice-internal limit surface.

3. Metrics capture:
   - sluice emits `applier: apply latency` (per-change DEBUG) and
     `applier: batch latency` (per-batch DEBUG). Both already
     exist; just need to aggregate.
   - The throughput script tails the log, computes p50/p99/max
     per minute, plots a histogram.
   - Optional: bind `--pprof-listen=127.0.0.1:6060` so the
     operator can capture a CPU profile mid-run.

4. Reproducible baseline numbers, recorded per release. Each
   release's cycle includes a "throughput regression" check:
   run the medium fixture for 5 min; assert the rows/sec is
   within ±15% of the recorded baseline. Regressions surface
   loudly in the cycle report.

### Sizing

- docker-compose + bootstrap scripts: ~150 LOC
- throughput driver (traffic_gen-style, parameterised): ~200 LOC
- metrics aggregator (log-tail → histogram): ~250 LOC
- report template + runbook: ~100 lines of markdown
- **Total**: ~600 LOC + docs

### Sequencing

Could land as a `sluice-testing/local-rig/` directory addition
alongside (or independent of) sluice releases. Doesn't gate any
sluice release; serves the cycle-test workflow.

Two phases:
- **Phase 1**: just the harness + bootstrap + manual-run
  workflow. ~300 LOC. Operator runs ad-hoc to spot-check
  throughput per release.
- **Phase 2**: automated cycle integration. The cycle subagent
  runs the medium fixture every release; the report includes
  a row-throughput line. ~300 LOC of integration glue.

## Recommended sequencing for v0.55.0+

After v0.54.0 cycle clears clean, candidate next chunks (in suggested order):

1. **v0.55.0 PII Phase 1.5 closure**: schema-preview annotation + backup-stream redaction. Closes the last two deferred items so PII Phase 1.5 is fully complete. ~300 LOC.

2. **v0.56.0 DDL fixture test surface** (idea 1 above). Test-only; doesn't change operator surface. Could be paired with v0.55.0 if scope feels right. ~430 LOC.

3. **sluice-testing local-rig Phase 1** (idea 2 above). Could land as a sluice-testing PR (separate repo); doesn't gate sluice releases. ~300 LOC across the bootstrap + manual-run pieces.

4. **v0.57.0 PII Phase 2.a**: generic `mask:inner` + `mask:outer` + Luhn helper. ~120 LOC.

5. **v0.58.0 + later**: PII Phase 2.b strategies (country/format-specific masks) in two waves.

6. **v0.60.0+**: PII Phase 2.c randomized generators, Phase 3 dictionary, Phase 4 keyset persistence.

7. **Inline rotation** (chunk 14b phase 2, prep doc already committed): land any time the operator prioritises closing the v0.51.0 backup-rotation track. ~600-800 LOC.

## Open questions

1. **createtable1.test vendoring vs git submodule**: vendoring is simpler (no submodule init step in fresh clones); git submodule keeps fixtures current automatically. Recommendation: vendor with a version pin documented in NOTICE; refresh manually when upstream meaningfully changes.

2. **Local-rig disk layout**: in-container Docker volumes vs bind-mounted host paths. Volumes are simpler and isolated; bind mounts let operators inspect the on-disk InnoDB files. Recommendation: Docker volumes default, bind-mount via env-var override for advanced use.

3. **Throughput baseline storage**: where do we record `medium fixture: 12,500 rows/sec on M2 Pro` so regressions are caught? Recommendation: in `sluice-testing/local-rig/baselines.yaml`, keyed by hardware fingerprint. Regression check is best-effort if no matching baseline exists.

4. **Which fixtures from sqllogictest beyond createtable1?** The Dolt repo has hundreds of `.test` files (SELECT, INSERT, UPDATE, DELETE, JOIN, etc.). For sluice's purposes the DDL ones are most relevant; selecting from the data-manipulation ones for CDC coverage is a follow-on. Phase 1 stays DDL-only.

5. **Cross-engine fidelity vs portability filter**: should the dialect filter aim for "applies on both engines" (intersection of MySQL + PG features) or "applies on at least one engine" (union)? Intersection is strict but cleaner; union surfaces more bugs but more noise. Recommendation: intersection for Phase 1; union when the harness is stable.

## Pointers

- sqllogictest upstream: https://github.com/dolthub/sqllogictest
- The proposed fixture: https://github.com/dolthub/sqllogictest/blob/master/test/ddl/createtable/createtable1.test
- PII Phase 2 catalog: `docs/dev/notes/prep-pii-redaction-phase-2-strategy-catalog.md`
- PII Phase 1 prep: `docs/dev/notes/prep-pii-redaction-phase-1.md`
- Inline rotation prep: `docs/dev/notes/prep-backup-chain-rotation.md`
- Vultr continuous-validation prep: `docs/dev/notes/prep-continuous-validation-on-vultr.md`
