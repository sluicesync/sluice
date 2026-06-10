# Testing Strategy

Testing is a first-class concern for this project, not an afterthought. The tool is the kind of thing that fails silently — wrong rows in, wrong rows out, no exception thrown — and the only protection against that class of failure is a layered, automated suite that exercises every level of the abstraction.

This document defines the layers, what each layer is responsible for, and the tooling chosen for each.

## The layers

```
            ┌─────────────────────────────────────────┐
            │  Performance regression                 │  benchmark
            ├─────────────────────────────────────────┤
            │  Property-based sync correctness        │  rapid / gopter
            ├─────────────────────────────────────────┤
            │  Semantic equivalence (sqllogictest)    │  curated corpus
            ├─────────────────────────────────────────┤
            │  End-to-end integration                 │  testcontainers-go
            ├─────────────────────────────────────────┤
            │  IR & translation (golden files)        │  table-driven unit
            └─────────────────────────────────────────┘
```

Each layer catches a different class of bug. They are all meant to run automatically — the bottom three on every commit, the top two on a slower schedule (nightly or pre-release).

## Layer 1: IR and translation (unit, golden files)

**What it covers:** the type model and the translator. These are the most correctness-critical components and also the cheapest to test, so they get the most coverage by a wide margin.

**How:** table-driven Go tests over the IR types, plus golden-file tests for translation. A golden file pairs a known-good input (a MySQL `CREATE TABLE` statement, a Postgres column definition, a parsed metadata struct) with the expected IR or expected emitted DDL. Updating a golden file is a deliberate act with a code-review trail.

```
test/golden/
├── mysql_to_ir/
│   ├── tinyint_one.in.sql         # CREATE TABLE x (active TINYINT(1))
│   ├── tinyint_one.out.json       # IR: Boolean{}
│   ├── enum.in.sql
│   ├── enum.out.json
│   └── ...
├── ir_to_postgres/
│   ├── enum_default.in.json       # IR: Enum{Values: [...]}
│   ├── enum_default.out.sql       # CREATE TYPE ... AS ENUM (...)
│   ├── enum_text_check.in.json
│   ├── enum_text_check.out.sql    # text + CHECK constraint
│   └── ...
└── ...
```

The runner walks each subdirectory, loads each `.in.*` file, runs it through the corresponding translator, and asserts the output matches the `.out.*` file. New cases are added by writing two files. There is no per-case test code.

**Speed budget:** the entire layer runs in under a second. No databases involved, no I/O, no fixtures.

## Layer 2: End-to-end integration (testcontainers)

**What it covers:** the full pipeline against real database engines — driver behaviour, charset bugs, type coercion at the wire level, the actual `COPY` and `LOAD DATA INFILE` paths.

**How:** [`testcontainers-go`](https://github.com/testcontainers/testcontainers-go) spins up real MySQL and Postgres containers per test. A test seeds the source, runs the migration, and verifies the target.

A single end-to-end test looks like:

```go
func TestMigrate_MySQL_to_Postgres_BasicTypes(t *testing.T) {
    src := startMySQL(t)
    dst := startPostgres(t)

    seedMySQL(t, src, `
        CREATE TABLE users (
            id BIGINT UNSIGNED NOT NULL AUTO_INCREMENT PRIMARY KEY,
            email VARCHAR(255) NOT NULL,
            active TINYINT(1) DEFAULT 1,
            metadata JSON
        );
        INSERT INTO users (email, active, metadata) VALUES
            ('a@example.com', 1, '{"plan": "free"}'),
            ('b@example.com', 0, '{"plan": "pro"}');
    `)

    err := sluice.Migrate(ctx, sluice.Config{
        Source: src.DSN(),
        Target: dst.DSN(),
        Mode:   sluice.Simple,
    })
    require.NoError(t, err)

    assertRowCount(t, dst, "users", 2)
    assertColumnType(t, dst, "users", "active", "boolean")
    assertColumnType(t, dst, "users", "metadata", "jsonb")
    assertJSONValue(t, dst, "users", "email", "a@example.com", "metadata", `{"plan": "free"}`)
}
```

**Coverage targets:** the full Cartesian product of {basic types, edge-case types, indexes, foreign keys, generated columns, partitioned tables} × {simple mode, sync mode bootstrap} × {four directions}. That is a lot of cases; they are organised as table-driven tests over a fixture catalogue rather than as individual functions.

**Speed budget:** the layer is allowed to take a few minutes locally and ten or fifteen minutes in CI. Container startup is the dominant cost, so containers are reused across tests in the same package via a shared setup.

## Layer 3: Semantic equivalence (sqllogictest)

> **Status: not built (design target).** `test/sqllogic/` does not exist
> yet and no sqllogictest runner is wired up; this layer describes the
> intended design, kept here so the curation buckets below don't get
> re-derived from scratch when it's picked up. The closest shipped
> relative is the real-world DDL fixture corpus
> (`internal/translate/ddl_fixture_test.go`, `ddlfixture` build tag),
> which replays large real-application schemas through translation —
> schema-level, not query-result-level.

**What it covers:** the question that matters most to a real user: "if I run my application's queries against the migrated database, do I get the same answers?"

**How:** a curated subset of the [SQLite sqllogictest](https://www.sqlite.org/sqllogictest/doc/trunk/about.wiki) corpus, plus DoltHub's MySQL-flavored adaptations. Each test case is a sequence of `statement` and `query` lines with expected results. Existing Go runners are available — the [CockroachDB runner](https://github.com/cockroachdb/cockroach/tree/master/pkg/sql/logictest) is the most mature.

The workflow per case:

1. Run the case's setup statements against MySQL (the source).
2. Run the migration to Postgres (the target).
3. Run the case's queries against both source and target, compare results.
4. Pass if results match; fail with a diff if they don't.

**Curation buckets:**

- **Portable cases** — should pass cross-dialect post-migration. These are the assertion set.
- **Documented divergences** — cases where MySQL and Postgres legitimately differ (string concatenation, NULL ordering, integer division semantics, collation/case-sensitivity defaults). Each documented divergence has a note explaining why and a reference to where it surfaces in the type-mapping doc.
- **Skip** — cases that depend on dialect-specific syntax with no useful equivalent.

The curation itself is a deliverable: it forces us to enumerate the real semantic gaps between the engines instead of discovering them in production. The curation lives in `test/sqllogic/` with one subdirectory per bucket and a generated index that lists the count and source of each case.

**Speed budget:** runs nightly, not on every commit. Aiming for under thirty minutes for the portable bucket against all four migration directions.

## Layer 4: Property-based sync correctness

> **Status: partially built, differently than described.** The
> `pgregory.net/rapid` stateful property test sketched below is not
> built (`rapid` is not a dependency). What *does* exist is the
> generative **migrate round-trip fuzz harness**
> (`internal/pipeline/migrate_fuzz_roundtrip_integration_test.go`):
> random schema + data generation across all four directions, a smoke
> budget in every PR's integration run, and a weekly deep run with a
> fresh seed (`.github/workflows/fuzz-roundtrip.yml`) that uploads
> replayable failure fixtures. That covers the *snapshot/migrate*
> surface. The random-op **sync-convergence** property (CDC apply under
> arbitrary interleavings — historically the buggiest surface) remains
> unbuilt and is the single highest-value new test investment on this
> page; whether to build it is an open decision tracked in the repo
> audit (2026-06-09).

**What it covers:** the continuous-sync engine's ability to converge under arbitrary sequences of operations. Hand-written tests miss the interactions; a fuzzer finds them.

**How:** [`pgregory.net/rapid`](https://github.com/flyingmutant/rapid) is the recommended library — it supports stateful property tests with shrinking, which is essential for diagnosing sync bugs. Each property generates a random schema and a random sequence of inserts, updates, and deletes, applies them through the sync pipeline, and asserts the target converges to the source state.

```go
func TestSync_ConvergesUnderRandomOps(t *testing.T) {
    rapid.Check(t, func(t *rapid.T) {
        schema := genSchema(t)            // random table shape
        ops := genOps(t, schema, 1000)    // random sequence of CRUD

        src := startMySQL(t)
        dst := startPostgres(t)
        applySchema(t, src, schema)

        sync := sluice.StartSync(ctx, src, dst)
        for _, op := range ops {
            applyOp(t, src, op)
        }
        sync.WaitForCatchup(ctx)

        assertEqual(t, dump(src), dump(dst))
    })
}
```

**What it has caught historically (in similar tools):** out-of-order apply when the source emits events from concurrent transactions; lost updates when an `UPDATE` and an immediately-following `DELETE` collapse on the wire; type-coercion mismatches between the bulk-load path and the streaming-apply path.

**Speed budget:** runs nightly with a high iteration count; runs on every PR with a small smoke iteration count.

## Layer 5: Performance regressions

> **Status: manual harnesses, no CI gate.** There is no automated
> CI benchmark comparison. What exists: the `benchmarks/{pgcopydb,cdc,
> mysql}/` harnesses, run manually per performance arc with results
> recorded in the relevant ADR (house style — e.g. ADR-0076/0077/0078
> carry measured numbers from the 110 GB pgcopydb comparison, ADR-0080
> the MySQL index numbers), plus targeted `go test -bench` micro-
> benchmarks (e.g. `BenchmarkColdStartCopyWriter`). The CI-gated
> regression threshold below is a design target.

**What it covers:** the tool getting slower over time without anyone noticing.

**How:** a checked-in benchmark dataset (small enough to live in the repo or be generated deterministically; large enough to be meaningful) and a `make bench` target that records timings to a file. CI compares against a baseline and fails the build if a regression exceeds a threshold (e.g. 15%).

The dataset shape:

- Small (~10 MB): runs on every PR, catches obvious regressions.
- Medium (~1 GB): runs nightly, catches regressions that only show under realistic data volume.
- Large (~100 GB): runs pre-release on a dedicated machine.

**Metrics tracked:** total wall-clock time, peak memory, rows-per-second by table, indexing phase duration. Each metric is recorded with the commit hash so trends can be visualised over time.

## What we deliberately don't do

- **Mock the database.** Mocked tests have nothing to say about real database behaviour. The whole tool is about real database behaviour. The integration layer hits real engines.
- **Fuzz the parser separately.** The parsers (`vitess` for MySQL, `pg_query_go` for Postgres) are already extensively fuzzed by their upstream maintainers. We test our use of them, not them themselves.
- **Test the configuration loader exhaustively.** A handful of unit tests covering the override application logic is sufficient. Configuration is data; the data tests live in the golden-file layer.

## Local developer workflow

```bash
make test          # Layer 1 only (note: -race needs CGO; see make pre-commit for the conditional path)
make test-it       # Layers 1 + 2, ~5 minutes (containers)
make test-all      # today: same as test-it — the sqllogic/property tags match no files yet (Layers 3-4 unbuilt)
make bench         # Go micro-benchmarks; the at-scale harnesses live in benchmarks/ (manual)
```

The default `go test ./...` runs only the unit layer. Container-based and slow tests live behind build tags (`//go:build integration`) so the fast loop stays fast.
