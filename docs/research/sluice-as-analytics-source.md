# Sluice as an analytics-friendly source — research findings

**Status:** Research-only. No code changes proposed. Companion to [`docs/research/apache-arrow-findings.md`](apache-arrow-findings.md), which deferred Shape A (Arrow as IR row representation). This doc covers the narrower, demand-driven framing: how sluice surfaces data to operators' analytics stacks without becoming an analytics tool itself.

**Bottom line.** Three orthogonal surface candidates exist; the right v1 pick is **`sluice backup export-as-parquet` one-shot transcode** built on `parquet-go/parquet-go`. DuckDB integration is a documentation recipe, not a sluice subcommand — operators with the appetite for DuckDB already know how to drive it. Arrow Flight stays deferred — the dep-weight cost is too high for the operator persona breadth it serves today.

## What this doc is for

Operators running OLTP databases increasingly want the migration tool to also be the bridge to their analytics stack. Three orthogonal ideas surfaced in conversation share an underlying theme: sluice as the data-out point for analytics-friendly consumption. This doc names them, sketches their concrete shape, and ranks them by `(dependency weight × operator persona breadth)`.

It does NOT propose a code chunk. The output is a recommendation the roadmap can promote to a chunk when an operator with concrete demand surfaces.

## Operator personas

| Persona | Description | Demand for analytics surfaces |
|---|---|---|
| **OLTP-only** | Single production database, no analytics tier; sluice's job is migration and backup. | None. Adding any analytics surface is wasted complexity for this operator. |
| **OLTP + ad-hoc** | Production database; occasional analytics via direct queries, BI tools, or a power user with DuckDB / pandas. | Light. A documented "read backup chunks with DuckDB" recipe would help; a dedicated subcommand probably overkill. |
| **OLTP + warehouse pipeline** | Production database with an ETL pipeline into Snowflake / BigQuery / Redshift. Sluice currently isn't part of that pipeline. | Medium. A Parquet export from sluice's already-captured backup chunks could replace an upstream Debezium → S3 → COPY step. The operator's existing pipeline already speaks Parquet. |
| **Analytics-first / lakehouse** | Sluice IS the data-out point. Backups feed a lakehouse (Iceberg, Delta, Hudi); CDC streams feed columnar buffers. | High but rare today. Arrow Flight or columnar CDC would matter. Persona shows up infrequently; the value-per-implementation ratio is low. |

The bell-curve of operator demand sits squarely on **OLTP + ad-hoc** and **OLTP + warehouse pipeline**. The analytics-first persona is real but not the majority — every roadmap chunk it justifies needs evidence (a specific operator request) before commit.

## Surface candidates

### Surface 1 — `sluice backup export-as-parquet` (one-shot transcode)

**Shape.** A subcommand that reads existing JSON-Lines backup chunks (from local FS, S3, GCS, Azure Blob — every backend `BackupStore` already supports) and emits Parquet files alongside them or to a separate destination. Read-side semantics stay unchanged: round-trip back into MySQL / PG keeps the existing JSON-Lines path. Parquet is **exit-only** — sluice never reads its own Parquet output back.

**Why exit-only matters.** The proto-ADR for Apache Arrow flagged the type-mapping problem as the core difficulty: PG `DECIMAL(38, 12)` → Arrow `Decimal128` is lossy; PG `TIMESTAMP WITH TIME ZONE` → Arrow `Timestamp[us, UTC]` strips the operator-visible TZ; PG `UUID` → Arrow `FixedSizeBinary[16]` requires byte-order handling. Lossless round-trip is a 30-rule translation surface plus per-rule unit tests. **Exit-only collapses the problem to "best-effort columnar":** sluice emits the closest reasonable Arrow type, documents the lossy edges, and lets the downstream tool re-impose semantics if needed. Operators choosing the export are choosing a one-way bridge.

**Library.** `parquet-go/parquet-go`. The arrow-findings doc already worked through the comparison: `parquet-go/parquet-go` is the smaller / more focused choice when "Parquet files written to FS or object storage" is the entire scope. ~5 new direct/indirect modules vs ~15+ for `apache/arrow-go/v18`. Both are pure Go; no CGO.

**CLI shape (sketch).**
```bash
# Transcode an entire chain from one bucket to another.
sluice backup export-as-parquet \
  --source-bucket s3://prod-backups/postgres-main/ \
  --output s3://analytics-lake/postgres-main/parquet/ \
  --since 2026-04-01

# Transcode a single full backup locally.
sluice backup export-as-parquet \
  --source ./backups/full-20260501-100000/ \
  --output ./parquet/full-20260501-100000/
```

Per-table chunk-by-chunk transcode; Parquet row groups align 1:1 with sluice chunks (preserves the existing operator-visible chunk concept). Parquet's metadata block records source-chunk SHA-256 hashes so the operator can cross-reference the Parquet file with the original chunk via `backup verify`. Compression: Parquet's default zstd (matches the Phase 2 recommendation in [`compression-benchmark.md`](../dev/notes/compression-benchmark.md)).

**Type-mapping edges (incomplete list — covered in code chunk if/when this lands):**

- `UUID` → Parquet `BYTE_ARRAY` with `LOGICAL_TYPE=UUID`. Bytes laid out as canonical hyphenated form (matches sluice's IR contract). Operators using PyArrow / DuckDB / Spark will see the value as a string they can re-parse.
- `Geometry` → Parquet `BYTE_ARRAY` with WKB body + GeoParquet `geometry_columns` metadata key. Follows [the GeoParquet spec](https://geoparquet.org/); preserves SRID + subtype on round-trip into GeoPandas / DuckDB-Spatial. The dep is metadata-only (no library import) so the cost is documentation only.
- `JSON` / `JSONB` → Parquet `BYTE_ARRAY` with `LOGICAL_TYPE=JSON`. Native types in PG (`JSON`) and MySQL (`JSON`); the columnar tool can elect to `json_extract()` into nested columns on read if it wants.
- `Decimal` → Parquet `DECIMAL(precision, scale)` when precision ≤ 38; falls back to `BYTE_ARRAY` with operator-visible note for `NUMERIC(unbounded)` (PG's unbounded numeric isn't representable in Parquet's bounded decimal).
- `Array` → Parquet `LIST<element>` recursively. Nested arrays supported up to a documented depth limit (Parquet itself imposes one).
- `Enum` → Parquet `BYTE_ARRAY` with `LOGICAL_TYPE=STRING`. The values list lands in the Parquet schema metadata for tooling to recover the discrete-value semantics.
- `Time WITHOUT TIME ZONE` → Parquet `INT64` with `LOGICAL_TYPE=TIME(MICROS, isAdjustedToUTC=false)`.
- `Timestamp WITH TIME ZONE` → Parquet `INT64` with `LOGICAL_TYPE=TIMESTAMP(MICROS, isAdjustedToUTC=true)`. The operator-visible TZ is stripped (Parquet stores normalized UTC); document this as a known lossy edge.

**Estimate.** ~600-1000 LOC for the writer + tests + 4 worked-example integration tests (one per persona-relevant corpus shape). Goes through the existing `BackupStore` abstraction so cloud-backend reuse is automatic.

**Worked example — Persona 3 (warehouse pipeline).** A Postgres-source operator runs `sluice backup full` nightly into S3 (today's behaviour). After Surface 1 lands, an additional cron step runs `sluice backup export-as-parquet --since=yesterday` against the same chain and writes to an analytics S3 prefix. Their Snowflake `COPY INTO` job consumes from that prefix using Snowflake's native Parquet reader. No more Debezium-and-jq pipeline gluing JSON-Lines into Parquet.

### Surface 2 — DuckDB integration (recipe, not subcommand)

**Shape.** DuckDB already reads Parquet, JSON-Lines, and CSV natively. The `read_json_auto` / `read_parquet` functions accept S3 / GCS URIs directly. Sluice doesn't need a subcommand; it needs a documentation page showing the recipe.

**The recipe** (proposed for `docs/cookbook/duckdb-on-sluice-backups.md` once Surface 1 ships):

```bash
# Read sluice backup chunks directly with DuckDB. JSON-Lines path —
# works against any backup, no Parquet export needed.
duckdb -c "SELECT * FROM read_json('s3://prod-backups/postgres-main/full-*/chunks/users-*.jsonl.gz') WHERE created_at > '2026-04-01' LIMIT 100;"

# Or with the Parquet export from Surface 1 — faster, predicate-pushdown.
duckdb -c "SELECT * FROM read_parquet('s3://analytics-lake/postgres-main/parquet/*/users.parquet') WHERE created_at > '2026-04-01';"
```

DuckDB's `httpfs` extension covers S3 + GCS auth via standard env vars; Azure Blob support landed in DuckDB 0.10. The operator already has DuckDB installed (Persona 2 by definition); sluice just makes its outputs greppable from there.

**Why not a sluice subcommand.** Two reasons:

1. The DuckDB ecosystem moves faster than sluice's release cadence. A `sluice backup query --duckdb` subcommand pins a DuckDB version sluice has to track; operator-driven DuckDB usage doesn't have that constraint.
2. The operator with appetite for DuckDB already knows how to drive it. Wrapping the recipe in a sluice subcommand adds an abstraction layer they didn't ask for.

**Estimate.** Zero code; ~1 day to write the recipe doc + verify the examples against a real chain.

### Surface 3 — Apache Arrow Flight (deferred)

**Shape.** [Apache Arrow Flight](https://arrow.apache.org/blog/2019/10/13/introducing-arrow-flight/) is a gRPC-based protocol for sending large Arrow-encoded datasets between systems with parallel-stream + columnar-batch semantics. Two roles sluice could play:

- **Flight server** — operators run sluice, point a Flight client at it, sluice streams CDC + bulk-copy data via Arrow batches.
- **Flight client** — sluice fetches from a Flight-speaking source (some warehouses already speak Flight).

**Why defer.** Three reasons:

1. **Dep weight.** Flight requires `apache/arrow-go/v18` (the heavy library — Substrait + Avro + gonum tail per `apache-arrow-findings.md`) plus a gRPC server runtime. The `parquet-go/parquet-go` MVP from Surface 1 doesn't pull this in. Adopting Flight is a binary-size step-change relative to Surface 1.
2. **Persona breadth.** Flight is the analytics-first / lakehouse persona's preferred surface. That persona is rare today; the dep cost is real for everyone.
3. **Mapping fit.** Flight assumes columnar semantics in-flight. Sluice's `RowReader` / `RowWriter` are row-oriented (per the IR-first tenet). Grafting Flight onto the existing interfaces is a Shape C from `apache-arrow-findings.md` (in-flight columnar) — explicitly rejected for IR-tenet violation. Surface 3 would need to ride alongside the row interfaces, not replace them. That's doable but it's a parallel pipeline path, not an add-on.

**Revisit when.** An operator with a concrete Flight-speaking consumer surfaces and asks for it AND Surface 1 is shipped (so the dep-weight question is anchored to a real before/after).

## Dep-cost × persona-breadth matrix

| Surface | Dep weight | Persona breadth (today) | Estimate | Verdict |
|---|---|---|---|---|
| 1 — `export-as-parquet` | Low (5 new modules; pure Go; no CGO) | OLTP + ad-hoc + warehouse pipeline (broad) | ~600-1000 LOC | **Promote to roadmap chunk when an operator surfaces with concrete demand.** |
| 2 — DuckDB recipe | Zero (docs-only) | OLTP + ad-hoc (medium) | ~1 day, no code | **Land alongside Surface 1.** It's documentation; near-zero cost. |
| 3 — Arrow Flight | High (15+ modules; apache/arrow-go/v18; gRPC server runtime; ~2× binary size) | Analytics-first / lakehouse (narrow today) | ~2000-3000 LOC | **Defer.** Revisit when (Surface 1 shipped) AND (concrete operator demand surfaces). |

## Open questions for the eventual code chunk

These are surfaced now so the eventual chunk's prep doc doesn't re-derive them:

1. **Parquet file granularity.** One Parquet file per chunk (preserves the chunk concept; many small files), or one per table per backup (fewer files; ETL-friendlier; loses the chunk-level cross-reference)? The cross-reference matters for `backup verify`; the small-files cost matters for cloud storage list operations. Recommendation: **one Parquet file per source chunk, with file names that encode the chunk index, plus a manifest-level `parquet_index.json` mapping for convenience.**
2. **Encryption pass-through.** Sluice's Phase 6 encryption wraps chunks at-rest. Does `export-as-parquet` decrypt then re-encrypt? Decrypt then write plaintext Parquet (operator's choice)? The natural shape is **decrypt with the operator-supplied passphrase / KMS key, write plaintext Parquet** (the operator chose the analytics destination's encryption posture separately; sluice doesn't carry the wrap into the export by default). A future `--re-encrypt-parquet` flag could change that if demand emerges.
3. **Incremental mode.** Today's chains have full + incrementals. Should `export-as-parquet` emit only new chunks since a watermark (incremental export), or always re-emit the whole chain (simpler, idempotent, more bytes)? Recommendation: **incremental by default; full re-emit via `--from-scratch`.**
4. **GeoParquet adoption.** Documented in the type-mapping section above. Strong recommendation: yes — sluice's PostGIS support (ADR-0035, v0.28.0) is meaningless if Parquet export drops the SRID. GeoParquet is metadata-only, no library import; cost is documentation only.
5. **Decimal precision overflow.** PG `NUMERIC` without precision is unbounded; Parquet's decimal type is bounded (precision ≤ 38). Recommendation: **emit unbounded NUMERIC columns as `BYTE_ARRAY` strings with a documented note**; loud failure on read-back is the wrong tenet here because Parquet export is exit-only and the columnar tool can re-parse the string.

## Stance vs the proto-ADR

The proto-ADR ([`docs/dev/design/apache-arrow-integration.md`](../dev/design/apache-arrow-integration.md)) made a conditional-yes call gated on Phase 1 logical-backup picking Parquet. Phase 1 picked JSON-Lines + gzip; that conditional dissolved. The arrow-findings doc made deferral on Shape A explicit. This doc narrows the question: **don't defer everything — the export-as-parquet slice has cheap-enough dep weight and broad-enough persona reach to be worth a roadmap chunk once an operator asks for it.**

## Tenet check

- **IR-first.** Surface 1 reads JSON-Lines chunks (which already roundtrip IR rows) and writes Parquet (one-way). The IR contract isn't disturbed.
- **Contain Postgres complexity.** Surface 1 doesn't surface PG-specific knobs — it surfaces the IR's already-engine-neutral type set, with documented lossy edges where Parquet's type system is narrower than PG's. Operators don't have to know what PG type drove a column.
- **Loud failure.** The lossy-edge cases (unbounded NUMERIC, TZ-stripped Timestamps) emit operator-visible warnings at export time, not silent type-narrowing.
- **Validate end-to-end.** The eventual chunk needs four integration tests, one per persona corpus. The compression-benchmark's corpora are reusable templates.

## When to revisit this doc

- An operator surfaces with concrete Parquet demand → promote Surface 1 to a roadmap chunk; this doc becomes the chunk's prep doc.
- An operator surfaces with Arrow Flight demand → revisit Surface 3's dep-weight assumption against the current upstream state of `apache/arrow-go` (the version may have consolidated or the Substrait / Avro / gonum tail may have been trimmed).
- DuckDB ships breaking changes to `read_json_auto` / `read_parquet` → re-verify the Surface 2 recipe before promoting it to the cookbook.

## Prior-art update (2026-07-15): burnside-project `pg-warehouse` / `pg-cdc`

Two shipping Apache-2.0 Go tools from one org (operator-flagged 2026-07-15) land squarely on this doc's territory and independently validate its three calls. Both are **PG-only**.

- **[`pg-warehouse`](https://github.com/burnside-project/pg-warehouse)** — local-first PG→DuckDB mirror via `pglogrepl`, with a dbt-like model DAG / contracts / versioned releases over three DuckDB files, plus Parquet/CSV export. Embeds DuckDB via `marcboeker/go-duckdb` — **CGO**, and drags `apache/arrow-go/v18` into the module graph.
- **[`pg-cdc`](https://github.com/burnside-project/pg-cdc)** — PG WAL → typed, compacted Parquet/**Iceberg** on S3 (AWS Marketplace listing; Glue/Lake Formation governance + an MCP server for AI-agent consumption). Uses **`parquet-go/parquet-go`** in production; snapshot-at-LSN init mirroring sluice's snapshot→CDC handoff; delta-epoch compaction; CAS-committed (If-Match ETag) S3 manifest with a dual-writer circuit breaker; `confirmed_flush_lsn` advanced only after Parquet+manifest are durable.

**What they validate (all three of this doc's calls):**

1. **Surface 1's library choice is production-proven.** `pg-cdc` ships `parquet-go/parquet-go` for exactly the typed-CDC-to-Parquet job, including live schema evolution. The library-risk question on `export-as-parquet` is effectively retired.
2. **"DuckDB is a recipe, not a subcommand" — confirmed from both directions.** `pg-warehouse` demonstrates the embedded path's cost (CGO + the Arrow v18 tail — exactly what would break sluice's CGO=0 Windows builds and the goreleaser cross-compile matrix), and even `pg-cdc` — whose whole product is this space — delegates queries to DuckDB rather than embedding it in the write path.
3. **Exit-only columnar is the shared posture.** `pg-cdc` is explicitly a "one-way valve … no return path to PG" — the same collapse of the lossless-round-trip type-mapping problem Surface 1 chose.

**Demand-gate update.** Two shipping tools, a Marketplace listing, and the "governed operational data for AI agents" framing are concrete market evidence for the Persona-2/3 demand this doc gated Surface 1 on. Promoted to roadmap **item 63** (export-as-parquet + the DuckDB cookbook recipe). sluice's differentiation is engine breadth: both tools are PG-only; sluice's export would cover MySQL/Vitess/PlanetScale/SQLite/D1 backup chains through the same `BackupStore` path. `pg-cdc`'s Iceberg catalog commit is noted there as a possible extension, not MVP scope.

**Cross-checks that came back clean (verified against sluice's code, no action):** unchanged-TOAST columns already carry the correct preserve-target semantics (`internal/engines/postgres/cdc_reader.go` `decodeTuple`, DataType `'u'` omits the key — not silently NULL); the slot-retention-pressure signal already exists with pre-emptive 70%/85% thresholds (ADR-0059, `slot_health_reporter.go`); and the reader already keepalives an idle slot every 10s (`keepaliveInterval`), covering the Aurora-Serverless idle case. Two genuinely-borrowable hardening ideas were filed as roadmap **item 64**: promoting the ADR-0059 slot-health WARNs (slog-only today) into the ADR-0157 notification sinks, and a backup-store concurrent-writer guard (CAS/ETag + breaker, the `pg-cdc` manifest pattern). RDS/Aurora IAM-token auth rides along there as demand-gated.

## References

- [`docs/research/apache-arrow-findings.md`](apache-arrow-findings.md) — the prior Arrow research; Shape A deferred.
- [`docs/dev/design/apache-arrow-integration.md`](../dev/design/apache-arrow-integration.md) — the proto-ADR that named the three shapes.
- [`docs/dev/notes/compression-benchmark.md`](../dev/notes/compression-benchmark.md) — recommendation surface for Phase 2 chunk compression (zstd) which Parquet export inherits.
- [GeoParquet specification](https://geoparquet.org/) — metadata-only convention; relevant if `ir.Geometry` round-trips through Parquet matter.
- [DuckDB `read_json`](https://duckdb.org/docs/data/json/overview.html), [`read_parquet`](https://duckdb.org/docs/data/parquet/overview.html), [`httpfs` extension](https://duckdb.org/docs/extensions/httpfs/overview.html) — the read-side primitives Surface 2 leans on.
- [Apache Arrow Flight intro](https://arrow.apache.org/blog/2019/10/13/introducing-arrow-flight/) — Surface 3's protocol.
