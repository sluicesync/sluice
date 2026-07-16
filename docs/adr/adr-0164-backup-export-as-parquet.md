# ADR-0164: `backup export-as-parquet` — the analytics exit surface

**Status:** Accepted (shipped v0.99.251; roadmap item 63)

## Context

Operators running OLTP databases increasingly want the migration/backup tool to also be the bridge into their analytics stack. The research doc ([`docs/research/sluice-as-analytics-source.md`](../research/sluice-as-analytics-source.md)) worked the problem in 2026-06 and gated its Surface 1 (`backup export-as-parquet`) on concrete demand; that demand arrived externally on 2026-07-15 — burnside-project's `pg-warehouse` (PG→DuckDB local warehouse) and `pg-cdc` (PG WAL→typed Parquet/Iceberg on S3, with an AWS Marketplace listing) are two shipping tools built on exactly the OLTP→columnar personas the doc bell-curved. Both are PG-only; sluice's differentiation is engine breadth — one export path over MySQL/PlanetScale/Vitess/Postgres/SQLite/D1 backup chains, because the export reads the engine-neutral chunk format, not any engine.

## Decision

### Exit-only, over the existing store — never a new capture path

`sluice backup export-as-parquet` is a one-shot, **read-only** transcode of an EXISTING backup's JSON-Lines row chunks into one Parquet file per table plus a `parquet_index.json` export manifest. sluice never reads its own Parquet output back; `sluice restore` keeps the JSON-Lines path. Exit-only collapses the lossless-round-trip type-mapping problem to "faithful columnar with documented edges" — operators choosing the export are choosing a one-way bridge (the same posture `pg-cdc` ships: "a one-way valve, no return path").

The exporter (`internal/pipeline/backup/export_parquet.go`) rides the restore-side machinery wholesale: `lineage.ListAllSegmentManifests` for chain discovery, `blobcodec.FetchChunkVerified` + `NewChunkReader` for SHA-256-verified, decrypted, header-validated chunk reads, `verifyChainSignatures` for the ADR-0154 signature policy, and the same per-chunk/per-table layer-2 row-count backstops (including the Bug-183 chunkless-table and zeroed-RowCount tamper refusals). Every integrity property restore enforces holds for the export.

### Library: `parquet-go/parquet-go`, DuckDB as a recipe

- **`parquet-go/parquet-go` v0.30** is the writer — pure Go, CGO-free (sluice's Windows builds and the goreleaser cross-compile matrix stay intact), and now production-proven for exactly this job in `pg-cdc`. Transitive footprint is small and clean: `parquet-go/{bitpack,jsonlite}`, `twpayne/go-geom`, `andybalholm/brotli`, `pierrec/lz4` — **no `apache/arrow-go`**, no gRPC.
- **DuckDB is a documentation recipe** ([`docs/cookbook/duckdb-on-sluice-backups.md`](../cookbook/duckdb-on-sluice-backups.md)), never a dependency or subcommand. `pg-warehouse` demonstrates the embedded path's cost (`marcboeker/go-duckdb` = CGO + the arrow-go v18 tail), and even `pg-cdc` — whose whole product is this space — delegates queries to DuckDB rather than embedding it. Operators with the appetite for DuckDB already know how to drive it.

### File shape

- **One Parquet file per table** (`<schema>.<table>.parquet`, bare `<table>.parquet` for flat-namespace engines), zstd-compressed (klauspost pure-Go, matching the chunk codec's default).
- **Row groups never span source chunks; each chunk owns a contiguous run of ≥ 1 groups** — the operator-visible chunk concept survives into the file, and each file's footer metadata records the full source-chunk list (`sluice:source_chunks`: file/sha256/row_count) so operators can cross-reference `backup verify`. Further provenance keys: `sluice:backup_id`, `sluice:source_engine`, `sluice:backup_created_at`, `sluice:schema`, `sluice:table`; enum/set value universes in `sluice:enum_values` / `sluice:set_values`; a GeoParquet `geo` block for WKB geometry columns. *(Amended 2026-07-15, audit MED-P3: originally strict 1:1 — one group per chunk — which made the writer's retained-page memory scale with the chunk's BYTE size, unbounded for BLOB-heavy rows since backup chunks roll on row count alone. The exporter now rolls a row group early when a chunk's accumulated encoded bytes pass a 128 MiB target, so an oversized chunk maps to several consecutive groups; chunks under the target keep exactly one group, and `parquet_index.json` records the actual group count. The chunk-fetch buffer — shared with restore, sized by the compressed chunk — remains the inherited bound.)*
- **`parquet_index.json`** at the output root maps tables → files → rows/row-groups/source-chunks/type-notes. Written LAST, it is both the completion marker and the overwrite sentinel (a second export refuses without `--force-overwrite`).
- Parquet files are written **plaintext** even from an encrypted chain (research doc Open Question 2): the operator chose the analytics destination's encryption posture separately; sluice does not carry the chain's wrap into the export.

### Chain-to-a-point = snapshot granularity

The export represents ONE snapshot: the latest segment full by default, or the full named by `--backup-id`. Incremental change-windows after the selected full are **not** folded in — a loud WARN names the count of excluded incrementals, so the boundary is never silent; operators needing point-in-time row state restore the chain and re-export. Naming an incremental's id (or an unknown id) is a refusal that lists the exportable fulls. Rationale: incremental chunks are `ir.Change` event streams, and materializing them into per-table row state without a target database is a replay engine this surface doesn't need — restore already is that engine.

### Type mapping (the value-fidelity contract)

Inputs are exactly the `docs/value-types.md` Row shapes (what the chunk decoder produces); a deviating Go type refuses loudly as an upstream-bug signal. Outputs:

| IR type | Parquet | Value handling |
|---|---|---|
| Boolean | BOOLEAN | as-is |
| Integer (signed, any width) | INT64, `INT(64, signed)` | as-is |
| Integer (unsigned) | INT64, `INT(64, unsigned)` | full uint64 range; negative refuses |
| Decimal p ≤ 9 / ≤ 18 / ≤ 38 (0 ≤ s ≤ p) | DECIMAL over INT32 / INT64 / FLBA(16) | exact unscaled integer; excess fraction digits (non-zero), excess precision, and `NaN`/`Infinity` refuse — never rounded |
| Decimal unbounded, p > 38, or negative scale | STRING | the exact decimal text, with an operator-visible note (research doc OQ 5) |
| Float (single/double) | DOUBLE | bit-exact incl. NaN/±Inf/-0.0/denormals |
| Char/Varchar/Text, Bit ('0'/'1' string), Enum, UUID, Inet, Cidr, Macaddr, Interval | STRING | the IR text verbatim (Enum/Set value universes ride in footer metadata; UUID as the canonical hyphenated string — the spec's UUID logical type is FLBA(16)-only, and the string is what DuckDB/PyArrow consume directly) |
| Binary/Varbinary/Blob | BYTE_ARRAY | as-is; empty ≠ NULL |
| JSON | BYTE_ARRAY + `JSON` | raw JSON bytes |
| Geometry | BYTE_ARRAY + GeoParquet `geo` footer | raw WKB; metadata-only, no geometry library |
| Date | INT32 `DATE` | days since epoch (integer math — `time.Duration` overflows at ±292y); non-midnight refuses |
| Time (no tz) | INT64 `TIME(MICROS, utc=false)` | parsed micros; negative / ≥ 24h (MySQL ±838h durations, PG `24:00:00`) / sub-micro refuse |
| Time (tz) | STRING + note | Parquet TIME has no offset form; the exact text is carried |
| DateTime, Timestamp (no tz) | INT64 `TIMESTAMP(MICROS, utc=false)` | UnixMicro; sub-microsecond and out-of-int64-micros-range refuse |
| Timestamp (tz) | INT64 `TIMESTAMP(MICROS, utc=true)` | same (the IR already carries the instant UTC-normalized) |
| Set | LIST\<STRING\> | members in declaration order; empty ≠ NULL |
| Array\<T\> | LIST\<T-mapping\> | NULL elements supported; a **multi-dimensional value refuses loudly** (below) |
| Domain | its base type's mapping | values flow in the base shape (Bug 122) |
| ExtensionType / VerbatimType | STRING | the type's text I/O verbatim |

Every column is Parquet-OPTIONAL regardless of IR nullability — nullability is re-imposable downstream and an exit never validates it.

**Multi-dimensional arrays refuse.** PG's type system does not declare array dimensionality (`int[]` and `int[][]` are one column type), so the derived `LIST<element>` schema can only hold 1-D values; a nested value refuses with `SLUICE-E-EXPORT-UNREPRESENTABLE` naming the column. The silent alternative is exactly Bug 74's flatten. This diverges from the research sketch's "nested arrays supported up to a documented depth limit" — the depth isn't knowable from the type, so v1 refuses rather than guessing a schema the data may violate.

**The refusal code.** `SLUICE-E-EXPORT-UNREPRESENTABLE` (exit 3) covers every value-level unrepresentability: multi-dim arrays, out-of-day TIME, NUMERIC NaN/Infinity, precision/scale overflow, sub-microsecond temporals. The remedy is always named: `--exclude-table`, or query the JSON-Lines chunks directly (the cookbook's zero-export path).

### The zero-value-as-null wart (named, pinned)

parquet-go's `map[string]any` row deconstruction treats every Go **zero value** in an optional column as parquet NULL (`isNullValue` → `reflect.Value.IsZero`): `false`, `0`, `-0.0`, `""` would all silently export as NULL — a silent-loss class of its own, and invisible to naive round-trip tests because a null's accessors read back as the zero value. `boxLeafValue` (in `internal/pipeline/parquetexport`) wraps every scalar leaf value in a pointer before it enters the writer (a non-nil pointer is never "zero"), restoring SQL-faithful null semantics. Pinned by explicit `IsNull()==false` assertions on every zero-shaped value (false, 0, ±0.0, "", epoch instants, midnight, day 0, unscaled-0 decimals) in the roundtrip matrix AND at the live-PG integration level. List elements are unaffected (parquet-go's repeated path doesn't zero-collapse; also pinned).

### CLI shape

`sluice backup export-as-parquet --from-dir/--from <chain> --output-dir/--output <dest> [--backup-id ID] [--include-table/--exclude-table] [--force-overwrite] [encryption/signature flags]`. Source flags follow the house `--from-dir/--from` convention of verify/prune/compact/restore rather than the research sketch's `--source-bucket`; encryption + `--verify-key`/`--require-signature` are the shared `EncryptionFlags` embed with restore-identical semantics. Parquet compression is fixed at zstd (no flag — fewer surfaces; the chunk-side benchmark already ratified zstd).

## Consequences

- Any engine whose chains the `BackupStore` holds is exportable today — MySQL/Vitess/PlanetScale/SQLite/D1 chains ride the same path with zero engine code (the chunk format is the contract). The integration pin runs the full live path on Postgres; the unit matrix covers every IR family engine-independently.
- The export inherits restore's threat posture: signed chains verify (strict under `--require-signature`), encrypted chains demand the key and refuse plaintext splices/GCM-auth failures with the same coded refusals, and the layer-2 row-count backstops fire identically.
- v1 non-goals, deliberately: transcoding incremental change-chunks to CDC-event Parquet (pg-cdc's shape; a possible later surface), an incremental/watermark export mode (research OQ 3 — re-runs are whole-snapshot; `--force-overwrite` replaces), Iceberg catalog commits, and Arrow Flight (still deferred per the research doc).
- New dependency: `parquet-go/parquet-go` (Apache-2.0). The arrow-findings dep-weight concern was re-verified at `go mod tidy` time: no arrow-go, no gRPC, ~6 new modules.

## Alternatives considered

- **One Parquet file per source chunk** (research OQ 1's lean). Rejected for v1: per-table files are what warehouse `COPY INTO`/`read_parquet` globs consume naturally, and the chunk-level cross-reference survives as row-group alignment + the footer's `sluice:source_chunks` list — the verify cross-reference without the small-files tax.
- **Materializing chain-to-a-point row state by replaying incrementals in the exporter.** Rejected: it duplicates restore's replay engine without a database to hold state; restore + re-export composes the same result honestly.
- **`parquet.UUID()` logical type for UUID columns.** Rejected: the spec binds it to FLBA(16); the IR value is the canonical hyphenated string and that string is the most consumable shape downstream. Divergence from the research sketch, documented there and here.
- **Refusing -0.0 / relying on parquet-go's default null handling.** The pointer-boxing wart is uglier than a clean writer API would be, but the alternative was refusing legitimate values or silently nulling zeros; both violate the tenet the export exists under.
