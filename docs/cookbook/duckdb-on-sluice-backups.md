# Recipe — DuckDB analytics on sluice backups

Query the backups you already take — with DuckDB, either directly over
the JSON-Lines chunks (zero extra steps) or over a Parquet export
(`sluice backup export-as-parquet`, faster + predicate pushdown). No
warehouse, no ETL pipeline, no load on the production database.

DuckDB is deliberately **not** a sluice dependency or subcommand
(ADR-0164): its ecosystem moves faster than sluice's release cadence,
and the operator with the appetite for DuckDB already knows how to
drive it. This recipe is the integration.

## When to use this recipe

- You take `sluice backup full` (nightly cron, DR chain, pre-migration
  snapshot) and want to run analytics queries against that data
  without touching production.
- Your warehouse pipeline speaks Parquet (Snowflake `COPY INTO`,
  BigQuery load, Spark, pandas/Polars) and you want sluice's backups —
  from **any** source engine: MySQL, PlanetScale, Vitess, Postgres,
  SQLite, D1 — to feed it through one export path.
- You want an ad-hoc "what did this table look like at last night's
  snapshot?" without a restore target.

## The flow at a glance

1. **`sluice backup full`** — the backup you (probably) already take.
2. **`sluice backup export-as-parquet`** — one-shot transcode: one
   zstd Parquet file per table + a `parquet_index.json` export
   manifest. Read-only against the backup store.
3. **DuckDB `read_parquet(...)`** — query the files locally or
   straight from object storage.

Step 2 is optional for casual queries — DuckDB reads the JSON-Lines
chunks directly (see the last section) — but Parquet is the right
shape for repeated queries and warehouse handoff: columnar scans,
statistics, predicate pushdown.

## 1. Take (or reuse) a backup

```bash
sluice backup full \
  --source-driver postgres \
  --source "$SOURCE_DSN" \
  --output-dir ./backups/prod
```

Any existing chain works — local directory or `s3://` / `gs://` /
`azblob://`. Nothing about the export requires a fresh backup.

## 2. Export it as Parquet

```bash
sluice backup export-as-parquet \
  --from-dir ./backups/prod \
  --output-dir ./lake/prod
```

Or between object stores (the S3 flags apply to both ends):

```bash
sluice backup export-as-parquet \
  --from s3://prod-backups/postgres-main/ \
  --output s3://analytics-lake/postgres-main/parquet/
```

What you get at the destination:

- `<schema>.<table>.parquet` per table (bare `<table>.parquet` for
  flat-namespace sources like MySQL/SQLite), zstd-compressed.
- Row groups aligned 1:1 with the backup's chunk files; each file's
  footer metadata carries the source-chunk list with SHA-256s
  (cross-reference `sluice backup verify`), the backup id, and the
  source engine.
- `parquet_index.json` — the export manifest: per-table file, row
  count, row groups, source chunks, and any type-mapping notes.

Worth knowing (full detail in
[ADR-0164](../adr/adr-0164-backup-export-as-parquet.md)):

- **The export represents ONE snapshot** — the chain's latest full by
  default, or an earlier one via `--backup-id <id>`. Incremental
  change-windows after that full are NOT folded in; the command WARNs
  with the excluded count. For point-in-time state, `sluice restore`
  the chain and re-export.
- **Encrypted / signed chains work exactly like restore**: pass
  `--encrypt` + the chain's passphrase/KMS reference to decrypt, and
  `--verify-key` / `--require-signature` for signed chains. The
  Parquet files themselves are written plaintext — encrypt the
  analytics destination on its own terms.
- **Values are faithful or the export refuses.** Decimals stay exact
  (Parquet DECIMAL, never float64), unsigned BIGINT keeps its full
  range, timestamps are microsecond instants. The two documented
  string downgrades (unbounded `NUMERIC`, `timetz`) carry the exact
  value text and are WARNed. A value Parquet cannot hold faithfully —
  a multi-dimensional array, a MySQL `TIME` beyond 24h, a `NUMERIC`
  `NaN` — fails loudly with `SLUICE-E-EXPORT-UNREPRESENTABLE` naming
  the column; `--exclude-table` it and query its JSON-Lines chunks
  instead.
- Re-running into the same destination refuses unless you pass
  `--force-overwrite` (the export manifest is the sentinel).
- `--include-table` / `--exclude-table` scope the export.

## 3. Query with DuckDB

```bash
# Local files:
duckdb -c "SELECT count(*), max(created_at)
           FROM read_parquet('./lake/prod/public.users.parquet');"

# Straight from S3 (httpfs reads AWS_* env credentials):
duckdb -c "INSTALL httpfs; LOAD httpfs;
           SELECT status, count(*)
           FROM read_parquet('s3://analytics-lake/postgres-main/parquet/*.parquet',
                             filename = true)
           GROUP BY ALL;"

# A persistent local mart from last night's snapshot:
duckdb mart.duckdb -c "
  CREATE OR REPLACE TABLE users  AS FROM read_parquet('./lake/prod/public.users.parquet');
  CREATE OR REPLACE TABLE orders AS FROM read_parquet('./lake/prod/public.orders.parquet');
"
```

The export's footer metadata is visible too:

```sql
SELECT key, value
FROM parquet_kv_metadata('./lake/prod/public.users.parquet');
-- sluice:backup_id, sluice:source_engine, sluice:source_chunks,
-- sluice:enum_values, geo (for WKB geometry columns), ...
```

Type notes for consumers:

- `TIMESTAMPTZ` columns arrive as `TIMESTAMP(MICROS,
  isAdjustedToUTC=true)` — UTC instants (the source-session zone was
  never in the backup either). Plain `TIMESTAMP`/`DATETIME` arrive
  unadjusted.
- `UUID`, `INET`, `ENUM`, `BIT` arrive as strings (the enum's declared
  value list is in `sluice:enum_values`). `JSON`/`JSONB` are
  JSON-annotated byte arrays — `SELECT col->>'$.a'` etc. work after a
  cast in DuckDB.
- Geometry columns are raw WKB with GeoParquet `geo` metadata:
  `INSTALL spatial;` then `ST_GeomFromWKB(col)`.

## Zero-export path: DuckDB straight over the JSON-Lines chunks

For a one-off question, skip the export — sluice's chunks are gzip/zstd
JSON Lines and DuckDB reads them natively (each chunk file's first line
is a sluice header row; filter it out by a column that's never NULL in
real rows):

```sql
SELECT *
FROM read_json_auto('./backups/prod/chunks/users/users-*.jsonl.gz')
WHERE id IS NOT NULL
LIMIT 100;
```

Caveats of the direct path (all solved by the Parquet export): wide
values wear sluice's tagged envelopes (`{"_t":"u64","v":"..."}` for
big unsigned ints, `"bytes"` base64 for binary — see
`internal/pipeline/blobcodec`), everything else is inferred by
DuckDB's JSON sniffing rather than typed, encrypted chunks are opaque,
and there's no predicate pushdown. Fine for a peek; export for real
work.

## See also

- [Backup chain with at-rest encryption](recipe-backup-encrypted.md) —
  producing the chains this recipe consumes.
- [`docs/research/sluice-as-analytics-source.md`](../research/sluice-as-analytics-source.md)
  — why Parquet-export-plus-DuckDB-recipe is the chosen analytics
  surface (and why Arrow Flight isn't, yet).
- [ADR-0164](../adr/adr-0164-backup-export-as-parquet.md) — the full
  type-mapping table and the export's integrity guarantees.
