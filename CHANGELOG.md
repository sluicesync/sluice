# Changelog

All notable changes to sluice are recorded here. The format follows
[Keep a Changelog](https://keepachangelog.com/en/1.1.0/) and the
project follows [Semantic Versioning](https://semver.org/).

## [Unreleased]

## [0.8.0] - 2026-05-06

Schema-diff release plus five real-world bug fixes from v0.7.0 testing. Headline addition is `sluice schema diff` (ADR-0029): drift detection between sluice's expected target shape and the schema actually present, with text + JSON output, copy-paste-ready ALTER suggestions, and CI-friendly exit codes. The diff round picked up cross-engine type retargeting plus default / generated-expression / CHECK comparison along the way. The five bug fixes — including Bug 19's silent TIMESTAMP corruption on non-UTC hosts and Bug 20's cross-engine resume dispatch — closed the remaining real-world gaps the v0.7.0 stretch testing surfaced.

### Added

- **`sluice schema diff` (ADR-0029).** Drift detection between what
  sluice would produce on a target (source schema → translation
  pipeline → expected target shape) and the schema that's actually
  there. Reads both sides through the existing `SchemaReader`
  surface — no new engine API; every engine that already implements
  `SchemaReader` (today: PG, MySQL) gets diff support immediately.
  Renders text (default; per-table sections with copy-paste
  ALTER/DROP suggestions and a preamble noting they're starting
  points, not verified migration scripts) or JSON (stable shape for
  CI consumers) and supports `--output FILE` with the same atomic
  temp+rename semantics as `schema preview`. Filter and mapping
  flags mirror `schema preview` so the diff and preview pipelines
  stay aligned. CI-friendly exit codes: 0 on no drift, 1 on drift
  detected (suitable for failing a `schema-drift.yml` job), 2 on
  operational error like a bad DSN — distinct so CI scripts don't
  conflate "the gate failed" with "we couldn't run the gate."
  `--ignore-extras` suppresses extra-on-target entries (useful when
  the target hosts other applications' tables); `--ignore-charset-
  collation` is plumbed for the v1.x extension when those fields
  land in the IR. Out of scope per the ADR: column reordering,
  index column ordering, FK constraint name normalisation, and
  trigger/function/view comparison — surfacing those as drift
  produces too much noise for too little operator value, and
  reconciliation is a different tool's job (Atlas, sqitch).

- **Schema diff: defaults, generated expressions, CHECK constraints,
  per-column ALTER rendering.** Three categories originally listed
  as out-of-scope in ADR-0029 are now compared because the IR
  already carries the underlying fields and the comparison shape is
  additive on `ColumnDiff` / `TableDiff`: column defaults
  (`ExpectedDefault` / `ActualDefault`, with a small cross-engine
  equivalence map for the common pairs like `now()` ↔
  `CURRENT_TIMESTAMP`; mismatches outside the map are flagged
  low-confidence rather than silently equated), generated-column
  expressions (verbatim string comparison after trim — engines don't
  support in-place generated-expr ALTERs, so the renderer emits a
  comment plus a DROP+ADD reconciliation hint), and table-level
  CHECK constraints (matched by name; unnamed CHECKs are dropped
  from the comparison to avoid cross-engine spelling false
  positives). Renderer fills the actual column type, default, and
  generated expression on `ALTER TABLE ... ADD COLUMN` suggestions
  for missing-on-target columns via a new optional
  `ir.ColumnDDLPreviewer` interface (implemented on both PG and
  MySQL); the prior `-- TYPE` placeholder remains as a defensive
  fallback for engines that don't implement it.

- **Cross-engine type-policy retarget on schema diff.** New
  `internal/translate.RetargetForEngine` rewrites the source-side
  schema's PG-native IR types (`UUID`, `Inet`, `Cidr`, `Macaddr`,
  `Array`) to the MySQL-storage IR shapes (`Char(36)`, `Varchar(45)`,
  `Varchar(30)`, `JSON[binary]`) the target engine's DDL writer
  would land them on. Wired into `pipeline.Differ.Run` between
  `ApplyMappings` and the target schema read so cross-engine
  `sluice schema diff` no longer flags every translated column as
  drift when the target storage is exactly what sluice would
  produce. Same-engine pairs and unknown engine pairs return the
  schema unchanged. Operator-supplied `--type-override` mappings
  take precedence (override replaces the IR type via
  `ApplyMappings`; the retarget pass only fires on still-source-
  native types). v0.8.0 scope is the PG→MySQL direction.

### Tests

- Cross-engine integration test for `sluice schema diff`
  (`internal/pipeline/diff_cross_engine_integration_test.go`) booting
  a PG source + MySQL target. Asserts the retarget pass collapses
  the noisy cross-engine type drift so only the deliberately
  injected drift surfaces (narrowed VARCHAR, missing column, extra
  table on target). Also covers JSON / text rendering and
  `IgnoreExtras` semantics on the cross-engine path.

### Fixed

- **Bug 16 — MySQL functional / expression indexes wall the schema
  reader.** `information_schema.statistics` rows for
  functional/expression indexes (MySQL 8.0.13+) carry
  `COLUMN_NAME = NULL` and put the actual expression in the
  `EXPRESSION` column. The reader scanned `column_name` into a plain
  `string`, so the first such index produced
  `converting NULL to string is unsupported` and aborted the
  schema-read for the whole database — a hard wall blocking every
  operation against production schemas that use the feature.

  Fix: scan into `sql.NullString`, add `EXPRESSION` to the SELECT,
  and route NULL-column rows into a new `ir.IndexColumn.Expression`
  field (run through the same `normalizeMySQLExpressionText`
  identifier-quote scrubbing the reader applies to generated columns
  and CHECKs). MySQL and Postgres DDL writers render expression
  entries as parenthesised expression text. Cross-engine MySQL→PG
  emit is best-effort: portable expressions round-trip; non-portable
  ones still fail loudly on `CREATE INDEX`. Regression guards:
  `TestEmitCreateIndex/expression_entry`,
  `TestEmitCreateIndex/mixed_plain_and_expression_entries` (unit) and
  `TestSchemaReader_FunctionalIndex` (integration).

- **Bug 17 — MySQL bool-idiom CHECK / generated expressions reject
  on PG (ADR-0016 addition).** MySQL's tinyint(1)→PG BOOLEAN mapping
  silently broke CHECK constraints and generated columns that compared
  the column against an integer literal — `0 <> is_active`,
  `is_active = 1`, `coalesce(is_active, 0)` — because PG's strict
  typing rejects integer↔boolean comparisons that MySQL accepts via
  implicit coercion. Real-world report: 3 of 138 tables on
  `schema_example_02` blocked by this until columns were dropped
  manually.

  Fix extends the writer-side translator (`translateExprForPG`) with
  an `ExprContext` carrying the table's bool-mapped column names.
  When the rewrite recognises `<int_lit> <op> <bool_ident>` /
  `<bool_ident> <op> <int_lit>` (op ∈ `=`, `!=`, `<>`; lit ∈ `0`, `1`)
  or `COALESCE(<bool_ident>, <int_lit>)` and the symmetric form, the
  int literal is replaced with `false` / `true`. `IFNULL` is renamed
  to `COALESCE` by an earlier pass so it falls in too. Anything else
  passes through verbatim — same loud-failure tenet as the rest of
  ADR-0016. Same-engine emits unaffected (the translator only fires
  when the IR's dialect tag differs from the writer's). New
  integration test `TestMigrate_MySQLToPostgres_CheckBoolIdiom`
  verifies a real `CHECK (0 <> is_active)` lands on PG and enforces
  correctly. ADR-0016 updated with an "Added in v0.8.0" subsection.

- **Bug 18 — `--reset-target-data` left orphaned PG enum types.**
  The destructive-recovery path (ADR-0023) dropped tables and the
  bookkeeping row; enum types created during a partially-failed
  cold-start survived and caused the next reset's `CREATE TYPE` to
  fail with "type X already exists" until operators manually
  `DROP TYPE`d. Fix extends the reset path with a
  `dropSchemaTypes` pass that runs after the table drops, walking
  the source schema for `ir.Enum` columns and emitting
  `DROP TYPE IF EXISTS "schema"."<table>_<col>_enum" CASCADE`. PG-
  only via the new optional `ir.SchemaTypeDropper` interface; MySQL
  embeds enum values inline and is unaffected. Idempotent across
  partial failures. New integration test
  `TestMigrate_ResetTargetData_DropsOrphanEnumTypes` simulates the
  stuck state, runs reset, and asserts the next migrate succeeds
  with rows landing.

- **Bug 19 — silent TIMESTAMP corruption in MySQL→PG CDC on non-UTC
  hosts.** TIMESTAMP values delivered through CDC drifted by the host
  process's local UTC offset (e.g. seven hours early on a US/Pacific
  host during DST). Cold-start bulk copy was correct, CDC was not, so
  the destination silently held the wrong instant for every row
  updated post-cold-start until an operator happened to compare
  source and target epochs. Loud failures beat silent corruption;
  this one snuck past v0.7.x.

  Two distinct corruption surfaces landed under the same symptom:

  - **CDC binlog path.** MySQL's binlog wire format encodes
    TIMESTAMP as a UTC seconds-since-epoch integer, but go-mysql's
    `decodeTimestamp2` builds the resulting `time.Time` via
    `time.Unix(sec, ...)` whose `Location` defaults to `time.Local`.
    With the parser's `ParseTime=false` setting (sluice's configured
    path), `fracTime.String()` then formats that instant in
    process-local TZ unless
    `BinlogSyncerConfig.TimestampStringLocation` is pinned. The
    formatted wall-clock string flowed into sluice's `decodeTime`,
    which parses naked datetime strings as UTC — silently
    re-interpreting a PT wall clock as a UTC instant.

  - **Cold-start / database/sql path.** A second, latent surface:
    if the MySQL session's `time_zone` inherits the server's
    `default_time_zone` (often `SYSTEM`, which follows the host),
    MySQL converts the column's UTC-stored TIMESTAMP into the
    session TZ for the wire format. The driver — running with
    `cfg.Loc=UTC` — re-interprets that wall-clock as UTC, producing
    the same offset. This wasn't observed because test containers
    default to UTC; production deployments against MySQL servers
    with non-UTC `default_time_zone` would have hit it.

  Fix lives at the connection-protocol layer in two places — no
  Go-side runtime-TZ conversion that could drift with deployment
  changes: the binlog client sets
  `BinlogSyncerConfig.TimestampStringLocation = time.UTC`, and
  every database/sql connection injects `time_zone='+00:00'` into
  `cfg.Params` so the driver issues `SET time_zone='+00:00'`
  immediately after handshake (covers schema reader, row reader,
  row writer, CDC schema cache, change applier, migration-state
  store). DATETIME is unaffected (its binlog encoding is the
  broken-down date/time directly with no TZ conversion).
  Regression guard: `TestCDCReader_TimestampNonUTCHost`
  (integration tag) pins `time.Local` to America/Los_Angeles,
  inserts a TIMESTAMP, and asserts the value comes back as the
  same UTC instant from both the cold-start `RowReader` and the
  CDC stream's update event.

- **Bug 20 — cross-engine warm-resume dispatch on the wrong driver.**
  `sluice sync start --resume` failed on
  `--source-driver=planetscale --target-driver=postgres` because the
  persisted CDC position came back from the target's
  `sluice_cdc_state` tagged with the applier's (target's) engine
  name, so the source CDC reader's decoder rejected it as belonging
  to the wrong engine. v0.1.0's Bug 2 fix patched the symmetric
  same-family PS↔MySQL pair by widening MySQL's decoder; it didn't
  generalise to truly cross-engine pairs. Fix is a re-stamp at the
  streamer level: every persisted position picked up via
  `applier.ReadPosition` has its `Engine` field set to
  `s.Source.Name()` before reaching the source CDC reader. All four
  pairs (MySQL↔MySQL, MySQL↔PG, PG↔PG, PG↔MySQL, plus the
  PlanetScale flavor) round-trip cleanly without per-pair special-
  casing. The from-now sentinel (`Engine="" Token=""`) is preserved.
  The `--reset-target-data --yes` workaround is no longer needed for
  cross-engine zero-downtime resumes. New unit tests
  `TestRetagPositionForSource_*` (helper-level pinning across the
  four pairs) and `TestStreamer_WarmResume_CrossEngine_Retag`
  (end-to-end-shape pin via recording reader/applier).

## [0.7.0] - 2026-05-05

Performance round 2 + ergonomics + reliability follow-ups. Four new ADRs (0025 graceful-drain stop, 0026 LOAD DATA INFILE writer, 0027 source-tx CDC batching, 0028 memory-bounded streaming). Closes Bug 12 (MySQL CDC silent-stall on temporal columns) and Bug 15 (CLI sync-stop drain in the warm-up window) — both classified during v0.6.0 testing as the remaining reliability gaps from the v0.4.0 night soak.

### Added

- **MySQL `LOAD DATA LOCAL INFILE` row-writer (ADR-0026).** Vanilla
  MySQL bulk-copy now streams TSV over `LOAD DATA LOCAL INFILE` via
  go-sql-driver's `RegisterReaderHandler` mechanism (no real file
  written, no `?allowAllFiles=true` needed). Typically 5–10× faster
  than the parameter-bound multi-row `INSERT` path on wide-row
  tables. The `BulkLoadLoadDataInfile` capability constant has been
  declared on vanilla MySQL since v0.1; this release brings the
  implementation up to the declaration. PlanetScale stays on
  BatchedInsert (the flavor doesn't allow `LOAD DATA LOCAL INFILE`).

  Per-call fallback to BatchedInsert when (a) the server has
  `local_infile=OFF` (default on MySQL 8.0+) — one structured WARN
  surfaces the speedup-pending hint, and (b) the table contains a
  geometry column (the SRID-prefixed WKB wire format isn't
  expressible in a column-only LOAD DATA). The TSV serializer
  escapes the four MySQL LOAD DATA defaults
  (tab/newline/CR/backslash/NUL) and emits `\N` for NULL. Statement
  uses `CHARACTER SET binary` plus per-column `SET col = CONVERT(@cN
  USING utf8mb4)` for VARCHAR/TEXT/SET/JSON columns to round-trip
  binary blobs and JSON cleanly in the same statement.

- **Source-transaction-boundary aware CDC batching (ADR-0027).** New
  `ir.TxBegin` / `ir.TxCommit` change variants surface source-side
  transaction boundaries to the applier. Postgres emits from
  `BeginMessage` / `CommitMessage` (with `StreamStart` / `StreamStop`
  mapping to boundaries for the streaming-in-progress chunked path);
  MySQL emits from `BEGIN` QueryEvent / `XIDEvent`. The batched
  applier (`ApplyBatch`) flushes on `TxCommit` so a 5000-row source
  transaction commits as one 5000-row target transaction instead of
  being split by the row-count cap. The cap remains the upper bound;
  idle flush, channel close, and Truncate flush behave as before.
  Empty source transactions produce no target commits (lazy-tx-open
  absorbs them). Per-change `Apply` treats boundary events as
  no-ops; the table filter explicitly bypasses them so a filter
  never drops a boundary signal. Position-and-data atomicity
  (ADR-0007) and idempotency (ADR-0010) preserved. Closes the
  follow-up explicitly deferred from ADR-0017.

- **`--max-buffer-bytes N` (ADR-0028).** Default `67108864` = 64 MiB,
  on `sluice migrate` and `sluice sync start`. Bounds per-batch
  buffered memory by total byte size in addition to the existing
  row-count caps. Wide-row workloads (TEXT / BYTEA / JSON at MB
  scale) no longer have to manually retune `--bulk-batch-size` /
  `--apply-batch-size` to control heap usage; the byte cap fires
  whichever way is tighter. The cap is a soft target — a single row
  larger than the cap still applies. Implemented in the bulk-INSERT
  writer, idempotent-INSERT writer, and CDC `ApplyBatch` paths for
  both engines via the new `ir.MaxBufferBytesSetter` optional
  surface; the COPY-protocol and LOAD DATA paths are streaming and
  unaffected. The byte-counting helper (`approximateRowBytes`) was
  hoisted from the pipeline to `internal/ir/bytes.go` so engine
  packages can reuse it.

- **PG-native types auto-emit on MySQL targets.** `Inet` / `Cidr`
  (PG → MySQL) auto-emit as `VARCHAR(45)`; `Macaddr` as
  `VARCHAR(30)`; `Array` as `JSON` (matches the v0.5.0 Bug 14 fix
  where array values are serialized as JSON for the writer).
  Pre-v0.7.0 these returned an error pointing operators at
  `--type-override`; the auto-emit removes the toil for every
  PG→MySQL migration that touches these types. Operators wanting
  strict syntactic validation still use `--type-override` to a
  custom shape with their own CHECK constraint; the schema-preview
  command (ADR-0024) surfaces the auto-emit choice so it isn't
  silent. Closes roadmap §6.

- **Throughput tuning guide** (`docs/throughput-tuning.md`).
  Operator reference for the knobs that matter at scale —
  `--apply-batch-size`, `--bulk-parallelism`, network compression
  (MySQL `compress=true`, PG TLS+gss settings), and
  `--max-buffer-bytes`. Cross-references the relevant ADRs.

- **`migrate --dry-run` cross-reference to schema preview.** The
  dry-run plan output now includes a one-line pointer to
  `sluice schema preview` for full DDL inspection with translation
  notes and advisory hints. Closes roadmap §10.

### Fixed

- **Bug 12 — MySQL CDC silently dropped events with TIMESTAMP /
  DATETIME / DATE columns.** The decoder for binlog row events
  (`decodeTime` in `internal/engines/mysql/value_decode.go`) only
  accepted `time.Time` directly — but the binlog protocol hands
  temporal values back as their raw string form ("YYYY-MM-DD
  HH:MM:SS[.ffffff]" / "YYYY-MM-DD") regardless of the schema-cache
  DSN's `parseTime=true` setting. The first row event on any table
  with a temporal column raised `cannot decode string as time.Time
  (parseTime=true should be set)`; the binlog pump exited with that
  error stored on the reader (only surfaced via `Err()`, not logged),
  the change channel closed, and the applier saw zero events.
  Symptom: cold-start bulk-copy completed cleanly, then CDC mode
  produced no further inserts on the destination — looked exactly
  like a network/heartbeat issue, which sent the original Bug 12
  hypothesis chasing port-forwarding ghosts.

  Fix: `decodeTime` now parses MySQL's canonical temporal string
  formats — second-precision, microsecond-precision, date-only —
  plus byte-slice equivalents and the `0000-00-00` zero-value (maps
  to `time.Time{}` for clean cross-engine round-trip). Regression
  guard: `TestDecodeTimeFromString` covers all five shapes; the
  pre-existing `TestDecodeValueErrors/timestamp_from_string` case
  was inverted to test the unparseable-string error path instead
  (parseable strings now succeed).

  Empirical confirmation against `bug12_repro_dev.sh` (local mysql:8.0
  containers, table with `t TIMESTAMP DEFAULT CURRENT_TIMESTAMP`):
  pre-fix dropped 100% of CDC events on tables with a temporal
  column; post-fix all events flow.

- **Bug 15 CLI sync-stop drain (data loss in warm-up window,
  ADR-0025).** The v0.5.0 slot-ack-after-apply work (ADR-0020)
  closed the post-restart wedge but left a residual data-loss path
  in the warm-up window between stream start and the first applied
  commit. Pre-fix, `ackLSN` returned `streamedLSN` (the highest
  commit-LSN parsed off the WAL) when the applier-feedback tracker
  was still at zero; the keepalive routine ack'd that to the slot,
  advancing `confirmed_flush_lsn` past events that hadn't been
  durably applied. A subsequent `sync stop` mid-batch then lost
  the events between persisted_position and confirmed_flush_lsn —
  warm-resume's slot stream started past them and the rows never
  landed. Empirical repro on local docker: 25-42 row gap with
  `--apply-batch-size=50` and a sustained 10/sec writer.

  Fix has two layers:

  1. **`ackLSN` anchors at startLSN until first apply commit.** The
     load-bearing data-correctness fix. When the tracker is fresh
     (`applied=0`), ack returns the LSN the pump started from
     (cold-start: snapshot LSN; warm-resume: persisted_position's
     LSN). The slot can't advance past that point until the applier
     reports a higher value via the tracker. One-line, one-parameter
     change.

  2. **Graceful-drain shape for `sync stop`.** The pre-fix
     `pollStopSignal` cancelled `applyCtx`, rolling back the open
     batch — relying on warm-resume to redeliver. With the ackLSN
     fix that worked correctly but produced unnecessary redelivery
     storms. Stop-signal now cancels a separate `streamCtx` (which
     scopes the CDC reader's pump); the channel closes cleanly,
     the applier's existing `channelClosed` branch commits the
     in-flight partial batch, position writes naturally. A
     30-second watchdog escalates to hard-cancelling `applyCtx` if
     the drain wedges.

  Unit-level regression guard: `TestAckLSN_AnchorsAtStartLSNUntilFirstApply`
  pins the contract. Empirical integration repro lives at
  `C:\code\sluice-testing\workspace\bug15_repro_dev.sh` (sustained
  writer, mid-stream `sync stop`): pre-fix dropped 25-42 rows;
  post-fix drops 0. The existing programmatic-RequestStop integration
  test (`TestStreamer_PostgresToPostgres_StopRestartNoLoss`) still
  passes — it happened to time RequestStop past first-batch commit,
  masking the warm-up window. See ADR-0025.

- **Windows CI: `TestPreviewer_Golden_Text` fails with CRLF/LF
  mismatch.** The test compared `bytes.Equal(buf.Bytes(), want)` —
  buffer with LF newlines (Go's native `\n`) vs. file content that
  git's default `core.autocrlf=true` had converted to CRLF on
  Windows checkouts. The diff showed visually identical content;
  byte comparison failed.

  Two-part fix:
  1. New `.gitattributes` enforces `eol=lf` on text files so
     Windows checkouts no longer get CRLF on golden fixtures.
  2. The test normalises CRLF→LF on the read side before comparing
     — belt-and-suspenders against any future checkout that
     bypasses the attribute (e.g. zip-download, alternate clones).

  No behavioural change to runtime code; CI-only fix.

## [0.6.0] - 2026-05-05

Feature release. Headline additions are `sluice schema preview` (operator-facing target-DDL inspection with translation notes and advisory hints) and `--reset-target-data` (one-command destructive recovery on top of v0.5.2's slot-missing fall-through). Plus four reliability items uncovered during v0.5.x testing: a CI-only data race in the parallel-copy state-write path, batched-apply idle flush on quiet streams, MySQL binlog-purged fall-through (extends ADR-0022 to the MySQL side), and two parallel-copy hygiene follow-ups. Two new ADRs (0023 schema preview, 0024 reset-target-data); ADR-0022 extended for MySQL.

### Fixed

- **Data race in parallel-copy state-write path.** v0.5.0's
  `migrate_parallel.go::copyChunk` checkpoint sites took `stateMu`,
  mutated their slot in `state.TableProgress`, then did a shallow
  copy `stateCopy := *state` and released the lock before calling
  `writeState`. The shallow copy left `stateCopy.TableProgress`
  pointing at the same map backing storage as `state`, so the JSON
  encoder iterating outside the lock raced peer chunk goroutines
  taking the lock to mutate their own slots. Surfaced as a CI -race
  failure in `TestMigrate_PG_ParallelCopy_Resume` for the v0.5.x
  releases.

  Fix: a `cloneStateForWrite` helper re-allocates the
  `TableProgress` map and each entry's `Chunks` slice under the
  lock; the encoder gets a fully independent snapshot. Per-chunk
  reference fields (`LowerPK`/`UpperPK`/`LastPK`) are not deep-
  cloned because they're either written once at resolution time or
  replaced wholesale (not mutated in place) on each checkpoint.
  Pre-existing behaviour preserved bit-for-bit; the fix is sync-
  primitive-only.

- **Two parallel-copy hygiene follow-ups.** `progressTicker.startedAt`
  swaps the `Load → Store` check-then-set for an `atomic.CompareAndSwap`
  so the contract stays correct if `loop` ever runs from multiple
  goroutines (single-goroutine today; one-line future-proofing).
  `kickOffRowCount` now suppresses the `row-count probe failed`
  WARN when the parent context was already cancelled, and skips
  the `setTotalRows` store when the ticker is already stopped —
  removes interleaved teardown-time noise during test cleanup.

### Added

- **`sluice schema preview` subcommand.** Reads the source schema,
  applies the translation pipeline (mappings + cross-engine type
  policy), and emits the target DDL with inline cross-engine
  translation notes and advisory hints — without touching either
  database's data. Operators see exactly what the target schema will
  look like before any migration runs, including the `--type-override`
  invocation for known operator-preferable alternatives (e.g. PG
  `uuid` → MySQL `BINARY(16)` instead of the default `CHAR(36)`).
  Supports `--format text|json`, `--include-table`/`--exclude-table`,
  `--type-override`, and `--output FILE` (atomic temp-file +
  rename, so a Ctrl-C mid-write never corrupts the destination).
  New `ir.DDLPreviewer` engine surface; both Postgres and MySQL
  implement it on the same struct as their `SchemaWriter` (the
  emitTableDef/emitCreateIndex/emitAddForeignKey helpers are now
  shared between the execute and preview paths). Initial advisory-
  hints registry seeds five high-traffic surprises from real-world
  testing reports (UUID, large-TEXT, JSON-vs-JSONB note, DATETIME
  timezone, unbounded numeric). Translate package gains
  `binary_uuid`, `mediumtext`, `timestamptz`, and parameterised
  `decimal` aliases to support the suggested overrides. See
  ADR-0024.

- **`--reset-target-data` for destructive recovery.** New flag on
  `sluice migrate` and `sluice sync start` that DELETEs the
  bookkeeping row (`sluice_migrate_state` / `sluice_cdc_state`),
  DROPs every source-schema table on the target, then proceeds with
  cold-start. Collapses the post-`slot drop` recovery flow to a
  single command (no more enumerating tables for `DROP TABLE`).
  Confirmation prompt requires the operator to type `reset`
  verbatim — bypassed by `--yes` for non-interactive use. Mutually
  exclusive with `--resume` at parse time. New optional engine
  surfaces: `ir.TableDropper`, `ir.StreamCleaner`, and
  `ir.MigrationStateStore.ClearMigration`. See ADR-0023.

  An additional optional surface, `ir.BulkTableDropper`, lets
  engines collapse the per-table DROP loop into one statement —
  the recovery flow on a 500-table source pays one network round-
  trip instead of 500. Both Postgres (`DROP TABLE … CASCADE`) and
  MySQL (`DROP TABLE …`) implement the bulk path; engines without
  it fall back to per-table `DropTable` automatically. Audit log
  lines name every dropped table on either path.

  `docs/postgres-source-prep.md` cross-references the flag from the
  `wal_status='lost'` recovery section so the doc trail through the
  destructive-recovery flow stays connected.

- **Batched-apply idle flush on quiet streams.** Closes the trailing-
  row latency footnote from ADR-0020. The batched applier now commits
  a partial in-flight batch (n < `--apply-batch-size`) within
  `defaultIdleFlushPeriod` (5s) when no further change arrives. On
  Postgres this lets the slot's `confirmed_flush_lsn` advance past
  in-flight work on idle streams, so warm-resume from a quiet stream
  starts at the most recent commit rather than the previous full
  batch boundary; on MySQL the same logic keeps `source_position`
  current so the replay window on warm-resume stays bounded. Both
  engines use the same 5s default for symmetry. Existing flush
  triggers (channel close, Truncate, ctx cancel) are unchanged; idle
  flush is purely additive. Integration test:
  `TestChangeApplier_ApplyBatch_IdleFlushCommitsPartial` (PG;
  partial-batch persistence on MySQL was already covered by
  `TestChangeApplier_ApplyBatch_PartialFlushPersistsPosition`).

- **MySQL binlog-purged fall-through to cold-start.** Extends the
  v0.5.2 PG slot-missing recovery to the MySQL side. The MySQL CDC
  reader's `resolveStartPosition` now pre-flights the persisted
  position before handing off to go-mysql's binlog syncer:
  - **File/pos mode**: queries `SHOW BINARY LOGS` and checks the
    persisted file is still present. If missing (typical when
    `expire_logs_seconds` rolled it off, or an operator ran
    `PURGE BINARY LOGS`), returns
    `mysql: binlog file %q is no longer available on the source
    (purged); cannot resume: ir: persisted position is no longer
    valid`.
  - **GTID mode**: runs `SELECT GTID_SUBSET(@@gtid_purged, ?)` with
    the resume set. Returns 0 when the source has purged GTIDs the
    resume set hasn't consumed — meaning we'd be missing data on
    resume — and surfaces `mysql: source has purged GTIDs not
    present in resume set; cannot resume`.

  Both branches wrap with `ir.ErrPositionInvalid`; the streamer's
  existing v0.5.2 fall-through (added engine-neutrally) detects the
  sentinel and re-enters `coldStart` with the same `lsnTracker`.
  No new code in the pipeline package; the engine-neutrality of the
  v0.5.2 design pays off here. ADR-0022 extended.

  Pre-fix shape: a sluice stream restarted after the source's
  binlog had rotated past the persisted file would surface
  go-mysql's raw "Could not find first log file name in binary log
  index file" error mid-stream. Post-fix: the WARN fires at startup,
  cold-start runs, dest is reseeded.

  Integration test:
  `TestStreamer_MySQLToMySQL_BinlogPurgedFallsThroughToColdStart`
  exercises the file/pos branch end-to-end. GTID branch is covered
  by the same `verifyPositionResumable` dispatch and the SQL-side
  semantics of `GTID_SUBSET` (no separate integration test;
  GTID-mode setups are tested elsewhere in the resume coverage).

## [0.5.2] - 2026-05-05

Single-feature patch release closing Item F from the v0.4.0
real-world testing report: PG CDC streams whose replication slot
was dropped (typically after `wal_status='lost'`) now recover via
auto-fall-through to cold-start instead of erroring out with no
flag to bypass.

### Added

- **Slot-missing fall-through to cold-start (Item F).** When a
  Postgres CDC stream's persisted position references a replication
  slot that no longer exists on the source — typically because the
  operator dropped it after sluice surfaced `wal_status='lost'` —
  the streamer now logs a loud WARN naming the slot + persisted LSN,
  then falls through to the cold-start path automatically. No flag
  required; no manual `DELETE FROM sluice_cdc_state` step. Bug 9's
  pre-flight refusal still gates populated-dest operations, so
  operators who want a fresh bulk-copy still pass `--force-cold-start`
  or drop dest tables manually. The fall-through is engine-neutral:
  CDC readers signal the condition via `ir.ErrPositionInvalid`
  (wrapped on their specific diagnostic via `%w`); the pipeline
  detects it via `errors.Is`. PG slot-missing is the only emitter
  in this release; MySQL binlog-purged is queued as a follow-up.
  See ADR-0022.

  Recovery flow before this fix: drop slot → DELETE cdc_state row
  → drop publication → drop dest tables (or `--force-cold-start`)
  → re-run sluice. With this fix: drop slot → drop dest tables
  (or `--force-cold-start`) → re-run sluice. The two manual SQL
  steps disappear.

  Integration test:
  `TestStreamer_PostgresToPostgres_SlotMissingFallsThroughToColdStart`.

## [0.5.1] - 2026-05-05

Single-issue patch release fixing a misleading flag name in the
Postgres `wal_status='unreserved'`/`'lost'` recovery hint. No
behavioural change.

### Fixed

- **`wal_status` recovery hint named `--target` instead of
  `--source` (Item F).** When sluice refused to start CDC against an
  invalidated slot, the error message pointed operators at
  `sluice slot drop <name> --target ...`. The slot lives on the
  *source* database and `slot drop`'s actual flag is `--source` —
  operators following the hint hit a flag-not-found error and had
  to consult `slot drop --help` to recover. Both the `unreserved`
  and `lost` branches of `checkSlotUsable` now emit
  `--source-driver=postgres --source ...`. `docs/postgres-source-prep.md`
  is corrected in lockstep. Real-world testing surfaced this as the
  one polish item against an otherwise gold-standard error message.
  Test coverage extended to assert the recovery hint references
  `--source` so the regression doesn't return.

## [0.5.0] - 2026-05-05

Reliability + performance release. Headline feature is parallel
within-table bulk copy (the pgcopydb-class signature win for multi-TB
migrations), throughput metrics extended to MB/s + ETA, plus four
fixes uncovered during real-world v0.4.0 soak testing — one of which
(Bug 15) was a CRITICAL silent-data-loss path on Postgres CDC. Three
new ADRs (0019, 0020, 0021).

### Added — performance

- **Parallel within-table bulk copy.** Tables above
  `--bulk-parallel-min-rows` (default 100k) with a single integer PK
  are now split into N PK ranges and copied concurrently, with per-
  chunk cursor checkpoints in `sluice_migrate_state`. Tables below
  the threshold, with composite PKs, or without a PK fall through to
  the v0.4.x single-reader behaviour. Postgres readers share a single
  exported snapshot via `SET TRANSACTION SNAPSHOT` (`SnapshotImporter`
  optional engine surface) so all chunks see a consistent view; MySQL
  uses per-chunk `REPEATABLE READ` transactions because per-session
  REPEATABLE-READ snapshots have no shareable name. Boundaries are
  computed once via `MIN`/`MAX` on the PK and persisted, so a resume
  run aligns exactly with completed chunks rather than recomputing
  ranges (which would shift if rows landed concurrently). New flags:
  `--bulk-parallelism` (default `min(8, NumCPU)`) and
  `--bulk-parallel-min-rows`. See ADR-0019.
- **Throughput metrics: MB/s + ETA.** The bulk-copy progress ticker
  now emits `total_rows`, `bytes`, `rate_mb_per_sec`, and
  `eta_seconds` alongside the existing `rows`/`rate` attributes;
  per-chunk progress lines carry a `chunk=` attribute so operators
  can see which range is in flight. Row-byte estimation walks the
  `ir.Row` value-side: string/`[]byte` by length, fixed-width
  numerics by Go size, `time.Time` as 24, bool as 1, recursive on
  `[]any`/`[]string`. Approximate but stable enough that MB/s tracks
  observed network throughput within a few percent.
- **`CountRows` / `RangeBounds` optional engine surfaces.** Postgres
  estimates row counts via `pg_class.reltuples` (autovacuum-
  maintained); MySQL via `information_schema.TABLE_ROWS`. Both short-
  circuit when called against a snapshot-pinned reader where a
  concurrent query would deadlock the single shared connection. The
  ETA computation falls back gracefully when the surface isn't
  available.

### Fixed

- **Postgres CDC: slot ack advanced before apply commit (Bug 15,
  CRITICAL — silent data loss on crash).** The PG CDC reader was
  sending the *streamed* LSN in `StandbyStatusUpdate`, so a crash
  between `Send` and `tx.Commit` advanced `confirmed_flush_lsn` past
  events that were never applied — and a warm resume started at the
  acked position, dropping the in-flight batch on the floor. Real-
  world soak observed silent row drift after a clean stop/restart
  cycle when the streamer happened to interrupt a partial batch.

  Fix: a single-producer/single-consumer `lsnTracker` plumbed
  engine-neutrally via `lsnTrackerProvider`/`lsnTrackerAttacher`
  structural interfaces. The applier reports `appliedLSN` after
  `tx.Commit()`; the reader sends `min(streamed, applied)` in the
  next status update. Trailing-row latency under `--apply-batch-size
  > 1` is bounded by the batch interval since the LSN only advances
  on commit boundaries — acceptable today; idle-flush is on the
  roadmap. See ADR-0020.

  Integration test: `TestStreamer_PostgresToPostgres_StopRestartNoLoss`
  exercises a stop in the middle of a batched apply and asserts
  every source change lands on the target after warm resume.

- **Postgres CDC: publication scope was `FOR ALL TABLES` (Bug 13).**
  The v0.4.0 publication was created `FOR ALL TABLES`, so a brand-
  new unrelated table on the source — created after sluice started
  streaming — would land in the pgoutput stream. The applier either
  crashed on the unknown table OID or, worse, silently dropped the
  events.

  Fix: `Engine.EnsurePublication(ctx, dsn, tables)` now creates
  `FOR TABLE <list>` from the resolved migration set after
  `applyTableFilter`. Existing v0.4.0 `FOR ALL TABLES` publications
  are migrated by drop-and-recreate during cold start (the slot is
  unaffected; only the publication is replaced). The applier now
  has defence-in-depth: an unknown table OID is logged at WARN and
  the change is skipped rather than crashing the stream. See
  ADR-0021.

  Integration test: `TestStreamer_PostgresToPostgres_NewTableOnSourceIgnored`
  creates a fresh table on the source mid-stream and asserts the
  applier ignores it.

- **PG array → MySQL JSON conversion (Bug 14).** A PG source column
  of array type (e.g. `text[]`, `int[]`) migrating to a MySQL JSON
  target arrived at the writer as `[]any`, a PG-array literal string
  (`{a,b,c}`), or `[]byte` holding the same — none of which MySQL's
  driver knows how to bind to a JSON column. `prepareValue` now
  branches `convertArrayLikeToJSON` for all three shapes. Empty
  arrays serialize as `[]` (disambiguated from `{}`, which would be a
  JSON object). Integration test:
  `TestMigrate_PostgresToMySQL_ArrayToJSONOverride`.

- **MySQL CDC: silent stalls on quiet upstream (Bug 12).**
  go-mysql's binlog syncer can hang silently if the upstream goes
  quiet for long enough that the TCP keepalive doesn't fire — the
  reader has no signal to distinguish "no events" from "connection
  dead". v0.5.0 sets `defaultBinlogHeartbeatPeriod = 10s` on the
  syncer so the upstream emits keep-alive heartbeats, and adds a
  30s no-events watchdog that surfaces a stalled-stream error if no
  row-relevant event arrives in that window (filtered by
  `isRowRelevantEvent` so heartbeat and rotation events don't reset
  the timer indefinitely, which would mask a real stall). Not
  reproducible in CI without a multi-minute idle, so manually
  validated against real PlanetScale/vanilla MySQL streams.

### Added — architecture documentation

Three new ADRs in `docs/adr/`:

- **ADR-0019**: Parallel within-table bulk copy — chunk-boundary
  computation, snapshot-import strategy per engine, boundary
  stability invariant, fallback matrix.
- **ADR-0020**: Slot-ack-after-apply — LSN tracker design, SPSC
  contract, why `min(streamed, applied)` instead of just `applied`,
  trailing-row latency tradeoff.
- **ADR-0021**: Publication scope by table — `FOR TABLE <list>`
  rationale, drop-and-recreate migration from v0.4.0 publications,
  applier defence-in-depth on unknown OIDs.

## [0.4.0] - 2026-05-04

Feature release with four substantive responses to measured production
concerns from the v0.3.x robustness testing rounds, plus three new
ADRs (0016, 0017, 0018) documenting the design decisions.

### Added — performance

- **`--apply-batch-size N`** on `sluice sync start` (and
  `Streamer.ApplyBatchSize` for programmatic callers) batches up to N
  CDC changes per target transaction with the position write of the
  last change in the batch. Default 1 keeps v0.3.x conservative
  one-change-per-tx behaviour; production tuning is 100–500. v0.3.0
  testing measured the per-change applier at ~6.5 rows/sec on
  PG→MySQL CDC with a 5000-row source transaction; batched mode
  amortises commit overhead 50–100× on production hardware (3.5×
  observed locally without fsync). Idempotency preserved via the
  existing ON CONFLICT / ON DUPLICATE KEY UPDATE semantics on
  Insert. Schema-change events (Truncate, DDL) flush the in-flight
  batch before applying. See ADR-0017.
- **`--bulk-batch-size N`** on `sluice migrate` (default 5000)
  controls the per-batch checkpointing size for resume. Cold-start
  migrations continue to use the faster plain-INSERT (and PG COPY)
  path with no per-batch overhead.

### Added — operability

- **Per-batch checkpointing for `sluice migrate --resume`.**
  Previously, resume on an in-progress table truncated and re-copied
  from row 0. v0.4.0 tracks a per-table PK cursor in
  `sluice_migrate_state.table_progress`, reads the source via
  `WHERE pk > cursor ORDER BY pk LIMIT batch_size`, and applies
  rows with `ON CONFLICT` / `ON DUPLICATE KEY UPDATE` so the brief
  replay window between batch commit and cursor write is tolerated
  cleanly. Multi-hour copies of 100M+ row tables can resume mid-
  table. Composite PKs descend via row-comparison cursors
  (`(a,b) > ($1,$2) ORDER BY a,b`). Tables without a PK fall back
  to the v0.3.0 truncate-and-redo behaviour with a clear log line.
  v0.3.0-shape state rows are read backward-compatibly. See
  ADR-0018.
- **Cross-engine expression translation for generated columns and
  CHECK constraints.** v0.3.2's verbatim-passthrough policy held
  the fail-loud claim (no silent corruption), but the set of
  "non-portable" expressions included very common idioms.
  Bidirectional translation pass at the writer boundary now covers:
  - **MySQL → Postgres**: `CONCAT(a,b)` → `(a || b)`, `IFNULL` →
    `COALESCE`, `IF(cond,a,b)` → `CASE WHEN cond THEN a ELSE b END`,
    `JSON_UNQUOTE(JSON_EXTRACT(j,'$.k'))` → `(j->>'k')`,
    `JSON_EXTRACT(j,'$.k')` → `(j->'k')`.
  - **Postgres → MySQL**: `(expr)::type` → `CAST(expr AS …)`,
    `a || b` → `CONCAT(a, b)`, `~~`/`~~*` → `LIKE`/case-insensitive
    `LIKE`, `= ANY(ARRAY[…])` → `IN (…)`.

  Unrecognized constructs still pass through verbatim and rely on
  the loud-failure-on-target fallback. Translator uses a string-
  literal-aware walker that respects single-quoted literals and
  balanced parens — no full SQL parser. See ADR-0016.

### Fixed

- **Cold-start hangs when dest tables have pre-existing data
  (Bug 9, open since v0.3.0).** Three-part fix:
  1. **Pre-flight refusal**: cold-start now checks each source
     table for non-empty dest data and refuses with a clear error
     pointing at recovery commands. Skipped on `--resume` (resume
     expects partial state).
  2. **Goroutine-leak fix**: `copyTable` now derives a child
     context and cancels it on every return path. Previously, when
     `WriteRows` errored mid-stream, the row-reader goroutine
     blocked forever on `out <- row` against an abandoned
     consumer, holding the snapshot transaction open and surfacing
     as PG's "idle in transaction" sessions.
  3. **Clearer log shape**: progress ticker's Stop now takes the
     writer error and logs `bulk copy aborted table=foo rows=N
     err="…"` on failure instead of the misleading `bulk copy
     complete rows=N`. New `--force-cold-start` flag bypasses the
     pre-flight refusal for the rare legitimate "bulk-copy into a
     populated target" case.
- **`stop_requested_at` not cleared after consumption (Bug 11,
  open since v0.3.2).** A `sluice sync stop` left the timestamp
  set after the streamer drained and exited; the next
  `sluice sync start` would see the stale signal and exit within
  the first poll interval. The streamer now clears the flag at
  startup (after `EnsureControlTable`, before reading the persisted
  position). Idempotent and tolerant of a missing row. New
  `ChangeApplier.ClearStopRequested` interface method on the
  applier.

### Changed

- **`docs/type-mapping.md` corrected for PG→MySQL `Inet`/`Cidr`/
  `Macaddr`/`Array` types.** The doc previously claimed auto-emit
  as `VARCHAR(N) CHECK (format)`; v0.3.x and v0.4.x actually refuse
  loudly with a copy-paste-ready `mappings:` YAML snippet pointing
  at the `--type-override` CLI flag. Auto-emit is queued as a
  future enhancement; manual override is the supported path today.

## [0.3.2] - 2026-05-04

Patch release adding CHECK constraint support, a CLI form of the
type-override YAML config, and an opportunistic improvement to
the generated-column expression normalizer that the CHECK work
surfaced.

### Added

- **CHECK constraint support across both engines.** Source schemas
  declared with `CHECK (qty >= 0)` or `CHECK (status IN ('open',
  'closed'))` now round-trip cleanly: the schema readers capture
  the expression on `Table.CheckConstraints`, the DDL writers
  emit `CONSTRAINT name CHECK (expr)` inline in CREATE TABLE,
  and the constraint is enforced on the target.

  Translation policy is verbatim passthrough — non-portable
  expressions fail loudly on the target rather than be guessed
  at. Identifier and string-literal decoration is normalized at
  the read boundary (see below).

  Integration coverage: MySQL→MySQL, PG→PG, and MySQL→PG cross-
  engine snapshot migrations each verify (1) the CHECK lands on
  the target's `information_schema.check_constraints`, (2)
  bulk-copied rows survived, (3) a violating INSERT is rejected
  by the target, and (4) a satisfying INSERT is accepted.

- **`--type-override TABLE.COLUMN=TYPE` CLI flag** on `sluice
  migrate` and `sluice sync start`. Repeatable; format mirrors
  the YAML `mappings:` shape but in a single string. Wholesale
  CLI-over-YAML precedence (matches the existing `--include-table`
  / `--exclude-table` precedence policy). For target-type options
  (e.g. `jsonb` with `binary=true`) operators still need the YAML
  form — the CLI deliberately doesn't try to encode key/value
  options in a single string.

### Fixed

- **Generated-column cross-engine expressions with string
  literals**. The v0.3.1 generated-column work normalized MySQL's
  backtick identifier quotes but missed two more layers of
  decoration MySQL applies to the stored expression text:

  - **Charset introducers** — every string literal is wrapped as
    `_<charset>'literal'` (e.g. `_utf8mb4'open'`). PG rejects this
    as a syntax error.
  - **Delimiter-escape form** — every string literal's apostrophes
    are stored as `\'`. PG with `standard_conforming_strings=on`
    (the default since 9.1) rejects `\'` outright.

  v0.3.1 didn't catch these because the test fixtures used
  `qty * price` — no string literals. The CHECK constraint work
  in this release surfaced both immediately (via `status IN
  ('open', ...)`) and the new `normalizeMySQLExpressionText`
  helper now strips all three layers. **Generated columns benefit
  from the same fix**: a column declared as `CONCAT(name, ' ')`
  cross-engine that would have silently failed on v0.3.1 now
  works.

## [0.3.1] - 2026-05-04

Patch release — adds first-class generated-column support and
includes the CI-pipeline fixes that surfaced during the v0.3.0
release rebuild.

### Added

- **Generated column support across both engines.** Source columns
  declared as `GENERATED ALWAYS AS (expr) STORED` (or `VIRTUAL` on
  MySQL) now round-trip cleanly: the schema readers capture the
  expression on `ir.Column.GeneratedExpr`, the DDL writers emit
  the corresponding `GENERATED ALWAYS AS (...)` clause, and the
  bulk-copy / CDC paths skip the column from INSERT/UPDATE column
  lists so the target re-computes via its own GENERATED clause.

  Translation policy is verbatim passthrough — non-portable
  expressions (e.g. MySQL `CONCAT(a, b)` vs PG `a || b`) fail
  loudly on the target rather than be guessed at. Identifier
  quoting *is* normalized at the read boundary (MySQL's stored
  expression text uses backticks that PG can't parse), since
  that's a mechanical dialect-quoting issue rather than a
  function/operator translation. Cross-engine sources with
  VIRTUAL columns are silently promoted to STORED on PG (which
  doesn't support VIRTUAL) with a `slog.Warn` documenting the
  shift.

  Integration coverage on MySQL→MySQL, PG→PG, and MySQL→PG
  (cross-engine) for both the migrate and streamer paths.

### Fixed

- **CI pipeline fixes uncovered during the v0.3.0 release rebuild**:
  - Migrated `.golangci.yml` to v2 schema (top-level `version: "2"`,
    `linters.default: none`, formatters split into the new
    top-level `formatters:` section, drop deprecated `gosimple`
    which is merged into `staticcheck`).
  - Bumped `golangci/golangci-lint-action` to `@v8` so `version:
    latest` resolves to the v2 module path.
  - Re-enabled `install-mode: goinstall` so the linter compiles
    with our Go 1.26 toolchain rather than the prebuilt-binary's
    older Go (which couldn't typecheck stdlib `chacha20poly1305`'s
    Go-1.26-only file).
  - **MySQL binlog composite-PK test**: corrected `int32` type
    assertions to `int64`. The binlog reader's `decodeInteger`
    widens every integer to `int64`, so the v0.3.0 test asserted
    a type that doesn't exist in the row map.
  - Five new lint findings v1 missed (caught by v2): `any`
    variable shadowing the builtin, an embedded-field selector
    simplification, a capitalised error string, two De-Morgan'd
    conditional reads.

### Changed

- **Schema readers exclude `sluice_*_state` tables**. Already done
  in v0.3.0 for the migrate-state table; this release extends the
  list to fully cover both bookkeeping tables on re-migrations.

## [0.3.0] - 2026-05-04

Feature release. Three substantial additions to the operator surface
(`sluice migrate --resume`, `sluice sync stop`, `--include-table` /
`--exclude-table`), one silent-data-loss fix on Postgres CDC, and
five new ADRs documenting the v0.2.x and v0.3.0 design decisions.

### Added — resumable simple-mode migrations

- **`sluice migrate --resume --migration-id ID`** picks up a failed
  migration where it left off rather than forcing a drop-and-redo.
  Per-target `sluice_migrate_state` row tracks phase
  (`tables`/`bulk_copy`/`identity_sync`/`indexes`/`constraints`/
  `complete`) and per-table bulk-copy progress as a JSON map.
  In-progress tables are TRUNCATEd before re-copy. Failure paths
  persist the in-flight phase plus a 1KB-truncated error message;
  a state-write failure during cleanup is joined with the primary
  error via `errors.Join` so the operator never loses the root
  cause.
- **Behavior matrix** is conservative for non-resume runs: existing
  state row + no `--resume` errors out (no silent overwrites), and
  `--resume` against a `complete` row exits cleanly with an
  "already complete" log. New `MigrationStateStore` and
  `TableTruncator` are optional engine surfaces (type-assertion
  pattern, mirroring `SlotManagerOpener`); engines without the
  primitives error clearly when `--resume` is requested.
- **`CREATE TABLE IF NOT EXISTS`** is now universal in the DDL
  emitters on both engines, so the resume tables-phase is a clean
  no-op on re-run. Schema readers exclude `sluice_*_state` so
  re-migrations don't propagate sluice's bookkeeping as user data.

### Added — selective table inclusion / exclusion

- **`--include-table TABLE,...`** and **`--exclude-table TABLE,...`**
  on `sluice migrate` and `sluice sync start`. Comma-separated,
  repeatable, glob patterns supported via stdlib `path.Match`
  (`audit_*`, `tmp_*`). Mutually exclusive at the CLI parse layer.
  Same fields available in YAML config as `include_tables` /
  `exclude_tables`; CLI takes precedence wholesale (no merge).
- **Filtering happens at the orchestrator boundary**: schema
  pruning after `ReadSchema` and a CDC dispatch wrapper that drops
  events for excluded tables before the applier sees them. Engines
  remain agnostic to the spec, so behaviour is identical across
  MySQL/Postgres/future engines.
- **Position-advancement caveat**: positions only commit when an
  event applies, so a stream that consists entirely of dropped
  events lags within the source-side WAL/binlog retention window.
  Documented on the `Streamer.Filter` field.

### Added — graceful stream stop

- **`sluice sync stop --target-driver X --target DSN --stream-id ID`**
  asks a running sync stream to drain in-flight changes, persist
  the final position, and exit cleanly. Mechanism is a control-
  table flag (`stop_requested_at` column on `sluice_cdc_state`)
  polled by the running streamer every 5s. Survives operator
  machine boundaries, container lifecycles, and process restarts —
  the flag persists; a restarted streamer sees it on next poll.
- **Additive to `Ctrl-C` / `SIGTERM`** which still work via the
  existing signal path. The new mechanism fits Kubernetes lifecycle
  hooks, systemd `ExecStop`, and remote orchestrators that can't
  send signals to a different machine.
- **Idempotent schema migration**: existing v0.2.x deployments pick
  up the new column on next `EnsureControlTable` call without
  losing data. PG uses `ADD COLUMN IF NOT EXISTS`; MySQL uses
  detect-then-ALTER for portability across all 8.x versions.

### Added — observability

- **Structured logging via `log/slog`** (replacing
  `fmt.Fprintf`-to-stdout). `--log-level` is now wired into the
  default handler; `debug` / `info` / `warn` / `error` actually
  change verbosity. Pipeline records emit as
  `time=... level=INFO msg="..." key=value` to stderr; CLI table
  outputs (`engines`, `sync status`, `slot list`) keep using stdout
  unchanged — they're table renders, not log streams.
- **Bulk-copy progress reporting**: a per-table `progressTicker`
  emits `bulk copy progress table=foo rows=N rate=R` every 2s
  while a copy is in flight, plus a final `bulk copy complete`
  line on table completion. Long migrations are no longer 30
  minutes of silence.
- **Phase-aware error hints**: wrapped pipeline errors gain an
  optional one-line `hint:` suffix for common operator-facing
  failures (missing target table, bad DSN host, auth failures,
  missing `REPLICATION` grant, missing `CREATE` on schema).
  Registry is intentionally tiny (7 entries, scoped by phase);
  hints are appended via `fmt.Errorf("%w\nhint: %s")` so
  `errors.Is`/`As` traversal is unaffected.

### Added — architecture documentation

Five new ADRs in `docs/adr/`:

- **ADR-0011**: `SlotManager` as an optional engine surface.
- **ADR-0012**: Bypass `pglogrepl` to send raw
  `CREATE_REPLICATION_SLOT FAILOVER true` for PG 17+.
- **ADR-0013**: Applier value-shaping via column-type cache and
  `CAST(? AS JSON)` (the Bug 6 fix shape).
- **ADR-0014**: Phase-aware error-hint registry (substring + phase
  matching, deliberately tiny).
- **ADR-0015**: Migration resume design — per-target state table,
  truncate-and-redo for in-progress tables, `errors.Join` on
  state-write-during-failure paths.

### Fixed

- **Postgres CDC: composite-PK DELETE silently lost (Bug 8)**.
  pgoutput's `DeleteMessage` with `REPLICA IDENTITY DEFAULT`
  carries an `OldTuple` whose `ColumnNum` equals the relation's
  full column count, with `'n'` (null) markers for non-key
  columns. `decodeTuple` translated those into present-but-nil
  entries on the row map; the applier's `WHERE` then emitted
  `non_key IS NULL` predicates that matched zero rows on the
  destination. The applier's resume-idempotency tolerance for
  zero-rows-affected (ADR-0010) absorbed the silence; the
  position advanced; `DELETE`s disappeared. Real-world soak
  testing observed a 30-row drift on a composite-PK
  `order_items` table.

  Fix: `filterDeleteBefore` narrows the emitted Before to columns
  flagged `KeyColumn=true` on the relation cache. Correct under
  every `REPLICA IDENTITY` mode (DEFAULT drops `'n'` entries; FULL
  drops non-identity columns; USING INDEX is a no-op on the
  already-narrow OldTuple; PK-less FULL falls back to the full row
  to honour the operator's deliberate setting). `REPLICA IDENTITY
  NOTHING` is rejected loudly — DELETE is unreplicatable in that
  mode.

  MySQL is unaffected: `binlog_row_image=FULL` (the default)
  carries every column with real values, so the WHERE matches
  exactly. The user's PG→MySQL drift was the PG source-side bug
  propagating through.

### Test gap closed

- **Composite-PK CDC coverage on MySQL paths**. Bug 8 reached
  real-world soak because no existing CDC integration test
  exercised composite-PK tables across any direction. Added
  `TestCDCReader_CompositePK` (MySQL binlog, asserts both PK
  columns survive INSERT/UPDATE/DELETE) and
  `TestStreamer_MySQLToPostgres_CompositePKDelete` (cross-engine,
  asserts row-count drop on the target). VStream coverage punted
  to a follow-up — the test infrastructure (vtgate setup) is
  heavier and the protocol surface differs enough to warrant its
  own pass.

## [0.2.2] - 2026-05-04

Patch release closing a CDC-applier JSON-encoding bug that surfaced
during v0.2.1 revalidation testing — affecting both PG→MySQL (loud
crash) and MySQL→MySQL (silent data divergence). Plus a small
dry-run output clarification and a debug-level zero-rows-affected
log so the silent class of bug is one filter away from being
spotted in the future.

### Fixed

- **MySQL applier: shape JSON column values for the wire on CDC
  Insert/Update/Delete**. The MySQL `ChangeApplier` bound row values
  straight from `ir.Row` to the parameterised SQL, bypassing the
  `prepareValue` used by the bulk-copy path. Two production failures
  shared the same root cause:

  - **Loud (PG → MySQL CDC on Vitess/PlanetScale)**: `[]byte` JSON
    values arrived `_binary`-tagged on the wire and Vitess rejected
    them with "Cannot create a JSON value from a string with
    CHARACTER SET 'binary'". Sluice exited.
  - **Silent (MySQL → MySQL CDC, vanilla MySQL included)**: `WHERE`
    on a JSON column with a bare `?` placeholder never matched —
    MySQL's `=` operator does not implicitly cast a bound parameter
    to JSON regardless of whether it's `[]byte` or `string`. The
    applier (which tolerates zero-rows-affected for resume
    idempotency) silently advanced past UPDATEs and DELETEs that
    should have matched. The destination row stayed stale forever
    with no error signal — data divergence with no observability.

  The fix has two parts: (1) a per-table column-type cache lets every
  bound value go through `prepareValue` (so JSON `[]byte` → `string`,
  Set `[]string` → comma-joined, Geometry gets the SRID prefix); and
  (2) `WHERE` placeholders on JSON-typed columns are wrapped in
  `CAST(? AS JSON)` so the comparison is JSON-vs-JSON rather than
  JSON-vs-text. The Postgres applier got the parallel cleanup for
  symmetry and for Array/Geometry shaping (its WHERE didn't need a
  CAST equivalent — pgx inspects per-column type metadata natively).

  A new `TestChangeApplier_JSONColumn` integration test on each
  engine exercises the silent path end-to-end; without the fix it
  fails loudly in PG→MySQL and quietly in MySQL→MySQL.

### Added

- **Debug-level zero-rows-affected log on Update/Delete**. The
  applier still tolerates zero-rows-affected (resume idempotency
  depends on it), but a `slog.Debug` line now fires when it
  happens — a single observability footprint that lets future
  silent-divergence bugs be one log filter away from being spotted.

### Changed

- **Dry-run table output: split `indexes` into `primary_key` +
  `secondary_indexes`**. The IR stores the primary key on a separate
  field from secondary indexes, so the v0.2.0 `indexes=N` field
  silently excluded PK and confused operators comparing against
  psql / SHOW INDEX output. The new shape (`primary_key=true
  secondary_indexes=1 foreign_keys=2`) is explicit from the field
  names alone.

## [0.2.1] - 2026-05-03

Single-issue patch release fixing a regression introduced in v0.2.0:
PG-source CDC is unblocked on PlanetScale Postgres (and any other
PG 17+ deployment whose option-list parser is strict).

### Fixed

- **PG 17+ slot creation: use named `SNAPSHOT 'export'` option**.
  v0.2.0 sent `CREATE_REPLICATION_SLOT ... (EXPORT_SNAPSHOT,
  FAILOVER true)` on PG 17+, which is a syntax mismatch — the bare
  `EXPORT_SNAPSHOT` keyword is the *pre-PG-17* form. Inside the new
  parenthesised option-list grammar the snapshot option must be the
  named form `SNAPSHOT 'export'`. PlanetScale Postgres rejected the
  v0.2.0 form with `ERROR: unrecognized option: export_snapshot`,
  blocking every `sluice sync start` against a PG source. Cold-start
  CDC (without snapshot export) was unaffected; snapshot+CDC handoff
  is the path that hit it.

## [0.2.0] - 2026-05-03

Bug-fix and operator-UX release driven by real-world v0.1.0
testing against PlanetScale Postgres + MySQL. Four target-side
data-correctness bugs fixed; the slot lifecycle on PG sources
gets a first-class CLI plus auto-drop on failed setup; logical
slots now opt into PG 17 `FAILOVER`; CLI output moves to
structured logging with bulk-copy progress lines and phase-aware
error hints.

### Added — operator surface

- **`sluice slot list` / `sluice slot drop`**: source-side
  replication-slot management for Postgres CDC. List shows
  every slot's plugin, active flag, `wal_status`, `restart_lsn`,
  and `confirmed_flush_lsn`; drop is destructive and prompts
  for confirmation by default (`--yes` skips, `--force` allows
  dropping an active slot, `--if-exists` swallows the not-found
  error). Engines without slot management (MySQL today) surface
  a clear error rather than silently no-op. Backed by a new
  `ir.SlotManager` interface that engines opt into via
  `OpenSlotManager`.
- **Auto-drop slot on failed cold-start**: when sluice creates a
  fresh slot in `StreamChanges` and any later setup step fails
  (IDENTIFY_SYSTEM, START_REPLICATION, ctx cancellation), the
  slot is dropped before `StreamChanges` returns. Slots that
  already existed when the call started are never touched. Once
  the channel is in the caller's hands the auto-drop is
  suppressed: emitted change positions reference the slot, and
  that's user data we don't auto-clean.
- **Refuse to start on invalidated slots**: `pg_replication_slots
  .wal_status` of `unreserved` or `lost` (the latter caused by a
  slow consumer falling behind `max_slot_wal_keep_size`) now
  surfaces a clear, actionable error pointing at
  `sluice slot drop` and `max_slot_wal_keep_size` for prevention,
  instead of letting `START_REPLICATION` fail mid-stream with
  "requested WAL segment has already been removed".
- **Structured logging via `log/slog`**: `--log-level` is now
  wired into the slog default handler (stderr text format), so
  `debug`/`info`/`warn`/`error` actually changes verbosity. The
  pipeline's `Migrator` and `Streamer` types drop their `Stdout`
  fields and emit structured records (`migration complete
  tables=N`, `bulk copy complete table=foo rows=N`, etc.).
  Operator-facing CLI tables (`engines`, `sync status`,
  `slot list`) keep using stdout — they're table renders, not
  log streams.
- **Bulk-copy progress reporting**: a new `progressTicker` sits
  in the row pipe between `RowReader` and `RowWriter` for each
  bulk-copied table. It atomically counts rows, emits
  `bulk copy progress` every 2s while rows are advancing, and a
  final `bulk copy complete` line on Stop. Counting at the
  pipeline layer keeps engines unchanged.
- **Phase-aware error hints**: wrapped pipeline errors get an
  optional one-line `hint:` suffix for common operator-facing
  failures — missing target table, bad DSN host, auth failures,
  missing REPLICATION grant, missing CREATE on schema. Hints are
  appended via `fmt.Errorf("%w\nhint: %s")` so `errors.Is`/`As`
  traversal is unaffected. Registry is intentionally tiny (7
  entries) and scoped by phase.

### Added — Postgres slot HA

- **`FAILOVER true` on PG 17+ slot creation**: both slot-creation
  sites — the cold-start path in the CDC reader and the
  snapshot+CDC handoff — now go through a version-aware helper.
  PG 17+ sends a raw `CREATE_REPLICATION_SLOT ... (FAILOVER true)`
  protocol command via `pgconn.Exec` (pglogrepl's options struct
  doesn't yet expose the flag); PG ≤ 16 falls back to the
  FAILOVER-less path and emits a one-time stderr warning naming
  the slot and pointing at the manual workaround. Closes the
  silent slot-loss-on-failover gotcha for PlanetScale and any
  Patroni-fronted PG 17+ deployment.

### Added — orchestration

- **`sluice sync start --dry-run`** (`-n`): symmetric with the
  existing `migrate --dry-run` flag. Reads the source schema,
  looks up the persisted position on the target, and prints the
  plan (cold-start vs warm-resume; source schema summary or
  position token) without modifying the target or starting the
  stream. The position lookup is tolerant of the control table
  being absent — both engines' `readPosition` helpers now fall
  through "missing relation" errors as "no row".

### Added — managed-service support

- **Multi-shard Vitess snapshot+CDC handoff**: the snapshot path
  (`Engine.OpenSnapshotStream` on the `planetscale` flavor) now
  fans out to every shard in a sharded keyspace, buffers rows
  from all shards into a unified per-table view, and uses the
  global `COPY_COMPLETED` event (both `Keyspace` and `Shard`
  empty) as the snapshot→CDC handoff boundary. The captured
  `ir.Position` carries one `shardGtid` entry per shard. Pairs
  with `vstream_auto_discover_shards=true` for shard discovery
  via `SHOW VITESS_SHARDS`. Validated against
  `vitess/vttestserver` with `NUM_SHARDS=2`.
- **Reshard-during-COPY signalling**: a `JOURNAL` event during
  the snapshot path's COPY phase now surfaces the typed
  `ShardLayoutChangedError`, matching the standalone CDC reader.
  v1 of the multi-shard snapshot does not recover in place — the
  caller drops the snapshot stream and reopens against the new
  layout.

### Fixed

- **MySQL target rejects JSON values labelled `_binary`**: PG
  source columns of type JSONB arriving through a MySQL writer
  were being sent over the wire with the `_binary` charset
  prefix, which Vitess (and MySQL strict mode) reject with
  "Cannot create a JSON value from a string with CHARACTER SET
  'binary'". `prepareValue` now converts `[]byte` to `string`
  for `ir.JSON` columns. Surfaced during PlanetScale-target
  testing.
- **Warm-resume engine alias**: `ChangeApplier.ReadPosition`
  stamps every recovered position with the applier's engine
  name (always `mysql` for the MySQL applier) regardless of
  which reader produced the original. Strict engine-name checks
  in `decodeBinlogPos` / `decodeVStreamPos` rejected warm-resume
  on PlanetScale streams with `wrong engine "mysql"; want
  "planetscale"`. Both decoders now accept the mysql-family
  aliases (`mysql` or `planetscale`); the cross-engine guard
  still rejects `postgres` positions.
- **Postgres UPDATE empty-WHERE under REPLICA IDENTITY DEFAULT**:
  pgoutput omits `OldTuple` on UPDATEs that don't modify the
  identity-key columns (the common case under the server-default
  identity). The CDC reader previously left `Before` nil, and
  the applier built `UPDATE t SET ... WHERE` with an empty
  predicate that Postgres rejects with "syntax error at end of
  input". The reader now synthesises a key-only `Before` from
  the after-tuple's identity columns. REPLICA IDENTITY NOTHING
  and tables without identity columns surface a clear error
  instead of a malformed statement.
- **MySQL `CURRENT_TIMESTAMP` default precision mismatch**: MySQL
  rejects `TIMESTAMP(6) DEFAULT CURRENT_TIMESTAMP` because the
  function-call precision must equal the column's. The most
  common path that hit this was a PG `TIMESTAMPTZ DEFAULT now()`
  migrating to MySQL — PG reports `Precision=6`, the translator
  turned `now()` into bare `CURRENT_TIMESTAMP`, leaving
  precisions mismatched. `emitDefault` now promotes a bare
  `CURRENT_TIMESTAMP` to `CURRENT_TIMESTAMP(N)` on a
  `TIMESTAMP`/`DATETIME`/`TIME` column with non-zero precision.
  Expressions that already carry an explicit precision pass
  through unchanged.

### Added — docs

- **`docs/postgres-source-prep.md`**: operator checklist for
  running sluice CDC against a Postgres source — required GUCs,
  connecting role attributes, slot lifecycle, `wal_status`
  recovery workflow, and the failover-survival mechanisms
  (Patroni `slots:`, PlanetScale "Logical slot name" UI,
  PG 17 `sync_replication_slots`). The PlanetScale section is
  load-bearing: slot loss on failover is silent without proper
  permanent-slots config.
- **README hero example** showing `migrate` / `sync start` /
  `sync status` end-to-end against the same DSN pair.
- **CONTRIBUTING test-tag layering**: documents the four build
  tags (default, integration, integration+postgis,
  integration+vstream, psverify) and which container images each
  pulls.

## [0.1.0] - 2026-05-03

The initial tagged release. Captures everything from the design
pass through the multi-shard Vitess + `sluice sync status`
chunks. Entries are grouped by capability rather than
chronologically; `git log` is the source of truth for commit-
level history.

### Added — orchestration

- **Simple-mode `Migrator`**: one-shot schema-and-data migration
  with three-phase apply (tables-without-constraints → bulk row
  copy → identity-sequence sync → indexes → foreign keys). Wired
  into the kong `migrate` subcommand. CLI signals (Ctrl-C) cancel
  cleanly via context.
- **Continuous-sync `Streamer`**: long-running snapshot+CDC
  orchestrator. Cold start captures a consistent snapshot, runs
  the bulk-copy phase, then tails CDC events through to a target
  `ChangeApplier`. Warm resume reads the persisted position from
  the target's control table and skips the snapshot phase
  entirely. Wired into the `sluice sync start` subcommand.
- **Translation layer (`internal/translate`)**: per-column
  type-override layer that consumes the `mappings:` block from
  `sluice.yaml` and rewrites column types in the IR before the
  schema-write phase sees them. Strict on missing tables/columns
  (typos surface as startup errors). Initial alias set covers
  `text`, `text_array`, `jsonb`, `json`, `bytea`, `varchar`
  (with optional `length` option), and the eight `postgis_*`
  geometry shapes (with optional `srid`).
- **`sluice sync status`** subcommand: prints every continuous-
  sync stream the target database has been the destination for
  (one row per `sluice_cdc_state` entry) with stream-id, last-
  updated wall-clock, human "5m ago" age, and a truncated
  position token. Filterable to a single stream via
  `--stream-id`. Tolerant of the target's control table being
  absent — operators querying status against a fresh target see
  "no streams recorded" rather than an error. Backed by a new
  `ChangeApplier.ListStreams` interface method, implemented on
  both MySQL and Postgres.

### Added — engines

- **MySQL engine** (vanilla, `mysql:` driver): SchemaReader,
  SchemaWriter, RowReader, RowWriter (LOAD DATA INFILE),
  CDCReader (row-based binlog via go-mysql), ChangeApplier,
  SnapshotStream (REPEATABLE READ + WITH CONSISTENT SNAPSHOT
  pinned to the binlog position).
- **PlanetScale MySQL flavor** (`planetscale:` driver): same code
  paths as vanilla, with a capability declaration that disables
  `LOAD DATA INFILE` (uses BatchedInsert), turns off
  user-defined partitioning, and selects the VStream gRPC
  protocol for CDC.
- **Postgres engine** (`postgres:` driver): SchemaReader,
  SchemaWriter (with three-phase apply, identity-sequence sync,
  PostGIS-aware geometry emission, MySQL SET → TEXT[] with a
  CHECK constraint), RowReader, RowWriter (COPY FROM STDIN),
  CDCReader (pgoutput logical replication via pglogrepl),
  ChangeApplier, SnapshotStream (CREATE_REPLICATION_SLOT +
  EXPORT_SNAPSHOT + SET TRANSACTION SNAPSHOT for atomic
  snapshot-to-CDC handoff).

### Added — managed-service support

- **PlanetScale Postgres** (PS-PG): the vanilla `postgres` engine
  works against PS-PG without code changes. All six verification
  phases pass against a real PS-PG account: connectivity, schema
  reader, simple-mode migration, CDC reader, snapshot+CDC
  streamer, and cross-engine PS-MySQL → PS-PG. See
  [docs/managed-services.md](docs/managed-services.md).
- **PlanetScale MySQL via VStream**: Vitess's gRPC streaming
  protocol is now sluice's CDC path for the PlanetScale flavor.
  Capability declaration declares `CDCVStream` so the streamer
  accepts the flavor. Position encoding is JSON `[]shardGtid`
  matching Debezium's persistence shape, future-proofing for
  multi-keyspace migrations.
- **Vanilla Vitess deployments**: the same `planetscale` flavor
  covers self-hosted Vitess, with DSN flags to opt out of
  PlanetScale-specific defaults: `vstream_transport=plaintext`,
  `vstream_auth=none`, `vstream_shards=<custom>`,
  `vstream_endpoint=<host:port>`. Verified against
  `vitess/vttestserver` via testcontainers.
- **Sharded Vitess keyspaces** are now supported: the VStream
  reader streams from N shards concurrently (per-shard cursor
  tracking is built into the `[]shardGtid` position), and the
  new `vstream_auto_discover_shards=true` DSN flag asks the
  reader to populate the layout via `SHOW VITESS_SHARDS LIKE
  '<keyspace>/%'` at Open time. Reshards are detected via the
  typed `ShardLayoutChangedError` (matchable with `errors.Is`
  against `ErrShardLayoutChanged`); callers resume on the new
  layout via `vstreamCDCReader.Reopen`. Validated against
  `vttestserver` with `NUM_SHARDS=2` (`-80,80-`).

### Added — types and translation policies

- **MySQL SET → PostgreSQL TEXT[]** (default policy): SET columns
  emerge on the target as `TEXT[]` with a table-level
  `CONSTRAINT <table>_<column>_set CHECK (... <@ ARRAY[...])`
  enforcing membership. Comma-separated MySQL DEFAULTs translate
  to PG array literals so the source default survives the
  rewrite.
- **PostGIS-aware GEOMETRY emission**: PG engine detects PostGIS
  at writer-open time. With the extension installed, ir.Geometry
  columns emit as `geometry(<subtype>, <srid>)`; without it the
  existing loud rejection persists (sluice doesn't auto-install
  extensions). MySQL SRID-prefixed WKB → PostGIS EWKB framing
  via `wkbToEWKB`. Per-column SRID flows through the translate
  layer's `postgis_*` aliases. The PG schema reader queries
  PostGIS's `geometry_columns` view at read time so geometry
  columns surface in the IR with their precise subtype + SRID
  (cleanly degrades to `GeometryUnspecified+SRID=0` when PostGIS
  isn't installed).
- **TRUNCATE detection in CDC** for both binlog and VStream
  paths. The narrow `parseTruncateTable` parser recognises
  `TRUNCATE [TABLE] [<schema>.]<table>` shapes and emits
  `ir.Truncate`; out-of-shape statements fall through to the
  cache-invalidation path.
- **MySQL TINYINT(1) → PG BOOLEAN** through both the snapshot
  bulk-copy path and the CDC stream, validated by the
  cross-engine integration test.
- **MySQL UNSIGNED BIGINT → PG NUMERIC(20,0)**, with auto-
  increment widening to BIGINT IDENTITY when applicable.
- **MySQL ENUM → PG enum type** with per-column generated type
  names, default-value casting handled inline.
- **MySQL JSON → PG JSONB** by default (canonical fast path);
  override to `json` (text) via mappings if needed.

### Added — testing

- **Integration suite** (`integration` build tag): testcontainers
  pairs cover MySQL→MySQL, PG→PG, MySQL→PG, PG→MySQL one-shot
  migrations, plus PG→PG and MySQL→PG continuous-sync streaming
  with restart-resume. The cross-engine seed exercises every
  type-translation policy in one fixture.
- **PostGIS suite** (`integration && postgis` build tag): boots
  `postgis/postgis:16-3.4`, exercises end-to-end MySQL → PG
  geometry round-trip with `ST_AsText` verification.
- **PlanetScale verification suite** (`psverify` build tag):
  exercises sluice's PG and MySQL paths against a real
  PlanetScale account using credentials from
  `PLANETSCALE_CREDENTIALS.env` or env vars. Includes
  connectivity probe (logs version, wal_level, REPLICATION
  attribute, PostGIS state), schema reader round-trip, simple-
  mode migration, CDC reader, continuous-sync streamer, and
  cross-engine verification. CI workflow at
  `.github/workflows/psverify.yml` (manual-trigger only).
- **VStream suite** (`integration && vstream` build tag):
  testcontainers-based against `vitess/vttestserver:mysql80`,
  exercises the FlavorPlanetScale CDC path against vanilla
  Vitess (plaintext + no-auth) including INSERT/UPDATE/DELETE
  and TRUNCATE.

### Added — CI

- Four-job CI workflow: cross-platform unit Test (Linux, macOS,
  Windows), Integration on Linux, Lint, and cross-platform
  Build smoke-test. Required for branch protection on main.
- Manual-trigger PlanetScale verification workflow with
  per-environment secrets for the four PS DSNs.

### Architecture and process

- 10 ADRs in [docs/adr/](docs/adr/) capture the load-bearing
  design decisions: IR-first translation, sealed interfaces,
  kong+koanf, three-phase schema apply, MySQL flavors, pgoutput
  over wal2json, position persistence on the target, go-mysql
  for binlog parsing, Streamer as separate orchestrator, and
  idempotent applier semantics.
- Documentation under [docs/](docs/): architecture overview,
  type-mapping policies, runtime value contract, testing guide,
  managed-services compatibility matrix, and a sakila-based
  end-to-end walkthrough.

### Removed

- The pre-translate placeholder mappings handling in `Migrator`
  and `Streamer`. Replaced by `translate.ApplyMappings` between
  schema-read and schema-write.

### Known limitations

(none currently — see the closed entries above.)

[Unreleased]: https://github.com/orware/sluice/compare/v0.7.0...HEAD
[0.7.0]: https://github.com/orware/sluice/releases/tag/v0.7.0
[0.6.0]: https://github.com/orware/sluice/releases/tag/v0.6.0
[0.5.2]: https://github.com/orware/sluice/releases/tag/v0.5.2
[0.5.1]: https://github.com/orware/sluice/releases/tag/v0.5.1
[0.5.0]: https://github.com/orware/sluice/releases/tag/v0.5.0
[0.4.0]: https://github.com/orware/sluice/releases/tag/v0.4.0
[0.3.2]: https://github.com/orware/sluice/releases/tag/v0.3.2
[0.3.1]: https://github.com/orware/sluice/releases/tag/v0.3.1
[0.3.0]: https://github.com/orware/sluice/releases/tag/v0.3.0
[0.2.2]: https://github.com/orware/sluice/releases/tag/v0.2.2
[0.2.1]: https://github.com/orware/sluice/releases/tag/v0.2.1
[0.2.0]: https://github.com/orware/sluice/releases/tag/v0.2.0
[0.1.0]: https://github.com/orware/sluice/releases/tag/v0.1.0
