# Changelog

All notable changes to sluice are recorded here. The format follows
[Keep a Changelog](https://keepachangelog.com/en/1.1.0/) and the
project follows [Semantic Versioning](https://semver.org/).

## [Unreleased]

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

[Unreleased]: https://github.com/orware/sluice/compare/v0.5.1...HEAD
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
