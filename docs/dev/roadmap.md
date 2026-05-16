# Roadmap

Living list of work items beyond the current state, with enough context per entry that any one of them could be picked up as a self-contained chunk. Priority order is *suggested*, not strict.

Each entry has the same shape: a one-line summary, a *why* (the user-visible payoff), a *what* (load-bearing technical detail), and any *gotchas / open questions* known going in.

---

## Recently landed

For continuity when a chunk references "the previous work":

### Bulk-copy throughput arc — ADR-0042 / ADR-0043 (v0.62.0 → v0.64.0)

- **v0.62.0 — `--bulk-parallel-min-rows` default 100,000 → 80,000.** Absorbs the typical InnoDB `table_rows` catalog undershoot so 100k-actual tables consistently engage parallel-copy by default. `defaultBulkParallelMinRows` constant change; explicit-value behaviour unchanged.
- **ADR-0042 — MySQL bulk-copy throughput investigation.** Accepted as a discovery doc; Phase A added DEBUG-gated per-chunk `adr0042:` instrumentation (retained as a permanent diagnostic artifact, like the ADR-0033/0036 verify probes).
- **v0.63.1 — finding N1: PG cold-start parallel-copy fix.** `internal/engines/postgres` `CountRows` falls back to exact `SELECT COUNT(*)` when `pg_class.reltuples` is the never-analyzed sentinel (`-1`/`0`), so freshly loaded/restored PG sources no longer silently single-thread. PG-specific/asymmetric (MySQL's `information_schema.tables.table_rows` is InnoDB-populated on load). Biggest impact PG → MySQL.
- **v0.64.0 — ADR-0043: native fast loader on the cold-start parallel-copy path.** The parallel chunk writer is now situation-driven (no new flag): a fresh cold-start chunk into a proven-empty target streams through one native `COPY`/`LOAD DATA` call; resume / `--force-cold-start` / live-add stay on the idempotent upsert path. Four-gate rule; crash-safe (next invocation is a resume → idempotent replay). MySQL → MySQL medium fixture ~31s → ~24s wall; PG → PG no regression (schema/DDL-bound — ADR-0042 finding N2). Pinned by a permanent both-engines proof-of-falsification integration test.

### PII redaction track — Phases 1 → 4 complete (v0.53.0 → v0.63.0)

- **Phase 1 + 1.5 (v0.53.0–v0.55.0).** `--redact TABLE.COLUMN=STRATEGY[:options]` + YAML `redactions:`; `null` / `static` / `hash:sha256` / `hash:hmac-sha256` / `truncate`; `internal/redact` package; per-bulk-copy-variant wiring; audit log line. Phase 1.5 closed CDC apply-path redaction + schema-preview annotation + backup-stream redaction. Bug 58 (CDC schema-namespace key mismatch) v0.54.1; Bug 59 (kong comma-split) v0.56.1.
- **Phase 2 (v0.56.0–v0.60.0).** Phase 2.a generic `mask:inner`/`mask:outer` + Luhn helper; Phase 2.b mask presets (ssn/pan/pan-relaxed/email/ca-sin/uk-nin/iban/uuid; Bug 60 `mask:uuid` preflight v0.58.1); Phase 2.c replay-stable `randomize:*` generators (ADR-0039 per-row seed contract).
- **Phase 3 (v0.61.0).** Dictionary strategies `randomize:dict` (PK-keyed) + `tokenize:dict` (input-value-keyed) + YAML `dictionaries:` block. ADR-0040 documents the two determinism contracts.
- **Phase 4 (v0.63.0, ADR-0041 Accepted 2026-05-15).** Operator-keyset persistence — `--keyset-source=file:|env:|db:`, `key:` rule option, `sluice_keysets` DDL on PG + MySQL, startup audit line, preflight refusal. **Breaking, no shim** (zero-users tenet): `--redact-key-source` + the hardcoded v0.61.0 `tokenize:dict` key deleted; `hash:hmac-sha256` and `tokenize:dict` now require `--keyset-source`. Two ADR-0041 deviations: D1 startup-snapshot only (no hot-reload — Phase 4.5), D2 clean break. The roadmap's "15c JSON-path redaction" (Phase 3 in the original four-phase plan) is the only PII piece still genuinely pending.

### GEOMETRY / SPATIAL — PostGIS-aware translation (v0.28.0)

- **PG → MySQL geometry round-trip with SRID preserved (Bug 26 closed).** PG `geometry(POINT, 4326)` lands as MySQL `POINT NOT NULL SRID 4326` instead of dropping to SRID 0; ST_SRID(loc) on the target returns the source SRID. The MySQL DDL emit grew a `SRID <n>` clause on geometry columns when the IR carries a non-zero SRID; the IR's `ir.Geometry.SRID` field threads from the PG schema reader's `geometry_columns` lookup through to the MySQL writer.
- **VStream POINT bytes prefix stripping (Bug 27 closed).** VStream's `query.Type_GEOMETRY` cell decoder now strips the 4-byte little-endian SRID prefix that MySQL's GIS storage format prepends to the OGC WKB body. Downstream consumers see raw WKB matching the IR contract for `ir.Geometry` values, identical to the vanilla `database/sql` driver path. Fix is fixture-tested at the unit-test layer; operator-run end-to-end verification via `psverify` (PlanetScale credentials) follows the existing PS test pattern.
- **Cross-engine geometry no longer refuses.** `pipeline.checkCrossEngineSupportable` previously refused PG → MySQL `ir.Geometry` blanket; with SRID round-trip closed, the conservatism is no longer load-bearing. Refusal narrows to `ir.ExtensionType` (ADR-0032 pgvector / hstore / etc.) which has no portable MySQL equivalent. Chain-restore + sync-from-backup paths now ship geometry through cross-engine without operator intervention.
- **`postgis/postgis:16-3.4` integration image.** New `integration postgis` build-tag layer for the cross-engine PG ↔ MySQL geometry round-trip tests. Image is ~500 MB (heavier than `postgres:16` and `pgvector/pgvector:0.7.4-pg16`) so the postgis-tagged tests run as a separate go-test invocation in the CI Integration job rather than expanding the default `integration` tag's footprint.
- **PostGIS-absent target stays loud-failure.** PG SchemaWriter and RowWriter detect PostGIS at engine open time via `pg_extension`; without the extension installed, ir.Geometry columns refuse with `GEOMETRY requires PostGIS; install with CREATE EXTENSION postgis;` rather than silently downgrading to `bytea`. See [ADR-0035](adr/adr-0035-postgis-geometry-spatial-support.md).

### PG → PG extension passthrough — v1 shortlist complete (v0.26.0 → v0.38.0)

- **Framework + pgvector landed in v0.26.0.** `--enable-pg-extension EXT` opt-in allowlist on `migrate`, `sync start`, `schema preview`, `schema diff`. PG → PG only — cross-engine targets (MySQL) keep the loud-failure default; the cross-engine refusal in `pipeline.checkCrossEngineSupportable` extends to refuse `ir.ExtensionType` regardless of the source-side flag. New IR variant `ir.ExtensionType{Extension, Name, Modifiers}`; new optional engine surface `ir.ExtensionAware`. PG schema reader recognises extension-owned column types when enabled; PG schema writer dispatches through `pgExtensionCatalog`; the index emitter passes `ivfflat` / `hnsw` access methods verbatim when the owning extension is enabled. Source + target presence preflight via `pg_extension`. See [ADR-0032](adr/adr-0032-pg-extension-passthrough.md).
- **pg_trgm (v0.30.0).** Tier 2 lite — operator-class passthrough on GIN/GiST; cross-engine PG → MySQL pg_trgm-index refusal made loud at preflight (v0.30.0) instead of failing cryptically in the index phase.
- **hstore + citext (v0.31.0).** Tier 1 opaque-text + collation catalog entries; cross-engine PG → MySQL default translators (hstore → JSON, citext → collated VARCHAR) wired into the MySQL writer. Bug 48 (hstore under `local_infile=ON` LOAD DATA) closed v0.31.1; the PG → PG hstore binary COPY codec landed v0.32.1.
- **PostGIS (v0.33.0).** Last Tier 2 in v1 — PG → PG passthrough as an ADR-0032 catalog entry (cross-engine PG ↔ MySQL geometry stays parented under GEOMETRY/SPATIAL / ADR-0035). Follow-ups: Bugs 49–50 (geography passthrough + SP-GiST/BRIN opclasses, v0.33.0), Bug 51 (`geography(POINT,srid)` mixed-case, v0.33.2), Bugs 52–53 (Z/M/ZM dimensional variants + `coord_dimension` capture, v0.33.3).
- **pgcrypto presence-gate (v0.38.0).** No new types — joins the catalog purely as a target-presence gate for the SHA1/SHA2 translator rules (catalog rule #10). ADR-0032's status block now records all five v1-shortlist extensions under "shipped".
- **Tier 3 (uuid-ossp + pgcrypto function-defaults) remains the only forward-looking piece** — see "Next up" item 11 (demand-gated; the type-passthrough machinery is done, Tier 3 is an expression-translator-catalog chunk).

### Multi-source aggregation Phase 1 + 2 (`--target-schema` + stream-id collision detection)

- **`--target-schema NAME` flag** on `migrate`, `sync start`, `schema preview`, `schema diff`. PG-only — flat-namespace engines (MySQL) refuse with a clear "use a different --target DSN database" message. When set, every emitted CREATE TABLE / ALTER TABLE / CREATE INDEX / CREATE TYPE prefixes the identifier with the schema name; the schema is auto-created via `CREATE SCHEMA IF NOT EXISTS`. Type-name derivation (PG enums) namespaces through the schema so two sources both having `accounts.status` enums coexist without collision. Implements Shape B (microservices → analytics warehouse) of the [proto-design](design-multi-source-aggregation.md). See [ADR-0031](adr/adr-0031-multi-source-aggregation-target-schema.md).
- **Stream-id collision detection.** `sluice_cdc_state` grew a `source_dsn_fingerprint TEXT NULL` column (additive, idempotent migration via `ADD COLUMN IF NOT EXISTS`). Streamer fingerprint check at startup refuses if a different source DSN tries to reuse an existing stream-id. Catches operator-typo / wrong-source cases loudly before any data moves. Fingerprint is truncated SHA-256 of host+port+database — credential rotation doesn't trip false-positives.
- **New IR surfaces.** `ir.SchemaSetter`, `ir.SourceFingerprintRecorder`, `ir.StreamStatus.SourceDSNFingerprint`. PG implements via SetSchema on SchemaReader / SchemaWriter / RowReader / RowWriter / ChangeApplier; the applier carries a separate `controlSchema` field so per-target `sluice_cdc_state` stays in the DSN's default schema while user-data INSERT/UPDATE/DELETE land in the per-source schema. MySQL deliberately doesn't implement (the validate-time refusal is the load-bearing gate).
- **Operator UX.** `sluice sync status` renders cross-source streams from the same control table; the existing query already lists every row by stream-id. Each per-source stream stays a separate `sluice sync start` invocation — N-process aggregation rather than single-process multi-source (failure isolation, resource isolation, k8s/systemd lifecycle).

### Mid-stream live add-table — strict zero-loss correctness (v0.32.0)

- **v0.24.0's documented best-effort gap closed (ADR-0036).** A four-round Phase A diagnose campaign (A.1 ruled out long-txn-straddle / snapshot-LSN-race / broker-re-emit; A.2 v0.29.0 falsified reframed M3 via per-row source-LSN instrumentation; A.3 v0.31.x confirmed applier-side drop 10/10) localized the drop to `internal/engines/postgres/change_applier.go::dispatch` returning `nil` (silently dropping) when `colTypesFor` hit `errUnknownTable` because the target table didn't exist yet. Fix (v0.32.0, ~150 LOC): `AddTable.Run` step 3a creates the target table BEFORE `ALTER PUBLICATION ADD TABLE`; `runBulkCopyForAddTable` routes the copy through `ir.IdempotentRowWriter`. Engine-symmetric (MySQL's ADR-0034 filter-flip didn't manifest the race but inherits the cleaner shape). `TestAddTable_LiveMode_PG_UnderLoad` tightened to a strict zero-loss assertion; the Phase A.3 capture probe stays as a permanent proof-of-falsification artifact. Forward options B (dual-slot) / C (operator quiesce) shed for this surface but retained for unrelated edge cases.

### Mid-stream add-table — MySQL Phase 2 (live add via filter-flip)

- **`sluice schema add-table TABLE --stream-id ID --no-drain`** — now also works against a MySQL source. Same CLI surface as the PG path; the orchestrator dispatches by source-engine capability and routes binlog sources through the new filter-flip mechanism. Operator-facing UX is identical (one flag, no operator restart). See [`adr-0034-mysql-phase-2-live-add-table.md`](adr/adr-0034-mysql-phase-2-live-add-table.md).
- **New surfaces.** Pipeline-side optional interfaces `liveAddedTablesWriter` / `liveAddedTablesReader` (target applier records / streamer reads the filter-flip column on `sluice_cdc_state`). MySQL `ChangeApplier.RecordLiveAddedTable` writes the new table into the comma-separated `live_added_tables` column; `ChangeApplier.ReadLiveAddedTables` is what the streamer's poll consults. Streamer side: `liveAddedFilter` (atomic-pointer-to-immutable-map) + a poll goroutine paired with the existing stop-signal poll. The dispatch filter merges base + live-added with OR semantics — additive grants without touching the operator's existing `--include-table` / `--exclude-table`.
- **Best-effort caveat (parallel to PG Phase 2).** Events on the new table arriving at the streamer's dispatch BEFORE its poll observes the new column value are dropped (poll cadence 5s default). Operators with high write rates on the new table at the moment of live-add use the drained flow or quiesce briefly. Same shape as PG Phase 2's in-flight gap (ADR-0030 § "what could go wrong"); the strict-zero-loss roadmap entry now covers both engines.

### Mid-stream add-table — Phase 2 (live add, no drain) — PG

- **`sluice schema add-table TABLE --stream-id ID --no-drain`** — runs add-table against an actively-streaming sync without first running `sync stop --wait`. Strategy C variant (c) from the proto-ADR: single slot, publication-add-then-snapshot ordering. v0.27.0 extended the same CLI surface to MySQL via filter-flip (entry above + ADR-0034). See [`adr-0030-mid-stream-live-add-table.md`](adr/adr-0030-mid-stream-live-add-table.md).
- **New surfaces.** Pipeline-side optional interfaces `slotPositionReader` / `snapshotLSNExtractor` / `lsnComparer` so engines opt into the live-mode invariant check structurally. `Postgres.Engine.ReadSlotPosition` reads `confirmed_flush_lsn` from `pg_replication_slots`; `Postgres.Engine.ExtractSnapshotLSN` and `Postgres.Engine.CompareLSN` close the loop without leaking the engine's position envelope into the orchestrator.
- **Operator safeguards.** Live mode refuses on engine pairs that expose neither `publicationAdder` (PG path) nor `liveAddedTablesWriter` (MySQL path) — clear error directing at the drained flow. Captures the active stream's slot `confirmed_flush_lsn` before publication-add; verifies snapshot-LSN ≥ slot-LSN after the snapshot opens — refuses loudly if the invariant fails (would silently drop events on the new table; ADR-0030 explains why standard ordering can't trip this in practice but the check pins the invariant against future regressions).

### Mid-stream add-table (Phase 1 MVP)

- **`sluice schema add-table TABLE --stream-id ID`** — brings a new source table into an active CDC stream's scope without a destructive `--reset-target-data` cycle. Drained-stream workflow only (Phase 1): operator runs `sync stop --wait`, then `schema add-table`, then `sync start --resume`. Live add-table without the drain shipped in Phase 2 (entry above). Implements the design in `docs/dev/design-mid-stream-add-table.md`.
- **New surfaces.** `pipeline.AddTable` orchestrator (mirrors `Migrator` shape); pipeline-side optional interfaces `publicationAdder` / `snapshotSlotOpener` / `slotDropper` so engines opt in structurally. `Postgres.Engine.AddPublicationTables` issues `ALTER PUBLICATION ... ADD TABLE` (additive; existing scope untouched; idempotent on partial-add re-run). MySQL participates with no engine surface change — binlog already covers every table.
- **Operator safeguards.** Refuses if no row exists for the supplied `--stream-id` (catches typos / wrong-target). Refuses if `stop_requested_at` is set (catches "forgot to drain"). Refuses if the target table already has rows (`TableEmptyChecker` preflight, same shape as cold-start). Refuses if the named table doesn't exist on the source (catches "ran add-table before CREATE TABLE landed"). All refusals surface clear messages with recovery steps. Typed-confirmation prompt mirroring `--reset-target-data`'s friction tier; `--yes` bypasses.
- **Persisted position is intentionally NOT updated.** The stream's existing `sluice_cdc_state` position is still the right resume point for the other tables; the applier's idempotent upsert handles the [persisted_LSN, snapshot_LSN] overlap on the new table on resume.

### v0.1.0 foundations

- **Simple-mode orchestrator** — three-phase apply, wired into `sluice migrate`.
- **Integration coverage in all four directions**: MySQL→MySQL, PG→PG, MySQL→PG, PG→MySQL. CI Integration job runs them on every PR.
- **MySQL CDC reader** — binlog client (go-mysql-org/go-mysql), GTID and file/pos modes, schema cache invalidated on DDL, Insert/Update/Delete/Truncate events.
- **Postgres CDC reader** — pgoutput plugin via pgx replication-mode connection, RELATION-message-driven schema cache, wal_status checks on resume.
- **MySQL VStream CDC reader** — FlavorPlanetScale, multi-shard with auto-discovery and reshard detection, snapshot+CDC handoff.
- **Snapshot→CDC handoff** — gapless cutover via `START TRANSACTION WITH CONSISTENT SNAPSHOT` (MySQL) and `EXPORT_SNAPSHOT`+`SET TRANSACTION SNAPSHOT` (PG).
- **Position persistence** — per-target `sluice_cdc_state` control table, position commit in the same tx as data writes.
- **Postgres COPY-protocol writer** — `chanCopySource` adapter wrapping pgx `CopyFrom` for ~3-5x faster bulk load on PG targets.
- **Identity sequence sync** — post-bulk `setval(pg_get_serial_sequence(...), MAX(id))` so user inserts don't collide with bulk-copied IDs.
- **`sluice sync start` / `sync status` / `sync start --dry-run`** — operator-facing CLI for streams.

### v0.2.x bug-fix and operator-UX waves

- **`sluice slot list` / `slot drop`** — operator-facing slot management; auto-drop on failed cold-start; `wal_status='unreserved'|'lost'` detection on resume.
- **Postgres slot creation with `FAILOVER true` on PG 17+** — slots survive Patroni / `sync_replication_slots` failover when configured. Warning on PG ≤ 16.
- **Translation policy fixes**: JSON wire encoding for MySQL targets (no `_binary` charset prefix), warm-resume engine alias, PG UPDATE empty-WHERE under REPLICA IDENTITY DEFAULT, TIMESTAMP precision matching on `CURRENT_TIMESTAMP` defaults, applier `CAST(? AS JSON)` for JSON-typed WHERE columns (Bug 6), composite-PK DELETE filter (Bug 8).
- **Operator docs**: `docs/postgres-source-prep.md` covers required GUCs, slot lifecycle, wal_status recovery, and the failover-survival mechanisms (Patroni `slots:`, PlanetScale "Logical slot name", PG 17 `sync_replication_slots`).

### v0.3.x feature wave

- **`sluice migrate --resume`** — resumable simple-mode migrations via per-target `sluice_migrate_state` table, phase + per-table progress tracking, truncate-and-redo for in-progress tables. See ADR-0015.
- **`sluice sync stop`** — graceful drain via control-table polling; works across machines, fits k8s lifecycle hooks.
- **`--include-table` / `--exclude-table`** — table filtering at the orchestrator boundary, glob patterns, YAML config parity.
- **Structured logging via `log/slog`** — `--log-level` actually works; bulk-copy progress lines every 2s; phase-aware error hints.
- **Composite-PK CDC regression coverage** — every direction (PG→PG, MySQL→MySQL via binlog and VStream, MySQL→PG cross-engine).
- **Generated column support** — read-side capture (`Column.GeneratedExpr` + `GeneratedStored`), write-side emission, row-path filtering so the target's GENERATED clause does the recomputation. Verbatim expression passthrough; non-portable expressions fail loudly on the target.
- **CHECK constraint support** — same shape as generated columns: schema-read capture into `Table.CheckConstraints`, DDL emission, verbatim expression passthrough. Discovered (and now strips) two more layers of MySQL stored-form decoration: charset introducers (`_utf8mb4'literal'`) and delimiter-escape forms (`\'literal\'`). Generated columns benefit from the same normalizer.
- **`--type-override TABLE.COLUMN=TYPE`** — CLI form of the YAML `mappings:` config; one-off overrides without writing a YAML file. Wholesale-precedence over YAML when both are supplied.

### v0.4.x feature wave

- **Batched CDC apply** — `--apply-batch-size N` accumulates up to N changes per target transaction, with the position write of the last change committed alongside. Default 1 keeps v0.3.x behaviour; production tuning is 100–500. Source-transaction-boundary aware flushing deferred to a follow-up. See ADR-0017.
- **Per-batch bulk-copy checkpointing** — resume mid-table from a PK cursor rather than truncate-and-redo; idempotent INSERTs tolerate the brief replay window between batch commit and cursor write; tables without a PK fall back to v0.3.0 behaviour. See ADR-0018. CLI: `--bulk-batch-size`.
- **Cross-engine expression translation for GENERATED + CHECK** — bidirectional translation pass at the writer boundary covers the common-idiom set across MySQL ↔ PG (CONCAT/||, ::cast, ~~/LIKE, ANY/IN, JSON_EXTRACT/->>). Verbatim passthrough remains the policy for unrecognized constructs. See ADR-0016.
- **Bug 9 fix** — cold-start no longer hangs on populated dest tables (pre-flight refusal + goroutine-leak fix + `--force-cold-start` escape hatch + clearer log shape).
- **Bug 11 fix** — `stop_requested_at` cleared at sync-start so a previous `sync stop` doesn't leave a sticky signal.

### v0.5.x feature wave (multi-TB credibility)

- **Parallel within-table bulk copy** — tables above `--bulk-parallel-min-rows` (default 100k) with a single integer PK split into N PK ranges and copy concurrently with per-chunk checkpoints. PG readers share a single exported snapshot via `SET TRANSACTION SNAPSHOT`; MySQL uses per-chunk REPEATABLE-READ. Boundaries computed once + persisted so resume aligns with completed chunks. See ADR-0019.
- **Throughput metrics: MB/s + ETA** — bulk-copy progress ticker emits `total_rows`, `bytes`, `rate_mb_per_sec`, `eta_seconds`; per-chunk progress lines carry `chunk=` attribute.
- **`CountRows` / `RangeBoundsQuerier` / `SnapshotImporter`** — three new optional engine surfaces in support of the parallel path; ETA fallback is graceful when the surface isn't available.
- **Postgres CDC slot-ack-after-apply (Bug 15, CRITICAL)** — `lsnTracker` SPSC tracker; reader sends `min(streamed, applied)` in standby status updates so a crash mid-batch doesn't drop the in-flight buffer. ADR-0020.
- **Postgres publication scope by table (Bug 13)** — `EnsurePublication` creates `FOR TABLE <list>` instead of `FOR ALL TABLES`; v0.4.0 publications migrated by drop-and-recreate; applier defence-in-depth WARNs and skips on unknown table OIDs. ADR-0021.
- **PG array → MySQL JSON conversion (Bug 14)** — `prepareValue` branches `convertArrayLikeToJSON` for `[]any`, PG-array literal strings, and `[]byte` shapes.
- **MySQL CDC heartbeat + watchdog (Bug 12)** — 10s `defaultBinlogHeartbeatPeriod`, 30s no-events watchdog filtered by `isRowRelevantEvent`.
- **`wal_status='lost'` recovery hint flag fix (v0.5.1)** — now points operators at `--source-driver=postgres --source ...` instead of `--target ...`; matches the actual `slot drop` flag.
- **Slot-missing fall-through to cold-start (v0.5.2, Item F)** — `ir.ErrPositionInvalid` engine-neutral sentinel; CDC readers wrap with `%w` when the slot/file referenced by a persisted position is gone; streamer detects via `errors.Is`, logs WARN, falls through to cold-start. Bug 9 pre-flight still gates populated dest. ADR-0022.

### v0.6.0 feature wave

- **`sluice schema preview`** — operator-facing target-DDL inspection with translation notes and advisory hints; `--format text|json`, `--output FILE` (atomic). New `ir.DDLPreviewer` surface; initial registry seeds five high-traffic surprises (UUID, large-TEXT, JSON note, DATETIME timezone, unbounded numeric). ADR-0024.
- **`--reset-target-data`** — one-command destructive recovery on top of the v0.5.2 slot-missing fall-through. Confirmation prompt requires typed `reset` (bypassed by `--yes`); mutually exclusive with `--resume`. New surfaces: `ir.TableDropper`, `ir.BulkTableDropper`, `ir.StreamCleaner`, `ir.MigrationStateStore.ClearMigration`. ADR-0023.
- **MySQL binlog-purged fall-through** — `verifyPositionResumable` pre-flights `SHOW BINARY LOGS` (file/pos) or `GTID_SUBSET(@@gtid_purged, ?)` (GTID); wraps `ir.ErrPositionInvalid` so the v0.5.2 streamer fall-through engages engine-neutrally. ADR-0022 extended.
- **Batched-apply idle flush** — partial in-flight batches commit within `defaultIdleFlushPeriod` (5s); slot/`source_position` stay current on quiet streams. Closes ADR-0020 trailing-row latency footnote.
- **Parallel-copy data race fix** — `cloneStateForWrite` deep-clones `TableProgress` map + `Chunks` slice under `stateMu` so the JSON encoder doesn't iterate shared backing storage. CI -race surface.
- **Parallel-copy hygiene follow-ups** — `progressTicker.startedAt` → `CompareAndSwap`; `kickOffRowCount` suppresses post-cancellation noise.

### v0.7.0 feature wave (performance round 2 + reliability)

- **MySQL `LOAD DATA LOCAL INFILE` writer (ADR-0026)** — TSV-over-RegisterReaderHandler bulk path, typically 5–10× faster than batched INSERT on wide-row tables. Per-call fallback to BatchedInsert when `local_infile=OFF` or a geometry column is present. PlanetScale stays on BatchedInsert.
- **Source-transaction-boundary aware CDC batching (ADR-0027)** — `ir.TxBegin`/`ir.TxCommit` events let the applier flush on commit boundary instead of arbitrary row-count chunks; row cap remains as the upper bound. Closes the ADR-0017 deferred follow-up.
- **Memory-bounded streaming (ADR-0028)** — `--max-buffer-bytes` (default 64 MiB) caps per-batch buffered memory by total bytes in addition to row count. Wide-row workloads (TEXT/BYTEA/JSON at MB scale) no longer need manual `--apply-batch-size` tuning. New `ir.MaxBufferBytesSetter` optional surface.
- **Graceful-drain `sync stop` (ADR-0025, Bug 15 CLI)** — `ackLSN` anchors at `startLSN` until first apply commit; `pollStopSignal` cancels a separate `streamCtx` that scopes the CDC pump, letting the applier's existing `channelClosed` branch commit the in-flight partial batch. 30s watchdog escalates to hard-cancel if drain wedges.
- **MySQL CDC temporal-string decoder (Bug 12 root cause)** — `decodeTime` parses MySQL's canonical temporal string formats (second/microsecond precision, date-only, byte-slice equivalents, `0000-00-00` zero-value); pre-fix the decoder rejected the binlog protocol's actual wire format and silently dropped 100% of CDC events on tables with TIMESTAMP/DATETIME/DATE columns.
- **PG-native types auto-emit on MySQL targets** — `Inet`/`Cidr` → `VARCHAR(45)`, `Macaddr` → `VARCHAR(30)`, `Array` → `JSON`. `--type-override` continues to override.
- **Throughput tuning guide** (`docs/throughput-tuning.md`) — operator reference for the knobs that matter at scale.
- **`migrate --dry-run` cross-reference to schema preview** — small UX nudge.

### v0.8.0 feature wave (schema diff + real-world bug bundle)

- **`sluice schema diff` (ADR-0029)** — drift detection between sluice's expected target shape and the schema actually present. Text + JSON output, copy-paste ALTER suggestions, atomic `--output FILE`, CI exit codes (0/1/2 = clean/drift/op-error). Reads both sides through the existing `SchemaReader`. Compares defaults, generated expressions, and CHECK constraints (categories originally listed as out-of-scope; lifted in the same release because the IR already carried the fields). New optional interfaces: `ir.ColumnDDLPreviewer` (filled-in `ADD COLUMN` rendering), `ir.SchemaTypeDropper`, `ir.DefaultTableExcluder`.
- **Cross-engine type-policy retarget on schema diff** — `internal/translate.RetargetForEngine` rewrites PG-native IR types (UUID, Inet, Cidr, Macaddr, Array) to MySQL-storage IR shapes before the diff runs, so cross-engine `sluice schema diff` no longer flags every translated column as drift.
- **Bug 16 — MySQL functional/expression indexes** read correctly (NULL `column_name` + `EXPRESSION` column scanned via `sql.NullString`; new `ir.IndexColumn.Expression` field).
- **Bug 17 — MySQL bool-idiom CHECK / generated expressions translate to PG.** ADR-0016 extended with column-context-aware `ExprContext`; rewrites `0 <> is_active`, `coalesce(is_active, 0)`, etc. to bool literals.
- **Bug 18 — `--reset-target-data` drops orphan PG enum types** via the new `ir.SchemaTypeDropper` interface.
- **Bug 19 — silent TIMESTAMP corruption in MySQL→PG CDC on non-UTC hosts** — fix at the connection layer: `BinlogSyncerConfig.TimestampStringLocation = time.UTC` + `time_zone='+00:00'` injected into every database/sql connection. Cold-start was latently affected on servers with non-UTC `default_time_zone`; same fix covers it.
- **Bug 20 — cross-engine warm-resume dispatch** — streamer re-stamps persisted CDC positions with the source engine name so all four engine pairs (and the PlanetScale flavor) round-trip cleanly. v0.1.0's same-family fix generalised.
- **Bug 21 — PG snapshot tx no longer holds AccessShareLock for the CDC lifetime.** New `ir.SnapshotStream.ReleaseRowsFn` lets the streamer commit the snapshot tx after bulk-copy without disturbing CDC. ALTER on the source unblocked.
- **Bug 22 — Vitess `_vt_*` shadow tables auto-excluded** when `--source-driver=planetscale`. New `ir.DefaultTableExcluder` engine surface; operator-supplied `--include-table` short-circuits.

### v0.8.1 patch wave

- **PlanetScale Vitess hostname auto-detect.** Vanilla MySQL DSNs targeting `*.connect.psdb.cloud` or `*.private-connect.psdb.cloud` now inherit the v0.8.0 `_vt_*` auto-exclusion without needing `--source-driver=planetscale`. DSN-keyed sniff at orchestrator startup; no connection probe. PG-side hostname suffixes are documented for future symmetry but no-op today (PlanetScale Postgres isn't Vitess-backed).
- **CI fix:** `TestMigrate_MySQLToPostgres_CheckBoolIdiom` referenced columns the test schema didn't have — leftover assertions from a sibling test. Removed.

### v0.9.0 patch wave

- **`sluice sync stop --wait`** (extends ADR-0025). Blocks the CLI until the running streamer confirms graceful-drain completion; `--timeout` (default 5m) bounds the wait. Mechanism: streamer clears `stop_requested_at` only on stop-signal-driven exits (not Ctrl-C), so the cleared-flag signal accurately reflects "drain completed". CLI polls `ReadStopRequested` at 1s cadence.
- **TIMESTAMP/DATETIME precision integration tests** across `(0/3/6)` precisions in both directions; outcome was "audited, no gaps found" — Bug 19's TZ fix had already closed the silent-corruption door, and the IR's `Precision` field rounds-trips end-to-end.
- **CONTRIBUTING.md release-process section + `docs/dev/release-template.md`** — formalises the `chore: cut vX.Y.Z` commit + annotated-tag pattern and the GitHub release-notes structure (Highlights / Fixed / Compatibility / Who-needs-this).
- **Bug 16 follow-up — index expressions translate cross-engine.** New `ir.IndexColumn.ExpressionDialect` field; PG's index-list emit routes MySQL-source expressions through the ADR-0016 translator. `((json_unquote(json_extract(...))))` → `(((... ->>...)))`.
- **Bug 17 follow-up — COALESCE rewrite recognises bool-returning sub-expressions** (comparisons, `IS NULL`, `IS NOT NULL`, parenthesised wrappers), not just bare bool idents.
- **Bug 22 follow-up — `schema preview` and `schema diff` now also auto-exclude PlanetScale `_vt_*` tables** (the v0.8.1 fix only covered Migrator/Streamer paths).
- **Bug 23 — MySQL `DEFAULT ('value')` parens-form enum cast.** PG enum-cast emit was gated on `DefaultLiteral` only; now also fires on `DefaultExpression` whose body is shape-equivalent to a string literal. True-expression defaults stay uncast.

### v0.9.1 patch wave

- **Bug 16 residual — `CAST(x AS CHAR(N) [CHARSET y] [COLLATE z])`** translates to `CAST(x AS VARCHAR(N))` on PG emit; charset/collate decorations dropped, CHAR(N) → VARCHAR(N) (matches MySQL no-padding semantics). Bare `CAST(x AS CHAR)` becomes `CAST(x AS TEXT)`.
- **Bug 17 residual — outer-column-type-aware COALESCE direction.** New `ExprContext.OuterColumnIsInteger` flag flips the rewrite: integer-typed generated columns whose body returns bool now wrap the bool side with `::int` instead of converting the int literal to bool.
- **Bug 23 (refined) — STORED GENERATED column body cast for enum targets.** Generated-column body wrapped with `::"<enum_type>"` when the column is enum-typed; works for any text-returning shape (CASE / COALESCE / literal). Mirrors the existing DEFAULT cast.

### v0.9.x doc wave

- **`docs/schema-change-runbook.md`** — operator runbook for the `ADD COLUMN` / `DROP COLUMN` / `MODIFY` workflow against a running sluice stream. Covers the standard `sync stop --wait` → ALTER → `sync start --resume` pattern, the per-class behaviour pinned by v0.8.0 stretch testing, planning with `sluice schema diff`, and when to reach for Atlas / sqitch / liquibase instead. Closes the documentation half of "Schema-change ergonomics" — tooling beyond what `sync stop --wait` already provides hasn't earned its weight yet.

### v0.10.x feature wave (translator escape hatch + reactive bug bundle)

- **`--expr-override TABLE.COLUMN=EXPRESSION` (v0.10.0)** — operator-supplied target-dialect expression text bypasses the writer-side translator. Available on `migrate`, `sync start`, `schema preview`, `schema diff`; YAML form `expression_mappings:`. Generated columns only in v1; CHECK / index / DEFAULT slated to follow the same shape if real-world testing surfaces the need. Strict load-time validation of table/column existence + generated-column gate. ADR-0016 extended.
- **Bug 25 — enum-typed STORED generated columns now emit as TEXT + table-level CHECK (v0.10.1).** PG rejects the `(body)::"enum"` cast inside generated bodies because `enum_in()` is STABLE not IMMUTABLE; sluice sidesteps by emitting the column as TEXT and adding a CHECK that enforces the value-list. Mirrors the existing SET → TEXT[] + CHECK fallback.
- **Bug 17 — int-context COALESCE rewrite drops the bool-detector gate (v0.10.1).** v0.9.x's hand-coded `isBoolReturning` detector kept missing real-world expression shapes (function calls returning bool, AND/OR chains, NOT prefixes, EXISTS subqueries). v0.10.1 casts the non-literal side `::int` unconditionally when the outer column is integer-typed; safe under the column-type signal alone.
- **`--slot-name NAME` (v0.10.2)** — operator-supplied replication-slot name with the `sluice_` prefix convention. New `ir.CDCReaderWithSlotOpener` / `ir.SnapshotStreamWithSlotOpener` optional surfaces. Lets multiple sluice instances target one Postgres source without colliding on the hard-coded default.
- **`migrate --dry-run` row counts (v0.10.2)** — per-table `row_count` attribute populated via the existing `ir.RowCounter` interface; best-effort with `-1` + Warn-level log when unavailable.
- **Bug 26 — MySQL geometry SRID preserved on cross-engine emit (v0.10.3).** Reader now scans `information_schema.columns.srs_id`; writer already honoured `Geometry.SRID`. PG side now lands `geometry(POINT, 4326)` instead of `geometry(POINT, 0)`. MySQL 8.0+ baseline.
- **Bug 27 (deferred) — VStream POINT bytes mis-parsed.** VStream doesn't strip MySQL's internal 4-byte SRID prefix before the OGC WKB; sluice's WKB decoder reads the SRID's low byte as the byte-order flag and fails. Fix needs the `integration vstream` build tag and slated for a later patch.
- **CI matrix is conditional on trigger (v0.10.4).** Routine push/PR runs Linux-only; tag pushes (`v*`) and `workflow_dispatch` runs the full 3-OS matrix. macOS runners cost ~10× Linux per-minute and were running on every push under the old shape. Branch-protection required-checks list trimmed to match.
- **CHARSET/COLLATION cross-engine diff (committed as v0.11.0; tagged with the next release).** PG schema reader now reads per-column collation via `pg_attribute.attcollation`; `ir.DiffOptions.IgnoreCharsetCollation` becomes load-bearing instead of inert; `diffColumn` compares charset/collation as separate `ColumnDiff` fields; renderer emits MySQL `MODIFY COLUMN` / PG `ALTER COLUMN` suggestions; columns whose only drift was charset/collation are dropped under the suppression flag.
- **`docs/dev/translator-coverage.md`** — research catalog of 30 candidate MySQL→PG rewrite rules sourced from sqlglot, pgloader, dolt's function registry. Top 5 highest-priority for real DDL bodies named explicitly so the next round of translator work has a concrete shopping list.
- **`docs/dev/design-mid-stream-add-table.md`** + **`docs/dev/design-multi-source-aggregation.md`** — proto-ADRs lay out the design space for the two heavier roadmap items so the implementation pass starts from a structured doc, not a blank page.
- **goreleaser cross-platform release (live since `.goreleaser.yaml` + `release.yml` landed earlier).** Tagging `v*` triggers a draft GitHub release with Linux/macOS/Windows × amd64/arm64 binaries. CHANGELOG-driven release notes; operator publishes the draft after review.

### v0.11.x feature wave (proactive translator + reactive bug bundle)

- **v0.11.0 + v0.11.1 — translator catalog two-batch (16 rules).** First batch of MySQL→PG rewrites mined from `docs/dev/translator-coverage.md`'s research catalog: NOW family / UNIX_TIMESTAMP / FROM_UNIXTIME / CHAR_LENGTH / LCASE / UCASE / SUBSTR / MID (v0.11.0); RAND / UUID / ISNULL / REGEXP_REPLACE / INSTR / LOCATE arg-swap / DATE_ADD/DATE_SUB / DATE_FORMAT (v0.11.1). Plus operator-form INTERVAL rewrite and DEFAULT-expression dialect-gating in v0.11.3 closing Bugs 28/29/30 from the v0.11.2 test cycle. ADR-0016 cumulative-scope table now lists 30 MySQL→PG translator entries.
- **v0.11.2 — `schema diff` cross-engine retarget fix.** Empty-source charset/collation now treated as "no opinion" rather than as a sentinel for comparison; pre-fix every PG→MySQL retargeted column (UUID/Inet/Macaddr/Array → MySQL CHAR/VARCHAR/JSON) surfaced as bogus drift. CI Integration job had been red since v0.11.0 push; fix surfaced via the autonomous test-cycle loop.
- **v0.11.3 — DEFAULT-expression translator gating + INTERVAL operator-form rewrite.** New `Dialect` field on `ir.DefaultExpression` (mirrors `Column.GeneratedExprDialect` / `CheckConstraint.ExprDialect`); PG writer's `emitDefault` routes `DefaultExpression` through translator when source dialect differs. New `rewriteIntervalLiteral` operates on the operator-form `INTERVAL <int> <unit>` since MySQL canonicalizes `DATE_ADD(...)` to the operator form before sluice ever sees it. All three v0.11.2 bugs closed.
- **OSS-hygiene track complete.** SPDX license headers swept across all 211 `.go` files in `internal/` via reproducible Go script (commit 575b134). Public README rewrite for operator-scanning audience (commit d749700). License at repo root + per-file headers + goreleaser-published binaries + CONTRIBUTING.md = the OSS-readiness checklist done.
- **CI Integration timeout bumped 10m→18m per package.** Slow CI runners + `-race` overhead were hitting Go's per-package default test timeout. Job timeout-minutes 20→30 to match. Pure infra change; no runtime behaviour.
- **Autonomous release-test-fix loop authorized 2026-05-07.** After each release publishes (Option B 5-gate verification), main session auto-spawns next test cycle (sluice-testing's localhost-docker + PlanetScale harnesses both in scope), reacts to results (fix bugs OR pick next roadmap item), loops until stop condition. Stop conditions: user interrupt, saturation (3 clean cycles + roadmap exhausted), unfixable bug, infra blocker. See `feedback_automation_loop.md` in agent memory.

### Logical backups Phase 6 (at-rest encryption) — closes the logical-backups track

- **6.1 Passphrase mode (v0.22.0).** No cloud dependency. AES-256-GCM bulk cipher, Argon2id KEK derivation from operator passphrase + per-chain salt. Per-chain CEK default; `--encrypt-mode=per-chunk` opt-in. CLI `--encrypt` / `--encryption-passphrase[-env|-file]` on backup full / incremental / stream / restore / sync from-backup. Mixed-mode chains refused; `backup verify` (sha256-only) needs no keys.
- **6.2 AWS KMS (v0.23.0).** `--kms-key-arn` + `--kms-region`; AWS KMS replaces Argon2id for KEK derivation. Per-chain CEK cache → one KMS Decrypt per restore. Construction-time DescribeKey preflight; operator-actionable error translation. `kmsverify` build-tag harness skeleton for operator-run localstack verification.
- **6.3 GCP Cloud KMS + Azure Key Vault (v0.34.0).** Same shape behind the `EnvelopeEncryption` interface. `--gcp-kms-key-resource=...` / `--azure-key-vault-id=...` (+ `--azure-wrap-algorithm`). ADC (GCP) / DefaultAzureCredential (Azure). All four key sources pairwise mutually exclusive at flag-parse. v1 KMS shortlist (AWS + GCP + Azure) complete.
- **6.4 Operator guide + key-management ADR (docs-only, commit `ec6665f`).** [ADR-0037](adr/adr-0037-key-management.md) (key-management decision record) + [`docs/operator/encryption.md`](../operator/encryption.md). Docs-only completion — not tied to a release-version line; closes the Phase 6 deliverable set. The full logical-backups track Phase 1 → Phase 6 is now complete.

### Logical backups Phase 5 (cross-engine chain restore)

- **Cross-engine `sluice restore --from=<chain-url> --target-driver=<engine>`** — was supported for full-only chains since v0.16.x; v0.21.0 extends it to chains with incrementals. Lifts the loud refusal at `chain_restore.go:99` (`"cross-engine chain restore is a Phase 5+ topic"`). Schema deltas in incremental manifests now route through `internal/translate.RetargetForEngine` before invoking `ir.SchemaDeltaApplier.AlterAddColumn` on the target. PG-source `ADD COLUMN UUID` lands as MySQL `CHAR(36)`; INET → VARCHAR(45); Array → JSON. Existing `RetargetForEngine` rules are reused verbatim — no new translation surface.
- **Cross-engine `sluice sync from-backup run --target-driver=<engine>`** — same delta-translation pass on each tick's incremental in `pipeline.SyncFromBackup.applySchemaDeltas`. Detects cross-engine at startup and logs `INFO broker: cross-engine chain — chain's EndPosition not written to sluice_cdc_state; use --at-chain-id for cross-engine resumption assertions`.
- **Change-event value translation reuses live-CDC machinery.** Cross-engine row payloads in change chunks land at the engine appliers' existing live-CDC value-translation path: each applier looks up its own *target* column types and routes every value through `prepareValue` for target-shape preparation. PG → MySQL: UUID strings bind to `CHAR(36)`; JSONB `[]byte` → string for MySQL JSON. MySQL → PG: TINYINT(1) → `bool` handled at the CDC reader's decode layer.
- **Cross-engine broker drops chain `EndPosition`.** PG LSN ↔ MySQL GTID set is not a meaningful translation; the broker writes only its own `_engine="backup-broker"` envelope to `sluice_cdc_state`. Operators continuing CDC from a cross-engine restored target run a fresh `sluice sync start` against the source's native engine (the source's CDC pump opens a new slot at current LSN/GTID; the target is anchored by the restore's data).
- **Loud refusal for unsupportable types.** PG-source PostGIS `Geometry` columns refuse cross-engine restore to MySQL with the offending table + column named and an `--exclude-table` recovery hint. Pre-flighted at chain start so the operator gets a clear failure before any work happens. Same refusal pattern as full cross-engine restore, extended to cover incremental schema deltas (a delta that introduces a PostGIS column refuses with the incremental's BackupID named).
- **Integration coverage on PG and MySQL.** `chain_restore_cross_integration_test.go` covers acceptance criteria 1-4 (PG → MySQL chain, MySQL → PG chain, broker schema-evolution mid-stream, PostGIS refusal); same-engine paths regression-clean via the existing `incremental_pg_integration_test.go` / `incremental_mysql_integration_test.go` running unchanged.

### Logical backups Phase 4.5 (backup-as-broker / `sync from-backup`)

- **`sluice sync from-backup run` / `sluice sync from-backup stop`** — new CLI surface implementing `docs/dev/design-logical-backups-phase-4-5.md`. The consumer-side companion to Phase 4's `backup stream`: a long-running broker that polls a backup chain at the configured `--poll-interval` cadence (default `30s`) and replays new incrementals into a target via the existing `ChangeApplier.ApplyBatch` path. Decouples source from target via the chain as the message log — no direct source-target connectivity required.
- **`pipeline.SyncFromBackup` orchestrator** — opens the target's `ChangeApplier` ONCE for the broker's lifetime; each tick lists manifests, filters to those NOT yet applied (via the persisted `last_applied_backup_id` in `sluice_cdc_state`), and replays each in chain order — schema deltas first (via `ir.SchemaDeltaApplier.AlterAddColumn` from Phase 3.2), then change chunks through the engine's batched applier.
- **Replay state via existing `sluice_cdc_state`** — no schema change. New position-shape sentinel: `position_engine = "backup-broker"`, `position_token = '{"chain_url":"...","last_applied_backup_id":"<id>"}'`. ADR-0007 transactional position-and-data atomicity makes broker crashes mid-replay safe to re-apply (ADR-0010 idempotent applier). New `ir.PositionWriter` optional surface lets the broker record cold-start positions and schema-delta-only-incremental positions without an accompanying data write.
- **`broker_state.json`** — new informational liveness file at `manifests/broker_state.json` carrying `{pid, host, stream_id, started_at, last_apply_at, stop_requested_at}`. Mirrors Phase 4's `stream_state.json` shape at the consumer side; coexists with it when a stream + broker run against the same destination. Reuses the v0.19.1 heartbeat read-modify-write helper to preserve concurrent stop_requested_at across heartbeat writes.
- **Cooperative stop** — `sluice sync from-backup stop --backup-target=<url>` writes `stop_requested_at` to `broker_state.json`; the running broker polls between ticks and exits cleanly. Cross-machine: an operator on machine B can stop a broker on machine A without process access. New exported helper `pipeline.RequestSyncFromBackupStop`. In-process channel registry (`broker_stop_registry.go`) closes same-process stops with zero file I/O — separate from `streamStopRegistry` so a stream + broker against the same destination don't cross-signal.
- **Cold-start safeguards** — first-start refusal when `sluice_cdc_state` has no row for the supplied `--stream-id` (mirrors `migrate --force-cold-start` friction tier). Two override flags: `--reset-target-data` runs an inline `ChainRestore` to land the full + every incremental, then transitions to live polling. `--at-chain-id=<BACKUP-ID>` is the operator's assertion that the target is already at that chain ID (typical workflow: manual `sluice restore --from=<chain-url>`); broker writes a fresh `sluice_cdc_state` row and transitions to live polling.
- **Integration coverage on PG and MySQL** — `broker_pg_integration_test.go` covers acceptance criteria 1, 3, 4, 5, 6, 7 (happy-path catch-up, schema evolution, cooperative stop, restart resumes, cold-start refusal, cold-start with --reset-target-data); `broker_mysql_integration_test.go` covers MySQL happy-path (criterion 2); `broker_fanout_integration_test.go` covers criterion 9 (1 source + 2 brokers reading the same chain → both targets converge with source slot count unchanged).
- **Same-engine only in v0.20.x** — cross-engine `sync from-backup` (PG-source-chain → MySQL target) shipped in Phase 5 (v0.21.0) alongside cross-engine chain restore.

### Logical backups Phase 4 (continuous-incremental long-running stream)

- **`sluice backup stream run` / `sluice backup stream stop`** — new CLI surface implementing `docs/dev/design-logical-backups-phase-4.md`. The stream is a single long-running process that produces rolling incrementals at a configured cadence; no per-incremental cron orchestration. Fits k8s "always-on protection" deployments naturally and pairs with continuous CDC + chain-restore for full DR coverage.
- **Rollover policy.** Hybrid time-bound + size-bound + change-count-bound, first-fired wins (mirrors single-shot incremental's existing shape). `--rollover-window=DURATION` (default `5m`), `--rollover-max-changes=N` (default `100k`), `--rollover-max-bytes=BYTES` (default `64Mi`). Window extends to the next `TxCommit` so the chain doesn't end mid-tx (same as Phase 3.1). Empty rollovers skipped by default; `--rollover-include-empty` opts in for heartbeat-shape monitoring.
- **`pipeline.BackupStream` orchestrator** — opens the engine's CDC pump ONCE for the lifetime of the stream and reuses it across rollovers. Each rollover writes a NEW manifest at `manifests/incr-<unix-millis>-<seq>.json` (mirrors Phase 3.1 + Bug 35's per-Run namespace). The chain-walker (Phase 3.2) handles stream-written chains transparently — restore + verify don't care that the chain came from `backup stream` vs `backup incremental`.
- **`stream_state.json`** — small mutable file at `manifests/stream_state.json` carrying `{pid, host, started_at, last_rollover_at, stop_requested_at}`. Updated on every successful rollover. Concurrent-writer protection: refuses to start a second stream when the file shows a recent (`< 2 × rollover-window`) `last_rollover_at` from a different (pid, host); `--force` bypasses with a WARN. Operator-actionable error message names the conflict and the override.
- **Cross-machine stop** — `sluice backup stream stop --target=<url>` writes `stop_requested_at` to the state file; the running stream polls between rollovers and exits cleanly when set. Mirrors the `sync stop` pattern (ADR-0025) but via the backup destination as the rendezvous point — both sides agree on the destination, not on the host. New exported helper `pipeline.RequestStreamStop`.
- **Signal handling** — SIGINT/SIGTERM via the existing `kongContext` notifier propagates as ctx.Done through the rollover loop; mid-rollover cancel surfaces as a clean nil exit (the rollover's chunks may be partially-written but the manifest never finalises; on restart the stream picks up at the previous rollover's EndPosition).
- **Rollover hooks** — `--rollover-hook=<cmd>` runs after each rollover commits with env vars `SLUICE_ROLLOVER_MANIFEST_PATH`, `SLUICE_ROLLOVER_PARENT_BACKUP_ID`, `SLUICE_ROLLOVER_BACKUP_ID`, `SLUICE_ROLLOVER_CHANGES`, `SLUICE_ROLLOVER_BYTES`, `SLUICE_ROLLOVER_ELAPSED_MS`. 30 s timeout. Hook errors WARN-log but don't fail the stream — the rollover already committed. Examples in docs: push to Prometheus pushgateway / send Slack notification / write to monitoring datastore.
- **Integration coverage on PG and MySQL** — `stream_pg_integration_test.go` covers acceptance criteria 1, 4, 5, 6, 7, 8 (long-running stream produces rolling incrementals, chain restore round-trip, ctx-cancel clean exit, cross-machine stop request, concurrent-writer refusal); MySQL counterpart covers the binlog flavour. Bounded windows (2 s) and small ceilings (10 changes) keep test runtime under a minute per scenario.

### Logical backups Phase 3.3 (full-backup `EndPosition` + chain → CDC handoff)

- **Full-backup `EndPosition` recording (v0.17.2 + v0.18.0).** v0.17.2 added the optional `ir.BackupPositionCapturer` engine surface that captures `pg_current_wal_lsn()` (PG) or `@@global.gtid_executed` (MySQL) at end-of-backup. v0.18.0 closed the v0.17.2 "during-backup write window" gap by adding `ir.BackupSnapshotOpener` that captures `EndPosition` at snapshot START — the row sweep reads from the snapshot's consistent view, and CDC from `EndPosition` forward covers every write after the snapshot. Backup-only DR is now byte-perfect under heavy write load.
- **`sluice sync start --position-from-manifest=<chain-url>`.** Loads the chain's terminal manifest's `EndPosition` and uses it as the resume position, bypassing the per-target `sluice_cdc_state` lookup. Use after `sluice restore --from=<chain-url>` to resume CDC from the chain's tail without re-bulking from source.
- **PG soft-warning preflights** (Phase 3.3.C) for `--position-from-manifest`: `wal_keep_size` sufficiency, Patroni / HA-managed source detection (six signals after v0.17.3 broadened heuristics for tenant-isolated managed PG), slot existence + health. `--strict-preflight` promotes warnings to refusals; `--patroni-mode=auto|on|off` gates the Patroni warning explicitly.
- **`docs/postgres-source-prep.md`** — operator setup including the idle-slot failover trap and the slot-creation flow for managed-PG services (PlanetScale Postgres, Aurora, Cloud SQL, Azure Database for PostgreSQL, Archil).

### Logical backups Phase 3.1 + 3.2 (incremental backups + chain-aware restore)

- **`sluice backup incremental --since=<full-id-or-url>`** — single-shot incremental writer; bounded by `--window=DURATION` or `--max-changes=N`, first-fired wins. Window extends to next `TxCommit` so the chain doesn't end mid-tx. Reuses the existing CDC pump; writes serialised `ir.Change` events to `chunks/_changes/<run-namespace>/changes-<idx>.jsonl.gz` chunks.
- **Chain-linked manifests** — `Manifest.Kind`, `ParentBackupID`, `StartPosition`, `EndPosition`, `SchemaHash`, `SchemaDelta`, `ChangeChunks` fields capture the per-incremental position window + DDL deltas (option (b) from the design — schema fingerprint + restore-side replay). Manifest path: `manifests/incr-<unix-millis>-<seq>.json` (lexically sortable; per-Run namespace closes Bug 35 from the v0.17.0 cycle).
- **`sluice restore --from=<chain-url>` walks the chain.** `pipeline.chain_restore.go` lists manifests, sorts by `Kind` + `ParentBackupID` linkage, applies the full first then each incremental in order. Reuses the existing applier path (idempotent per ADR-0010).
- **Cross-engine chain restore was refused** loudly through v0.20.x (Phase 5+ topic at the time); v0.21.0 lifts this — see "Logical backups Phase 5" above.

### Logical backups Phase 2 (cloud backends + resumable writer)

- **`pipeline.BlobStore`** — S3 / GCS / Azure backed `ir.BackupStore` implementation via `gocloud.dev/blob`. Operators name the destination via `--target=s3://bucket/prefix` / `gs://...` / `azblob://...` / `file:///...`. S3-compatible providers (MinIO, R2, B2, Wasabi, Tigris, Archil's S3 read API) work via `--backup-endpoint`, `--backup-region`, `--backup-path-style` overrides.
- **Resumable backups** — re-running `sluice backup full` against the same destination resumes a partial backup; refuses to clobber a completed one without `--force-overwrite`. Per-table checkpoints + per-chunk SHA-256 skip already-uploaded chunks. Bug 34a/b (per-chunk-resume + manifest-checkpoint cadence) closed.

### Logical backups Phase 1 (full snapshot to local filesystem, IR-format chunks)

- **`sluice backup full` / `sluice backup verify` / `sluice restore`** — new CLI surface implementing the MVP slice from `docs/dev/design-logical-backups.md`. Manifest+chunks layout: a JSON `manifest.json` carrying the full IR schema, per-table row counts, and per-chunk SHA-256s, alongside one or more gzipped JSON Lines chunk files under `chunks/<table>/`. Restore round-trips through `translate.RetargetForEngine` so cross-engine restore (PG backup → MySQL target, etc.) works using the same machinery `sluice schema diff` uses.
- **New types:** `ir.BackupStore` (storage interface designed for Phase 2 cloud backends from day one), `ir.Manifest`, `ir.TableManifest`, `ir.ChunkInfo`. Tagged-union JSON envelopes (`ir.MarshalType` / `ir.UnmarshalType` / `ir.MarshalDefault` / custom `Column.MarshalJSON`) so the IR's sealed `Type` / `DefaultValue` interfaces round-trip through `encoding/json`.
- **New pipeline orchestrators:** `pipeline.Backup`, `pipeline.Restore`, `pipeline.LocalStore` (local-FS implementation of `BackupStore`), `pipeline.VerifyBackup` (chunk-level rehash without restoring).
- **Restore-time integrity:** per-chunk SHA-256 checked at restore (loud-failure tenet — corruption surfaces as `ErrChunkHashMismatch`); per-table row count compared against manifest after streaming.
- **Phase 2 (cloud backends — S3/GCS/Azure), Phase 3 (incremental backups), and Phase 6 (KMS-backed encryption) follow** — interface is ready; implementations and the manifest version bump are the only remaining work.

### View support Phase 1 (regular + materialized views, schema-only round-trip)

- **`ir.View` type + `Schema.Views` field** wired end-to-end through readers, writers, pipeline, CLI, schema diff, and schema preview. Both shipping engines populate `Schema.Views` (MySQL via `information_schema.views`; Postgres via `pg_views` + `pg_matviews` with `Materialized=true` on matviews).
- **`ir.SchemaWriter.CreateViews`** new interface method; both engines implement. New Phase 6 in the simple-mode orchestrator (after constraints) emits `CREATE OR REPLACE VIEW` (regular) and `CREATE MATERIALIZED VIEW ... WITH DATA` (matviews; PG-only). View-to-view dependency ordering uses single-pass-with-up-to-2-retries policy — no SQL parser, surfaces clear error if budget exhausted.
- **CLI flags** `--include-view PATTERN`, `--exclude-view PATTERN`, `--skip-views` on `migrate` / `sync start` / `schema preview` / `schema diff`. `ViewFilter` mirrors `TableFilter` shape; filtered independently from tables.
- **Schema diff for views** — `ir.SchemaDiff.ViewsMissing` / `ViewsExtra` / `ViewsMismatched` populated by `DiffSchemas`; text + JSON renderers updated. Definition comparison is trim-and-equal; cross-engine drift is high-noise by design (PG canonicalises view bodies via `pg_get_viewdef`); the renderer hedges with a low-confidence comment.
- **Phase 1 limitations documented** — cross-engine view-body translation (PG → MySQL or vice-versa) emits the source dialect verbatim and relies on the loud-failure tenet. View definitions with explicit column lists (`CREATE VIEW v(a,b,c) AS ...`), MySQL `CREATE ALGORITHM=UNDEFINED VIEW`, PG `WITH (security_invoker=true)`, and PG RULE-based pseudo-views are out of scope for Phase 1.

### Foundational ADRs (0001–0029)

IR-first, sealed interfaces, kong+koanf, three-phase apply, MySQL flavors, pgoutput, position persistence, go-mysql, Streamer-as-separate-orchestrator, idempotent applier semantics, SlotManager optional surface, pglogrepl bypass for FAILOVER, applier value-shaping with `CAST(? AS JSON)`, phase-aware error-hint registry, migration resume design, layered expression translation (extended in v0.8.0 with bool-idiom rewrites and v0.9.0 with index-expression and bool-sub-expression coverage), batched CDC apply, per-batch bulk-copy checkpointing, parallel within-table bulk copy, slot-ack-after-apply, publication scope by table, slot-missing fall-through (extended for MySQL in v0.6.0), `--reset-target-data`, `sluice schema preview`, graceful-drain `sync stop` (extended in v0.9.0 with `--wait`), LOAD DATA INFILE writer, source-tx-boundary CDC batching, memory-bounded streaming, `sluice schema diff`.

---

## Next up

The v0.10.x cycle closed reactive-bug loops on the v0.9.x translator gaps and shipped the `--expr-override` escape hatch. The v0.11.x cycle landed two batches of proactive translator catalog work (16 rules) plus three real-world bug fixes from the autonomous test-cycle loop. The v0.13.x–v0.21.x autonomous-release block (May 2026) shipped the entire "100% confidence" backbone: `sluice verify` (count + sample + full-mode), `sync-health` monitoring + Prometheus listener, **and the full logical-backups track Phase 1 → Phase 5** (local-FS → cloud backends → incremental + chain-aware restore → CDC handoff → continuous-incremental long-running stream → backup-as-broker → cross-engine chain restore). OSS-hygiene track is closed.

The v0.22.x–v0.64.0 block then shipped, end-to-end: **logical-backups Phase 6** (passphrase + AWS/GCP/Azure KMS encryption, Phase 6.1–6.4 including the ADR-0037 key-management + operator-encryption-guide deliverables), the **PG → PG extension-passthrough v1 shortlist** (pgvector, pg_trgm, hstore, citext, PostGIS — ADR-0032), **GEOMETRY/SPATIAL** (ADR-0035 + the v0.33.x geography/Z-M closure), **mid-stream strict zero-loss correctness** (ADR-0036), multi-source aggregation Shape B, the translator catalog (28 of 30 rules), backup-chain retention chunks **14a/14c + 14b Phase 1**, the full **PII redaction track Phase 1 → Phase 4** (ADR-0039/0040/0041), and the **ADR-0042/0043 bulk-copy throughput arc** (v0.62.0 default tune, v0.63.1 PG cold-start N1 fix, v0.64.0 native fast-loader on the cold parallel-copy path).

What remains are the **harder-frontier, demand-gated items** that have always been here as design-first work — multi-source Shape A, MySQL multi-source native parity, ADR-0032 Tier 3 (uuid-ossp + pgcrypto function-defaults), the remaining 5 deferred translator rules, View Phase 3, backup-chain 14b Phase 2 / 14d / 14e, and the analytics-export / Arrow research items pending operator demand.

### 1. Logical backups Phase 6 — KMS-backed at-rest encryption — **SHIPPED (Phases 6.1–6.4)**

Closed end-to-end. See "Recently landed: Logical backups Phase 6 (at-rest encryption)" for the v0.22.0 → v0.34.0 summary plus the Phase 6.4 docs-only completion (ADR-0037 key-management + `docs/operator/encryption.md`, committed `ec6665f`; docs-only, not tied to a release-version line). The full logical-backups track (Phase 1 → Phase 6) is now complete.

---

### 2. Apache Arrow integration (deferred — research-doc updated)

**Why.** Original conditional-yes recommendation in `design-apache-arrow-integration.md` was gated on logical-backup picking Parquet. **Phase 1 logical backups shipped JSON-Lines + gzip instead** (`internal/pipeline/backup_chunk.go:8-32`); the gate dissolved. Updated research lives in [`docs/research/apache-arrow-findings.md`](../research/apache-arrow-findings.md): Shape A (Arrow as IR row representation) is recommended **defer** — sizable refactor, zero current operator demand. The narrower analytics-export angle moved to its own research item — see item 9 below.

**What.** No code chunk. Revisit when an operator with a real Parquet/columnar requirement surfaces, or when item 8's research doc names a winning surface.

**Gotchas (preserved for the eventual revisit).** Silent type drift at Decimal-256 / Time-out-of-range / UUID-string-parse / Arrow-null-vs-empty-string boundaries — each needs explicit loud-failure branches per the loud-failure tenet. ~2× binary growth under the build tag (mitigated by keeping default untagged build slim). Shape C (Arrow as in-flight row-pipeline format) explicitly rejected as IR-tenet-violating.

---

### 3. Mid-stream live add-table — strict zero-loss correctness (PG Phase 2 follow-up) — **SHIPPED v0.32.0 (ADR-0036)**

Closed under [ADR-0036](adr/adr-0036-mid-stream-loss-surface-characterization.md) after a four-round Phase A diagnose campaign. See "Recently landed: Mid-stream live add-table — strict zero-loss correctness (v0.32.0)" for the trimmed summary; ADR-0036 for the full Phase A trace + Phase B identification; CHANGELOG [0.32.0] for the operator-facing summary.

---

### 4. Multi-source aggregation — Shape A (sharded → consolidated)

**Why.** Multi-source Phase 1 + 2 (Shape B microservices → analytics warehouse) shipped in v0.25.0 — see "Recently landed". Shape A (N functionally-identical sources, sharded by key, consolidated into one target table) is the still-outstanding pattern. ADR-0031 explicitly defers it because it requires meaningfully more machinery; the proto-design at [`design-multi-source-aggregation.md`](design-multi-source-aggregation.md) covers the full surface area.

**What.** Three new pieces:
- **Discriminator-column injection.** New CLI flag `--inject-shard-column NAME=VALUE` (mirrored on each shard's `sluice sync start`); sluice injects the column at translation time + populates it during writes so the consolidated PK stays unique across shards.
- **Populated-target bulk-copy.** Today's cold-start preflight refuses bulk-copy into a non-empty target (Bug 9 protection). Shape A needs a "discriminator-aware" bypass that knows which rows belong to which shard so the second/third/Nth shard's bulk-copy can land cleanly alongside the first's data.
- **Cross-shard schema-migration coordination.** When the operator alters the source schema, every shard's stream needs to coordinate the ALTER on the consolidated target. ADR-0030's `--no-drain` add-table is single-source; Shape A needs cross-stream consensus.

**Gotchas.**
- The discriminator column shape needs a name in the IR (column origin = sluice-injected vs source-derived) so the applier can tell the two apart and the diff doesn't flag an "extra column on target."
- The cold-start populated-target bypass is a sharp tool — getting it wrong means silent data corruption (one shard overwrites another's rows). Loud preflight: "you've set --inject-shard-column on a fresh stream into a populated target; rows from other shards already present must have shard-column NOT NULL and the new shard's value must be unique."
- Shape A is heavier than Shape B; it waits for a real operator request with a concrete workload before committing to a design pass.

---

### 5. Translator catalog continuation

**Status (v0.38.0).** Twenty-eight of 30 catalog rules shipped after a second re-assessment pass. v0.35.0 landed 6 rules; v0.37.0 added 3 more (TIMESTAMPDIFF, JSON_OBJECT/ARRAY, LAST_DAY); v0.38.0 added 3 more from rule #10's hash family (MD5 unconditional via PG core; SHA1 + SHA2 gated on `--enable-pg-extension pgcrypto`). pgcrypto joins the extension catalog as a presence-gate (no types) — sluice's preflight confirms it's installed on the target before the SHA rewrites fire. The remaining 5 rules stay deferred per the catalog's load-bearing per-rule analysis:

- **#11 GREATEST/LEAST** — same function name in both engines but NULL semantics differ; auto-rewrite would mask divergence.
- **#13 REGEXP_LIKE** — MySQL ICU vs PG POSIX regex flavours diverge beyond clean rewrite.
- **#21 FIND_IN_SET** — full position semantic needs a LATERAL subquery, invalid in CHECK/GENERATED contexts.
- **#23 CONVERT_TZ** — AT TIME ZONE has subtle timestamp-vs-timestamptz semantics.
- **#29 INET_ATON/INET_NTOA** — no portable PG equivalent without a custom function.

All five have an actionable workaround via `--expr-override`. The remaining gaps would only land if a real operator workflow surfaces one of the deferred shapes as a regression — at which point the catalog's per-rule analysis already names the cost.

---

### 6. GEOMETRY / SPATIAL support — PostGIS-aware translation (LANDED v0.28.0)

Closed under [ADR-0035](adr/adr-0035-postgis-geometry-spatial-support.md). See "Recently landed" above for the v0.28.0 summary covering all three sub-phases: PG writer emit + WKB → EWKB framing (Phase A), VStream POINT prefix stripping (Phase B), and cross-engine PG ↔ MySQL geometry round-trip with SRID preserved (Phase C). Bugs 26 + 27 closed.

---

### 7. Backup chunk compression investigation — **SHIPPED**

**Landed** as build-tagged harness in `internal/pipeline/internal/compressbench/` plus decision doc at [`docs/dev/notes/compression-benchmark.md`](notes/compression-benchmark.md). Benchmarked stdlib `compress/gzip`, klauspost drop-in `gzip`, `zstd` (SpeedDefault + SpeedBetterCompression), and `snappy` across the four corpora at 50k rows each (operators can crank via `SLUICE_COMPRESSBENCH_ROWS`).

**Recommendation captured in the decision doc:**

- **Short-term** — swap stdlib `compress/gzip` for `klauspost/compress/gzip` in `backup_chunk.go`. Drop-in (same `NewWriter` / `NewReader` surface, same gzip wire format → no chunk-format change, no version bump). Buys 2-6× encode speedup with <5% ratio loss across all four corpora. klauspost/compress is already in the module graph (indirect via pgx), so promoting it adds zero binary-size cost.
- **Phase 2** — add `--compression=<algo>` flag with `gzip` default and `zstd` opt-in. Justification only after operator demand; the wins (storage cost on numeric/json corpora) trade against a chunk-format version bump and a backward-compat reader path. zstd at SpeedDefault is the right Phase-2 target; SpeedBetterCompression's marginal ratio gain doesn't pay back its 2× encode cost.

The harness lives at the package; future runs against new corpus shapes or new algorithms (the catalog in `algos.go` is one-entry-per-algo additive) regenerate the markdown table. Reproduction commands at the bottom of the decision doc.

---

### 8. Analytics-friendly source — research doc (Parquet export + DuckDB + Arrow Flight) — **research SHIPPED**

**Landed** as [`docs/research/sluice-as-analytics-source.md`](../research/sluice-as-analytics-source.md). The doc covers operator personas, three surface candidates, worked examples, and a dep-cost × persona-breadth matrix.

**Conclusion captured in the doc:**

- **Promote to a code chunk on operator demand** — Surface 1 (`sluice backup export-as-parquet` one-shot transcode, built on `parquet-go/parquet-go`). Low dep weight (~5 new modules; pure Go; no CGO); broad persona reach (ad-hoc + warehouse-pipeline operators). ~600-1000 LOC. The doc serves as the chunk's prep document when promoted.
- **Land alongside Surface 1** — Surface 2 (DuckDB recipe in `docs/cookbook/`). Zero code; ~1 day. Operators with DuckDB appetite already know how to drive it; sluice just makes its outputs greppable from there.
- **Defer** — Surface 3 (Apache Arrow Flight). High dep weight (`apache/arrow-go/v18` + gRPC server runtime; ~2× binary size), narrow persona reach (analytics-first / lakehouse, rare today). Revisit when an operator with a concrete Flight-speaking consumer surfaces AND Surface 1 is shipped.

The doc also flags five open questions the eventual chunk's prep doc should NOT re-derive (Parquet file granularity, encryption pass-through, incremental mode, GeoParquet adoption, decimal precision overflow). All five have a recommendation captured.

---

### 9. Multi-source aggregation — MySQL native parity

**Why.** v0.25.0's `--target-schema` is PG-only by design (PG schemas are first-class; MySQL collapses schema-and-database). MySQL operators with the same multi-source-microservices pattern get equivalent coverage today via `--target` DSN choice (different MySQL databases on the same server). For most operators this is enough — the analytics queries cross databases the same way they would cross PG schemas, and MySQL's namespace model genuinely is "database = schema."

**What.** If a real operator surfaces "I want N sluice processes targeting one MySQL server but landing in one logical database with namespacing," the design would need to invent a per-table prefix mechanism (option (b) from the proto-ADR's schema-collision discussion: `table_renames: { source_table: target_table }` mapping). New CLI flag `--rename-table SOURCE=TARGET` mirrored on each source's stream.

**Gotchas.** Per-table renaming is more verbose than schema-prefixing, but the proto-ADR notes it's the universal flexible option. Hardest UX problem: keeping the rename map in sync with source-side schema changes (operator must update the map when a source adds a table).

**Operator demand check.** Currently zero. The DSN-choice workaround is good enough for the cases sluice has seen. Track here so the option doesn't get lost; revisit when an operator asks.

---

### 10. PlanetScale MySQL+Vitess test-matrix expansion

**Why.** Sluice has a `psverify` build-tag harness for PlanetScale-backed Vitess source coverage (`docs/dev/notes/psverify-status.md`), gated by `PLANETSCALE_CREDENTIALS.env`. Coverage today is operator-driven (the harness exists; the user runs it before releases when they have credentials available) rather than continuously exercised in CI. Several VStream-specific edge cases live in this gap:

- **Bug 27** (VStream POINT bytes mis-parsed) is the canonical example — explicitly deferred to the "GEOMETRY/SPATIAL support" entry's Phase B because the test infrastructure is operator-run.
- **Mid-stream MySQL Phase 2.5** — VStream is a separate code path from vanilla MySQL binlog. v0.27.0's MySQL Phase 2 (ADR-0034 filter-flip mechanism) ships only the binlog-source path. VStream's own table-scope semantics (per-shard streams, COPY-mode handoff) need their own analysis before the same `--no-drain` UX can extend to PlanetScale operators. Demand-driven; track here when an operator surfaces a real PS workload that needs live add-table.
- **PlanetScale Postgres slot lifecycle** — Patroni-managed; slot loss on failover is silent without `Logical slot name` cluster config (operator memory `project_planetscale_postgres_slots.md`). The check is documented in `docs/postgres-source-prep.md` but not exercised in CI.
- **VStream POINT/POLYGON/cross-shard-PK edge cases** — generally any PS-specific behavior beyond what vanilla MySQL exhibits.

**What.** Three paths (Path C added 2026-05-10 alongside the Vultr-box bootstrap):

- **Path A — Operator-run coverage matrix.** Document a "before each release" PlanetScale checklist: spin up a PS branch, run sluice against it for the canonical scenarios (vanilla migrate, CDC stream, slot recovery, geometry types, slot rename via `--slot-name=Logical slot name`). Output: `docs/dev/notes/ps-release-checklist.md`. ~1 day to write + populate; no code chunk.
- **Path B — CI-integrated coverage.** Move `psverify` (and/or `vstream`) from operator-run to CI-conditional (PR labels, scheduled workflows). Requires a non-revocable PlanetScale credential surface in CI; operationally heavier. Defer until Path C's signal surfaces enough recurring gaps to justify the CI cost.
- **Path C — Vultr-box pre-release validation (LANDED).** The always-on Vultr instance runs `integration vstream` (vttestserver-based VStream coverage) on every release-validation pass without burning CI minutes. See `docs/dev/notes/release-validation-on-vultr.md` for the runbook. Reference timing: ~4 min per run. Captures the Vitess-side coverage gap that CI explicitly skips for cost reasons. The remaining gap vs `psverify` is real-PlanetScale-specific behavior (TLS, vendor pgwire-proxy quirks); Path A still covers that surface.

**Operator demand check.** Path C closed the loop on the most-frequent vstream-edge-case worry without a CI cost spike. Path A remains the right answer for real-PlanetScale-only quirks. Path B is a "if PS coverage gaps keep biting *and* Path C alone isn't enough" follow-on.

---

### 11. PG → PG extension passthrough — Tier 3 (uuid-ossp + pgcrypto function-defaults) — *the only live forward-looking piece*

**Status:** the entire ADR-0032 v1 shortlist **SHIPPED** — framework + pgvector (v0.26.0), pg_trgm (v0.30.0), hstore + citext (v0.31.0, with the hstore PG→PG binary COPY codec landing in v0.32.1 / Bug 48 in v0.31.1), PostGIS (v0.33.0, with Bugs 49–53 closing geography / SP-GiST / BRIN / Z-M-ZM dimensional follow-ups through v0.33.3). pgcrypto joined the catalog as a presence-gate (no types) in v0.38.0 for the SHA1/SHA2 translator rules. See "Recently landed: PG → PG extension passthrough — v1 shortlist complete" + [ADR-0032](adr/adr-0032-pg-extension-passthrough.md) (status block records all five under "shipped").

**Why (Tier 3 — what remains).** uuid-ossp + pgcrypto are universal across all four hosted-PG providers, but their hard part is the **function-default catalog** (`uuid_generate_v4()`, `gen_random_uuid()`, pgcrypto `digest()` / `crypt()` in column DEFAULT / GENERATED bodies), not the type passthrough. The type-passthrough machinery is done; Tier 3 is an expression-translator-catalog chunk, not a new IR/engine surface.

**What.** Per-extension expression-translator catalog entries so a PG → PG passthrough preserves `DEFAULT uuid_generate_v4()` / pgcrypto function-defaults verbatim, with loud failure on cross-engine unless explicitly translated (parallel to ADR-0016's policy). pgcrypto's presence-gate already exists from v0.38.0; uuid-ossp would add the analogous catalog entry. **Design is signed off in [ADR-0044](adr/adr-0044-extension-function-defaults-tier3.md) (Accepted 2026-05-16) — implementation pending.** ADR-0044 adopts the same-engine PG → PG opt-in gate as drafted: extension-function defaults/generated-exprs require `--enable-pg-extension <ext>` and are refused early-and-clearly otherwise (a deliberate behaviour change vs. today's implicit pass-through; core PG functions like `gen_random_uuid()` / `now()` are never gated). ADR-0032 §"Consequences" originally named this as the natural Tier 3 chunk.

**Gotchas.** Extension version skew (v1 checks presence, not version) is a documented limitation that carries forward. Cross-engine `DEFAULT gen_random_uuid()` → MySQL is already handled (Bug 42, v0.23.1); Tier 3 is specifically the *PG → PG passthrough fidelity* of the function-default expressions plus uuid-ossp's `uuid_generate_*` family.

**Operator demand check.** Demand-gated — promote when a real operator surfaces a PG → PG sync where uuid-ossp / pgcrypto function-defaults round-trip incorrectly. Estimated ~150–300 LOC + ADR-0032 catalog-table update + integration tests.

---

### 11.original (historical context, retained for traceability)

**Why.** Postgres' extensibility — PostGIS, pgvector, pg_trgm, hstore, citext, ltree, pgcrypto, uuid-ossp, etc. — is a major reason operators choose PG specifically. Today sluice's IR doesn't represent extension types, so PG-source columns of those types either get dropped (silent — not OK per the loud-failure tenet) or surface a loud refusal at schema-read time. **For PG → PG syncs where both sides have the same extensions installed, those columns should "just work"** — pass through with native fidelity rather than being treated as hostile. Cross-engine targets (PG → MySQL) keep the loud-failure default; explicit operator-supplied translations stay the escape hatch (parallel to ADR-0016's expression translator catalog).

**What.** Allowlist-based opt-in passthrough mode. New CLI flag `--enable-pg-extension EXT` (repeatable). When set:
- PG schema reader recognizes columns of those extension types and emits them into a new IR variant (`ir.ExtensionType{Extension, Name, ...}`).
- PG schema writer detects same-engine target + matching extension installed + recognized extension type → emits the column verbatim.
- CDC reader / row reader pass through the binary representation as-is (no value-shaping).
- Pre-flight refuses cleanly if target doesn't have the extension installed (`SELECT 1 FROM pg_extension WHERE extname = $1`).
- Cross-engine targets (MySQL): existing loud-error path preserved; operator can supply explicit `--type-override` to cast to a fallback type.

**Three tiers based on what fidelity needs.** Tier classification informs which extensions are cheap to support vs. need real engineering:
- **Tier 1: Type-only.** Extension defines new column types; values are opaque bytes/text from sluice's POV. Examples: hstore, citext, ltree, cube, intarray. ~50-100 LOC per extension.
- **Tier 2: Type + indexes.** Type plus index access methods (GIN, GiST, BRIN-via-extension) operators rely on. Examples: PostGIS (gist), pgvector (ivfflat / hnsw), pg_trgm (gin). ~150-300 LOC per extension; need index-method awareness in schema reader so round-trip preserves indexes.
- **Tier 3: Type + functions in defaults / generated columns.** Extension-defined functions appear in column defaults or generated expressions. Examples: uuid-ossp's `uuid_generate_v4()`, pgcrypto's `digest()`. Adds expression-translator catalog entries for PG-to-PG passthrough; loud failure on cross-engine unless explicitly translated. Per-extension policy.

**Gotchas.**
- Extension version skew (PostGIS 3.4 source → 3.0 target) could surface subtle behavior gaps. v1 only checks extension presence, not version; document the limitation; operator-supplied version-pinning could come later if real-world drift causes pain.
- Per-extension allowlist is more conservative than auto-detect ("we have the same extensions, pass them through automatically") but also higher operator burden. Worth considering an `--auto-pg-extensions` opt-in escape hatch in v2 once the allowlist is battle-tested.
- The PostGIS PG-to-PG passthrough overlaps with the "GEOMETRY/SPATIAL support" entry (which today addresses cross-engine PG ↔ MySQL geometry). Decision: this entry covers the PG-to-PG path for PostGIS as part of the broader extension-passthrough mechanism; the GEOMETRY/SPATIAL entry retains its cross-engine + VStream focus. Bug 26/27 stay parented under GEOMETRY/SPATIAL.

**Operator demand check.** Strong indirect signal — the PG ecosystem has well-documented extension-heavy adoption. pgvector specifically has had massive AI/ML adoption since 2023. PG-to-PG syncs where extensions are blocked are a real pain point. v1 ships the allowlist + 3-5 most-deployed Tier 1+2 extensions; further extensions follow demand.

**v1 shortlist (pinned by item 12's research doc, [`docs/research/pg-extensions-deployment-frequency.md`](../research/pg-extensions-deployment-frequency.md))** — implementation order:

1. **pgvector** (Tier 2) — leads; establishes the Tier-2 index-method-passthrough machinery PostGIS will reuse. Strongest demand trajectory (AI/ML).
2. **pg_trgm** (Tier 2 lite — operator classes only, no new column type) — validates the index path on something simpler than full pgvector.
3. **hstore** (Tier 1) — first Tier 1 ship; type-only opaque-text validation.
4. **citext** (Tier 1) — second Tier 1, even simpler (text + collation); pair with hstore in one PR if the IR shape is clean.
5. **PostGIS** (Tier 2) — last in v1; coordinates with the "GEOMETRY/SPATIAL support" entry above (the cross-engine PostGIS path stays parented there; this entry covers the PG-to-PG passthrough as the broader extension mechanism).

**Surprises documented in item 13's doc**: uuid-ossp + pgcrypto are universal across all four sources but are Tier 3 (function-in-defaults expression-translator work) — strong v2 candidates after the v1 Tier 1+2 machinery is in place. ps-extensions.io's #1 (`pg_search`, 116 votes) is single-vendor (paradedb) and not on Neon — poor v1 fit despite vote count. hstore + citext have clean cross-engine paths to MySQL JSON / VARCHAR-collated respectively (worth ADR-0016 default-translator entries when ready).

Estimated ~800-1500 LOC for v1 + ADR + integration tests, depending on which Tier 2 extensions land in scope.

---

### 12. PG extensions deployment-frequency research doc — **shipped** (research-only, see `docs/research/pg-extensions-deployment-frequency.md`)

**Status:** Survey landed (research doc) → v1 shortlist pinned → item 11 Phase 1 (framework + pgvector) shipped in v0.26.0. The doc remains the canonical reference for the v1 shortlist's implementation order. Remaining text retained for traceability.

**Why.** Item 12 ships an extension-passthrough allowlist; the v1 list has to be picked from somewhere. Operator-perceived priorities differ wildly (a pgvector shop disagrees with a PostGIS shop disagrees with an hstore shop), so a survey of which extensions are most-deployed in the wild is the cheapest input to that decision.

**What** (research-only — no code yet). A `docs/research/pg-extensions-deployment-frequency.md` covering:

- **Sources surveyed**: managed-PG provider extension lists are the primary signal — they reflect what the providers' customers actually request.
  - **Supabase** — extensions list at <https://supabase.com/docs/guides/database/extensions>
  - **Neon** — extensions list at <https://neon.com/docs/extensions/pg-extensions>
  - **PlanetScale Postgres** — extensions list at <https://planetscale.com/docs/postgres/extensions>
  - **PlanetScale's requested-extensions tracker** at <https://ps-extensions.io/> (operator-voted demand signal — strongest direct read on what operators want that providers don't yet support)
- **Per-extension classification** (matrix): name, primary use case, Tier (1/2/3 per item 11's framework), provider availability (Supabase / Neon / PlanetScale / PostgreSQL contrib), license, sluice-passthrough complexity estimate, "must-have for v1" yes/no with rationale.
- **Recommended v1 allowlist** for item 11: 3-5 extensions with the strongest (deployment frequency × tractable Tier) signal. My initial guess for the shortlist: PostGIS, pgvector, pg_trgm, hstore, citext — but the survey may surface alternates.
- **Cross-engine policy revisit**: which extensions, if any, have practical cross-engine translations (PostGIS → MySQL spatial, pgvector → MySQL JSON-of-floats) worth ADR-0016 expression-translator entries vs. operator-override-only? Mostly a "no" — the loud-failure default keeps stepping right.

Output: `docs/research/pg-extensions-deployment-frequency.md` (matrix + recommended allowlist + cross-engine policy section). Estimated 1-2 days research; no code chunk until the doc names the v1 shortlist.

---

### 13. View support Phase 2/3 — materialized-view refresh + cross-engine view-body translation

**Phase 2 shipped v0.36.0.** `sluice matview refresh --target=DSN [--matview NAME] [--concurrently] [--target-schema=NAME]` — one-shot subcommand that drives `REFRESH MATERIALIZED VIEW [CONCURRENTLY]` against PG matviews on the target. PostgreSQL-only (refuses on MySQL targets — MySQL has no matview concept). Operator drives cadence from their own scheduler (cron / k8s CronJob / Airflow); sluice deliberately does NOT own a refresh loop because (1) cadence is operator-policy, and (2) integrating with operator scheduling means alerting, backoff, and observability for the cron itself are already in place. Concurrent refresh is pre-checked for the required unique index on the matview; missing-index matviews are skipped with a clear operator-actionable reason. Missing-matview filters surface as loud-failure errors. Per-matview timing is captured in both text and JSON output for metrics scraping.

**Phase 3 — cross-engine view-body translation.** Phase 1 emits the source-dialect SELECT verbatim on cross-engine pairs; non-portable definitions surface as a target-side parse rejection at apply time (loud-failure tenet). The right Phase 3 path is to extend ADR-0016's expression translator to a SELECT grammar — a strict subset (column refs, function calls, JOIN/WHERE/GROUP BY) that covers the high-frequency patterns. `--view-override TABLE.VIEW=DEFINITION` is the always-works escape hatch for cases the translator can't handle. Deferred until operator demand surfaces a specific cross-engine view that hits the loud-failure at apply time.

**Gotchas.**
- Phase 2 MVP intentionally skips dependency-ordered refresh. Operators with nested matviews (matview A reads from matview B) should either pass `--matview` repeatedly with the right ordering, or invoke the subcommand twice (idempotent). A dedicated dependency-ordered refresh is a Phase 2.1 follow-on if real workloads surface the need; pg_depend has the data, just not wired up.
- Phase 3 SELECT translation is open-ended — a real SQL parser dependency would simplify but adds a heavy library; the existing hand-coded ADR-0016 approach stays preferable until rule count exceeds ~30 entries.
- Phase 1 deliberately punted: view definitions with explicit column lists `CREATE VIEW name (a, b, c) AS ...`, MySQL `CREATE ALGORITHM=UNDEFINED VIEW`, PG `WITH (security_invoker=true)`, and PG RULE-based pseudo-views. Each has different demand signals — pick what to surface based on real-world operator reports.

---

### 14. Backup chain retention, pruning, and compaction (GitHub #20)

**Why.** `sluice backup stream run` grows the chain forever — no built-in rotation, expiry, or compaction. Object-storage operators get a partial pass via bucket lifecycle policies, but **local-filesystem chains** (`--output-dir`, the air-gapped / compliance / SneakerNet use case) hit operationally-painful growth surprisingly quickly: a 3-min-rollover stream at ~43 ops/sec wrote 494 files / 132 MB in 7h 48m on the validation rig — extrapolating to ~40k inodes / ~12 GB / ~12 hour restore in a month and ~430k inodes / ~145 GB / ~7-day restore in a year. The bottleneck isn't disk bytes (compression helps); it's **inode count** (`find` / `ls` / `du` slow past ~50k subdirs) and **linear-in-chain-length restore time** (~2.5s per incremental on the rig; multi-day restores aren't a credible DR posture). See [GitHub issue #20](https://github.com/orware/sluice/issues/20) for full numbers + repro.

**What — four-chunk sequence:**

- **14a. `chain.json` catalog at chain root** — the keystone. Single file listing `[backup_id, parent_backup_id, created_at, end_position, bytes, file_path]` per manifest in chain order. Restore / verify read it once instead of `ListObjects` / `readdir`-ing `manifests/`. Atomic write-temp + rename on every rollover. Backwards-compat: missing-file fall-back to today's directory scan, so existing chains keep working without migration. Unblocks every subsequent chunk because they all need O(1) chain-content lookup. **SHIPPED v0.47.0.**

- **14c. `sluice backup prune --from-dir CHAIN [--keep-incrementals N | --keep-duration DUR]`** — one-shot cleanup for existing chains. Drops oldest-prefix incrementals; first kept incremental's manifest gets re-stitched (`ParentBackupID` → full's ID, `StartPosition` → full's `EndPosition`) so chain restore's parent-link walk + StartPosition validation pass on the re-stitched chain. Event windows in dropped incrementals are LOST from restore range (operator opts in explicitly). Refuses on chain.json absent / both flags / neither flag / structural-break catalogs. **SHIPPED v0.50.0.**

- **14b. Rotation.** **Phase 1 — rotation-EXIT thresholds — SHIPPED v0.51.0.** `--exit-after-age=DUR` and `--exit-after-chain-length=N` on `backup stream run`: at the configured chain age (from `chain.json`'s `CreatedAt`) or incremental count, commit the current rollover and exit cleanly; chain.json gets `RotatedAt` + `RotationReason` markers (`SucceededBy` reserved for Phase 2). Operator wraps the process in cron / systemd to restart at a fresh `--output-dir`. The correctness analysis (`docs/dev/notes/prep-backup-chain-rotation.md`, committed v0.50.0) shows rotation-EXIT has the same position-monotonic guarantee as inline rotation.
  **Phase 2 — `--retain-rotate-at=DUR` in-process inline rotation — PENDING (~v0.52.0+ when promoted).** When the threshold trips, the *same goroutine* opens a new snapshot stream, bulk-copies source state into a sibling chain root, writes the `SucceededBy` auto-stitch pointer, and resumes streaming — no operator wrapper. The hard part stays **snapshot/CDC overlap during rotation**: the new full's snapshot anchor MUST be ≥ the previous stream's last-committed incremental position. The `prefixedStore` wrapper + the three reserved chain.json fields shipped in v0.51.0 as scaffolding; the inline-rotation consumer is not yet built. `--retain-rotate-at` is the reserved Phase 2 flag name.

- **14d. `sluice backup compact --from-dir CHAIN --merge-window DUR`** — merge K consecutive incrementals into a single combined incremental with the same end-position but smaller file/inode footprint. **v1: naive concat** — read all change events in order, write to a single new `.jsonl.gz` chunk under a new `_changes/<merged-ts>/`, delete sources, tombstone merged manifests in `chain.json`. The new manifest gets the last source's end-position. Restore-side is no-change: merged manifests look like larger incrementals to the existing chain iterator. **~v0.52.0+** (after 14b lands).

- **14e. Smart compaction (same-row event collapsing)** — within the merge window, collapse INSERT→UPDATE→DELETE on the same PK into just the final state (or just the DELETE). Materially smaller for high-update workloads. Requires PK awareness + DDL handling across the window + a new ADR. Defer until naive concat ships and operators report on it. **No release target.**

**Gotchas.**

- **Tombstone semantics on object storage.** S3 / GCS / Azure lifecycle policies can race `sluice prune` — physical deletion isn't atomic with chain-state update. Use a `<chain>.tombstone` marker file (or `state: tombstoned` in `chain.json`) that restore/verify check first; physical-cleanup is a separate, idempotent step.
- **Crash-safety during compaction.** Mid-compact crash must leave the chain restorable. Write the new chunk + tombstoned chain.json atomically (write-temp + rename) BEFORE deleting source chunks; if crash hits between rename and delete, the source chunks become orphaned but the chain is still restorable from the new merged form.
- **Restore-test coverage.** Today's restore tests assume linear chain. Compacted chains (with tombstones referencing merged-out manifests) need explicit test coverage — both same-engine and cross-engine restore paths.
- **No effect on bucket-lifecycle operators.** This is a strict superset of today's behaviour; operators who already manage retention via S3 lifecycle policies see no behaviour change unless they opt in. The local-FS operator is the headline beneficiary.

**Sizing.** 14a is ~200-300 LOC + tests (one tight release). 14b + 14c together ~500-800 LOC + an overlap-correctness ADR for the rotate-at snapshot/CDC handoff. 14d ~400-600 LOC + tests; 14e is a much larger chunk (probably its own ADR + 800-1200 LOC). Doesn't compete with item 1 (KMS Phase 6.4) or the AIMD controller (GitHub #18 Phase 3) — different surfaces, different operators benefit.

---

### 15. PII redaction during logical replication and migration (GitHub #24)

**Why.** Common operator ask in the replication space: *"I want a copy of this production database, but without the PII."* Typical destinations: staging/dev environments seeded from production schema + realistic-shape data without exposing real users; analytics warehouses that aggregate but never identify individuals; cross-region / cross-tenancy moves where compliance (GDPR / CCPA / HIPAA) requires PII to stay in the source jurisdiction; vendor handoffs where a third-party data processor needs the schema + the data shape but not the PII. Today operators reach for separate tools (Tonic, Privacy Dynamics, Debezium SMTs, custom ETL); sluice could absorb the use case as a first-class feature since the IR-first row pipeline already passes every value through a typed transform stage. See [GitHub issue #24](https://github.com/orware/sluice/issues/24) for the full motivation + comparable-products analysis.

**What — four-phase sequence:**

- **15a. Phase 1 — simple strategies + framework. SHIPPED v0.53.0** (plus Phase 1.5 closure v0.54.0–v0.55.0: CDC apply-path redaction, YAML `redactions:` config, schema-preview annotation, backup-stream redaction; Bug 58 schema-namespace fix v0.54.1, Bug 59 kong-comma fix v0.56.1). `--redact TABLE.COLUMN=STRATEGY[:options]` with `null` / `static` / `hash:sha256` / `hash:hmac-sha256` / `truncate`; `internal/redact` package; audit log line. See "Recently landed: PII redaction track".

- **15b. Phase 2 — format-preserving + tokenize. SHIPPED v0.56.0 → v0.61.0.** Phase 2.a generic `mask:inner`/`mask:outer` + Luhn helper (v0.56.0); Phase 2.b country/format mask presets — ssn/pan/pan-relaxed/email (v0.57.0), ca-sin/uk-nin/iban/uuid (v0.58.0), Bug 60 `mask:uuid` preflight (v0.58.1); Phase 2.c replay-stable `randomize:*` — int/email/us-phone/uuid (v0.59.0, ADR-0039), ssn/pan/ca-sin/uk-nin/iban (v0.60.0); Phase 3 dictionary strategies `randomize:dict` / `tokenize:dict` + YAML `dictionaries:` (v0.61.0, ADR-0040). See "Recently landed: PII redaction track".

- **15c. Phase 3 — JSON-path redaction. PENDING (no release target).** `jsonpath` strategy with child redactions per JSON path: `paths: [$.payment_method: tokenize, $.last_visit: truncate_to_day]`. Requires an in-IR JSON walker that can rewrite nested values without re-encoding the whole document. Most invasive but smallest fraction of real-world asks (most PII is in dedicated columns, not nested JSON). Genuinely not yet shipped — v0.53.0's CHANGELOG explicitly defers it and no later release lands it. Demand-gated.

- **15d. Phase 4 — deterministic-tokenize keyset persistence. LANDED (v0.63.0; ADR-0041 Accepted 2026-05-15).** Persists the HMAC keyset across multiple sluice streams pulling from the same source so user `alice@example.com` becomes the same surrogate on streams to staging-1 AND staging-2. Shipped: `Keyset` type + loader, `--keyset-source=file:|env:|db:`, `key:` rule option, `sluice_keysets` DDL on PG + MySQL, startup audit-log line, preflight refusal. **Two deviations from the ADR draft (recorded verbatim in ADR-0041's "Decisions / deviations" section):** (D1) startup-snapshot only — the keyset is resolved once at process start and is immutable for the run; rotation takes effect on next restart only; no file-watch / db-poll hot-reload (deferred to Phase 4.5). (D2) clean break — the Phase 1 `--redact-key-source` flag and the hardcoded v0.61.0 `tokenize:dict` key were deleted with no back-compat shim; `hash:hmac-sha256` and `tokenize:dict` now REQUIRE `--keyset-source` (loud preflight refusal otherwise). Out of v1 scope: `sluice keyset rotate`/`list` CLI (manual SQL/YAML), KMS/Vault adapters, encryption-at-rest of the `bytes` column.

**Key invariants the design enforces (from the issue body):**

1. **Determinism per stream-id.** Two different sluice sync streams over the same source produce *consistent* redactions for the same input — same hash key, same tokenize seed. Otherwise CDC apply would produce row-version drift.
2. **Schema preview shows redactions.** `sluice schema preview` and `sluice schema diff` annotate redacted columns (`-- REDACTED via tokenize:email_format`) so operators see what they're agreeing to.
3. **Verify modes downgrade gracefully.** `sluice verify --depth=count` is unaffected (row counts unchanged). `sluice verify --depth=sample` automatically excludes redacted columns from the row hash (otherwise it'd always fail by design).
4. **Generated-column behavior unchanged.** Redactions apply at the reader's IR-emit step, BEFORE the target's `GENERATED ALWAYS AS` recomputes. So a generated column on the target derives from the redacted inputs naturally.
5. **Refuse to start without acknowledgment when no redactions declared.** Optional `--require-redactions` flag for the safety-conscious operator who wants the pipeline to refuse if they forgot to configure them.
6. **Audit logging.** Single INFO line at stream start summarizing how many columns are redacted (`columns_redacted=12 strategies=[...]`).

**Where redaction sits in the pipeline.** Bulk-copy reader → IR rows → redact → bulk-copy writer. CDC reader → decoded events → IR row → redact → applier. Migrate uses the same path. The redaction layer is a single typed function composed with the existing translation step; adds no new I/O.

**What it explicitly does NOT try to be (per the issue body).**

- Not a PII discovery scanner (column-name regex / heuristics). Operators declare what to redact; sluice applies it. Discovery is a separate problem.
- Not encryption at rest. Sluice runs in the operator's environment; the redacted output is plaintext on the destination. (Encryption during transport is TLS; encryption at rest is destination-side.)
- Not reversible. Hash + tokenize are one-way by design. Operators needing reversibility should add their own reversible-encryption middleware outside sluice.

**Gotchas.**

- **Determinism across stream restarts**: the HMAC keyset (for tokenize / hash strategies) must persist somewhere durable so a stream restart produces the same surrogate for the same input. Phase 1 can punt this by deriving the keyset from `--stream-id` + a static salt (stable across restarts of the same stream); Phase 4 lands the proper keyset persistence.
- **Backup chain integration** (cross-reference with item 14): redactions declared in YAML need to flow through `backup full` / `backup stream run` paths so the backup chain itself is PII-clean. Phase 1 lands sync-path redaction; backup-path follow-on is a small extension since both paths share the IR pipeline.
- **Cross-engine + redaction**: same-column redactions must work whether the target is same-engine or cross-engine. The redaction layer operates on IR values, which are engine-neutral; should compose cleanly with existing cross-engine translation.

**Sizing.** Phase 1 (the simple strategies + framework) is ~500-800 LOC + tests — IR-side `Redact()` function, YAML schema entries, four strategy implementations, CLI flag, `schema preview` annotation. Phase 2 adds ~400-600 LOC (format-preserving requires format-detection utility code). Phase 3 is ~600-900 LOC (in-IR JSON walker; non-trivial). Phase 4 is its own ADR + ~400-600 LOC (key-management + persistence).

**Sequencing.** Behind the backup-chain track (chunk 14 phases 1-3 close before phase 4 starts). Doesn't compete with #18 (AIMD) or #23 (silent-stall fix) — different surfaces, different operators benefit. Probably the most operator-facing new feature in the v0.55.0+ roadmap window.

---

### 16. Verbatim same-engine / backup extension-type passthrough (uncatalogued extensions)

**Design:** [ADR-0047](adr/adr-0047-verbatim-extension-passthrough.md) — **Accepted** (2026-05-16; `ir.VerbatimType`, implicit live determination + recorded backup capability marker, cross-engine stays loud-refuse). Implementation in progress, ships **v0.68.0**.

**Why.** ADR-0032's `pgExtensionCatalog` is an **enumerated allowlist** of 7 extensions (`vector`, `pg_trgm`, `hstore`, `citext`, `postgis`, `pgcrypto`, `uuid-ossp`). An uncatalogued extension type (`ltree`, `cube`, `timescaledb`, `pg_partman`, `age`, `h3`, in-house extensions) hits the `USER-DEFINED → enum/loud-failure` fallthrough in `lookupExtensionForType`, fired inside `ReadSchema` — so it refuses identically for **PG → PG sync/migrate AND for `backup full`/`backup stream run`**, and `--enable-pg-extension foo` for an uncatalogued `foo` is itself refused at `validateAndPreflightExtensions`. This is correct for cross-engine (no portable MySQL equivalent; loud-failure tenet) but a real, defensible gap for the paths that *provably do not need semantic understanding*: same-engine PG → PG and PG-backup → PG-restore only need to **carry the type faithfully**, not translate it. Operator payoff: "back up / replicate my PG database PG-to-PG even though it uses an extension sluice has never heard of," without a catalog PR per extension.

**What.** A new passthrough tier *below* the catalog — the rich catalog path (modifier/typmod fidelity, opclass round-trip, cross-engine translators, ADR-0044 default-expr gate) stays unchanged for the 7 catalogued extensions; this is the fallback for *uncatalogued* `USER-DEFINED` types only, and the cross-engine loud-refusal default is fully preserved. Mechanism: the schema reader captures the column's exact `pg_catalog.format_type(atttypid, atttypmod)` string and re-emits it **verbatim** (no typmod decode, no `emitColumn`); values round-trip via text I/O (the type's output/input functions; pgx text format). A new IR shape is needed — either an `ir.ExtensionType` variant carrying the verbatim `format_type` string with no `emitColumn` dispatch, or a distinct `ir.VerbatimType`. **Implicit vs explicit determination (the open design question):** mostly *implicit* — the orchestrator already knows source and target engine identity, so a live PG → PG run can enable the verbatim path automatically (both ends provably PG, no flag). The genuinely-needs-handling case is **backup**, because the restore target engine is unknown at backup time: a chunk written with verbatim extension types is **PG-restore-only**. The right mechanism there is *not* an operator opt-in flag but a **recorded capability marker** on the manifest/segment (e.g. `verbatim_extension_columns: [...]` or a segment-level "PG-only-restorable" flag), enforced *loudly at restore* against the actual target engine — consistent with the codebase's record-never-sniff / fail-loud-on-mismatch idioms (`DefaultExpression.Dialect` tags, recorded-never-sniffed codec, ADR-0046 per-segment codec). An explicit `--allow-verbatim-extension-passthrough` flag is only warranted as a conscious "I accept this backup is PG-restore-only" acknowledgement if implicit + recorded-marker proves insufficient. Needs an ADR (it's a new IR shape + a manifest capability field + a restore-time engine gate).

**Gotchas / open questions.**
- **Index AM / opclass for uncatalogued extensions.** Verbatim type passthrough covers the column type; an index `USING <unknown_am> (col <unknown_opclass>)` also needs verbatim carry. Reconcile with the **Bug 47 invariant**: sluice only populates `ir.IndexColumn.OperatorClass` for catalogued extension-owned opclasses, and the cross-engine refusal path treats non-empty `OperatorClass` as an honest "extension-owned" marker. Populating it verbatim for uncatalogued opclasses must keep that invariant true (it actually *helps* — a verbatim-passthrough backup correctly refuses cross-engine restore). Same-engine, verbatim-or-target-rejects is acceptable (the target PG's own parser is the loud failure).
- **Value fidelity.** Text I/O covers nearly all extension types; residual edges are binary-only types, arrays of extension types, composite/domain types wrapping extension types. Text representation + re-parse is the safe contract; note PG-version skew (an extension's text format is usually but not always version-stable — document the same-PG-major-version expectation).
- **Backup → cross-engine restore must be loud, not silent.** The recorded marker is load-bearing: a backup containing verbatim extension columns restored to MySQL must refuse with an operator-actionable message at restore preflight, never silently drop/mangle.
- **Determination layering is three-level:** (a) catalogued + enabled → rich path; (b) uncatalogued `USER-DEFINED` AND run is provably same-engine-PG (or backup-marked-PG-only) → verbatim path; (c) else → today's loud refusal. Keeps the cross-engine loud-failure default fully intact.

**Sizing.** ~400-700 LOC + an ADR (new IR shape, schema-reader `format_type` capture, verbatim emit, manifest capability field, restore-time engine gate, both same-engine-sync and backup/restore test coverage). Independent of the in-flight ADR-0046 lineage/rotation work and the v0.67.0 codec-default decision — sequence **after v0.67.0 ships** so it doesn't divert that release.

**Sequencing.** Demand-shaped but architecturally clean; lower priority than active release work. Pairs naturally with item 14 (backup chain) since the recorded-marker mechanism touches the manifest/segment metadata that the lineage model owns — best picked up once ADR-0046 has settled the segment metadata shape, so the capability field slots into the finalized lineage manifest rather than chain.json.

---

### Open bugs awaiting fix windows

Tracked in detail in `sluice-testing/BUG-CATALOG.md`; recap here for roadmap visibility:

- **Bug 17** (deferred, low priority) — `impact_items` cross-engine COALESCE handling in expression translator.
- **Bug 25** (deferred, low priority) — PG immutability of enum-typed STORED generated columns.
- **Bug 26** — PostGIS SRID dropped on schema translation. *Closed in v0.28.0 (ADR-0035).*
- **Bug 27** — VStream POINT bytes mis-parsed. *Closed in v0.28.0 (ADR-0035) at the unit-test layer; operator-run end-to-end verification via `psverify` follows the existing PS test pattern.*
- **Bug 41** — PG CDC decode crash on UUID columns. *Closed in v0.21.2.*
- **Bug 42** — cross-engine PG → MySQL `DEFAULT gen_random_uuid()` translator gap. *Closed in v0.23.1.* `pgToMySQLDefaultExpr` in `internal/engines/mysql/ddl_emit.go` carries `gen_random_uuid()` → `(UUID())` and `random()` → `(RAND())`.
- **Bug 44** — same-engine MySQL `DEFAULT (UUID())` / `(RAND())` lost outer parens. *Closed in v0.23.2.* `wrapMySQLExpressionDefault` helper in `internal/engines/mysql/ddl_emit.go` covers the function-call shape; bare `CURRENT_TIMESTAMP[(N)]` and already-wrapped forms pass through.
- **Bug 47** — `{}` → `[]` on MySQL targets when default JSON cast surfaces via cross-engine. *Closed in v0.29.1 (initial fix) + v0.29.1 v2 (proper upstream disambiguation).*
- **Bug 51** — PG `geography(POINT, srid)` widened to `geography(Geometry, srid)` due to mixed-case `geography_columns.type`. *Closed in v0.33.2.*
- **Bug 52** — PG `geometry(POINTZ, srid)` Z/M/ZM dimensional variants lost on emit. *Closed in v0.33.3 (partial in v0.33.2, full closure via `coord_dimension` capture in v0.33.3).*
- **Bug 53** — PostGIS `coord_dimension` not captured in schema reader. *Closed in v0.33.3 (same release as Bug 52 full closure).*

UUID PK support across cross-engine restore is fully landed: Bug 41 (CDC value decode) + Bug 42 (schema-side default translation, PG → MySQL) + Bug 44 (same-engine MySQL function-call default wrapping) + v0.11.3's Bugs 28/29 (the MySQL → PG direction). All four corners covered for modern PG schemas (Rails, Django, Hasura, Supabase).

### MySQL & PlanetScale parity tracker

Sluice's recent feature work has cadenced PG-first; MySQL/PlanetScale parity is intentional but tracked. One-stop summary of what's PG-only today and where the MySQL/PS follow-up lives:

| PG-shipped feature | MySQL parity status | Tracked at |
|---|---|---|
| Phase 6.1 passphrase encryption (v0.22.0) | Engine-neutral — applies to both already | n/a |
| Phase 6.2 AWS KMS encryption (v0.23.0) | Engine-neutral — applies to both already | n/a |
| Mid-stream live add-table (`--no-drain`, v0.24.0) | Both engines, v0.27.0 — MySQL via streamer-side filter-flip (ADR-0034) | "Recently landed: Mid-stream add-table — MySQL Phase 2" |
| Multi-source aggregation `--target-schema` (v0.25.0) | Deferred — DSN-choice workaround documented; per-table-rename flag if demand surfaces | "Multi-source aggregation — MySQL native parity" entry above |
| Phase 2 strict zero-loss correctness (PG, closed v0.32.0 via ADR-0036) | PG-specific (pgoutput decode-time publication semantics) — MySQL Phase 2 has its own correctness story | "Recently landed: Mid-stream live add-table — strict zero-loss correctness (v0.32.0)" |
| GEOMETRY / SPATIAL support (v0.28.0, ADR-0035) | Both engines — PG → MySQL geometry round-trip with SRID preserved; VStream POINT prefix stripped (Bug 27); cross-engine refusal lifted | "Recently landed: GEOMETRY / SPATIAL — PostGIS-aware translation" |
| `--type-override`, `--expr-override`, translator catalog | Both engines, both directions | n/a |

PlanetScale-specific tracking:

| PlanetScale concern | Status | Tracked at |
|---|---|---|
| Bug 27 VStream POINT bytes prefix | Closed in v0.28.0 (ADR-0035) at the unit-test layer; operator-run end-to-end verification via `psverify` build tag follows the existing PS test pattern | "Recently landed: GEOMETRY / SPATIAL — PostGIS-aware translation" |
| Mid-stream Phase 2.5 (VStream-specific add-table mechanism) | Demand-driven follow-on to v0.27.0's MySQL Phase 2; VStream is a separate code path from vanilla MySQL binlog | (no roadmap entry yet — track here when demand surfaces) |
| PlanetScale Postgres slot lifecycle (Patroni-managed; silent loss on failover without `Logical slot name` config) | Documented in `docs/postgres-source-prep.md`; not exercised in default CI | "PlanetScale MySQL+Vitess test-matrix expansion" entry above |
| Broader VStream coverage (cross-shard PK, geometry edge cases, slot recovery) | Operator-run via `psverify` build tag with `PLANETSCALE_CREDENTIALS.env`; not in default CI | "PlanetScale MySQL+Vitess test-matrix expansion" entry above |

### Bug 27 (closed in v0.28.0)

Closed by ADR-0035 — VStream's `query.Type_GEOMETRY` cell decoder now strips the 4-byte little-endian SRID prefix before delivering bytes downstream, matching the vanilla MySQL driver path. Operator-run end-to-end verification on a real PlanetScale source with a POINT column follows the existing `psverify` build-tag pattern (operators with PLANETSCALE_CREDENTIALS.env loaded can spot-check before each release; not in default CI). See [ADR-0035](adr/adr-0035-postgis-geometry-spatial-support.md).

---

## How to use this doc

When starting a new chunk in Claude Code:

1. Pick an item from "Next up". Earlier items have more context inheritance.
2. Open the relevant section in the prompt: *"Read CLAUDE.md and docs/dev/roadmap.md section 1 (Schema-change planning helper). Propose a design before writing code."*
3. Iterate on the plan.
4. Implement.
5. Update this file when the chunk lands — move the entry to "Recently landed" and trim it to one line.
