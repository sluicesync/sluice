# Roadmap

Living list of work items beyond the current state, with enough context per entry that any one of them could be picked up as a self-contained chunk. Priority order is *suggested*, not strict.

Each entry has the same shape: a one-line summary, a *why* (the user-visible payoff), a *what* (load-bearing technical detail), and any *gotchas / open questions* known going in.

---

## Recently landed

For continuity when a chunk references "the previous work":

### Mid-stream add-table — Phase 2 (live add, no drain) — PG

- **`sluice schema add-table TABLE --stream-id ID --no-drain`** — runs add-table against an actively-streaming sync without first running `sync stop --wait`. Strategy C variant (c) from the proto-ADR: single slot, publication-add-then-snapshot ordering. PG-only in this release; MySQL sources still require the drained workflow (separate chunk: streamer-side filter-flip mechanism). See [`adr-0030-mid-stream-live-add-table.md`](adr/adr-0030-mid-stream-live-add-table.md).
- **New surfaces.** Pipeline-side optional interfaces `slotPositionReader` / `snapshotLSNExtractor` / `lsnComparer` so engines opt into the live-mode invariant check structurally. `Postgres.Engine.ReadSlotPosition` reads `confirmed_flush_lsn` from `pg_replication_slots`; `Postgres.Engine.ExtractSnapshotLSN` and `Postgres.Engine.CompareLSN` close the loop without leaking the engine's position envelope into the orchestrator.
- **Operator safeguards.** Live mode refuses on engines without `publicationAdder` (clear PG-only error directing at the drained flow). Captures the active stream's slot `confirmed_flush_lsn` before publication-add; verifies snapshot-LSN ≥ slot-LSN after the snapshot opens — refuses loudly if the invariant fails (would silently drop events on the new table; ADR-0030 explains why standard ordering can't trip this in practice but the check pins the invariant against future regressions).

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

What remains are the **harder-frontier items** that have always been here as design-first work plus the now-front-of-queue Phase 6 (KMS encryption, the only piece of the logical-backups track still pending).

### 1. Logical backups Phase 6 — KMS-backed at-rest encryption

**Why.** v0.21.x ships chunked + manifested + chain-restorable backups, but **chunks land in cloud storage as plaintext** (correctly disclosed in v0.16.0 / v0.17.2's release notes — sluice doesn't currently encrypt at-rest; operators rely on bucket-level SSE / filesystem-level FDE). Phase 6 closes that with sluice-managed client-side AES-256-GCM, key material delivered via cloud KMS. Threat model: operators who can't trust the storage provider's encryption alone (compliance posture, regulated data, BYOK requirements) get end-to-end encryption that survives both the cloud provider and any intermediate storage handoff (SneakerNet, archival migrations, etc.).

**Sub-phasing.**

- **6.1 — Passphrase mode (shipped in v0.22.0).** No cloud dependency. AES-256-GCM bulk cipher, Argon2id KEK derivation from operator-supplied passphrase + per-chain salt. Per-chain CEK by default (single Argon2id derive per restore); `--encrypt-mode=per-chunk` opt-in for defense-in-depth. CLI: `--encrypt`, `--encryption-passphrase`, `--encryption-passphrase-env`, `--encryption-passphrase-file` on backup full / incremental / stream / restore / sync from-backup. Mixed-mode chains refused; backup verify (sha256-only) doesn't need keys.
- **6.2 — AWS KMS (shipped in v0.23.0).** `--kms-key-arn` + `--kms-region` flags; AES-256-GCM bulk cipher unchanged; AWS KMS replaces Argon2id for KEK derivation. Per-chain CEK cache keeps KMS Decrypt calls at one per restore regardless of chunk count. Construction-time DescribeKey preflight surfaces auth/region/key-not-found issues before the backup starts. Operator-actionable error translation for AccessDenied / NotFound / InvalidState / Disabled / IncorrectKey. `kmsverify` build-tag harness skeleton ships for operator-run localstack verification (main `integration` tag stays focused on real-database scenarios).
- **6.3 — GCP Cloud KMS + Azure Key Vault (next).** Same shape as 6.2; per-cloud SDK wrappers behind the `EnvelopeEncryption` interface. `--kms-key-resource=projects/.../cryptoKeys/...` and `--azure-key-vault-id=https://....vault.azure.net/keys/...`.

**Gotchas.** KMS API call cost + latency on restore — multi-table chains with many chunks could rack up KMS request charges. Mitigated in 6.1's design via per-chain CEK cache (single KEK derive / KMS unwrap at restore start; subsequent chunks reuse the unwrapped CEK in-memory for the duration of the restore process). Key rotation: operator rotates the KMS root key; existing manifests reference the old key version; KMS handles the version-chain transparently. Passphrase rotation in 6.1 is a fresh-chain workflow (start a new chain with the new passphrase). Re-encryption-at-rest for already-written chunks is out of scope.

Remaining size ~600-1000 LOC for 6.2 + 6.3 + a key-management ADR.

---

### 2. Apache Arrow integration (deferred — research-doc updated)

**Why.** Original conditional-yes recommendation in `design-apache-arrow-integration.md` was gated on logical-backup picking Parquet. **Phase 1 logical backups shipped JSON-Lines + gzip instead** (`internal/pipeline/backup_chunk.go:8-32`); the gate dissolved. Updated research lives in [`docs/research/apache-arrow-findings.md`](../research/apache-arrow-findings.md): Shape A (Arrow as IR row representation) is recommended **defer** — sizable refactor, zero current operator demand. The narrower analytics-export angle moved to its own research item — see item 9 below.

**What.** No code chunk. Revisit when an operator with a real Parquet/columnar requirement surfaces, or when item 9's research doc names a winning surface.

**Gotchas (preserved for the eventual revisit).** Silent type drift at Decimal-256 / Time-out-of-range / UUID-string-parse / Arrow-null-vs-empty-string boundaries — each needs explicit loud-failure branches per the loud-failure tenet. ~2× binary growth under the build tag (mitigated by keeping default untagged build slim). Shape C (Arrow as in-flight row-pipeline format) explicitly rejected as IR-tenet-violating.

---

### 3. Mid-stream live add-table — strict zero-loss correctness (PG Phase 2 follow-up)

**Why.** v0.24.0 shipped Phase 2 live add-table (`--no-drain`, PG-only) with a documented best-effort gap: events on the new table inserted during the brief publication-add window may not be delivered (~1–3 events lost per sub-second window in CI's under-load test). pgoutput evaluates publication membership per WAL record at decode time; events that the slot decoded-and-filtered BEFORE publication-add took effect are gone — the slot doesn't redeliver retroactively. Snapshot rows + post-publication-add events are delivered exactly-once-effectively (proven by integration tests `TestAddTable_LiveMode_PG` + the post-add sentinel pin in the under-load test); the gap is the in-flight events during the add window itself. ADR-0030's "What could go wrong" section documents the limitation.

**What.** Two viable paths:

- **Path A — Slot-pause.** Temporarily stop the streamer's apply ack so the main slot retains WAL across the publication-add boundary, then re-decode the retained WAL with the updated publication state. Lighter implementation than Path B but requires plumbing into the streamer's apply loop (a "pause ACK at LSN, do publication-add, resume ACK from before-add LSN" hook). Estimated ~400-700 LOC.

- **Path B — Strategy B dual-slot.** As originally sketched in `docs/dev/design-mid-stream-add-table.md`'s "Strategy B" section: open a second slot at the publication-add LSN that delivers events on the new table from that point forward; main slot continues without publication-scope change. Heavier (LSN race between the two slots' progress is the price ADR-0030 wanted to avoid). Estimated ~1500-2000 LOC.

**Gotchas.** Path A's slot-pause needs careful interaction with the existing slot-ack-after-apply mechanism (Bug 15, ADR-0020) and the LSN tracker. Path B's dual-slot needs deterministic test fixtures for the LSN race — historically the reason ADR-0030 picked Strategy C in the first place. Pick based on which path is easier to test deterministically; v0.24.0's under-load test will prove the fix in either case.

**Operational impact.** Low-frequency for typical operators (most live-adds happen on tables that aren't being actively written to at the moment of the add). High-impact for the specific pattern of "rolling out a new table that's already taking writes from the application before the bulk-copy completes." The drained add-table flow remains the zero-loss default for operators who care.

---

### 4. Mid-stream add-table — MySQL Phase 2 (live add for binlog sources)

**Why.** PG Phase 2 (`--no-drain`) shipped — see "Recently landed" + ADR-0030. The PG mechanism (publication-add-then-snapshot) doesn't translate to MySQL: binlog auto-includes every table, so the gate is in the streamer's table filter (`--include-table`/`--exclude-table`), not in a publication.

**What.** Either:
- A streamer-side filter-flip: tell the running streamer "now also include table foo" via the control table or signal channel, extending `applyTableFilter`'s scope mid-run; the bulk-copy of the new table runs alongside via the same temp-snapshot pattern Phase 1 uses for MySQL.
- Or accept the no-filter default (already permissive on MySQL): in which case the only mid-stream gap is the schema cache, and the WARN-and-skip-then-pick-up pattern from ADR-0021 might suffice.

**Gotchas.**
- MySQL binlog is positionally addressed; there's no per-event publication membership check. Events on the new table arrive whether or not sluice is "ready" for them. Filter-flip + idempotent applier handles overlap the same way ADR-0010 does for PG.
- Concurrent add-tables (two operators racing) for different tables: filter-flip should serialise on the control-table row.
- VStream (PlanetScale) is a separate code path; treat as Phase 2.5 if real demand surfaces.

---

### 5. Multi-source aggregation

**Why.** Some users have multiple source DBs replicating into one target (sharded → consolidated, microservices → analytics warehouse, etc.). Today each `sluice sync start` is a 1:1 stream. ADR-0024 flagged this as out-of-scope; proto-ADR exists at [`design-multi-source-aggregation.md`](design-multi-source-aggregation.md).

**What.** Multi-source streams: a single target hosting N `sluice_cdc_state` rows (one per source), with the schema preview / migrate / sync paths able to plan against the union of source schemas. Stream IDs disambiguate; collision detection on table-name overlap is load-bearing.

**Gotchas.**
- Schema collisions (two sources with `users` tables): the operator picks via filter, prefix, or per-source schema. Probably need a `--target-schema-prefix` or similar.
- Position-token resolution: warm-resume per source, not per target. Existing `sluice_cdc_state` row shape already keys by `stream_id`; should extend cleanly.
- Operator UX: `sluice sync status` should aggregate across all streams when run against a multi-source target.

---

### 6. Translator catalog continuation (lowest priority)

**Why.** `docs/dev/translator-coverage.md` is the research catalog (30 candidate rules). v0.11.0/v0.11.1/v0.11.3 closed the highest-leverage 16 rules. The remaining medium-priority entries are mostly passthroughs (NULLIF, GREATEST/LEAST), version-gated (JSON_OBJECT for PG 16+), or have semantic gotchas (REGEXP_LIKE dialect divergence). Diminishing returns now that the highest-frequency-in-DDL rules are in.

**What.** Pick the next batch from the catalog when a real-world test cycle surfaces a specific gap, OR opportunistically when a related code path is being touched. Reactive-driven rather than proactive-batch from this point on.

**Gotchas.** See ADR-0016's "v0.11.x batch caveats" sections for the per-rule notes already accumulated.

---

### 7. GEOMETRY / SPATIAL support — PostGIS-aware translation

**Why.** Today sluice declines `GEOMETRY` emission in the PG writer with a loud error (`GEOMETRY requires PostGIS; not supported in this writer version`). Workloads with spatial columns — common in mapping / IoT / location-aware SaaS, often via Rails/Django/Hasura schemas — can't migrate at all. Two open bugs sit behind this entry: **Bug 26** (PostGIS SRID dropped on schema translation) and **Bug 27** (VStream POINT bytes mis-parsed). Prep doc with proposed shape: [`notes/prep-postgis-geometry.md`](notes/prep-postgis-geometry.md).

**What.** Three sub-phases, each independently shippable:

- **Phase A — PostGIS detection + PG writer emit.** PG engine declares PostGIS as an optional capability; init-time `SELECT 1 FROM pg_extension WHERE extname = 'postgis'` populates the flag. When PostGIS is present, writer accepts `ir.Geometry` columns and emits `geometry(<subtype>, <srid>)`. WKB on the source converts to EWKB on the target (preserves SRID). Closes **Bug 26**'s PG side.
- **Phase B — VStream POINT bytes prefix handling.** VStream POINT bytes have a 4-byte little-endian SRID prefix that today trips the WKB byte-order-flag check. Strip the prefix, capture SRID, parse remainder as standard WKB. Closes **Bug 27**.
- **Phase C — Cross-engine integration tests.** MySQL POINT/POLYGON → PG geometry round-trip; PG geometry → MySQL POINT round-trip. Add `integration postgis` build tag for tests that need a PG container with the PostGIS image (`postgis/postgis:16-3.5`).

**Gotchas.** PostGIS-absent path stays loud-failure (don't silently downgrade to `bytea`). SRID auth-table differences (PG's `spatial_ref_sys` vs MySQL's hard-coded SRIDs) — start with the EPSG common subset, surface unrecognized SRIDs as a loud error. PostGIS image is large (~500MB layer); add it to the integration job's pre-pull list and consider whether to split into a separate CI job to keep the main `integration` job's runtime budget intact.

Estimated ~600-1000 LOC including tests + image-pull CI changes.

---

### 8. Backup chunk compression investigation

**Why.** Phase 1 logical backups ship gzip-compressed JSON Lines (`internal/pipeline/backup_chunk.go:8-32`). The Phase 1 design doc proposed zstd at level 3; gzip shipped because it's stdlib and Phase 1 prioritised correctness. There's no documented benchmark of compression ratio or throughput vs alternatives — operators reasoning about S3 storage cost or backup-window time-to-disk are guessing.

**What.** A focused benchmark + decision doc:
- **Library candidates**: stdlib `compress/gzip`, `github.com/klauspost/compress/zstd`, `github.com/klauspost/compress/gzip` (drop-in faster gzip), `github.com/klauspost/compress/snappy`. The [klauspost/compress](https://github.com/klauspost/compress) package is the standard Go recommendation for high-throughput compression and has a permissive license (BSD-3) compatible with Apache-2.0.
- **Test corpora**: text-heavy (varchar columns), numeric-heavy (DECIMAL/INT), binary-heavy (bytea/blob, base64 in JSON envelope), JSON-mixed (representative of typical OLTP table). Sample size: ~1M rows per corpus, drawn from existing integration test data + a synthetic generator.
- **Metrics**: compressed size, encode CPU time, decode CPU time, peak memory. Cross-tab with chunk size to pick the operating point.
- **Format-version implications**: chunk format header carries a version int (`{"_h":1,...}`); a compression swap needs version=2 + a backward-compat reader path. Worth weighing whether the zstd ratio improvement justifies the format-version bump or whether to ship as `--compression=<algo>` flag with gzip default.

Output is `docs/dev/notes/compression-benchmark.md` (data + recommendation) + a small benchmark harness under `internal/pipeline/internal/compressbench/` (build-tagged so it doesn't bloat default builds). Estimated ~200-400 LOC for the harness + benchmark.

---

### 9. Analytics-friendly source — research doc (Parquet export + DuckDB + Arrow Flight)

**Why.** Operators running OLTP databases increasingly want the migration tool to also be the bridge to their analytics stack. Three orthogonal ideas surfaced in conversation that share an underlying theme: sluice as the data-out point for analytics-friendly consumption. Replaces the deferred Shape A path from item 2 with a narrower, more demand-driven framing.

**What** (research-only — no code yet). A `docs/research/sluice-as-analytics-source.md` covering:

- **Operator personas.** OLTP-only operator (no analytics need); OLTP + occasional ad-hoc analytics (DuckDB power user); OLTP + warehouse pipeline (Snowflake/BigQuery/Redshift target); analytics-first operator with sluice as the lakehouse-feed source. Each has different demand for which surface.
- **Surface candidates** (not mutually exclusive):
  1. **`sluice backup export-as-parquet` one-shot transcode.** Reads existing JSON-Lines chunks, emits Parquet alongside (or to a separate cloud bucket). Read-side semantics stay JSON-Lines (round-trip into MySQL/PG keeps the existing path); Parquet is exit-only, so the type-mapping problem becomes "best-effort columnar" instead of "lossless restore." Cheaper dep weight than Shape A from `design-apache-arrow-integration.md`. Library candidate: `parquet-go/parquet-go` (lighter than full `apache/arrow-go/v18` if Arrow's broader surface isn't needed).
  2. **DuckDB integration on the consumer side.** DuckDB reads Parquet, JSON, and CSV directly; could ship a `sluice backup query --duckdb` subcommand that boots an embedded DuckDB engine pointed at the chunk store. Or simpler: document the recipe (`SELECT * FROM read_json_auto('s3://.../chunks/*.jsonl.gz')`) and let operators wire it themselves.
  3. **Apache Arrow Flight as a high-throughput transport.** [Arrow Flight](https://arrow.apache.org/blog/2019/10/13/introducing-arrow-flight/) is a gRPC-based protocol for sending large Arrow-encoded datasets between systems with parallel-stream + columnar-batch semantics. Two roles sluice could play: (a) **Flight server** — operators run sluice, point a Flight client at it, sluice streams CDC + bulk-copy data via Arrow batches; (b) **Flight client** — sluice fetches from a Flight-speaking source (some warehouses already speak Flight). Worth mapping against the existing `RowReader` / `RowWriter` interfaces to see how much grafting fits, and whether Flight's parallel-stream semantics align with sluice's per-table chunk model.
- **Worked example** for each: an end-to-end "operator wants X" scenario, with what sluice would emit and how the downstream consumer would consume it. Concrete demos beat speculative API design.

**Gotchas.** Apache Arrow has the same dep-weight concern that pushed Shape A to defer (see `docs/research/apache-arrow-findings.md`). Flight specifically pulls in `apache/arrow-go` + a gRPC server runtime; non-trivial. The research doc should make explicit which surface (export-as-parquet vs DuckDB recipe vs Flight) has the cheapest dep cost vs operator value. Recommendation: rank the three surfaces by (dep weight × operator persona breadth) and only promote one to a code chunk.

Output: `docs/research/sluice-as-analytics-source.md` (operator personas + surface analysis + worked examples + dep-cost matrix). Estimated 1-2 days research; no code chunk until the doc names a winner.

---

### 10. View support Phase 2/3 — materialized-view refresh + cross-engine view-body translation

**Why.** Phase 1 (schema-only round-trip + dependency-ordered apply + diff/preview integration + CLI filters) shipped. The two large open frontiers remain:

- **Phase 2 — materialized-view CDC refresh.** Phase 1 emits `WITH DATA` so cold-start populates the matview from the just-loaded tables. But matviews don't auto-update on CDC traffic — operators with `REFRESH MATERIALIZED VIEW` requirements need either a sluice-managed periodic refresh (cron-cadence, configurable per-matview) or a hook to integrate with their existing scheduler. Open question: does sluice ship a refresh loop in `sync start`, surface a `sluice matview refresh` subcommand, or just document the pg_cron pattern? Operator demand will pick.

- **Phase 3 — cross-engine view-body translation.** Phase 1 emits the source-dialect SELECT verbatim on cross-engine pairs; non-portable definitions surface as a target-side parse rejection at apply time (loud-failure tenet). The right Phase 3 path is to extend ADR-0016's expression translator to a SELECT grammar — a strict subset (column refs, function calls, JOIN/WHERE/GROUP BY) that covers the high-frequency patterns. `--view-override TABLE.VIEW=DEFINITION` is the always-works escape hatch for cases the translator can't handle.

**Gotchas.**
- Phase 2 MVP could be `sluice matview refresh --target ...` as a one-shot subcommand, deferring the running-loop integration to a later phase if operator demand picks "manual cron over my own scheduler" pattern over "sluice owns the loop."
- Phase 3 SELECT translation is open-ended — a real SQL parser dependency would simplify but adds a heavy library; the existing hand-coded ADR-0016 approach stays preferable until rule count exceeds ~30 entries.
- Phase 1 deliberately punted: view definitions with explicit column lists `CREATE VIEW name (a, b, c) AS ...`, MySQL `CREATE ALGORITHM=UNDEFINED VIEW`, PG `WITH (security_invoker=true)`, and PG RULE-based pseudo-views. Each has different demand signals — pick what to surface based on real-world operator reports.

---

### Open bugs awaiting fix windows

Tracked in detail in `sluice-testing/BUG-CATALOG.md`; recap here for roadmap visibility:

- **Bug 17** (deferred, low priority) — `impact_items` cross-engine COALESCE handling in expression translator.
- **Bug 25** (deferred, low priority) — PG immutability of enum-typed STORED generated columns.
- **Bug 26** — PostGIS SRID dropped on schema translation. *Picked up by item 6 (GEOMETRY/SPATIAL support) Phase A.*
- **Bug 27** (deferred, see below) — VStream POINT bytes mis-parsed. *Picked up by item 6 Phase B.*
- **Bug 42** (open as of v0.21.2, parallel to Bug 41) — cross-engine restore of `DEFAULT gen_random_uuid()` columns fails MySQL Error 1064. Root cause: `RetargetForEngine` rewrites `Column.Type` (UUID → CHAR(36)) but doesn't rewrite `Column.DefaultValue` of kind `DefaultExpression`; PG's `gen_random_uuid()` lands verbatim in MySQL CREATE TABLE. Mirrors v0.11.3's Bugs 28/29 in the opposite direction. Suggested fix: extend ADR-0016's expression-translator catalog to cover PG → MySQL UUID defaults (`gen_random_uuid()` → `(UUID())`). Estimated ~50-150 LOC.

Together, **Bug 41 (closed v0.21.2) + Bug 42** cover "first-class UUID support in cross-engine restore" — Bug 41 fixed the CDC value-decode side; Bug 42 fixes the schema-side default-translation gap. Worth bundling soon since UUID PKs are pervasive in modern PG schemas (Rails, Django, Hasura, Supabase patterns).

### Bug 27 (deferred, parked here)

**Why.** Deferred from v0.10.3 pending VStream test infrastructure (`integration vstream` build tag). VStream POINT bytes are mis-parsed because VStream doesn't strip MySQL's internal 4-byte SRID prefix before the OGC WKB; sluice's WKB decoder reads the SRID's low byte as the byte-order flag and fails. Only affects the VStream protocol; vanilla MySQL protocol path strips the prefix correctly.

**What.** Add a 4-byte SRID prefix detection + strip in the VStream-specific WKB decoder path. Gated by either a fixture-based test (cheap) or a real PlanetScale-Vitess integration test (`psverify` build tag).

**Gotchas.** Need to confirm the prefix is always present in VStream POINT bytes (vs. only when SRID != 0). Pre-VStream-fix verification: real PlanetScale source with PostGIS geometry column, sync to PG target.

---

## How to use this doc

When starting a new chunk in Claude Code:

1. Pick an item from "Next up". Earlier items have more context inheritance.
2. Open the relevant section in the prompt: *"Read CLAUDE.md and docs/dev/roadmap.md section 1 (Schema-change planning helper). Propose a design before writing code."*
3. Iterate on the plan.
4. Implement.
5. Update this file when the chunk lands — move the entry to "Recently landed" and trim it to one line.
