# Roadmap

Living list of work items beyond the current state, with enough context per entry that any one of them could be picked up as a self-contained chunk. Priority order is *suggested*, not strict.

Each entry has the same shape: a one-line summary, a *why* (the user-visible payoff), a *what* (load-bearing technical detail), and any *gotchas / open questions* known going in.

---

## Recently landed

For continuity when a chunk references "the previous work":

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

### Foundational ADRs (0001–0029)

IR-first, sealed interfaces, kong+koanf, three-phase apply, MySQL flavors, pgoutput, position persistence, go-mysql, Streamer-as-separate-orchestrator, idempotent applier semantics, SlotManager optional surface, pglogrepl bypass for FAILOVER, applier value-shaping with `CAST(? AS JSON)`, phase-aware error-hint registry, migration resume design, layered expression translation (extended in v0.8.0 with bool-idiom rewrites and v0.9.0 with index-expression and bool-sub-expression coverage), batched CDC apply, per-batch bulk-copy checkpointing, parallel within-table bulk copy, slot-ack-after-apply, publication scope by table, slot-missing fall-through (extended for MySQL in v0.6.0), `--reset-target-data`, `sluice schema preview`, graceful-drain `sync stop` (extended in v0.9.0 with `--wait`), LOAD DATA INFILE writer, source-tx-boundary CDC batching, memory-bounded streaming, `sluice schema diff`.

---

## Next up

The v0.10.x cycle closed reactive-bug loops on the v0.9.x translator gaps and shipped the `--expr-override` escape hatch. The v0.11.x cycle landed two batches of proactive translator catalog work (16 rules) plus three real-world bug fixes from the autonomous test-cycle loop. OSS-hygiene track is closed. Four new proto-ADRs were written 2026-05-07 to capture the design space for the user's "100% confidence that all data has been copied + synced" goal — `sluice verify`, sync-health monitoring, logical backups, and Apache Arrow integration. The roadmap reorders to put those at the top.

The remaining work splits into three buckets: (a) the "100% confidence" features — verify, sync-health, logical backups, Arrow — that directly serve the user's overarching goal; (b) the heavier design-first items still on deck — mid-stream add-table, multi-source aggregation; (c) translator-catalog continuation as lower-priority polish.

### 1. `sluice verify` — data-integrity verification command

**Why.** Operators today have `sluice schema diff` (structural) but no positive-confirmation surface for *data*. This is the most direct delivery on the "100% confidence" goal — operators can spend more or less verification time depending on their risk tolerance. Closes the no-Fivetran-silent-stop pain shape on the data-integrity side. See [`design-sluice-verify.md`](design-sluice-verify.md).

**What.** New CLI command with three depth modes:
- `--depth count` — row counts per table; cheapest probe.
- `--depth sample` — counts + N random PK-range row content hashes (default; ~99% confidence of detecting 5%+ corruption).
- `--depth full` — every row's content hash + bisection on mismatch.

New `ir.Verifier` optional interface (3 methods: `ExactRowCount`, `SampleRows`, `FullScanHash`); new `pipeline.Verifier` orchestrator mirrors `Differ`/`Migrator` shape. Sequencing: count-mode MVP (~1 week), then sample, then full. Total ~5 weeks for full feature.

**Gotchas.** Cross-engine value comparison must use the same `RetargetForEngine` canonicalization `schema diff` already does. Generated columns + CHECK constraints excluded from row-hash. CDC-position-aware verification ("verify against the LSN target has caught up to") is open question #1.

---

### 2. Sync-health monitoring + alerting hooks

**Why.** Companion to `sluice verify`. Verify covers data-integrity; sync-health covers liveness/lag — the orthogonal half of the "100% confidence" goal. Operators today have `sync status` (snapshot when they look) but no continuous staleness measurement, stalled-stream detection, or push-based alerting integration. See [`design-sync-health-monitoring.md`](design-sync-health-monitoring.md).

**What.** Two surfaces:
- `sluice sync health` — one-shot probe with `--max-lag-seconds` / `--max-events` / `--max-stale-seconds` threshold flags + structured exit code. Cron-friendly.
- `sluice sync start --metrics-listen ADDR` — Prometheus `/metrics` endpoint exposing the same metric set. Off by default; opt-in.

Metric set covers position-derived (`sluice_lag_events`, `sluice_lag_seconds`), liveness (`sluice_seconds_since_last_event`, `sluice_streamer_state`), and throughput (events/bytes/batch-size). New `ir.HealthReporter` optional interface (2 methods); both engines implement straightforwardly. Sequencing: probe MVP (~2 weeks), Prometheus listener (~1 week), per-table + PG slot health (~1 week).

**Gotchas.** Distinguishing "quiet source" (low write rate) from "broken connection" is the load-bearing design question — see open question #3 in the proto-ADR. Per-table metric cardinality blows up Prometheus storage on many-table schemas; gated behind `--metrics-per-table` flag.

---

### 3. Logical backups (full + incremental, local + cloud)

**Why.** PlanetScale customers and self-hosted operators want backups they own and store wherever they want — outside the vendor's built-in physical-backup system. Sluice already has the building blocks (snapshot + CDC + position tracking + checkpointing). User explicitly raised this and confirmed local-storage is in scope alongside cloud. See [`design-logical-backups.md`](design-logical-backups.md).

**What.** New `sluice backup full` / `sluice backup incremental` / `sluice restore` CLI surface. Backup format is hybrid manifest+chunks (JSON manifest pointing at zstd-compressed IR-format or Parquet chunks). New `BackupStore` storage abstraction with local-FS as a first-class peer to S3/GCS/Azure/B2. Per-chunk SHA-256 + per-table row-count verification on restore (reuses the `Verifier` infra from #1).

**Phase 1 MVP**: `sluice backup full` to local-FS only, IR-format chunks, manifest with per-chunk checksums, restore tooling that produces byte-perfect target. ~2-3 weeks. Cloud backends (Phase 2), incremental (Phase 3), encryption (Phase 6) follow.

**Gotchas.** Public format ownership — manifest becomes a forward-compat contract sluice carries forever, so v1 needs to bake on a stable IR. Scope creep risk: backups pull in encryption / KMS / retention / lifecycle / PITR; MVP must aggressively say no.

**Convergence with #4 (Arrow)**: if Arrow Shape A ships, the chunk format can be Parquet for free, with broader interop. Combined Phase-1 (logical-backup local-FS + Arrow Parquet writer) is ~3-4 weeks vs. ~5 weeks if shipped serially.

---

### 4. Apache Arrow integration (conditional)

**Why.** User raised as exploratory research. Arrow opens up data-lake offload (Parquet files on cloud storage), zero-copy interop with DuckDB/Polars/Spark, and downstream-system-friendly formats. See [`design-apache-arrow-integration.md`](design-apache-arrow-integration.md).

**What.** Recommendation is **conditional yes — gated on logical-backup choice**. If logical-backup picks Parquet as its chunk format, ship Arrow Shape A (new `internal/engines/arrow/` writer behind an `arrow` build tag, Parquet-only, local-FS-only) together since they share most implementation. ~2 weeks for Shape A alone; ~3-4 weeks combined with logical-backup Phase 1.

**Gotchas.** Silent type drift at Decimal-256 / Time-out-of-range / UUID-string-parse / Arrow-null-vs-empty-string boundaries — each needs explicit loud-failure branches per the loud-failure tenet. ~2× binary growth under the build tag (mitigated by keeping default untagged build slim). Shape C (Arrow as in-flight row-pipeline format) explicitly rejected as IR-tenet-violating.

---

### 5. Auto-translate-and-create on mid-stream new tables

**Why.** ADR-0021 deliberately punted on mid-stream `CREATE TABLE`: a new table on a CDC source is currently silently dropped (defence-in-depth WARN, no schema propagation). The right path for "the developer ran a routine DDL" is to translate the new table's schema, create it on the target, and bring it into the publication scope — but doing this safely requires a mid-stream snapshot capture for the new table. Proto-ADR exists at [`design-mid-stream-add-table.md`](design-mid-stream-add-table.md).

**What.** New subcommand `sluice schema add-table SOURCE.NAME ...` (or a flag on `sync start` / `migrate`) that:
1. Reads the new table's source schema.
2. Runs translation + creates it on the target.
3. Captures a snapshot for that one table.
4. Bulk-copies the table's rows.
5. Adds it to the publication scope so future CDC events flow.

**Gotchas.**
- Mid-stream snapshot capture is the load-bearing tricky bit — needs to coordinate with the in-flight CDC stream so the new table's snapshot LSN is past whatever's already been processed.
- Operator confirmation prompt (mirroring `--reset-target-data`'s typed confirmation) since this is a schema mutation.
- Sluice is not a schema-migration tool; this feature is about preserving streaming continuity, not about replacing tools like Atlas / sqitch / liquibase.

---

### 6. Multi-source aggregation

**Why.** Some users have multiple source DBs replicating into one target (sharded → consolidated, microservices → analytics warehouse, etc.). Today each `sluice sync start` is a 1:1 stream. ADR-0024 flagged this as out-of-scope; proto-ADR exists at [`design-multi-source-aggregation.md`](design-multi-source-aggregation.md).

**What.** Multi-source streams: a single target hosting N `sluice_cdc_state` rows (one per source), with the schema preview / migrate / sync paths able to plan against the union of source schemas. Stream IDs disambiguate; collision detection on table-name overlap is load-bearing.

**Gotchas.**
- Schema collisions (two sources with `users` tables): the operator picks via filter, prefix, or per-source schema. Probably need a `--target-schema-prefix` or similar.
- Position-token resolution: warm-resume per source, not per target. Existing `sluice_cdc_state` row shape already keys by `stream_id`; should extend cleanly.
- Operator UX: `sluice sync status` should aggregate across all streams when run against a multi-source target.

---

### 7. Translator catalog continuation (lowest priority)

**Why.** `docs/dev/translator-coverage.md` is the research catalog (30 candidate rules). v0.11.0/v0.11.1/v0.11.3 closed the highest-leverage 16 rules. The remaining medium-priority entries are mostly passthroughs (NULLIF, GREATEST/LEAST), version-gated (JSON_OBJECT for PG 16+), or have semantic gotchas (REGEXP_LIKE dialect divergence). Diminishing returns now that the highest-frequency-in-DDL rules are in.

**What.** Pick the next batch from the catalog when a real-world test cycle surfaces a specific gap, OR opportunistically when a related code path is being touched. Reactive-driven rather than proactive-batch from this point on.

**Gotchas.** See ADR-0016's "v0.11.x batch caveats" sections for the per-rule notes already accumulated.

---

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
