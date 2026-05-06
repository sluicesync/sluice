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

### Foundational ADRs (0001–0029)

IR-first, sealed interfaces, kong+koanf, three-phase apply, MySQL flavors, pgoutput, position persistence, go-mysql, Streamer-as-separate-orchestrator, idempotent applier semantics, SlotManager optional surface, pglogrepl bypass for FAILOVER, applier value-shaping with `CAST(? AS JSON)`, phase-aware error-hint registry, migration resume design, layered expression translation (extended in v0.8.0 with bool-idiom rewrites and v0.9.0 with index-expression and bool-sub-expression coverage), batched CDC apply, per-batch bulk-copy checkpointing, parallel within-table bulk copy, slot-ack-after-apply, publication scope by table, slot-missing fall-through (extended for MySQL in v0.6.0), `--reset-target-data`, `sluice schema preview`, graceful-drain `sync stop` (extended in v0.9.0 with `--wait`), LOAD DATA INFILE writer, source-tx-boundary CDC batching, memory-bounded streaming, `sluice schema diff`.

---

## Next up

v0.9.0 closed `sync stop --wait` (the operator-coordination half of "schema-change ergonomics"), audited TIMESTAMP precision (no gaps found), and landed the OSS-hygiene starter (`CONTRIBUTING.md` + release-notes template). v0.8.1's testing surfaced four follow-ups that landed in v0.9.0 too: index-expression translation, bool-returning sub-expressions in COALESCE, the auto-exclude reach into preview/diff, and Bug 23's enum-cast on the parens-form default. What's left is split between (a) low-priority items that have always been here and haven't surfaced as blockers, and (b) the heavier design-first items (mid-stream add-table, multi-source).

### 1. Schema-change planning helper

**Why.** v0.9.0 closed half of "schema-change ergonomics" with `sync stop --wait` — operators now have a clean drainage signal for ALTER coordination. The remaining thread: a **planning helper** that combines `schema diff` (current source vs current target) into an actionable workflow. Doc-only first; tooling later if it earns its complexity.

**What.** Today's flow is: operator runs `schema diff`, reads the JSON / text output, hand-translates the suggested ALTER statements, runs them on source and target in the right order. The doc gap: there's no recipe linking these steps, no example runbook for ALTER-on-source-then-ALTER-on-target sequencing, no guidance on how `sync stop --wait` slots into the flow.

A short doc — `docs/operator/schema-change-runbook.md` — would close the documentation gap without growing CLI surface. Tooling (a `sluice schema apply-alters` flow) only earns its weight if operators report they're hand-coding the same ALTER scripts repeatedly.

**Gotchas.**
- Sluice is not a schema-migration tool. The doc should explicitly hand off to Atlas / sqitch / liquibase for cases where the operator wants version-controlled migrations; sluice's role is preserving streaming continuity, not replacing those tools.

---

### 2. CHARSET/COLLATION cross-engine translation

**Why.** v0.9.0 audited TIMESTAMP precision and found no gaps; CHARSET / COLLATION is the remaining cross-engine type-edge tracker. The `--ignore-charset-collation` flag on `schema diff` is plumbed but inert — the underlying comparison needs the IR to carry charset/collation on read for both engines.

**What.** Two-step:

1. **IR enrichment.** Add `Charset` and `Collation` fields to `ir.Column` (or a sub-struct shared with `ir.Table`). Both schema readers populate from `information_schema.columns` (MySQL) and `pg_collation` (Postgres). Empty when the engine doesn't expose the column-level value.
2. **Diff comparison.** `schema diff` compares the fields when both sides have them; surfaces drift; `--ignore-charset-collation` becomes load-bearing.

Cross-engine emit is out of scope for this chunk — operators with charset-sensitive workloads use `--type-override` today; bringing translation in-band requires an equivalence map similar to the default-value one (utf8mb4 ↔ UTF8, latin1 ↔ LATIN1, etc.) that would be its own ADR.

**Gotchas.**
- MySQL stores charset/collation per-column; Postgres tracks collation per-column but charset is database-wide. The IR field shape needs to cover both.
- Default charset on MySQL column comes from the table default, which comes from the database default — surface only the explicitly-set values to avoid false positives.

---

### 3. Auto-translate-and-create on mid-stream new tables

**Why.** ADR-0021 deliberately punted on mid-stream `CREATE TABLE`: a new table on a CDC source is currently silently dropped (defence-in-depth WARN, no schema propagation). The right path for "the developer ran a routine DDL" is to translate the new table's schema, create it on the target, and bring it into the publication scope — but doing this safely requires a mid-stream snapshot capture for the new table.

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

### 4. Multi-source aggregation

**Why.** Some users have multiple source DBs replicating into one target (sharded → consolidated, microservices → analytics warehouse, etc.). Today each `sluice sync start` is a 1:1 stream. ADR-0024 flagged this as out-of-scope; deserves its own design pass.

**What.** Multi-source streams: a single target hosting N `sluice_cdc_state` rows (one per source), with the schema preview / migrate / sync paths able to plan against the union of source schemas. Stream IDs disambiguate; collision detection on table-name overlap is load-bearing.

**Gotchas.**
- Schema collisions (two sources with `users` tables): the operator picks via filter, prefix, or per-source schema. Probably need a `--target-schema-prefix` or similar.
- Position-token resolution: warm-resume per source, not per target. Existing `sluice_cdc_state` row shape already keys by `stream_id`; should extend cleanly.
- Operator UX: `sluice sync status` should aggregate across all streams when run against a multi-source target.

---

### 5. OSS-hygiene track (remaining items)

**Why.** v0.9.0 closed two of the OSS-hygiene starters (`CONTRIBUTING.md` release-process section + `docs/dev/release-template.md`). The remaining items round out the public-release-readiness story.

**What.** Three independent items, each landable on its own:

- **License-header sweep across `internal/`.** LICENSE file is in place at the repo root; per-file headers are not. Mechanical change with high diff churn (~50 files); best landed in a single doc-only commit so the diff is easy to skim and revert if convention changes.
- **`goreleaser` (or equivalent) for cross-platform binary builds.** Eight tagged releases; hand-rolled build artefacts are starting to be a chore. Gotchas: Windows code-signing, GitHub Actions secrets, the integration-test images on the release runner. Worth its own focused chunk with a real ADR addendum.
- **Public README pass.** Current README is reasonable but reads as project-internal. Audience the public README should target: an operator scanning for "does this fit my use case" in the first 30 seconds. Move the deeper architectural detail into `docs/architecture.md` (already exists); the README becomes a short pitch + quickstart + pointer to docs.

**Gotchas.** None for the license-header sweep or README pass; goreleaser carries the gotchas listed above and benefits from running through the whole release cycle once before being declared done.

---

## How to use this doc

When starting a new chunk in Claude Code:

1. Pick an item from "Next up". Earlier items have more context inheritance.
2. Open the relevant section in the prompt: *"Read CLAUDE.md and docs/dev/roadmap.md section 1 (Schema-change planning helper). Propose a design before writing code."*
3. Iterate on the plan.
4. Implement.
5. Update this file when the chunk lands — move the entry to "Recently landed" and trim it to one line.
