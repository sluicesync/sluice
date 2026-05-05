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

### Foundational ADRs (0001–0024)

IR-first, sealed interfaces, kong+koanf, three-phase apply, MySQL flavors, pgoutput, position persistence, go-mysql, Streamer-as-separate-orchestrator, idempotent applier semantics, SlotManager optional surface, pglogrepl bypass for FAILOVER, applier value-shaping with `CAST(? AS JSON)`, phase-aware error-hint registry, migration resume design, layered expression translation, batched CDC apply, per-batch bulk-copy checkpointing, parallel within-table bulk copy, slot-ack-after-apply, publication scope by table, slot-missing fall-through (extended for MySQL in v0.6.0), `--reset-target-data`, `sluice schema preview`.

---

## Next up

v0.6.0 testing surfaced two reliability follow-ups (Bug 12 still open + Bug 15 CLI path partial-fix) that take priority over the performance/ergonomics tracks. After those, multi-TB performance round 2 + ergonomics round 2: v0.5.0 closed the headline parallel-read gap; the remaining performance items are MySQL-side and CDC-batching tighter semantics. New items from recent ADRs add inspection/recovery tooling on top of the schema-preview foundation.

### 0a. Bug 15 CLI sync-stop drain ordering

**Why.** v0.5.0's slot-ack-after-apply work (ADR-0020) eliminated the post-restart wedge for Bug 15 and the integration test (`TestStreamer_PostgresToPostgres_StopRestartNoLoss`) passes. v0.6.0 testing confirmed: the **programmatic `RequestStop` path is fixed**, but the CLI `sync stop` control-table path still drops 8-21 in-flight rows under sustained-writer load. Cause: when `pollStopSignal` detects `stop_requested_at`, it calls `cancelApply()` which cancels `applyCtx`. The applier's `applyOneBatch` selects on `<-ctx.Done()` and rolls back the open transaction, dropping the partial batch. The CDC reader's buffered events also go nowhere — channel between reader and applier is abandoned.

**What.** Graceful-drain shutdown path: when stop is detected, the streamer should signal the CDC reader to stop accepting NEW events and close its output channel. The applier then sees `channelClosed` (which it already handles correctly: commits the partial batch, returns nil). The current cancel-applier-immediately shape is too aggressive — it's correct for Ctrl-C / external ctx cancel (where you want to stop now), but wrong for the "graceful drain" semantics CLI `sync stop` advertises.

Two implementation paths:
- **(a) New CDC-reader interface** `RequestStop(ctx context.Context)` that closes the output channel after the next event boundary. Streamer's stop-signal poll calls `RequestStop` first, then waits a bounded time for the apply loop to drain, then falls back to `cancelApply` as a hard timeout.
- **(b) Smaller change**: add a "stopping" flag on the streamer; the apply loop's batch-commit path checks it and returns nil instead of cancelling on ctx.Done. CDC reader stays as-is; the channel-close path triggers naturally from the outer ctx cancel (which now happens AFTER drain).

(a) is cleaner architecturally; (b) is smaller scope. ADR worth writing before code.

**Gotchas.**
- Drain timeout must be bounded — operator pressing Ctrl-C twice should escalate to hard cancel.
- The integration test should exercise the CLI `sync stop` path specifically (writes to control table, polls 5s, asserts no row gap). The existing Bug 15 test uses `RequestStop` directly and won't catch this regression class.

---

### 0b. Bug 12 — MySQL CDC localhost silence

**Why.** The v0.5.0 binlog heartbeat (10s) + 30s no-events watchdog was diagnostic-only — it logs a WARN line but doesn't surface as a stream error or attempt recovery. v0.6.0 real-world testing on Rancher Desktop / Windows confirms the underlying symptom is unchanged: localhost MySQL streams produce no row events even when the source is actively committing. Likely a TCP keepalive interaction between go-mysql's binlog connection and Docker's port-forwarding on Windows hosts.

**What.** Two-phase approach:
1. Promote the watchdog from "diagnostic WARN" to "actionable error". After the 30s grace period, return a sentinel error from the pump so the streamer can either retry-with-backoff or fail loudly. Today the silence is forever.
2. Add CDC-stream auto-reconnect with bounded retries. go-mysql's syncer has its own retry loop; the question is whether sluice should layer a reconnect-on-watchdog-fire on top.

**Gotchas.**
- Long-idle streams are legitimate (a dev DB with no traffic). The watchdog must distinguish "source is idle" (heartbeat events arriving but no rows) from "connection is dead" (no events at all, including heartbeats). Today the implementation already filters via `isRowRelevantEvent`, but the failure mode is "no events at all" — heartbeats don't reach us either, suggesting the connection itself is dead.
- Without a reliable repro environment, this is investigative work. The next step is probably adding fine-grained logging at the binlog-syncer level to pinpoint where the silence originates (read deadline hit? connection silently dropped? events being filtered upstream?).

**Open question for the next chunk**: is the Rancher Desktop / Windows testing environment available for repro work? If yes, this is implementable. If the environment isn't accessible to development, the chunk becomes "add fine-grained logging hooks so the next testing pass can pinpoint the failure point" rather than a fix.

---

### 1. MySQL `LOAD DATA INFILE` writer

**Why.** Vanilla MySQL bulk-load via `LOAD DATA LOCAL INFILE` is typically 5–10× faster than batched INSERT. The IR already declares `BulkLoadLoadDataInfile` as a capability but no engine implements it; vanilla MySQL falls through to `BulkLoadBatchedInsert`.

**What.** A new `RowWriter` strategy in `internal/engines/mysql/row_writer.go` selected by the engine's `Capabilities.BulkLoad` field. Streams rows as TSV/CSV over the local-infile protocol; bypasses per-row INSERT parsing. Fallback to BatchedInsert remains for PlanetScale (which doesn't allow `LOAD DATA LOCAL INFILE`).

**Gotchas.**
- The MySQL server has to be configured with `local_infile=ON` (default off in 8.0+). Document the prerequisite; surface a clear error when it's not enabled and fall back to batched INSERT with a warning.
- TSV escaping for binary columns is fiddly. The existing `prepareValue` helper handles per-type shaping; the LOAD DATA path needs an analogous serialiser.

---

### 2. Source-transaction-boundary aware CDC batching

**Why.** v0.4.x's batched applier flushes on row count + Truncate. PG `Begin`/`Commit` and MySQL `XID`/`GTID` events would let the applier preserve transactional cohesion: a 5000-row source transaction commits as one 5000-row target transaction instead of 50 batches of 100. Cleaner semantics; matches what operators expect when running CDC against an OLTP source.

**What.** Surface `Begin`/`Commit`-equivalent events in the IR (currently filtered before reaching the applier). The applier flushes its in-flight batch on `Commit` and starts a new one on `Begin`; the row-count cap remains as an upper bound for huge transactions.

**Gotchas.**
- Source transactions can span multiple seconds and many MB. The row-count cap is the safety valve.
- The IR-layer plumbing for these events is its own focused chunk. ADR-0017 calls this out as the deliberate v0.4.x scope cut.

---

### 3. Memory-bounded streaming

**Why.** For huge rows (TEXT columns with megabyte-scale content, BYTEA blobs) the channel + tee + writer-batch chain can hold significant buffered memory. Need to verify there's actual backpressure at high data volumes; today the channels are unbuffered but the writer's per-batch accumulation isn't bounded by bytes.

**What.** Audit the row-streaming path for memory accumulation. Add a `--max-buffer-bytes` knob (default ~64 MB) that bounds the writer's per-batch accumulation by total byte size in addition to row count. Bytes-aware chunking matches how pscale-cli batches by ~1 MB statement body rather than row count.

---

### 4. Network compression for cross-host copies

**Why.** Lower priority. Multi-TB at gigabit is hours of pure bandwidth time. Both pgx and the MySQL driver support compression but it's not configured in our DSNs. Probably mostly a documentation update — the connection-string knob exists on both sides.

**What.** Document the `compress=true` (MySQL DSN) and `sslmode=...` + `gssencmode=...` settings on PG DSNs as a tuning recommendation for cross-host copies. Only worth real implementation work if testing surfaces it as a specific bottleneck.

---

### 5. Other latent cross-engine type edges

Tracked here so they're not forgotten; each will surface once the relevant test exercises it.

- TIMESTAMP precision differences beyond the `CURRENT_TIMESTAMP` default fix (e.g. `TIMESTAMP(6)` ↔ `TIMESTAMPTZ` round-trips).
- CHARSET/COLLATION translation across dialects.

---

### 6. PG-native types auto-emit on MySQL targets

**Why.** v0.3.x onwards refuse `Inet`/`Cidr`/`Macaddr`/`Array` from PG sources with a clear error pointing at `--type-override`. v0.6.0's schema preview hints further surface the override path, but auto-emitting `VARCHAR(N) CHECK (regex)` matches the doc-promised behaviour and removes the toil for every PG→MySQL migration that touches these types.

**What.** Wire up the policy in `internal/engines/mysql/ddl_emit.go` — when an unsupported type arrives and a sensible auto-mapping exists, emit `VARCHAR(N)` plus a CHECK constraint with a per-type regex. CHECK regex registry: `Inet` → `^[0-9.]+$|^[0-9a-fA-F:]+$` (loose IPv4/IPv6), `Cidr` → above + `/[0-9]+$`, `Macaddr` → `^[0-9a-fA-F:.-]+$`. `--type-override` continues to work as the explicit override path.

**Gotchas.**
- MySQL's REGEXP support varies. Confirm against MySQL 8.0+ (sluice's baseline).
- Document the loosened validation (regex catches gross malformation but doesn't enforce all RFC details). Operators wanting strict format checking can use `--type-override` to a tighter shape.

---

### 7. Schema-diff against an existing target

**Why.** v0.6.0's `sluice schema preview` shows what sluice *would* produce on a fresh target. The complementary question — "does what's running on dst match what sluice would produce" — comes up during re-migration safety checks and post-deployment drift detection. ADR-0024's "out of scope" section flagged this as a separate ADR's worth of design.

**What.** New subcommand `sluice schema diff --against-target ...` (or extended flag on `schema preview`). Reads the current dest's `information_schema`, builds an IR-equivalent of the existing schema, runs the same translation pipeline, and emits a diff: tables/columns/types/indexes that differ, with categorisation (missing-on-target, extra-on-target, type-mismatch).

**Gotchas.**
- The reverse-engineering of the existing target schema needs to handle sluice's auto-emitted artefacts (e.g. PG's `CREATE TYPE … AS ENUM` shadows of MySQL ENUMs) so they don't surface as spurious "extra" objects.
- Output format mirrors `schema preview` (text + JSON + `--output FILE`).

---

### 8. Auto-translate-and-create on mid-stream new tables

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

### 9. Multi-source aggregation

**Why.** Some users have multiple source DBs replicating into one target (sharded → consolidated, microservices → analytics warehouse, etc.). Today each `sluice sync start` is a 1:1 stream. ADR-0024 flagged this as out-of-scope; deserves its own design pass.

**What.** Multi-source streams: a single target hosting N `sluice_cdc_state` rows (one per source), with the schema preview / migrate / sync paths able to plan against the union of source schemas. Stream IDs disambiguate; collision detection on table-name overlap is load-bearing.

**Gotchas.**
- Schema collisions (two sources with `users` tables): the operator picks via filter, prefix, or per-source schema. Probably need a `--target-schema-prefix` or similar.
- Position-token resolution: warm-resume per source, not per target. Existing `sluice_cdc_state` row shape already keys by `stream_id`; should extend cleanly.
- Operator UX: `sluice sync status` should aggregate across all streams when run against a multi-source target.

---

### 10. `migrate --dry-run` cross-reference to schema preview

**Why.** Small follow-up. Operators running `sluice migrate --dry-run` today see the orchestration plan but not the target DDL. ADR-0024 noted that `--dry-run` should print "for full target DDL inspection, run `sluice schema preview ...`" so operators land on the new tool when they're already in the dry-run mindset.

**What.** One-liner addition to the `--dry-run` output. ~10 LOC.

---

## How to use this doc

When starting a new chunk in Claude Code:

1. Pick an item from "Next up". Earlier items have more context inheritance.
2. Open the relevant section in the prompt: *"Read CLAUDE.md and docs/dev/roadmap.md section 1 (MySQL LOAD DATA INFILE writer). Propose a design before writing code."*
3. Iterate on the plan.
4. Implement.
5. Update this file when the chunk lands — move the entry to "Recently landed" and trim it to one line.
