# Roadmap

Living list of work items beyond the current state, with enough context per entry that any one of them could be picked up as a self-contained chunk. Priority order is *suggested*, not strict — earlier items unblock later ones in some cases (CDC needs the snapshot/CDC handoff to be the killer feature; the COPY writer is independent), but you can also skip down the list when something else is more interesting.

Each entry has the same shape: a one-line summary, a *why* (the user-visible payoff), a *what* (load-bearing technical detail), and any *gotchas / open questions* known going in.

---

## Recently landed

For continuity when a chunk references "the previous work":

- **Simple-mode orchestrator** (`internal/pipeline.Migrator`) — three-phase apply (tables-without-constraints → bulk row copy → indexes → constraints), wired into the kong `migrate` subcommand.
- **Integration test coverage**: MySQL→MySQL, PG→PG, MySQL→PG (cross-engine). Each lives in `internal/pipeline/migrate_*_integration_test.go` and is gated behind `//go:build integration`.
- **CI Integration job** — runs `go test -tags=integration -race -count=1 ./internal/...` on `ubuntu-latest` for every PR. Required by branch protection on `main`.
- **Bugfixes from cross-engine validation**: dropped the MySQL `ForeignKey.ReferencedSchema` leak that was qualifying Postgres FK DDL with the MySQL database name; added Postgres array-text-form parsing in `decodeArray` so pgx stdlib's `*any` array values decode correctly.

---

## Next up

### 1. Postgres→MySQL integration test

**Why.** Closes the four-direction story the README claims. Different bug surface than MySQL→PG: Postgres native types (JSONB, UUID, INET/CIDR, native arrays, custom enum types) have to land somewhere on the MySQL side without losing too much.

**What.** New file `internal/pipeline/migrate_pg_to_mysql_integration_test.go`. Boot both containers (the existing `startPostgres`/`startMySQL` helpers in the pipeline package give source/target DSN pairs — use the source DSN of PG, the target DSN of MySQL). Seed PG with a representative schema, run `Migrator`, assert MySQL target shape and rows.

**Gotchas / open questions.**
- Postgres `TEXT` has no fixed length; MySQL `TEXT` does. The MySQL writer needs a default sizing policy.
- Postgres `JSONB` → MySQL `JSON`. Value contract should already cover this (`[]byte`).
- Postgres native arrays (e.g. `int[]`) — MySQL has no array type. Open policy question: serialize as JSON, error out, or require a translator? For v1 the safest answer is "error with a clear message naming the column" so the user makes an explicit choice.
- Postgres `BOOLEAN` → MySQL `TINYINT(1)`. Should round-trip cleanly given the existing IR contract.
- `ENUM`: PG enums are types (`CREATE TYPE`), MySQL enums are inline column types. Translation will surface here.

**Scope.** Conservative seed first (BIGINT identity, VARCHAR, BOOLEAN, TIMESTAMP, FK with CASCADE) — same shape as the MySQL→PG test. Add the spicier types as separate cases or a follow-up once the spine is green.

---

### 2. MySQL CDC reader

**Why.** Unlocks low/zero-downtime migrations and ongoing replication. The simple-mode orchestrator does an offline cutover; CDC streams ongoing changes so the user can keep writing during the migration.

**What.** Implement `OpenCDCReader` on the MySQL engine. Use [`github.com/go-mysql-org/go-mysql`](https://github.com/go-mysql-org/go-mysql) — it's the mature Go binlog client (used by canal, TiDB DM, etc.); rolling our own binlog parser is months of work for no gain. Surface produces a stream of `ir.Change` events (Insert, Update, Delete, plus DDL events).

**Design questions to settle before code.**
- **Position type.** `binlog file + position` (classic) vs GTID (modern). GTIDs are easier for failover; both should work but pick one for v1. Recommendation: support both, default to GTID when the source has it enabled, fall back to file/pos.
- **Schema awareness.** Binlog row events carry table IDs and column data, not column names. The reader needs a schema cache populated from `information_schema` plus invalidation on DDL events. The existing `SchemaReader` is the right ingredient.
- **Filtering.** Replicate-do/ignore lists at the table level. Probably accept a `[]string` of fully-qualified table names; anything not in the list is dropped.
- **Buffering / backpressure.** Channel of `ir.Change`, similar to `RowReader`. Reader stops streaming if the receiver doesn't drain; ctx cancellation surfaces cleanly.

**Gotchas.**
- PlanetScale binlogs differ from vanilla MySQL — no `BINLOG_FORMAT=ROW` toggle to flip, but the format is row-based. Should mostly work; flag for testing.
- Some types decode through driver-version-dependent shapes (TIME, DECIMAL, JSON) — needs the same care as the row reader's value contract.
- DDL events are second-class until the Migrator knows what to do with them. For v1, surface them but the change applier can ignore them; a more sophisticated v2 replays the DDL.

---

### 3. Postgres CDC reader

**Why.** Symmetric with #2. The same low-downtime story for PG sources. PG's logical replication is a different protocol than MySQL's binlog but produces the same `ir.Change` stream when wrapped.

**What.** Implement `OpenCDCReader` on the Postgres engine using the `pgoutput` plugin (built into Postgres; no extension required). pgx exposes the streaming replication protocol via `pgconn` directly. The reader creates a publication (or expects one to exist), creates a logical replication slot, and streams `START_REPLICATION` keep-alives + change messages.

**Design questions.**
- **Publication scope.** `CREATE PUBLICATION sluice_pub FOR ALL TABLES` is simplest; the user might want per-table control. Expose a config field; default to all tables.
- **Slot lifecycle.** Slots persist on the server until dropped, and they hold WAL segments — a forgotten slot can fill the disk. Surface this aggressively in docs and emit a warning when a slot is created. Sluice should *create on demand* and offer a `--drop-slot-on-exit` flag, default to leaving the slot in place (so resume works).
- **pgoutput vs wal2json.** pgoutput is binary, native, no extension. wal2json is JSON, easier to debug, but requires a server-side extension. The "contain Postgres complexity" tenet says pgoutput.
- **Schema awareness.** pgoutput RELATION messages carry column metadata, so we don't need a separate cache lookup. But we still need to map PG OIDs to IR types — the existing type translator should be the source of truth.

**Gotchas.**
- Logical replication requires `wal_level = logical` on the server. Surface this as a precondition check at startup, not a mid-stream error.
- Replication slot creation requires the `REPLICATION` role attribute. Document it; do not silently elevate.
- TOAST values — large fields stored out-of-line — come through CDC as nulls when unchanged. Need to either ignore them on update or emit a "value unchanged" marker; for v1 the simple thing is to require `REPLICA IDENTITY FULL` or PK-based identity.

---

### 4. Snapshot-to-CDC handoff

**Why.** *This is the killer feature.* Without it, CDC starts "from now," and you've already missed whatever happened between the snapshot read and the CDC stream start. With it, you can run the simple-mode bulk copy and the CDC stream as one consistent migration: the bulk copy is *as of* a known LSN/binlog position, and CDC resumes *from exactly that position*. Zero gap, zero duplicates, no manual reconciliation.

**What.** Two engine-side primitives, plus orchestrator wiring:

- **MySQL.** `START TRANSACTION WITH CONSISTENT SNAPSHOT` to fix a read view, then `SHOW MASTER STATUS` (file/pos) or `SHOW BINARY LOG STATUS` (GTID) inside the same connection. Bulk-copy reads happen in that transaction. CDC starts from the captured position.
- **Postgres.** `CREATE_REPLICATION_SLOT` returns a snapshot name and an LSN atomically. `SET TRANSACTION SNAPSHOT '<name>'` lets the bulk-copy connection see exactly the state at slot creation. CDC then streams from that LSN.

The orchestrator gets a new mode (`pipeline.Migrator{Mode: ModeSnapshotPlusCDC}` or a separate `Streamer` type). Phases: open CDC reader → snapshot point captured → run schema apply + bulk copy with the snapshot view → start CDC from the captured position → continue indefinitely until cutover.

**Gotchas.**
- Long-running snapshot transactions block VACUUM on Postgres. For very large databases this is a real operational concern. Surface progress aggressively.
- Position bookkeeping needs persistence (see #5 control table).
- Cutover semantics — when do we stop CDC and switch traffic? — is a separate concern; for v1, "user runs `sluice sync stop` and they're responsible for traffic switch" is fine.

---

### 5. Position persistence (control table)

**Why.** CDC has to remember where it stopped so it can resume. Without it, every restart re-reads from the latest position and silently drops everything in between.

**What.** A small "control table" on the *target* database: `sluice_cdc_state(stream_id TEXT PK, source_position TEXT, updated_at TIMESTAMP)`. The CDC stream commits its progress periodically (every N changes or every M seconds, whichever first). On startup, the stream looks up its `stream_id` and resumes from the recorded position; if no row exists, it starts fresh.

**Why on the target, not a sidecar.** Cleaner ops: no separate state store to back up, survives target failover, the position is committed in the same transaction as the data changes (so progress and data can never diverge).

**Open questions.**
- Should there be one control table per target database or per deployment? Per-target is simpler.
- Naming — `sluice_cdc_state` is fine but the user might want a configurable prefix.

---

### 6. Postgres COPY-protocol writer

**Why.** Performance. The current Postgres `RowWriter` uses batched `INSERT ... VALUES (...)` statements, which is correct but ~3-5x slower than `COPY FROM STDIN` for bulk loads. pgcopydb (referenced in CLAUDE.md) leans on COPY for the same reason.

**What.** A new `BulkLoad` strategy in `internal/engines/postgres/row_writer.go`. Detect via the engine's `Capabilities.BulkLoad` field whether COPY is available (always true for vanilla Postgres). Use `pgconn.CopyFrom` (the native pgx interface, requires bypassing `database/sql` to get the underlying `*pgconn.PgConn`). Fallback to the existing batched-insert path remains for engines that don't support COPY.

**Gotchas.**
- COPY needs values in a specific binary or text format — pgx has helpers (`pgtype.NewMap`).
- COPY isn't transactional in the same way as INSERTs — error mid-stream rolls back the whole copy. This is actually what we want for the bulk-load phase, but document it.
- The IR `RowWriter` interface streams rows over a channel; COPY wants a `Reader` or callback. Adapter shim needed.
- Identity columns with `GENERATED ALWAYS AS IDENTITY` don't accept user-supplied values via COPY either. We're already using `GENERATED BY DEFAULT` for cross-engine compatibility; confirm the new writer doesn't change that.

---

### 7. Postgres slot creation with `failover=true`

**Why.** PlanetScale Postgres (and any Patroni-fronted PG ≥ 17 deployment) requires logical replication slots to be created with the `failover=true` flag *and* listed in the cluster's permanent-slots config to survive switchover or failover. Today sluice creates slots via the replication-protocol command `CREATE_REPLICATION_SLOT`, which on PG 14–16 doesn't accept the failover flag at all — so slots created by sluice on those versions can never be HA-promoted, and on PG 17 we're not opting in. **Result:** a PlanetScale customer using sluice will silently lose their CDC stream on the next failover, with recovery requiring drop + recreate + re-snapshot. The `docs/postgres-source-prep.md` doc raises this with operators, but the tool should default to the safe behavior on PG 17+.

**What.** Two paths:
- **Protocol-level:** PG 17 added a `FAILOVER` option to the `CREATE_REPLICATION_SLOT` replication command. If `pglogrepl.CreateReplicationSlotOptions` exposes it (or once it does), set it on PG 17+ sources. Detect the server version via `pglogrepl.IdentifySystem` (or a `SHOW server_version_num` precondition query) before the call.
- **SQL-function fallback:** `pg_create_logical_replication_slot('name', 'pgoutput', false, false, true)` — the 5-arg form, where the trailing `true` is `failover`. This needs a regular `*sql.DB` connection rather than the replication connection. The CDC reader currently runs `CREATE_REPLICATION_SLOT` on the replication conn; switching to the SQL function changes the order-of-operations a little but doesn't affect the snapshot-export path because the snapshot happens via `EXPORT_SNAPSHOT` on the replication conn separately.

For PG ≤ 16, there's no path: the flag doesn't exist, and Patroni's permanent-slots config (or PG 17's `sync_replication_slots`) is the only mechanism. Surface a warning when sluice creates a slot on PG ≤ 16 against a Patroni-fronted cluster, pointing at `docs/postgres-source-prep.md`.

**Gotchas / open questions.**
- Detecting "is this Patroni" or "is this PlanetScale" cleanly is non-trivial. Patroni doesn't expose a sentinel GUC. Pragmatic call: always set `failover=true` when the server is PG 17+ (it's a no-op on non-Patroni clusters), and emit the warning unconditionally on PG ≤ 16. Operators can suppress with a flag or by adding the slot to the permanent-slots config.
- The snapshot stream path (`internal/engines/postgres/cdc_snapshot.go`) creates the slot via `pglogrepl.CreateReplicationSlot` too — same fix needed there.
- Verification: a `psverify`-tagged test on a real PlanetScale cluster that creates a slot, queries `pg_replication_slots.failover` (PG 17+), and asserts it's `true`.

---

### 8. Translation policy edges

These will surface as bugs once cross-engine tests cover more types. Each is a small chunk on its own; they're listed together because they share a pattern (a type or feature exists in one engine and needs an explicit policy on the other).

- **`BIGINT UNSIGNED` → Postgres.** PG has no unsigned integers. Options: map to `BIGINT` and accept the range loss with a warning, map to `NUMERIC(20)` and pay the storage cost, or refuse with a clear error. Recommendation: warn + map to `BIGINT`, document the precision loss.
- **MySQL `ENUM` → Postgres.** PG enums are type-level (`CREATE TYPE foo AS ENUM ('a','b')`). MySQL enums are inline. Translator needs to synthesize a `CREATE TYPE` per enum column and reference it. The existing PG SchemaWriter already handles `ir.Enum` — just confirm the MySQL reader produces the right IR.
- **`JSON` vs `JSONB`.** MySQL `JSON` → Postgres `JSONB` is the right default (JSONB is what people actually want on PG). The IR has `Binary bool` on `ir.JSON`; readers and writers should respect it.
- **Default value translation.** `DEFAULT CURRENT_TIMESTAMP` in MySQL ≈ `DEFAULT now()` in PG. The IR carries defaults as `DefaultLiteral` or `DefaultExpression`; cross-engine translation needs a small lookup table for the common builtins. Anything else stays as the raw expression and may fail on the target — that's the right behavior (loud failure beats silent corruption).
- **Identity sequence sync.** PG's `GENERATED BY DEFAULT AS IDENTITY` allows manual inserts but doesn't auto-bump the sequence. After bulk copy, the next user-initiated insert collides. Fix: post-copy, run `SELECT setval(pg_get_serial_sequence(...), MAX(id))` for each identity column. Add this as a phase 4 in the orchestrator (or a phase 3.5 between row copy and indexes).

---

### 9. ADRs (Architecture Decision Records)

**Why.** The project has accumulated several non-obvious design decisions (IR-first, sealed interfaces with unexported method, kong + koanf over cobra + viper, three-phase schema apply, MySQL flavors as capability variants). Without ADRs, the *reasons* live in conversation history and risk being forgotten or relitigated.

**What.** A `docs/adr/` directory with one file per decision, following the [Michael Nygard ADR format](https://github.com/joelparkerhenderson/architecture-decision-record/blob/main/locales/en/templates/decision-record-template-by-michael-nygard/index.md): Status, Context, Decision, Consequences. Numbered (`adr-0001-ir-first.md`, etc.). Reference them from the relevant `docs/architecture.md` sections.

Initial set:
- ADR-0001: IR-first translation
- ADR-0002: Sealed interfaces via unexported method
- ADR-0003: kong + koanf over cobra + pflag + viper
- ADR-0004: Three-phase schema apply (tables → indexes → constraints)
- ADR-0005: MySQL flavors as capability variants
- ADR-0006: pgoutput over wal2json
- ADR-0007: Position persistence on the target (control table)

**Scope.** Each ADR is short — typically under 200 words. The whole batch is half a day's work; they can also drip in over time.

---

### 10. OSS hygiene

Lower priority than feature work but required before declaring v1.

- **CONTRIBUTING.md** — how to set up the dev environment, the pre-commit hook, the integration tests, the PR conventions (linear history, squash-or-rebase merges, pre-commit clean).
- **SECURITY.md** — how to report vulnerabilities. Standard template.
- **CODE_OF_CONDUCT.md** — Contributor Covenant standard text.
- **Issue / PR templates** — exist in `.github/`; review for completeness once the project has external contributors.
- **Logging.** Currently uses `fmt.Fprintf` to stdout. Consider switching to `log/slog` (stdlib) so log level flag actually does something. Small chunk.
- **Progress reporting.** During bulk copy, `pipeline: copying users (1234/50000)` is much friendlier than silence. `RowWriter.WriteRows` could surface counts via a callback or a `WithProgress` option.
- **Better error messages.** Wrap engine errors with hints — `postgres: ERROR: relation "users" does not exist` could become `postgres: target table "users" not found in schema "public" — did the schema-apply phase fail?`. Pattern: each phase wraps with a phase-specific prefix (already done) and the *first* error after a wrap can include a hint.

---

### 11. Operational features (post-v1)

Not blocking v1 but worth tracking:

- **Selective table inclusion / exclusion.** `--include-table users,posts` or `--exclude-table audit_log,sessions`. Glob patterns nice-to-have.
- **Schema rename mapping.** Source schema `app` → target schema `webapp`. Useful for environments where naming differs.
- **Type override config.** YAML hook for the user to say "treat MySQL `bigint(20) unsigned` in column X as Postgres `numeric(20)` regardless of default policy".
- **Resume-from-partial-migration.** Right now if simple-mode fails halfway, the user has to drop the target and start over. A resume path that picks up where it left off (using a state table, much like CDC's) is a real operational improvement.

---

## Cross-engine bug surface that hasn't been hit yet

Tracked here so they're not forgotten — each will surface once the relevant test exercises it:

- `BIGINT UNSIGNED` (MySQL) — see #7
- `ENUM` translation in both directions — see #7
- `JSON`/`JSONB` round-trip — see #7
- Default-value translation across dialects — see #7
- Identity sequence sync after manual ID inserts — see #7
- TIMESTAMP precision differences (MySQL fractional seconds quirks)
- CHARSET/COLLATION translation
- Generated columns (MySQL `GENERATED ALWAYS AS (...) STORED` vs PG `GENERATED ... STORED`)
- CHECK constraints (MySQL 8.0+ supports them; translation should be straightforward but untested)

---

## How to use this doc

When starting a new chunk in Claude Code:

1. Pick an item from "Next up". Earlier items have more context inheritance.
2. Open the relevant section in the prompt: *"Read CLAUDE.md and docs/dev/roadmap.md section 2 (MySQL CDC reader). Propose a design for the binlog reader: library choice, position type, schema cache approach, integration with `internal/ir.CDCReader`. Don't write code yet."*
3. Iterate on the plan.
4. Implement.
5. Update this file when the chunk lands — move the entry to "Recently landed" and trim it to one line.
