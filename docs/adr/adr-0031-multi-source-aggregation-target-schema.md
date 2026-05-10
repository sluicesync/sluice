# ADR-0031: Multi-source aggregation — `--target-schema` namespace + stream-id collision detection

**Status:** Accepted (v0.25.0)

**Companion docs:**
- Proto-design: [`docs/dev/design-multi-source-aggregation.md`](../dev/design-multi-source-aggregation.md)
- Position-and-data atomicity: [ADR-0007](adr-0007-position-persistence.md)
- Slot-name plumbing on `sluice_cdc_state` (architectural template): [ADR-0030](adr-0030-mid-stream-live-add-table.md)

## Context

Sluice is a 1:1 tool today: one source, one target, per `migrate` / `sync start` invocation. Real-world deployments increasingly want N→1 — multiple sources landing in one target. The proto-design enumerates three shapes (Shape A sharded → consolidated, Shape B microservices → analytics, Shape C multi-master) and selects Shape B for v1.

This ADR formalises the v0.25.0 implementation: minimal enabling changes to support **N independent sluice processes**, each writing to a per-source schema namespace on the same target.

## Decision

**Ship a focused, additive vertical:**

1. **Shape B only.** Per-source target-schema namespacing on the target. Shape A (sharded → consolidated) deferred — it requires discriminator-column injection, populated-target bulk-copy, and cross-shard schema-migration coordination, all of which are larger design problems.

2. **N independent processes.** Operators run N `sluice sync start` instances, one per source, with distinct `--stream-id` and `--target-schema` flags. No single-process multi-source orchestrator. Failure isolation, resource isolation, and OS-level lifecycle (k8s, systemd) are already solved at the orchestrator level; pulling them into sluice would duplicate that work without adding value.

3. **PG-only.** A new `--target-schema NAME` flag on `migrate`, `sync start`, `schema preview`, and `schema diff`. When set, every emitted CREATE TABLE / ALTER TABLE / CREATE INDEX / CREATE TYPE prefixes its identifier with the schema name. Default empty preserves today's behaviour (use the DSN's `schema` query parameter, which itself defaults to `public`). MySQL refuses the flag with a clear error directing operators at the DSN-choice pattern (different MySQL database for each source on the same server).

4. **Stream-id collision detection.** A new `source_dsn_fingerprint` column on `sluice_cdc_state` (idempotent migration). On `sync start`, after `EnsureControlTable`, if a row already exists for the given `stream_id` and its recorded fingerprint differs from the incoming source's, refuse loudly with a clear "stream-id reused with different source" message. Catches the operator-typo case before any data moves.

## Why Shape B (not A)

Shape A — N functionally-identical sources (same schema, sharded by key) consolidated into one target — needs additional machinery sluice doesn't have today:

- **Discriminator-column injection.** Operators typically want a `source_shard_id` column added on the target so the consolidated PK stays unique across shards. Sluice would need to inject the column at translation time + populate it during writes. New CLI flag, new IR shape (column origin = sluice-injected vs source-derived), new pre-flight validations.

- **Populated-target bulk-copy.** Today's cold-start preflight refuses to bulk-copy into a non-empty target (Bug 9 protection). Shape A requires the second/third/Nth shard to bulk-copy *into* a target the first shard already populated. The pre-flight needs a "discriminator-aware" bypass that knows which rows belong to which shard.

- **Cross-shard schema migrations.** When operator alters the source schema, every shard's stream needs to coordinate the ALTER on the consolidated target. ADR-0030's `--no-drain` add-table is single-source; multi-source needs cross-stream consensus.

Each is a meaningfully larger design problem than Phase 1 of multi-source. Shape A waits for a real operator request with a concrete workload. Shape B unlocks the microservices-→-analytics use case (significantly more common in observed deployments) with a much smaller surface area.

## Why PG-only

Postgres has first-class schemas (namespaces) within a database. `customer_svc.users` and `billing_svc.users` happily coexist in the same PG database with no extra work; the schema concept is exactly the namespacing primitive multi-source needs.

MySQL collapses schema and database — there is no intra-database namespace separator. The MySQL-equivalent of "different schema" is "different database on the same server," which sluice already supports natively via the `--target` DSN: `--target=mysql://host:3306/customer_svc` for one stream, `--target=mysql://host:3306/billing_svc` for the next. Cross-database joins are heavier than cross-schema joins (different connection / different context per query), but for analytics workloads against a MySQL target the trade-off is acceptable.

Adding `--target-schema` on MySQL would require either:
- Pretending MySQL has schemas it doesn't have (silently rewriting the flag to a database-name override, which is a footgun — the flag's name implies it doesn't change which database you connect to), or
- Building a database-creation orchestration layer (wrong place — operators already manage MySQL databases via DBA tooling).

The cleaner answer is to refuse cleanly with a message that names the workaround, so operators aren't stuck guessing. Documented in the help text + the engine-side construction error.

## Why N processes (not 1)

The N-processes shape:

- **Failure isolation is automatic.** One source's slot wedge or applier deadlock doesn't affect the other N-1 streams. Operators investigate via `sync status` and recover the wedged stream alone (`sync stop --wait` + `--reset-target-data` for that stream-id only).

- **Resource isolation is automatic.** Each stream gets its own connection pool, its own goroutine fan-out, its own metrics surface. No shared backpressure model to design.

- **OS-level lifecycle is the right layer.** Kubernetes pods, systemd units, and Nomad jobs already manage N-process lifecycles at scale. Sluice doesn't need to grow a process supervisor.

Single-process multi-source (one `sluice sync start --multi-source` opening N source streams) trades these benefits for "one process to monitor." That benefit is solved at the orchestrator level (k8s ReplicaSet, systemd target unit, Nomad job spec) without sluice involvement. The single-process shape becomes interesting only if a real operator surfaces a workload where shared state across sources is load-bearing — at which point the design needs cross-source ordering, conflict resolution, and shared-resource budgeting, which is a different product.

## Type-name derivation under `--target-schema` (open question 1 from proto-ADR)

PG enums are emitted as `CREATE TYPE <table>_<column>_enum AS ENUM (...)` today. The deterministic naming makes operators' lives easier (no anonymous types in `\dT`) but creates a collision risk when two sources both have `accounts.status` enums: both want `accounts_status_enum`, which collides on the same target schema.

**Decision:** When `--target-schema` is set, the type lives in that schema. PG's schema-qualified type references (`customer_svc.accounts_status_enum`) prevent the collision because each source's enum lives in its own namespace. Implementation: thread the resolved schema name through `emitCreateEnumType` and the column-def emitter (which references the type by qualified name in the table's column list). Existing emitter paths already use `schema` as a parameter, so the change is contained.

The fallback (no `--target-schema`) preserves today's behaviour — types live in `public` (or whichever schema the DSN selects). The collision risk in the multi-source-without-target-schema configuration is real but operator-driven; the help text on `--target-schema` documents it as the recommended namespacing knob.

## Stream-id collision detection — design

The architectural template is the v0.24.0 `slot_name` column on `sluice_cdc_state` (ADR-0030's Phase 2): an additive column, an idempotent migration via `ADD COLUMN IF NOT EXISTS`, COALESCE-tolerant lookup so legacy rows that pre-date the column gracefully degrade.

**Wire shape:**
```sql
ALTER TABLE sluice_cdc_state ADD COLUMN IF NOT EXISTS source_dsn_fingerprint TEXT NULL;
```

**Fingerprint shape:** A SHA-256 hex digest of the normalized DSN host+port+database tuple, truncated to 12 hex characters. Specifically:
- Parse the DSN, extract `host:port:database`.
- Normalize (lowercase host; default port for engine if absent; trim whitespace).
- Compute SHA-256 over the normalized string.
- Take the first 12 hex chars.

User and password are deliberately **not** included in the fingerprint. Operators rotate credentials regularly; a fingerprint that changes on credential rotation would surface false-positive "different source" errors after every rotation. Host+port+database is stable enough to identify the source for collision purposes while being insensitive to credential changes.

**Lookup flow on `sync start`:**
1. After `EnsureControlTable`, fetch the existing row's `source_dsn_fingerprint` for the supplied stream-id (or empty if no row / null column / table absent).
2. Compute the new fingerprint from `s.SourceDSN`.
3. If existing fingerprint is non-empty AND differs from new, refuse loudly:
   ```
   stream %q already exists on target with a different source DSN
   (existing fingerprint: %s, new: %s) — pick a different --stream-id
   or run with --reset-target-data to wipe and start fresh
   ```
4. On `WritePosition` / `ApplyBatch` commit, the row's fingerprint column is upserted to the new value (similar to `slot_name`'s COALESCE pattern — empty stays empty, non-empty wins). Since the streamer recorded its fingerprint at startup time before any apply tx commits, this is a one-time write per stream lifecycle.

**Caveats:**
- Legacy rows (pre-v0.25.0) have NULL fingerprint and pass the check by treating empty as "fingerprint unknown — allow." This means a sluice upgrade doesn't trip false-positives on existing streams. The first apply after upgrade populates the fingerprint going forward.
- A genuine source migration (operator moves the source DB to a new host but wants to keep the stream-id) tripps the check, by design — the operator must explicitly opt out via `--reset-target-data` or pick a fresh stream-id. Loud failure beats silent corruption.
- Fingerprint collisions (two different sources hashing to the same 12-char prefix) are statistically unlikely (~10^-14 birthday-probability for typical operator DSN counts) but mathematically possible. The full SHA-256 sits behind the truncation in the storage column → a future widening to 16+ chars is straightforward if a real collision ever surfaces.

## Threat model

Five operator-error scenarios this design surfaces loudly:

1. **Operator typos `--stream-id customer_svc` to `--stream-id customer-svc` on a second source.** Without collision detection, both streams write to their own state rows on the target — no immediate failure, but they collide on table-name overlap (both writing `users`, `accounts`). Detection: `--target-schema` namespacing prevents the table collision; the typoed stream-id is harmless because each stream has its own state row anyway. Phase 2 adds a check that the row's recorded fingerprint matches when the same stream-id is reused — catches the operator who *correctly* spells `customer_svc` but points at the wrong source.

2. **Operator forgets `--target-schema` on the second source.** Both streams write to `public`, hitting table-name collisions (both `users`). Pre-Phase-1: silent overwrite (one applier wins, data corruption). Phase-1 mitigation: with operator-driven `--target-schema`, both streams have explicit namespaces; forgetting it surfaces as the pre-existing Bug 9 cold-start refusal (target table already populated → loud refusal). Operator notices, adds `--target-schema`, retries cleanly.

3. **Two operators race the same stream-id.** Both run `sync start --stream-id default --source ...` against different source DBs simultaneously. Phase-2 mitigation: the second operator's `EnsureControlTable` + fingerprint check observes the first operator's row; the fingerprint mismatch refuses loudly. Without Phase 2, both writes interleave on the same row and the position token jumps backwards on every commit.

4. **Operator runs `sync start` against a PG target that already has tables in `public` from a previous non-target-schema run.** Phase-1 mitigation: cold-start preflight catches the populated target. Operator can resolve via `--target-schema customer_svc` (writes to a fresh schema) or `--reset-target-data` (drops the existing tables and recreates). Either path is loud — the ambiguity surfaces before any data moves.

5. **Operator changes `--target-schema` mid-flight (warm resume).** ~~Today's resume path reads from `sluice_cdc_state` (key: stream-id) — the persisted row is target-schema-agnostic.~~ **Closed in v0.25.1** (Bug 46 fix). The PG `sluice_cdc_state` table grew a `target_schema TEXT NULL` column; the streamer records the operator-supplied `--target-schema NAME` on every position-write via the `targetSchemaSetter` interface (PG implements; MySQL doesn't, since `--target-schema` is refused upstream for MySQL targets). `sluice schema add-table` reads the recorded value back and applies the resolution rule (operator flag empty → inherit; operator flag non-empty + matches recorded → proceed; operator flag non-empty + differs from non-empty recorded → refuse loudly with `pipeline: add-table: --target-schema=%q does not match the active stream's recorded target_schema=%q`). The same shape generalises to `sync start` with a different `--target-schema` against an existing stream-id — the streamer's own fingerprint check now has a target-schema sibling check pinned by the schema-add-table refusal path.

## Non-goals

- **Per-table renaming** (operator says `users → analytics_users` for source A, leaves source B's `users` alone). Useful for shape B operators with finer-grained naming preferences but adds significant config-shape and migration-state complexity. Deferred until requested.

- **Single-process multi-source.** See "Why N processes" above. The design space is reserved for if-and-when a real operator surfaces a workload where shared state across sources is load-bearing.

- **Cross-source temporal ordering.** Today's per-source ordering preservation is unchanged; cross-source ordering (event from source A at T=1 vs event from source B at T=2) is a different problem requiring a Lamport clock or vector clock. Out of scope for v0.25.0; operators who need cross-source consistency typically use a different replication topology (e.g., a single MySQL Group Replication primary with a single sluice instance pointed at it).

- **Status-aggregation UX changes.** `sluice sync status` already lists every row in `sluice_cdc_state` and works correctly for multi-source out of the box (each source has its own row). A future refinement could group by source-engine + DSN-host and add a "source fingerprint" column for the cross-source view, but the core data is already there.

## Implementation summary

**New surfaces:**
- `ir.SchemaSetter` (optional engine surface for `Open*` returns to receive a schema-name override). PG implements on `SchemaReader`, `SchemaWriter`, `RowReader`, `RowWriter`, `ChangeApplier`. MySQL deliberately does not implement.
- `ir.StreamStatus.SourceDSNFingerprint` field (empty for legacy rows / engines without fingerprint support).
- Pipeline helpers: `fingerprintSourceDSN(dsn)`, fingerprint-collision check at streamer startup.

**Pipeline-side fields added (Migrator / Streamer / Previewer / Differ):**
- `TargetSchema string` — operator-supplied schema name (empty preserves today's behaviour).

**CLI-side fields added (MigrateCmd / SyncStartCmd / SchemaPreviewCmd / SchemaDiffCmd):**
- `TargetSchema string` (mapped to the orchestrator field).

**Pipeline orchestration changes:**
- After Open*, type-assert for `ir.SchemaSetter` and call `SetSchema(targetSchema)` when targetSchema is non-empty.
- Engine refusal helper: if targetSchema is non-empty and the engine's `Capabilities().SchemaScope != SchemaScopeNamespaced`, refuse with the PG-only message.

**PG engine changes:**
- `SchemaReader` / `SchemaWriter` / `RowReader` / `RowWriter` / `ChangeApplier` gain a `SetSchema(name)` method (pointer receiver). Default schema (from DSN, typically `public`) is the field's initial value; `SetSchema` overrides.
- Schema writer's table emit ensures the named schema exists via `CREATE SCHEMA IF NOT EXISTS` before any DDL executes.
- Type-name derivation (`emitCreateEnumType`) already takes `schema` as a parameter; no change needed beyond the writer's `schema` field reflecting the override.

**MySQL engine changes:**
- No changes. The orchestrator-side capability check refuses the flag before any MySQL engine code runs.

**Control-table schema migration:**
- `ALTER TABLE sluice_cdc_state ADD COLUMN IF NOT EXISTS source_dsn_fingerprint TEXT NULL`.
- `WritePosition` upserts the column on every commit (COALESCE-tolerant).
- `listStreams` SELECTs `COALESCE(source_dsn_fingerprint, '')` so legacy rows surface as empty fingerprint (interpreted as "unknown — allow").

## Versioning

Lands in v0.25.0. Follow-on work is tracked in `docs/dev/roadmap.md` and the proto-design doc (`docs/dev/design-multi-source-aggregation.md` Phase 3+).

## See also

- [`docs/dev/design-multi-source-aggregation.md`](../dev/design-multi-source-aggregation.md) — proto-design with use-case enumeration and phased plan.
- [ADR-0007](adr-0007-position-persistence.md) — per-target control-table shape this design extends.
- [ADR-0023](adr-0023-reset-target-data.md) — destructive-recovery escape hatch operators use to resolve fingerprint-mismatch errors.
- [ADR-0030](adr-0030-mid-stream-live-add-table.md) — slot_name plumbing pattern (architectural template for the source_dsn_fingerprint column migration).
