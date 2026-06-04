# Design: multi-source aggregation

**Status:** Phase 1 + Phase 2 shipped in v0.25.0 — see [ADR-0031](../adr/adr-0031-multi-source-aggregation-target-schema.md). This doc remains the proto-ADR / design-space reference for the remaining phases (per-table renaming, status-aggregation UX, schema-collision detection, Shape A sharded → consolidated).

## Phase 1 + 2 status (v0.25.0)

Shipped:
- `--target-schema NAME` flag on `migrate`, `sync start`, `schema preview`, and `schema diff`. PG-only (engines whose `Capabilities().SchemaScope != SchemaScopeNamespaced` refuse the flag with a clear "use a different --target DSN database" message). Schema is auto-created via `CREATE SCHEMA IF NOT EXISTS`.
- Type-name derivation (PG enums) namespaced through the schema — `customer_svc.accounts_status_enum`, `billing_svc.accounts_status_enum` coexist cleanly.
- Stream-id collision detection: `sluice_cdc_state` grew a `source_dsn_fingerprint TEXT NULL` column (additive, idempotent migration). Streamer fingerprint check at startup refuses if the existing row's fingerprint differs from the incoming source's. Fingerprint is the truncated SHA-256 of host+port+database (user/password excluded so credential rotation doesn't trip false-positives).
- New IR surfaces: `ir.SchemaSetter`, `ir.SourceFingerprintRecorder`, `ir.StreamStatus.SourceDSNFingerprint`. PG implements; MySQL deliberately doesn't.

See ADR-0031 for the full design rationale, including the threat model and the controlSchema-vs-userDataSchema split on the PG applier (control table stays in the DSN's default schema; user-data INSERT/UPDATE/DELETE land in the per-source schema).

## Context

## Context

### What sluice does today

Sluice is 1:1 — one source, one target, per migrate or sync stream. Every command takes a `--source-driver` / `--source` and `--target-driver` / `--target` pair. The state tables (`sluice_migrate_state`, `sluice_cdc_state`) are scoped per-target and key on a stream-id, so technically a single target *can* host multiple stream-ids — but the orchestration to actually drive multiple sources into one target isn't there.

### Use cases

Three distinct shapes show up in real-world deployments. Each has different requirements; v1 of multi-source should pick one shape rather than try to be everything.

**Shape A — Sharded → consolidated.** N source databases that are functionally identical (same schema, partitioned by key range or hash). The target is the consolidated view. Examples: PlanetScale Vitess shards landing in a single PG analytics warehouse; multi-region MySQL → consolidated Postgres for cross-region reporting.

Schema collisions: every source has the same `users` table; sluice writes to a single `users` table on the target. No schema collision handling needed; might need a discriminator column added (`source_shard_id`).

**Shape B — Microservices → analytics.** N source databases that are functionally distinct (different schemas, different application teams). The target is the analytics warehouse where everything ends up.

Schema collisions: source A has a `users` table, source B also has a `users` table (different schemas). The target needs to keep them separate — typically via a per-source schema namespace (`source_a.users`, `source_b.users`) or a per-source table prefix (`a_users`, `b_users`).

**Shape C — Multi-master / multi-direction.** Two databases that each accept writes; sluice replicates writes from each to the other. Conflict resolution becomes the load-bearing question.

Shape C is significantly harder than A or B because it requires conflict-resolution policies (last-write-wins / version-vectors / CRDTs / operator-supplied resolvers). It's a different product. **This design treats Shape C as out of scope.** Sluice's "loud failure beats silent corruption" tenet doesn't tolerate the quiet conflict resolution multi-master typically requires.

Shapes A and B are addressable with similar machinery; the differences are in schema-collision handling.

## Design space

### Stream-id namespacing

Each source needs a unique stream-id on the target. Today's stream-id is operator-supplied or auto-derived from source/target host info; for multi-source, the operator probably wants something readable: `--stream-id shard-1`, `--stream-id shard-2`, or `--stream-id customer-svc`, `--stream-id billing-svc`.

The control table (`sluice_cdc_state`) already keys on stream-id; multi-row support is essentially free at the schema layer. What's missing is orchestration: today's `sync start` opens one stream and runs one applier; multi-source needs N appliers running concurrently against the same target.

### Concurrent applier coordination

Two architectural choices:

**(1) One sluice process per source.** Operator runs N `sluice sync start` instances, each with its own stream-id. The target sees N concurrent appliers writing to the same database. State isolation is automatic (each instance has its own stream-id row in `sluice_cdc_state`). Failure isolation is automatic (one source's stream-id wedge doesn't affect the others).

**(2) Single sluice process with N source streams.** Operator runs one `sluice sync start --multi-source`, which opens N source streams concurrently. Single-process coordination. Easier ops surface (one process to monitor) but harder failure semantics (does one source's wedge crash the whole thing?).

(1) is the simpler v1 — sluice's existing concurrency is per-process anyway, and (2) doesn't add real value beyond "one process to watch" which is solved at the orchestrator level (k8s, systemd) for free.

**This design assumes (1): N sluice processes, one per source.** Multi-source becomes a question of "what sluice features make N independent processes work cleanly together?" rather than "how do we redesign the orchestrator?"

### Schema collision handling

Three possible mechanisms; each has different operator UX:

**(a) Per-source target-schema prefix.** New flag `--target-schema customer_svc` writes the source's `users` table as `customer_svc.users` on the target. Schema-namespace separation. Works well on PG (schemas are first-class); MySQL has no schema-vs-database distinction so the operator would use a separate target *database* (already supported via `--target` DSN choosing the database).

**(b) Per-table renaming.** New mapping: `table_renames: { source_table: target_table }`. Each source has its own renames; on the target they land at the renamed names. More flexible than (a) but more verbose to configure.

**(c) Operator just uses different DSNs.** If `customer-svc` lands in target database `analytics_customer_svc` and `billing-svc` lands in `analytics_billing_svc`, no collision handling is needed at the sluice level — the operator handles it via DSN choice. But "different databases" is heavier than "different schemas" — analytics queries would need cross-database joins.

(a) is the cleanest for PG targets; (c) is the cleanest for MySQL targets given MySQL's database-vs-schema collapse. (b) is the universal flexible option.

For v1 a focused approach: **PG targets get `--target-schema` (option a); MySQL operators handle it via DSN choice (option c).** Per-table renaming (option b) is a future addition if shape B operators ask for finer-grained control.

### Status aggregation

`sluice sync status` today shows the state of all streams against a target. With multi-source, this naturally extends to "show me all N source streams" — same query, more rows. No design change needed; the existing query already lists every row in `sluice_cdc_state`.

### Schema-diff / schema-preview against multi-source

`sluice schema diff` and `sluice schema preview` are 1:1 today (one source, one target). Multi-source operators probably want:

- `schema preview --source customer_svc.dsn --target target.dsn --target-schema customer_svc`: shows the DDL sluice would produce for that one source's tables.
- `schema diff` similarly: per-source diff against the target's per-source schema.

Multi-source doesn't change the diff/preview shape — operators run them once per source. UX-friendly: the existing commands work. No new command needed.

### Failure modes

**One source's stream wedges.** With one-process-per-source, the wedge is isolated. The target keeps receiving writes from the other N-1 streams. Operator investigates the wedged stream via `sync status` and `sync stop` / `--reset-target-data` for that stream-id only.

**Target table created by source A is dropped on source B.** Shape A only — schema migrations need coordination across shards. Shape B has separate target schemas so this can't happen.

**Stream-id collision across operators.** If two operators independently choose `--stream-id default`, they overwrite each other's state. Operator-supplied stream-ids are the right surface; sluice can warn if a stream-id is reused with different source DSN. Optional v1 polish.

## Concrete implementation plan

Phased so each is independently shippable. Most of multi-source is "remove obstacles to running N sluice processes against the same target" rather than new orchestration.

### Phase 1: `--target-schema` flag (v0.12.0 candidate)

Smallest enabling change. Adds:

- New CLI flag on `migrate`, `sync start`, `schema preview`, `schema diff`: `--target-schema NAME`. Default empty (use the target DSN's default schema, today's behaviour). When set, every emitted CREATE TABLE / ALTER TABLE prefixes the table with the schema name.
- Pre-flight: target engine must support schemas (PG yes; MySQL: error suggesting separate database via `--target`).
- Schema reader on warm-resume reads from the configured schema, not the default.
- Schema-diff compares against the configured schema on the target side.

This alone unblocks Shape B (microservices → analytics) on PG targets. Operators run N `sluice sync start` instances with different `--target-schema` values; each lands in its own namespace.

Estimated size: ~400-600 LOC including tests + ADR.

### Phase 2: Stream-id collision detection (v0.12.x)

Optional polish: when `sync start` writes a row to `sluice_cdc_state` with a stream-id that already exists, check whether the source DSN matches. If different, surface a clear "stream-id reused with different source" error. Catches the operator-typo case without changing the data model.

Estimated size: ~80 LOC.

### Phase 3: Status-command multi-source UX (v0.13.x?)

`sluice sync status` already shows all rows. Make the output multi-source-aware: group by source-engine + DSN-host, sort by recent activity, etc. UX improvement, no behaviour change.

### Phase 4: Per-table renaming (TBD)

Only if Shape B operators ask. Add `table_renames:` YAML key + `--rename-table SOURCE=TARGET` CLI flag. Same shape as `--type-override`.

### Phase 5: Schema-collision detection (TBD)

Cross-source pre-flight: when running multiple sluice processes against the same target, detect if two source schemas would land conflicting tables on the target (without `--target-schema`). Today's silent overwrite becomes a loud failure. Lower priority because Phase 1 makes this collision unlikely if operators use `--target-schema`.

## What about Shape A (sharded)?

Most of the above targets Shape B. Shape A (sharded → consolidated) needs additional machinery:

- **Discriminator column injection.** When N sources have identical schemas, the operator typically wants a `source_shard_id` column added on the target. Sluice would inject the column at translation time + populate it with the stream-id during writes. New CLI flag `--inject-shard-column NAME=VALUE` mirrored on each shard's stream.
- **Stream-id-aware bulk copy.** Bulk-copy reads from N sources and writes to the same target table; shards can't conflict on PK because the discriminator column makes the composite PK unique. Sluice's pre-flight check (refuses to bulk-copy into populated target) needs to allow this when `--inject-shard-column` is set.

Shape A is heavier than Shape B and warrants its own design pass once the simpler shape is shipped.

## Why not now

Multi-source is real engineering work, but more importantly it's *unrequested* engineering work. No real-world testing has surfaced multi-source as a need; the v0.x cycle has been driven by single-source / single-target real-world workloads.

The right time to land Phase 1 is when an operator says "I have N sources I want to land in one target." Until then, this design is a reference: the path is clear when needed.

## Open questions

1. **`--target-schema` interaction with extension types.** PG enums get type-name derivation (`<table>_<column>_enum`). Multi-source means two sources might both have an `accounts` table with a `status` enum, and both would want `accounts_status_enum` — collision. The schema-prefix would namespace the type too: `customer_svc.accounts_status_enum`. Doable, but the type-name derivation needs to thread the schema through.
2. **CDC ordering across sources.** Does the operator care about cross-source temporal ordering (event from source A at T=1 vs event from source B at T=2)? Sluice today preserves per-source ordering only — multi-source preserves it independently per stream. Cross-source ordering is a different problem (typically requires a cross-source LSN like a Lamport clock).
3. **Backpressure across sources.** If one source is much faster than another, does that affect target write throughput? With N independent processes, no — they each get their own connection pool. Single-process multi-source would have to think about this.

## See also

- `docs/architecture.md` — the IR / engine pattern this design assumes.
- `docs/adr/adr-0007-position-persistence.md` — per-target control-table shape.
- `docs/adr/adr-0021-publication-scope-by-table.md` — the per-table publication design that multi-source builds on.
