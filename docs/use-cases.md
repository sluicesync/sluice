# Use cases for sluice

This page names the operator scenarios sluice is built for. The README's "Where sluice fits" matrix is the *category* claim; this page is the *concrete* one — specific situations where running sluice produces a result the named alternatives can't, or can only with more setup.

Each scenario lists the **pain**, the **sluice procedure** that addresses it, and the **load-bearing capability** that makes the procedure work. If the procedure doesn't end with `sluice` doing the actual work, the scenario doesn't belong on this page.

---

## Managed-Postgres version upgrades

The most common shape: an operator is on a managed-PG service (RDS, Aurora, Heroku, Supabase, Crunchy, Neon, PlanetScale) and needs to move to a newer major version. The provider's in-place upgrade either (a) doesn't exist for their plan, (b) requires a maintenance window longer than they can take, or (c) doesn't survive the extension or partition shape they have.

### Pain shapes

- **PostGIS version mismatch.** PostGIS X is installed on the source; the target only ships PostGIS Y. In-place upgrade either refuses or leaves geometry columns in an indeterminate state.
- **`pg_partman` → native partitioning.** Operator wants to drop `pg_partman` and move to PG 14+'s native declarative partitioning, but the existing partition tree can't be in-place re-shaped.
- **Citus migrate-off.** Citus extension installed for sharding that the operator no longer needs; un-installing in-place leaves orphaned schema artifacts.
- **`pgvector` versioning.** `pgvector` 0.5 → 0.7 added HNSW; existing IVFFlat indexes need rebuilding. Some managed providers don't ship the newer version on the same plan tier.
- **PG version-locked by the provider.** Source is on PG 13 because that's all the plan covers; target needs to be on PG 16 for partition-pruning or replication-slot improvements.

### sluice procedure

```sh
# 1. Cold-start: read source schema, translate, create on target, bulk-copy.
sluice migrate --config sluice.yaml

# 2. Continuous CDC catch-up — apply changes from source to target while
#    application traffic continues hitting source.
sluice sync start --resume

# 3. When CDC lag is acceptable, stop writes on source.

# 4. Cutover-time sequence priming — bump target's BIGSERIAL sequences
#    above source so the first post-cutover INSERT doesn't collide.
sluice cutover --config sluice.yaml --cutover-sequence-margin=1000

# 5. Verify row-count and schema parity.
sluice verify --config sluice.yaml

# 6. Switch application traffic to target.
```

### Load-bearing capability

- **Schema translation across PG major versions** keeps the *shape* — sluice reads PG 13's pg_catalog, emits PG 16 DDL with the same identifiers, indexes, constraints, and CHECK expressions.
- **Extension passthrough** (PostGIS, pgvector, hstore, citext, pg_trgm, uuid-ossp) keeps the *types* — values land on target in their original on-disk shape, indexes get re-emitted with the correct opclass.
- **`sluice cutover` (v0.83.0)** closes the PK-collision-on-first-INSERT class. Between snapshot and cutover, source advances sequences; CDC ships the rows but the target's sequence value still lags. Pre-v0.83.0, operators ran `setval` per table by hand.

### What doesn't carry across

- **Roles and grants.** sluice does not read `pg_roles` or `pg_auth_members`. Operators recreate the role-and-grant tree on target (or use the provider's role-import tool) as a separate pass before `sluice migrate`.
- **Replication slots and subscriptions on the source.** They stay on source; target gets a clean slate.
- **Provider-specific config** (e.g. AWS Aurora's custom GUCs, Neon's branching metadata, Supabase's row-level policies bound to Supabase auth roles). These are outside sluice's IR — recreate by hand if needed.

---

## Cross-cloud / cross-provider migration

A different version of the same shape: the operator is leaving the *provider*, not just the *version*.

### Pain shapes

- **AWS RDS → GCP Cloud SQL** (or any cross-cloud direction). The cloud's own DMS-equivalent service is either single-cloud or requires extensive setup.
- **Heroku Postgres → anywhere.** Heroku Postgres doesn't grant `REPLICATION` or expose logical replication ([Heroku's third-party-replication note](https://help.heroku.com/E10ZZ6IJ/why-can-t-i-use-third-party-tools-to-replicate-my-heroku-postgres-database-to-a-non-heroku-database)), so slot-based CDC is out — but sluice's **`postgres-trigger` engine** (ADR-0066, shipped) migrates a Heroku source with triggers instead of a slot: `--source-driver=postgres-trigger` + `sluice trigger setup` (add `--allow-polled-fingerprint` for Heroku's non-superuser role). Validated end-to-end Heroku → PlanetScale; the Go-native alternative to Perl-based Bucardo.
- **Self-hosted on-prem → managed cloud.** No cross-cloud DMS exists; AWS DMS only reads *into* AWS.
- **PlanetScale-MySQL → vanilla MySQL** (Aurora MySQL, GCP Cloud SQL MySQL, on-prem). The PlanetScale flavor handles Vitess-fronted MySQL specifics; the vanilla MySQL writer drops them cleanly.

### sluice procedure

Identical to the version-upgrade procedure above. sluice doesn't care about the provider boundary — it sees endpoints. The DSN you give `--source` and the DSN you give `--target` can be in different VPCs, different clouds, or one cloud-managed and one on-prem.

### Load-bearing capability

- **Single static binary.** No daemon, no Kafka, no SaaS dependency. Run sluice from a bastion / build host / laptop with network reach to both endpoints.
- **Per-row pricing: none.** A 50TB migration costs whatever compute and egress the operator already pays for. No `MAR` (Fivetran), no per-DMS-instance (AWS), no per-source license (HVR).
- **PlanetScale flavor (MySQL).** sluice ships a separate engine registration for PlanetScale-MySQL with the Vitess-aware capability set; the source-prep, snapshot, and CDC paths know about Vitess-fronted binlog quirks.

---

## Cross-engine MySQL ↔ Postgres consolidation

The shape that motivated sluice's existence: a polyglot architecture (MySQL for legacy app, Postgres for analytics) wants to converge on one engine, or split apart for a new constraint (PostGIS-spatial work needs PG; LAMP-stack legacy stays on MySQL).

### Pain shapes

- **One-time consolidation onto Postgres.** Existing MySQL has the operational system-of-record; Postgres has the analytics layer. The org wants to drop MySQL.
- **One-time consolidation onto MySQL.** Postgres-heavy stack with a small Postgres team and a much larger MySQL team — operational simplification by moving to the engine the team knows.
- **Bidirectional during a transition.** Engineering teams are mid-migration to PG. Some services have moved, some haven't. Running sluice MySQL → PG keeps the PG side fresh; running sluice PG → MySQL (a *different* sluice instance, opposite direction) keeps the MySQL side fresh as a hot standby. This is the procedural "rollback-friendly cutover" — both sides are continuously synced until the operator commits to the destination.

### sluice procedure

```sh
# Direction 1: MySQL → PG primary path
sluice migrate --config mysql-to-pg.yaml
sluice sync start --config mysql-to-pg.yaml --resume

# Direction 2 (optional, hot-standby for rollback): PG → MySQL reverse path
sluice migrate --config pg-to-mysql.yaml
sluice sync start --config pg-to-mysql.yaml --resume
```

The two configs name different stream IDs, different source-target pairings, and run as independent processes. sluice has no opinion about whether both directions are running; the source-of-truth question is the operator's.

### Load-bearing capability

- **IR-first translation.** sluice's `internal/ir` defines a typed schema/value model engine-agnostic; the MySQL reader produces IR, the PG writer consumes IR, and vice versa. The value contract is documented in [`docs/value-types.md`](value-types.md) — cross-engine semantics are explicit, not implicit.
- **Loud refusal on unsafe translation.** When sluice can't translate a construct safely (e.g. a MySQL `GENERATED ALWAYS AS (json_extract(...))` with an idiom PG doesn't support natively), it refuses with a structured error rather than silently dropping. The escape hatch is `--expr-override` (see ADR-0016) for operator-supplied translations.
- **`ADD COLUMN` auto-forwarding (v0.79.0+).** Schema evolution on the source mid-stream lands on target without operator action; both same-engine and cross-engine paths are covered.

---

## Operational data backups via logical-CDC capture

A shape that surprises some operators: sluice's continuous-sync engine is the same machinery a logical-backup tool needs. With `sluice backup` (the `backup_writer` blob-store target), the operator gets logical-CDC capture against a slot that survives source switchovers and failovers (with PG 17+ FAILOVER-flagged slots).

### Pain shapes

- **PITR with logical granularity.** `pg_dump` is a point-in-time snapshot; physical backups are PITR but tied to the original binary. Logical-CDC capture sits between — a stream of row-events that can be replayed by sluice into a fresh target at any LSN.
- **Cross-region backups without managed-service lock-in.** Capture the CDC stream into S3 / GCS / Azure Blob; restore later into any compatible target.
- **Audit-friendly change capture.** Logical-CDC capture is row-level; physical backups aren't. For compliance scenarios where the auditor wants "what rows changed when," the logical stream answers directly.

### sluice procedure

```sh
# Continuous backup to blob store
sluice backup start --config backup.yaml

# Compact + rotate
sluice backup compact --merge-window 1h
sluice backup retain --rotate-at 168h

# Restore: replay a backup chain into a fresh target
sluice backup restore --from-chain-id <id> --target <dsn>
```

### Load-bearing capability

- **Slot-health monitoring** (F13, v0.80.0). The PG slot powering the backup stream gets pre-emptive warnings at 70% / 85% consumption and at 30m inactive. Operators learn before the slot evicts.
- **Source-side heartbeat writer** (F17, v0.82.0). On quiet sources, the writer keeps the slot's `restart_lsn` advancing so off-hours periods don't accumulate WAL that PG eventually drops.
- **Backup chain compaction** (chain 14a/b/d). Multiple capture windows merge into one compact archive; smart compaction (same-row event collapsing, chain 14e — pending) trims further.

---

## Choosing sluice for a specific scenario

For each scenario, the alternatives are real and sometimes simpler. The README's "When NOT to use sluice" section names the four explicit non-fits. For the cases above, the decision usually comes down to:

| If you already have… | …consider |
|---|---|
| Kafka + Debezium running | Adding the sink connector for your target may be less work than running sluice |
| AWS-only stack | DMS is the default. sluice shines when you're going *out* of AWS, not within |
| Fivetran / SaaS sync | The pricing crossover happens around 1-10TB depending on row velocity. Under that, SaaS may stay cheaper |
| Same-engine PG → PG only | `pgcopydb` is purpose-built for that case and faster on the snapshot side |
| `pg_dump`-friendly schema with no CDC need | The native tools are simpler |

sluice's strongest case is when the alternative is "buy HVR / Striim / Qlik," "stand up Kafka from scratch," or "write the bash-glue around `pg_dump` + custom CDC processors yourself." The category claim is the README's; this page is the bench-test version.

---

## Cross-references

- [`README.md`](../README.md) — category positioning, feature matrix, when-not-to-use
- [`docs/architecture.md`](architecture.md) — the IR + engine pattern that makes cross-engine work
- [`docs/postgres-source-prep.md`](postgres-source-prep.md) — PG source-side WAL / role requirements
- [`docs/managed-services.md`](managed-services.md) — provider-specific notes (RDS, Aurora, Supabase, Heroku, …)
- [`docs/value-types.md`](value-types.md) — cross-engine value contract
- [`docs/throughput-tuning.md`](throughput-tuning.md) — bulk-copy + CDC-apply tuning for large workloads
