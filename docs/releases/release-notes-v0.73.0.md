# sluice v0.73.0 — Shape A Phase 2: live cross-shard DDL coordination

**Headline:** Shape A streams (`--inject-shard-column`) now coordinate cross-shard DDL via a hybrid TTL + heartbeat-extend lease (Vitess OnlineDDL-style pattern). The slowest-shard-drain hazard is gone: the first shard to notice a source DDL acquires the lease, applies the IR-delta-derived shape change to the consolidated target, records the applied schema version + DDL checksum; peer shards verify the recorded checksum and skip the apply, continuing CDC without a drain window.

ADR-0054 closes the deferred Phase 2 surface from ADR-0048 §4. All five decision points (DP-A through DP-E) signed off by the owner via dialogue; resolutions recorded inline in the ADR and the resolution-history is durable in the git log.

## Features

- **`feat(adr-0054): Shape A Phase 2 — live cross-shard DDL coordination`.** New control table `sluice_shard_consolidation_lease` (additive migration on both engines; one row per consolidated target table) tracks the lease state machine (ABSENT → HELD → APPLIED, with EXPIRED → takeover-via-probe-and-record on lease-holder crash). Streamer engagement is automatic when `--inject-shard-column` is set on `sync start`; `--no-coordinate-live-ddl` opts back to the v0.72.x drained model.

  - **Lease timing (DP-A — Kubernetes leader-election shape):** `--shard-coordination-lease-duration=30s` (TTL safety net) + `--shard-coordination-renew-deadline=20s` (heartbeat-on-fail cutoff) + `--shard-coordination-retry-period=10s` (heartbeat cadence). Operator-tunable for unusual ALTER patterns (e.g. tables >100GB might want a 5min lease).

  - **DDL idempotence (DP-B):** Recorded-version + DDL-text-checksum (SHA-256 hex on normalized DDL text — whitespace collapse + reserved-keyword lowercase, mirroring ADR-0049's `SchemaSignature.Equal`). All DDL types supported via the recorded-version path; cross-shard divergence (one shard sees ALTER X, another sees ALTER Y at the same boundary) refuses loudly with both checksums + drained-model recovery commands.

  - **Crash recovery (DP-C):** Probe-and-record on lease takeover. The takeover stream probes the target schema for the prior lease-holder's recorded shape; `Applied` → record only (no re-apply); `NotApplied` → re-apply + record; `Inconsistent` → refuse loudly with operator-actionable recovery hint. Uniform across PG (transactional DDL) and MySQL (non-transactional DDL — the named hazard from ADR-0048 §4 is closed via the post-state probe).

  - **Engagement (DP-D):** Always-on with `--no-coordinate-live-ddl` opt-out. Behaviour-change-by-default consistent with the ADR-0052 AIMD opt-out pattern.

  - **DDL apply derivation (DP-E, owner-resolved 2026-05-22 during implementation):** Recognized-shape catalog via IR-delta classifier. The lease-holder classifies the delta between the pre-DDL and post-DDL `SchemaSnapshot` IR tables into a finite catalog (ADD COLUMN, DROP COLUMN, CREATE INDEX, DROP INDEX, ALTER COLUMN type/nullability); unrecognized shapes (multi-shape combos, RENAME, CHECK constraints, generated-column changes) refuse loudly with the drained-model recovery hint. The classifier compares `*ir.Table` structs (sluice's own canonical schema representation) — not SQL text — so DP-B's "no allow-list, no parser" intent is preserved.

- **`feat(sluice sync status): consolidation_leases surface`.** The `sync status` JSON output gains a `consolidation_leases` array (omitted when no leases observed) listing per-table lease state (`held` / `applied` / `none`), lease-holder stream-id, expiry timestamp, applied schema version, and DDL checksum. Text output gets a one-line Shape A summary on the status row. Operators running fleets of sharded streams can now reason about cross-shard DDL state from a single command.

- **`feat(ir): new optional surfaces for the lease coordination`.** `ir.ShardConsolidationLeaseStore` (lease state CRUD), `ir.ShardConsolidationProber` (target schema probes for crash recovery), `ir.ShardConsolidationLeaseLister` (status surface enumeration), `ir.ShapeDeltaApplier` (per-shape DDL emit, extending the existing `AlterAddColumn` to cover the v1 catalog). Each engine (mysql + postgres) implements all four.

## Compatibility

- **Behaviour change for v0.72.x Shape A operators.** Streams started with `--inject-shard-column` will now engage live coordination by default. Operators who depend on the drained model (e.g. operationally-scripted `sync stop --wait` → cross-shard migrate → `sync start --resume` workflows) must add `--no-coordinate-live-ddl` to preserve pre-ADR-0054 semantics. The opt-out flag is permanent; not a deprecation period.

- **Non-Shape-A streams are unaffected.** When `--inject-shard-column` is unset, the new flags are no-ops; no control-table migration is required.

- **The new `sluice_shard_consolidation_lease` control table** is created via additive `CREATE TABLE IF NOT EXISTS` on first Shape A run; existing deployments don't need a manual migration step.

## Who needs this

- **Sharded source operators consolidating into one target** (PlanetScale Vitess shards → MySQL or PG consolidated, application-level sharding into analytics warehouses, hash-partitioned topologies). The drain-window-proportional-to-slowest-shard pause that v0.72.x required for any cross-shard DDL is gone. ALTERs on the keyspace propagate through the lease coordination automatically.

- **Operators with high-frequency DDL on sharded sources.** Schema-evolution workflows that wedge the v0.72.x drained model (each ALTER = drain all shards = pause = restart) now run continuously. The lease's hybrid TTL + heartbeat-extend semantics handle long-running ALTERs gracefully (heartbeat keeps the lease alive) without operator timeout-tuning.

- **Anyone not running Shape A** sees no observable change. The control table is created lazily, the flags are no-ops, the behaviour change is scoped narrowly to `--inject-shard-column` being set.

## Known follow-ups (informational)

- **ProbeAlterColumnType ships existence-only in v1** (Task #20). The probe distinguishes "column exists" from "column absent" but does not yet check the catalog's reported type against the IR-derived expected type. Catches the most likely silent-divergence shape (column dropped entirely); type-mismatch is a known gap to be filled in a follow-up release.

- **Lease GC sweep deferred** (Task #21). The lease table is bounded by distinct-DDLs × consolidated-tables; for normal operator workflows this stays small. A background GC sweep that prunes lease rows whose `applied_schema_version` is older than every stream's current cursor is tracked as a follow-up; promote when operational pain emerges or the row count is large enough to warrant a `sync status` warning.

- **Catalog expansion deferred** (Task #22). v1 catalog covers the high-frequency operator workflows (ADD/DROP COLUMN, CREATE/DROP INDEX, ALTER COLUMN type+nullability). RENAME COLUMN, CHECK constraint changes, generated-column expression changes, and multi-shape combo-deltas refuse loudly with the drained-model recovery hint. Each shape is bounded ~200-300 LOC × per-engine to add; promote one at a time as operator workflows surface need.

- **Phase 2e MySQL counterpart + remaining crash-injection paths** (Task #23). v1 Phase 2e ships 2 PG integration tests pinning the load-bearing exactly-once-apply contention + takeover-probe-Applied paths. The MySQL counterpart + the NotApplied / Inconsistent takeover paths are bundled as a follow-up release that closes the Phase 2e v2 correctness-completeness surface.

## Cross-references

- [ADR-0054 — Shape A Phase 2: live cross-shard DDL coordination](https://github.com/orware/sluice/blob/main/docs/adr/adr-0054-shape-a-phase-2-live-cross-shard-ddl-coordination.md) — full design with DP-A through DP-E resolutions
- [ADR-0048 — Multi-source aggregation Shape A](https://github.com/orware/sluice/blob/main/docs/adr/adr-0048-multi-source-aggregation-shape-a.md) §4 — the deferred Phase 2 sketch this ADR fills in
- [ADR-0030 — Mid-stream live add-table](https://github.com/orware/sluice/blob/main/docs/adr/adr-0030-mid-stream-live-add-table.md) — Strategy A drained-model precedent; v1 alternative now preserved behind `--no-coordinate-live-ddl`
- [ADR-0052 — AIMD apply-batch-size controller](https://github.com/orware/sluice/blob/main/docs/adr/adr-0052-aimd-apply-batch-size-controller.md) — the opt-out-by-default pattern this release follows
