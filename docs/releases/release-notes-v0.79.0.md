# sluice v0.79.0 — Online ADD COLUMN forwarding in CDC apply path (ADR-0058)

**Headline:** Minor release shipping **the marquee positioning feature**: live forwarding of source-side `ALTER TABLE ADD COLUMN` events through the CDC stream to the target, with optional source-backfill of the new column on already-shipped rows. Closes the F12 + F16 Reddit-research findings (severity-a) — every CDC competitor falls short on schema changes differently (SQL Server native CDC ignores ADD COLUMN entirely; AWS DMS / Qlik Replicate require manual restart; even Debezium propagates the column without backfilling already-shipped rows). Sluice's IR + dual-engine design lets it own the full lifecycle.

## Added

- **`feat(pipeline): online ADD COLUMN forwarding through CDC apply (ADR-0058) (#45)`**

  Two new operator-facing flags:

  - **`--forward-schema-add-column`** (default off): when set, the streamer intercepts `ALTER TABLE ADD COLUMN` events from the source CDC stream and replays them on the target via `ir.SchemaDeltaApplier.AlterAddColumn`. Without the flag, ADD COLUMN events refuse loudly (existing v0.78.x behavior preserved exactly).
  - **`--backfill-added-column`** (default off): when set in conjunction with the forwarding flag, sluice issues a bounded `SELECT pk, <new_col> FROM <source>` against the source database and replays the values as synthetic `ir.Update` events on the target, populating the new column for rows already shipped pre-ALTER. Reuses the existing `BatchedRowReader` + bulk-copy rate-limit primitives; no new throttling code.

  **Scope discipline (ADR-0058 §2)**: ADD COLUMN only in v0.79.0. DROP COLUMN, ALTER COLUMN TYPE, CHECK constraint changes, generated-column changes continue to refuse loudly with the drained-model recovery hint. RENAME COLUMN remains handled by Shape A's existing catalog (v0.78.0); the forwarding intercept is a no-op when Shape A is engaged (the lease's `BoundaryRouter` already handles every recognized shape per ADR-0054 DP-E).

  **Refuse-loudly cases** (each pinned by an integration test):
  - **Computed DEFAULT expression** (e.g. `ADD COLUMN created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP`): refuses loudly because target-session evaluation diverges from source per-row insert. Operator gets a recovery hint pointing at the drained model.
  - **Drop / rename / alter-type** caught by the existing classifier and continue to refuse loudly.
  - **No primary key + backfill requested**: backfill needs PK-keyed iteration; refuses with the operator-actionable hint to either skip backfill or add a PK.
  - **Applier error mid-ALTER**: cache rewind so the next apply tx retries cleanly; position does not advance past the ALTER boundary.

  **Cross-engine semantics**: MySQL → PG and PG → MySQL both work for ADD COLUMN forwarding; the IR's column-type translation (per ADR-0045 + value-types contract) already covers the column-shape mapping. ADR-0058 §5 documents the MySQL INSTANT ADD COLUMN tradeoff — sluice uses standard ADD COLUMN (not INSTANT) for cross-engine parity; INSTANT optimization is tracked for a future minor.

  **Forward-compat with F11 (task #47)**: the F11 schema-drift detection feature complements this — F11 is "tell the operator about the drift loudly," #45 is "do something about it automatically when the operator opts in." Both should be enabled together for the full Debezium-class schema-evolution story.

## Tests

- **`test(pipeline): schema_forward_intercept_test.go`** — 11 unit tests covering the intercept's dispatch: flag off → refuse; flag on → invoke AlterAddColumn exactly once; backfill scope (SELECT shape, batch size, rate-limit reuse); refuse cases (drop/rename/alter-type/computed-default/no-PK); cache rewind on applier error.
- **`test(pipeline): migrate_add_column_forward_pg_integration_test.go`** (3 tests) — PG → PG live CDC; cold-start with flags on; ALTER TABLE ADD COLUMN on source; verify target's column lands; verify post-ALTER INSERTs carry the new column; backfill flag pins target's already-shipped rows have the source's value on the new column.
- **`test(pipeline): migrate_add_column_forward_mysql_integration_test.go`** (2 tests) — MySQL → MySQL parity matrix.
- **`test(pipeline): migrate_add_column_forward_cross_integration_test.go`** (2 tests) — MySQL → PG and PG → MySQL cross-engine pins.
- **Lowercase control**: pre-v0.79.0 flag-off behavior pinned by an integration negative-pin (existing non-forwarded CDC tests continue to pass; ADD COLUMN without the flag still refuses loudly).

## Docs

- **ADR-0058 — Online schema-change forwarding in the CDC apply path** (480 lines, `docs/adr/adr-0058-online-schema-change-forwarding.md`). Covers motivation (F12 + F16 by name), scope split, opt-in flag rationale, backfill design, refuse-loudly catalog, MySQL INSTANT tradeoff, cross-engine spec, F11 forward-compat. Cites the Reddit-research operator quotes from F12 (`u/dani_estuary`, `u/Unlock-17A`, multi-thread schema-change pain cluster).

## Compatibility

- **Minor version bump (v0.78.4 → v0.79.0)** because new operator-facing flags are added. Drop-in upgrade otherwise; operators who don't set either flag see exactly v0.78.4 behavior (ADD COLUMN continues to refuse loudly with the existing recovery hint).
- **Severity a** of closing the silent-feature-gap. Pre-v0.79.0, sluice's only path for ADD COLUMN on a live stream was the drained model (stop sync → apply ALTER on both source + target → re-cold-start). For ops teams running 24/7 multi-tenant workloads, that's a stop-the-world operation. v0.79.0 makes it a continuous operation under opt-in.
- **Shape A streams unchanged**: when `--inject-shard-column` is set, the lease-based coordination at the consolidating boundary already handles ADD COLUMN per ADR-0054. The new intercept is a no-op on Shape A streams.

## Who needs this

- **Operators running continuous-sync mode** with evolving source schemas — the everyone-eventually-hits audience. Add a column on Monday morning without stopping the stream.
- **Operators migrating from Fivetran / Airbyte / DMS** who've been burned by their tool's schema-change handling — F12's "schema change is the dominant CDC pain" cluster cites them by name.
- **Operators NOT enabling the flag** see no observable change — pre-v0.79.0 behavior preserved exactly.

## Cross-references

- [ADR-0058 — Online schema-change forwarding](https://github.com/sluicesync/sluice/blob/main/docs/adr/adr-0058-online-schema-change-forwarding.md)
- [Reddit research findings F12 + F16](https://github.com/sluicesync/sluice/blob/main/docs/research/) — the operator-pain source
- [v0.78.4 release notes](https://github.com/sluicesync/sluice/releases/tag/v0.78.4) — prior release; this builds on the PG RLS preflight as the second positioning feature in the v0.78.x → v0.79.x arc
- [ADR-0054 — Shape A Phase 2](https://github.com/sluicesync/sluice/blob/main/docs/adr/adr-0054-shape-a-phase-2-live-cross-shard-ddl-coordination.md) — the per-shard lease coordination model that handles the same shape on Shape A streams; this ADR's intercept is the non-Shape-A counterpart
- Task #47 (F11 schema-drift detection) — the complementary "tell the operator" half; together with this, the full schema-evolution story
