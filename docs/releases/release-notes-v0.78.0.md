# sluice v0.78.0 — RENAME COLUMN shape (ADR-0054 catalog expansion, #22 sub-task) + #39 end-to-end GC pin

**Headline:** Minor bump driven by **RENAME COLUMN** entering ADR-0054's recognized-shape catalog — one of the three v1-deferred sub-shapes (the other two, CHECK constraints and generated-column changes, stay deferred). Also bundles in the **#39 real-engine GC integration test** the Bug 85 saga revealed was missing — closes the structural test/production-surface-mismatch hazard durably.

## Added

- **`feat(pipeline, engines/{postgres,mysql}): ADR-0054 catalog expansion — RENAME COLUMN shape (#22 sub-task)`**

  The classifier (`pipeline.ClassifyShape`) now recognizes RENAME when the IR delta between pre- and post-DDL `SchemaSnapshot` boundaries shows **exactly 1 added + 1 dropped column** with full `ir.Column` attribute equality minus `Name` (Type, Nullable, Default, comment, etc.) — the signal both PG and MySQL `RENAME COLUMN` preserve.

  - New `ShapeKindRenameColumn` shape carries `RenamedColumnBefore` + `RenamedColumnAfter`.
  - New `ir.ShapeDeltaApplier.AlterRenameColumn(ctx, table, oldName, newName) error` emits the per-engine DDL (PG: `ALTER TABLE "<schema>"."<table>" RENAME COLUMN "<old>" TO "<new>"`; MySQL: backtick-quoted equivalent) with detect-then-RENAME idempotency via `information_schema.columns`.
  - New `ir.ShardConsolidationProber.ProbeRenameColumn(ctx, table, oldName, newName, want)` mirrors the v0.76.0 ProbeAlterColumnType v2 silent-divergence catch: returns `Applied` when newName is present + oldName absent + observed IR type matches `want.Type`, `NotApplied` when oldName-still-present, `Inconsistent` + error naming the mismatch when newName is present with the WRONG type (catches a drop+re-add silent-divergence the existence-only check would miss).
  - The `BoundaryRouter` dispatches the new shape to `applier.AlterRenameColumn` on apply and to `prober.ProbeRenameColumn` on takeover.

  **Out of v1 scope (still refuses loudly)**: multi-column rename in a single source DDL (`added=N + dropped=N` for N>1 — the type-match heuristic makes the old→new pairing ambiguous); CHECK constraint changes; generated-column changes.

  **Indistinguishable-from-drop-add-same-attrs edge**: at the IR level a literal `DROP COLUMN foo; ADD COLUMN bar <same-attrs>` is byte-identical to `RENAME COLUMN foo TO bar`. The classifier treats both as rename — correct from a CDC apply perspective since the operator's load-bearing intent is preserving column data under a new identifier (PG and MySQL `RENAME COLUMN` both preserve row data; auto-applying as rename gives the operator the same data-preserving target outcome either way). Documented in the ADR-0054 v0.78.0 amendment as intentional.

## Docs

- **`docs(adr-0054): v0.78.0 closure update — RENAME COLUMN shape`** appended to ADR-0054 documenting the detection heuristic + indistinguishable-edge intentionality + multi-column scope decision + the two remaining #22 sub-shapes (CHECK + generated-column) still deferred.

## Tests

- **Bug 85 saga structural closure (#39)** — new `internal/pipeline/shard_consolidation_lease_gc_end_to_end_integration_test.go` exercises `engageShardCoordination` → real Source + Target PG engines → real ChangeApplier → `mgr.WithGC` populated → heartbeat-driven sweep observed via the lease table state. Uses a "keepalive lease" pattern (second never-Applied lease to keep the heartbeat alive, since `Apply` stops the holding lease's heartbeat) + `gcEveryNTicks=3` override + `RetryPeriod=100ms` for a ~300ms sweep cadence with a 30s deadline. Compile-pins on each `gcDeps` field would have caught both Bug 85 (nil gcDeps) AND Bug 85.b (nil Orderer) immediately. The structural fix for the Bug 85 saga's surface-mismatch class.

- **RENAME COLUMN pin matrix**:
  - Unit pins in `shard_consolidation_probe_test.go`: ClassifyShape recognizes simple RENAME, REJECTS rename-like cases that don't match the heuristic (same name change with type-differs / nullable-differs, multi-column rename `N=2`, just-added or just-dropped staying on existing paths).
  - Integration pins on both engines: `ALTER TABLE ... RENAME COLUMN ... TO ...` → Shape A coordination applies it on target → row data preserved + accessible under new name (`shard_consolidation_rename_pg_integration_test.go`, `shard_consolidation_rename_mysql_integration_test.go`).
  - Probe pins on both engines: rename idempotent roundtrip, wrong-type-on-target refuses as `Inconsistent`, both-names-still-present refuses loudly.

## Compatibility

- **Drop-in upgrade from v0.77.1.** Behavior change at the catalog level: a single source `RENAME COLUMN` previously hit the v1 catalog's Unrecognized refusal (forcing operators to drop the live-coordination via `--no-coordinate-live-ddl` and use the drained model). v0.78.0 auto-applies it. Operators who relied on the refusal as a soft no-op can add `--no-coordinate-live-ddl` for v0.77.x semantics.
- **Multi-column rename, CHECK constraint changes, generated-column changes** still refuse loudly (Unrecognized) and route through the drained-model recovery hint. The three remaining sub-shapes are tracked as #22 follow-ups; the catalog expansion intentionally lands one shape at a time per the loud-failure-before-feature-velocity tenet.
- **Bug 85 saga**: the v0.77.1 fix is verified by the new #39 end-to-end test against real engines. No production code change in this release for Bug 85; the test is the durable regression-guard.
- **MySQL paths**: receive the same recognized-shape catalog expansion. MySQL `RENAME COLUMN` ships in 8.0.3+ which is the sluice-supported floor.

## Who needs this

- **Sharded source operators consolidating into one target** running Shape A with `--inject-shard-column`. A single column rename in the source schema no longer drains every shard.
- **Anyone NOT running Shape A** sees no observable change — the new shape applies only when live coordination is engaged.

## Cross-references

- [ADR-0054 — Shape A Phase 2: live cross-shard DDL coordination](https://github.com/sluicesync/sluice/blob/main/docs/adr/adr-0054-shape-a-phase-2-live-cross-shard-ddl-coordination.md) *(v0.78.0 closure update appended)*
- [v0.77.1 release notes](https://github.com/sluicesync/sluice/releases/tag/v0.77.1) — prior release; this builds on the Bug 85.b wire-up correction
- [#22 catalog expansion](https://github.com/sluicesync/sluice) — RENAME shipped; CHECK + generated-column remain
