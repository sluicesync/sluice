# ADR-0187: sluice places its own control tables in Neki's authoritative shard group

- **Status:** Accepted — implemented 2026-09-14, shipped in the release that carries this line (see `CHANGELOG.md`). This is the control-table half of NEKI-NK306; the user-table half is the classification `SLUICE-E-TARGET-SHARD-KEY-MISSING` described below.
- **Date:** 2026-09-14
- **Related:** [ADR-0186](adr-0186-neki-is-a-postgres-flavor.md) (Neki is a flavor of the `postgres` engine, detected at runtime — the context this sits inside, and the source of probe R-1, which is why the heartbeat table is exempt from placement); `internal/engines/postgres/neki_control_placement.go` (the helper and the pure planner); `internal/engines/postgres/neki_shard_key_missing.go` (the NK306 classification and its own sibling sweep); `internal/appliershared/control_table_roster.go` (the single-sourced control-table names the gate cross-checks against); `docs/dev/neki-readiness.md` and `docs/operator/planetscale-postgres-to-neki.md` (the measured history and the operator-facing procedure).

## Context

sluice's control tables — the CDC position `sluice_cdc_state`, the migrate breadcrumbs `sluice_migrate_state` and `sluice_migrate_table_progress`, the schema-history ledger, the shard-consolidation lease, the skipped-table ledger, the target-metrics history and the keyset store — carry no shard key, because on every other engine they have no reason to. On a **sharded** PlanetScale Neki the database's default shard group covers `public`, so every `INSERT` into them is refused for want of a routing column they do not have:

```
ERROR: shard-key column "tenant_id" of primary index 0 is required
but missing from INSERT (SQLSTATE NK306)
```

Measured 2026-09-10 and bisected 2026-09-14 on live 2- and 3-shard clusters. It blocks **both** CDC apply lanes — the serial lane reaches its *position* write, meaning the data `INSERT` had already succeeded and only sluice's own bookkeeping failed — and it is what turned a 40-row keyless table into 80 rows across a `--resume`, because the breadcrumb that would have said *already copied* could not be written. That resume defect is documented in full in `docs/dev/neki-readiness.md`; the loud half of it now refuses as `SLUICE-E-MIGRATE-PROGRESS-UNRECORDABLE`, but the underlying inability to write the table remained.

**The structural fact that shapes every option:** Neki's data topology declares a `shard_group` **per table**, in an operator-declared document, and `CREATE TABLE` does not enrol anything into it — the MoveTables arm proved that independently with `NK604` on a table that plainly existed and held rows. So a table sluice creates is absent from the document *by construction* and falls to the sharded default. **sluice cannot fix this by creating the table differently.** Whatever the fix is, it is not a DDL change.

## Ground truth (live 2-shard PS-10 cluster, nekiverify run `34910229846`, 2026-09-14)

Three candidate placements were probed before any of this was designed (`nekiverify_control_schema_probe_test.go`), and two were rejected by measurement rather than by argument:

- **A separate SCHEMA does not escape the default group.** A shard-key-less control table in its own schema is refused with `NK306` **identically** to one in `public`; the control arm (the same table in `public`) held first, so the answer is the schema's, not the fixture's. The default group reaches unlisted schemas too. This was the preferred option going in, and the probe is the only reason it was not designed on.
- **A CONSTANT shard-key column works** through the full control-table lifecycle (`INSERT`, the `UPDATE` that rewrites `source_position` on every committed batch, and the unpinned `SELECT` a resume reads it back with). It is viable, and it makes control-table DDL depend on the **target's** routing column — a first for any engine.
- **Assignment to the AUTHORITATIVE shard group works, and sluice's own role can make it.** A control table assigned there accepts a shard-key-less `INSERT` and reads back unpinned; the topology accepted the placement at revision 36020 under sluice's own credentials. PlanetScale's own Neki guidance says the authoritative group "holds unsharded data" and should be "sized for metadata, catalog work, and sequences" — which is exactly what a CDC position and a migrate breadcrumb are.

Because placement means sluice issuing a **cluster-wide topology revision** as a side effect of creating a control table — a far heavier action than `CREATE TABLE` — the write semantics were measured before any production code called `set_data_topology` (`nekiverify_topology_write_probe_test.go`):

- **`__neki.get_data_topology_revision()` exists and returns `bigint`.** The vendor's data-topology page documents the `expected_revision` *option* of `set_data_topology` but not how the current revision is **read**; the function was found by enumerating `pg_proc` on a live router. A router that does not have it answers `42883` (`undefined_function`), which sluice treats as "no revision available" rather than as an error.
- **Writing an UNCHANGED document still mints a new revision** — 36469 then 36472 across two writes of the same bytes. So compare-before-write is **load-bearing**, not an optimisation: `EnsureControlTable` runs on every start, and without the comparison every start would bump a revision every router in the cluster has to converge on.
- **A stale `expected_revision` is refused as `success=false, revision=0`** — data, not an error. The current revision is accepted.
- **`__neki.wait_for_data_topology(revision bigint, timeout interval DEFAULT NULL)`** exists, and the one-argument form works.

## Decision

**On a Neki target, sluice assigns its own control tables to the target's authoritative shard group by writing the data topology itself, at control-table creation, and only when a shard index would otherwise route them.**

The helper is `ensureNekiControlTablePlacement`, called at the four doors that create control tables; the decision half is the pure function `planNekiControlPlacement`, which is graded against measured documents without a cluster. The behaviour, stated as the properties that matter:

- **A no-op off Neki.** Every other PostgreSQL target is byte-identical to before; the helper returns immediately when the flavor probe says this is not a Neki.
- **A no-op on a Neki that does not route the tables.** The predicate for "needs placing" is that a shard index currently resolves for the table, which is precisely the condition under which the router demands a shard key on `INSERT`. An unsharded Neki database therefore never has its topology written by sluice.
- **Compare before write.** The plan reports `changed=false` when the placement already holds, and nothing is written. A start does not mint a revision it does not need.
- **The document is edited as generic JSON with `UseNumber()`**, so every field sluice has never heard of round-trips intact in value — the platform is in preview, and a field sluice does not understand must survive a sluice write. The write carries the comment `sluice: place control tables in the authoritative shard group`, so the placement is attributable in the topology's own change log.
- **After a successful write sluice calls `wait_for_data_topology`** so the control-table write that follows cannot land on a router still routing the table by shard key. Best-effort: the topology write has already succeeded, and a router that has not converged refuses the next write **loudly** (`NK306`) rather than accepting it wrongly, so a failed wait is logged and not fatal.

Four decisions inside that are worth stating as decisions:

**1. Placement beat the constant-shard-key fallback.** No DDL shape change, no target-dependent column, no operator prerequisite, and the tables land where the platform's own guidance says metadata belongs. The constant-shard-key result **stays on file** as the fallback if topology writes prove unacceptable for *governance* reasons rather than technical ones — it is measured working, so choosing it later is a design change, not a research problem.

**2. A topology READ failure is a WARN; a topology WRITE failure is a coded refusal.** These are deliberately asymmetric. The consequence of *skipping* placement is the same loud `NK306` this exists to prevent — never silent loss — and refusing on a read failure would take down an **unsharded** cluster whose role merely lacks `SELECT` on the topology, which is a configuration that works perfectly today. A write failure is different: it is the governance door. A cluster that denies sluice's role the topology write gets `SLUICE-E-TARGET-CONTROL-TABLE-PLACEMENT` at control-table creation, before any data moves, naming the exact entry to add by hand — rather than dying later, mid-copy, on `NK306`. **There is deliberately no opt-out flag**: the next write to a control table would be refused anyway, and the refusal names the manual placement, so an operator is never stuck. If demand for a flag appears, it has a home; nothing about the shape prejudges it.

**3. Concurrency is read-modify-write with `expected_revision`, bounded to four attempts.** Two sluice processes starting against one cluster are a lost-update hazard on a shared document. Each attempt re-reads the topology **un-memoised** (a cached copy is exactly the wrong thing to plan a write from), re-plans, and passes the revision it read as `expected_revision`; a conflict re-enters the loop, and a process that loses the race normally finds the other's placement already in the document and writes nothing. Where the router exposes no revision the write is last-writer-wins — still correct for two sluice processes, because they compute the same placement, and unsafe only against a concurrent **operator** edit of the same document.

**4. NK306 itself is now classified** (`SLUICE-E-TARGET-SHARD-KEY-MISSING`), terminal, because the statement's *shape* is what is refused and the same shape is refused on retry. Until 2026-09-14 the code appeared in this repository only as prose — the engine graded `NK013`, `NK205` and `NK213` and nothing else in the NK3xx range — so a refusal the suite had measured twice still carried no verdict, which is the same class as the unclassified `NK205` that killed a full-volume copy in v0.152.1. With placement in front of it, the remaining reach of this code is the path where placement was **skipped** because the topology could not be read and the database turned out to be sharded after all, plus user tables whose target shard key the source rows do not carry.

### The sibling sweep

Eight control-table name constants across four doors, enumerated rather than promised:

| door | tables |
| --- | --- |
| `ChangeApplier.EnsureControlTable` (`change_applier.go`) | `sluice_cdc_state`, `sluice_cdc_schema_history`, `sluice_shard_consolidation_lease`, `sluice_cdc_skipped_tables` |
| `ChangeApplier.EnsureTargetMetricsHistory` (`target_metrics_history.go`) | `sluice_target_metrics_history` |
| `MigrationStateStore.EnsureControlTable` (`migration_state.go`) | `sluice_migrate_state`, `sluice_migrate_table_progress` — **migrate's door AND restore's** |
| `pgKeysetStore.EnsureKeysetTable` (`keyset_store.go`) | `sluice_keysets` |

Exempt, with reasons: the **source-side heartbeat** table, whose name arrives as a parameter from the pipeline rather than as a package constant and which is written on the SOURCE — and a Neki cannot be a continuous-sync source (ADR-0186 probe R-1), so it cannot be a sharded Neki table sluice writes; the **migrate-state breadcrumb's** NK306, which keeps surfacing as the more specific `SLUICE-E-MIGRATE-PROGRESS-UNRECORDABLE` rather than gaining a second, competing code on the same failure; and the **bulk-copy `COPY` writer**, which does not route through the applier's classifier at all (the same exemption `neki_blocked_table.go` already records for `NK213`) — a `COPY` without the shard key is a schema-level mismatch whose right home is the preflight's topology check, and it is filed as such.

The enumeration is a **gate**, not a list in a commit message: `TestEveryPostgresControlTableIsPlacedOnNeki` reads the package's own `*TableName` constants off the AST, requires each to be handed to `ensureNekiControlTablePlacement` somewhere in the package's non-test sources (or to appear in an exemption map *with a reason*), requires the reverse — nothing placed that is not such a constant, and nothing both placed and exempt — and cross-checks every placed name against `appliershared.IsControlTable` so the placement and the roster the schema readers exclude by cannot diverge. Anti-vacuity floor: at least eight constants and at least four call sites, because fewer means the scan broke rather than that the world shrank. A control table added tomorrow is covered the day its constant is declared.

## Alternatives considered

- **Put the control tables in their own schema.** Rejected by measurement: refused identically to `public`. This was the cleanest option on paper and it is simply false on this platform.
- **Give the control tables a constant shard-key column.** Measured working end to end, and rejected on cost: control-table DDL becomes target-dependent for the first time on any engine, and it carries a state-format change older binaries must still read. Kept on file as the fallback.
- **Reference tables / GSIs**, both named on the same vendor page. Not candidates on current evidence: the page documents neither's mechanics, a GSI maps a lookup key to an owner row's shard key (a control table has no owner row), and "duplicate across shards" has different semantics for a *written* checkpoint than for a read-mostly lookup. Worth revisiting only if the mechanics get documented.
- **Refuse on a topology READ failure too, for symmetry.** Rejected: it would break unsharded Neki clusters whose role lacks `SELECT` on the topology, to prevent a failure that is already loud.
- **An opt-out flag for the topology write.** Not built; demand-gated. The refusal already names the manual placement, so the operator's path is open without one.

## Consequences

- **On a sharded Neki, sluice now writes the cluster's data topology.** That is a genuinely heavier side effect than creating a table, and it is the thing an operator should know about this change. It happens at most once per (database, schema, control-table set) — compare-before-write means a steady-state start writes nothing — it is attributable in `__neki.list_data_topology_changes()` by its comment, and it never touches a topology sluice did not need to change.
- **Resume compatibility is unaffected.** The table NAME, schema and column shape do not change — only the topology group the router uses to route it. A binary that wrote these tables before this change finds them unchanged afterwards; a pre-fix cluster's control tables are placed on the first post-fix start, in one revision, once.
- **Two new coded refusals** join the operator-facing surface: `SLUICE-E-TARGET-CONTROL-TABLE-PLACEMENT` (placement could not be made — role denied, no `authoritative_shard_group`, that group itself declares a `default_shard_index`, or an operator has pinned a control table to a shard index sluice will not overwrite) and `SLUICE-E-TARGET-SHARD-KEY-MISSING` (`NK306` itself, terminal).
- **Two nekiverify arms are unblocked** and should go green on the next run: CDC into a sharded target, and the NK306 bisect. The filed **backup/restore-into-Neki** arm was deliberately blocked on this fix — restore writes `sluice_migrate_state` through the same door — and can now be built, where a failure would be information rather than a re-learning of something known.
- **A Neki SOURCE running the pgtrigger engine is out of scope and filed.** Trigger-CDC's capture tables live on the source and have never been run against a sharded Neki; nothing here reaches them, and nothing here claims to.
- **The probes convert.** `nekiControlTableSchemaProbe` and its siblings were written to pass on any *conclusive* answer because sluice had no behaviour to check; with the behaviour shipped, the authoritative-group arm becomes a premise check for the placement this ADR decides on — a weekly guard that the platform has not changed the answer underneath the design.
