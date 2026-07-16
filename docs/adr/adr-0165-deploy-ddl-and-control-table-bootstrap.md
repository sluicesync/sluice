# ADR-0165: `sluice deploy-ddl` + the control-table bootstrap (`sluice control-tables ddl`, coded Error-1105 refusal)

- **Status:** Accepted (shipped v0.99.254; roadmap item 66)
- **Date:** 2026-07-15
- **Related:** ADR-0162 (the expand-contract orchestration this extracts from — the leg machine, the stale-base freshness gate, the safe-migrations findings), ADR-0148 (the deploy-request ground truth), ADR-0082 (the migrate-state tables the bootstrap ships).

## Context

ADR-0162's "Future scope" demand signal arrived the day it shipped: operators want to ship *one arbitrary DDL statement* to a PlanetScale production branch with the same safety wrapper expand-contract built — not raw deploy-request CRUD (the pscale CLI owns that), but the wrapper: the **stale-base freshness gate** (a fresh PS branch can silently propose REVERTING recent production schema; sluice's gate is currently the only implementation anywhere), refuse-on-leftover deterministic branches, the tolerant DR poller, skip-revert finalize, and always-cleanup.

The load-bearing consumer is sluice's own **control-table bootstrap**. On a safe-migrations branch, PlanetScale refuses every direct DDL statement (Error 1105 "direct DDL is disabled") — including the `CREATE TABLE IF NOT EXISTS` for `sluice_migrate_state` / `sluice_cdc_state` and siblings. `sluice expand-contract` ships its tables inside the expand deploy request (the ADR-0162 live finding #2 fix), but plain `backfill` / `sync` / `migrate` have no deploy-request channel: their first run against a safe-migrations branch died on a raw driver error with no named way out.

## Decision

### 1. `sluice deploy-ddl` — the extracted single leg

A new command shipping ONE verbatim `--ddl` statement through the governed channel: preflight (token/org/database/branch + the safe-migrations prerequisite, refused coded, never auto-enabled) → dev branch with the freshness gate → apply the DDL on the branch → deploy request → deploy → skip-revert finalize → cleanup. `--dry-run` prints the plan with zero control-plane calls (pinned); deploy-ddl has no data plane at all, so a dry run touches nothing. The three ADR-0162 codes are reused unchanged (`SLUICE-E-PS-{SAFE-MIGRATIONS-DISABLED,DEPLOY-REQUEST-FAILED,BRANCH-STALE-BASE}`); no new failure classes exist in this command.

**Extraction, not a fork.** The deploy-leg machinery moved out of the expand-contract `Orchestrator` into a package-internal `legRunner` (`internal/planetscale/expandcontract/legrunner.go`): branch provisioning + freshness gate + rebase backup, the DR poller, drFailure, and `branchCleanup`. Everything command-specific is a field — narration/error prefixes and four operator-guidance strings spliced into the shared failure shapes (leftover branch, `no_changes`, the two timeouts) — so expand-contract's two legs and deploy-ddl's single leg compose the *same* machine. Expand-contract's guidance keeps its `--resume-from` shape; deploy-ddl's says "there is nothing left to run" (a deployed DDL ends the story). The dev-branch name is deterministic on the DDL alone (`sluice-ddl-<10-hex>`), preserving the refuse-on-leftover crash story. The runner stays package-internal (same package, unexported): a future `sluice deploy-request` generalization can promote it, and promoting an unexported type is additive.

### 2. `sluice control-tables ddl` — the bootstrap printer

A standalone read-only printer, not a `--emit-control-table-ddl` flag on deploy-ddl: deploy-ddl's org/database/token flags are kong-`required` and credentialed, so a print-only mode would need them all demoted plus a mode switch, muddying both contracts — while a separate printer needs no credentials and composes with any channel (deploy-ddl per statement, the PlanetScale UI, a reviewed migration file). The `control-tables` command group also gives future control-table tooling (roadmap item 65) a home.

It prints the five-table bootstrap set — `sluice_migrate_state`, `sluice_migrate_table_progress` (the migrate/backfill state store), `sluice_cdc_state`, `sluice_cdc_schema_history`, `sluice_shard_consolidation_lease` (the sync applier's ensure set) — via a new optional engine surface `ir.ControlTableDDLProvider`, **single-sourced**: the engine returns the same strings its own Ensure\* paths execute, extracted into shared builders (`controlTableDDL`, `schemaHistoryTableDDL`, `shardConsolidationLeaseTableDDL`, `migrateStateHeaderDDL`, `migrateProgressDDL`) and pinned byte-identical by test. Output is pure SQL + `--` comments, pasteable per statement. `--engine` defaults to `planetscale` (the bootstrap consumer); the whole mysql family prints the same dialect; engines without the surface are refused by name. Names are unqualified — the sidecar `--control-keyspace` variant is out of scope until a sharded-bootstrap consumer appears.

### 3. Error 1105 classified at every control-table DDL site — and detect-then-create everywhere

Every mysql control-table DDL site now classifies the safe-migrations refusal into a new coded refusal, **`SLUICE-E-PS-DIRECT-DDL-BLOCKED`** (ClassRefusal — retry won't help; the remedy is a named workflow), whose message and hint name the bootstrap path and echo the exact refused statement (whitespace-collapsed, deploy-ddl-pasteable). Sites: the five bootstrap CREATEs, the `sluice_keysets` and `sluice_target_metrics_history` CREATEs (not in the printed set — their coded refusal echoes the statement instead), every detect-then-ALTER column migration, and the item-65a LONGTEXT widen (which keeps its bespoke prose and `ErrSafeMigrationsBlocked` sentinel, now coded). The sentinel stays in the chain at every site for `errors.Is` callers.

A new code rather than generalizing `SLUICE-E-INDEX-DIRECT-DDL-DISABLED`: that code is frozen with INDEX-domain semantics (post-copy index build, ClassRuntime, remedy = disable the toggle), and codes are a compatibility promise — relabeling its meaning would break both the domain grouping and the published remedy row.

**Companion fix (required for the bootstrap story to be true):** the CDC applier's ensure set — `sluice_cdc_state`, `sluice_cdc_schema_history`, `sluice_shard_consolidation_lease` — was still bare `CREATE TABLE IF NOT EXISTS`, and PlanetScale refuses the *statement* whether or not the table exists (ADR-0162 live finding #2). Shipping the tables via deploy-ddl would therefore not have unblocked `sync`: its next start would still 1105 on the no-op CREATE. All three are now detect-then-create (zero DDL when current), mirroring the v0.99.248 migrate-state gate, with the zero-DDL property pinned per table. The roadmap item's wording already assumed this ("the v248 detect-then-create means it only fires when the tables genuinely don't exist yet"); this closes the gap between that assumption and the code.

## Consequences

- The safe-migrations bootstrap is a documented three-command story: `sluice control-tables ddl` → `sluice deploy-ddl --ddl '<statement>'` (×5) → run `backfill`/`sync`/`migrate` normally. The coded refusal names it at the exact moment an operator first hits the wall.
- One new code (`SLUICE-E-PS-DIRECT-DDL-BLOCKED`) + doc row; the deploy-ddl command reuses the three ADR-0162 codes unchanged.
- Applier/store startup on the ensure paths costs up to three extra `information_schema` lookups (once per start) in exchange for the zero-DDL idempotency safe migrations mandates.
- The expand-contract orchestrator's public behavior is unchanged (all prior pins pass); its error prefixes are preserved, and two guidance strings were generalized from "the %s DDL" to "the DDL" to share the template.
- `ir.ControlTableDDLProvider` is optional and type-asserted (the `MigrationStateStoreOpener` pattern); postgres can adopt it if a managed-PG DDL-governance analog ever appears.

## Alternatives considered

- **`--emit-control-table-ddl` on deploy-ddl (rejected).** See §2 — required-flag/credential contamination of a print-only mode.
- **deploy-ddl auto-bootstrapping the control tables itself (rejected).** A `--bootstrap-control-tables` convenience would couple the generic command to sluice's own schema and hide five production schema changes behind one flag; the printer keeps every shipped statement visible and operator-reviewed.
- **Generalizing `SLUICE-E-INDEX-DIRECT-DDL-DISABLED` (rejected).** See §3.
- **Multi-statement `--ddl` (deferred).** One statement per deploy request keeps the DR diff, the refusal echo, and the leftover-branch identity all trivially attributable; the bootstrap is five short commands. Revisit on demand.
