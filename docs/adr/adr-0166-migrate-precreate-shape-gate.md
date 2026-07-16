# ADR-0166: migrate's pre-create gate for existing target tables (schema-compare-then-skip)

- **Status:** Accepted (implemented; roadmap item 71b)
- **Date:** 2026-07-15
- **Related:** ADR-0165 (the deploy-ddl bootstrap this completes for `migrate`), ADR-0162 (the renderSchema/compare posture cited by the roadmap entry — considered and not chosen, see Alternatives), ADR-0029 (the schema-diff machinery the compare rides), ADR-0108 (the transient-retry wall the pre-gate failure mode hid inside), ADR-0004 (the three-phase apply whose phase 1 this gates).

## Context

Every engine's `CreateTablesWithoutConstraints` emits `CREATE TABLE IF NOT EXISTS`, so a pre-existing same-name table on the target was silently tolerated **whatever its shape**. Two documented consequences:

1. **The mid-copy 1054 retry wall (v0.99.256 cycle observation).** A conflicting pre-existing table surfaces only when the bulk copy's INSERT names a column the table doesn't have — MySQL Error 1054, which the shared applier classifier deliberately treats as retriable "schema drift" (the Bug-F8 self-healing design). A deterministic shape conflict therefore WARN-looped for the full ADR-0108 30-minute wall before failing. Loud, zero-loss, but a terrible diagnostic: ~30 minutes to learn the target had a stray table.
2. **The PlanetScale bootstrap dead end (roadmap item 71b).** On a safe-migrations branch, PlanetScale refuses the CREATE **statement** even when the table exists, so the ADR-0165 deploy-ddl bootstrap — ship the control tables + every user-table CREATE through deploy requests — could feed `backfill` and `sync --schema-already-applied`, but never a fresh `migrate`: its schema-apply phase died on the first user-table CREATE with the item-71c coded refusal, and the docs had to say "migrate cannot skip it yet."

Naive detect-then-skip is a silent-loss hazard: an existing table may carry a DIFFERENT schema, and skipping its CREATE would land rows in the wrong columns or fail late. The correct shape is **detect + schema-compare-then-skip**.

## Decision

A new pre-create gate in the migrate orchestrator (`internal/pipeline/migrate_existing_tables.go`, `phasePlanExistingTables`), engine-neutral by construction, running after the cold-start gate (so `--reset-target-data`'s drops have happened) and before the schema-apply phase:

1. **Detect** by reading the target's existing tables through the target engine's own `SchemaReader` — the exact surface (and trust base) `sluice schema diff` uses, scoped by `--target-schema` and `--enable-pg-extension` the same way. No new engine surface; every engine that can be a migrate target already has a reader.
2. **Compare** each pre-existing same-name table's **column shape** against the intended IR after `translate.RetargetForEngine` (the schema-diff command's own cross-engine normalization, so PG `uuid` vs a bootstrapped MySQL `CHAR(36)` compares equal). The compare is the new focused pure function `irdiff.TableColumnShape`: column **names** (order-insensitive), **types** (the same `ir.Type.String()` rendering the diff command trusts), and **nullability** — with one named carve-out: nullability is not compared for the intended PK's member columns, because both MySQL and PG force PK columns NOT NULL regardless of the declared flag and readers report the enforced state (comparing the redundant flag would false-refuse tables the engine itself normalized).
3. **Verdicts:**
   - absent → create exactly as before;
   - equal shape → **skip** the CREATE, with an INFO naming the table ("target table exists with matching column shape — skipping create");
   - differs → the new coded refusal **`SLUICE-E-TARGET-TABLE-SHAPE-MISMATCH`** (ClassRefusal, exit 3), upfront, naming the table and the first three differing columns (expected vs actual, rendered by the same function the compare uses) with remedies (drop/rename, `--exclude-table`, fix the shape, `--reset-target-data`);
   - compare uncomputable (target reader open/read failed) → WARN and fall back to today's create-everything behavior. The gate must never invent a new failure mode; `CREATE TABLE IF NOT EXISTS` remains the backstop;
   - **no storage-shape mapping for the engine pair** (post-audit-2026-07-16 amendment, see impl notes) → same WARN-and-proceed fallback, naming the tolerated tables. The compare only engages when `translate.HasStorageShapeMapping` holds — the pair shares a storage family (identity read-back is faithful) or a retarget rule exists. A raw compare of source-native IR against a foreign catalog's lossy read-back mistakes translation for drift and must never refuse.

**Scope decisions (each deliberate):**

- **Only the CREATE phase consumes the pruned set.** The gate returns a shallow schema clone with the skipped tables removed; bulk copy, identity sync, indexes, constraints, and views keep the full schema — a pre-created table still receives its data, and the index/constraint phases are already detect-then-skip idempotent, so a bootstrapped table that carries them converges cleanly.
- **Indexes, constraints, defaults, generated expressions, comments, and charset/collation are OUTSIDE the compare.** Later phases create indexes/constraints idempotently, and a deploy-ddl-bootstrapped table legitimately carries them already; defaults and charsets are the schema-diff's documented cross-engine noise sources and don't affect whether the copy can land rows faithfully. A default/generated divergence on an otherwise shape-equal table is accepted, exactly as `IF NOT EXISTS` accepted it before — the gate only ever tightens.
- **`--resume` skips the gate entirely.** The prior attempt already created (or validated) the tables and the long-standing resume contract is the idempotent re-CREATE; re-comparing would add a round-trip-fidelity failure mode (intended IR vs read-back IR) to a path that has none.
- **Migrate-only.** The sync cold-start and add-table paths keep the IF-NOT-EXISTS create: sync has `--schema-already-applied` plus the COLDSTART-TARGET-NOT-EMPTY data gate, and add-table's early-create races a live CDC stream — extending the gate there is future work, noted below.
- **No new flag.** The compare is strictly safer than blind `IF NOT EXISTS` (zero-value-safe: programmatic callers of `runBulkCopyPhases` pass a nil create-schema and keep the old behavior verbatim), and the uncomputable-compare path degrades to today's semantics with a WARN rather than a refusal.
- **Data in a pre-existing equal table is NOT this gate's concern** — the Bug 9 cold-start preflight (refuse populated target tables) runs before it and is unchanged. The bootstrap tables the gate exists for are empty.

## Consequences

- The ADR-0165 bootstrap story is complete for all three commands: control tables + user-table CREATEs shipped via `deploy-ddl` now feed `backfill`, `sync --schema-already-applied`, **and a fresh flagless `migrate`** — the "migrate cannot skip it yet" clauses in the 71c hint, error-codes rows, and managed-services doc are gone.
- A conflicting pre-existing table fails in seconds with a named-column diagnostic instead of ~30 retried minutes; nothing is created and zero rows are copied (pinned).
- Cost on the happy path: one target `ReadSchema` per migrate (per database in multi-DB fan-out) — the same catalog sweep `schema diff` performs.
- **Residual risk, stated honestly:** the equal-verdict relies on the write→catalog→read-back round trip landing on the same IR the (retargeted) intended schema carries. Same-engine round trips are exercised by the real-MySQL/PG integration pins across several type families; exotic cross-engine mappings that `RetargetForEngine` doesn't mirror (e.g. MySQL `BIGINT UNSIGNED` → PG `NUMERIC(20,0)`) can render a genuinely-matching pre-created table as a MISMATCH. That failure is a LOUD refusal with both renderings shown and `--exclude-table`/drop remedies — never silent — and matches the schema-diff command's existing noise profile for the same pairs. Extending `RetargetForEngine`'s rule table fixes both consumers at once.
- Follow-ups flagged: (a) a live psverify leg driving deploy-ddl-bootstrap → fresh migrate on a real safe-migrations branch (the fake-level story pin exists); (b) evaluating the same gate for the sync cold-start path; (c) ~~the `vitess` flavor name is missing from `retargetRuleFor`'s MySQL-family match~~ (closed by the amendment below — `retargetRuleFor` now keys on the storage families).

## Implementation notes (2026-07-16 amendment — audit HIGH-1)

The first cut shipped the "residual risk" above as a functional regression: `retargetRuleFor` covered only `postgres→{mysql,planetscale}`, so for **every mysql→postgres run** the compare put MySQL-native IR against the PG catalog's lossy read-back — `INT UNSIGNED` vs `BIGINT`, `TEXT` tier collapse, `VARBINARY` vs `BYTEA` — and refused rc=3 on tables sluice itself had created (re-runs after TRUNCATE, deploy bootstraps, sibling-shard consolidation), with a hint that led operators toward the data-destroying `--reset-target-data`. The AutoIncrement carve-out (`a53a081c`) had patched one leaf of exactly this class. Live-reproduced by the 2026-07-16 confirming audit (HIGH-1). The class fix:

- **The compare is gated on `translate.HasStorageShapeMapping`** (same storage family, or a retarget rule). Unmapped pairs get the WARN-and-proceed fallback naming the tolerated tables — restoring pre-ADR-0166 behavior for mysql→postgres/sqlite→anything rather than refusing on a comparison the translation layer can't normalize. A future `mysql→postgres` retarget rule (mirroring `postgres/ddl_emit.go`'s families) would re-arm the gate for that direction AND de-noise `schema diff` — deliberately deferred as a separate chunk with its own family-matrix pins.
- **`retargetRuleFor` keys on storage families**, closing follow-up (c): `postgres`/`postgres-trigger` sources × all MySQL-dialect targets (`mysql`, `planetscale`, `vitess`) share the one rule.
- **The refusal hint leads with the non-destructive remedies** (`--exclude-table`, inspect against `schema preview`); `--reset-target-data` is named last, as a last resort, with its drops-every-in-scope-table blast radius spelled out.
- Pins: `TestHasStorageShapeMapping` (translate), `TestMigrateShapeGate_PairMatrix` + `TestMigrateShapeGate_HintNeverLeadsWithReset` (pipeline unit), and the end-to-end `TestMigrate_ExistingTableGate_MySQLToPG_ReRun` integration repro (migrate → TRUNCATE → migrate again on real MySQL+PG).

## Alternatives considered

- **Per-engine writer-level detect+compare (catalog string compare against the emitter's rendering).** Faithful to "as the writer would emit" but needs three-plus engine implementations, each with its own normalization hazards (MySQL int display widths, PG `format_type` spellings, serial/identity rewrites) — every hazard a potential false refusal. The reader/IR route reuses the battle-tested schema-diff normalization once, at the orchestrator, and stays correct for future engines for free.
- **The ADR-0162 renderSchema/raw-DDL compare posture.** Byte-comparing rendered DDL works on PlanetScale because both sides come from the same renderer; on a general target the intended side has no rendered form until it's created — the IR is the only shared contract (the IR-first tenet).
- **Applying the gate inside `CreateTablesWithoutConstraints` (all callers).** Would cover sync/broker/add-table too, but puts an orchestrator-policy decision inside every engine writer, and the add-table path's early-create + live-applier interplay makes a new refusal there genuinely risky. Deliberately narrowed to migrate.
- **Comparing defaults/indexes/constraints too.** Rejected: false-refusal surface (cross-engine default spellings; bootstrapped indexes) with no copy-correctness payoff; documented as the gate's scoping instead.
