# sluice v0.72.2 — Shape A on MySQL with AUTO_INCREMENT PK works now (Bug 82)

**Closes the known follow-up from v0.72.1.** The canonical MySQL PK shape — `id BIGINT AUTO_INCREMENT PRIMARY KEY` — was incompatible with ADR-0048's Shape A rewrite in v0.72.0 + v0.72.1: the rewrite moved `id` from leading-PK position to trailing position behind the discriminator, and MySQL rejected the CREATE TABLE with `Error 1075 (Incorrect table definition; there can be only one auto column and it must be defined as a key)`. The v0.72.1 release notes named the workaround ("use a non-AUTO_INCREMENT PK on the source or migrate to PG"), but most operators don't control their source schemas — AUTO_INCREMENT is the typical MySQL identity-management shape, and operators expected Shape A to handle it.

**Drop-in upgrade from v0.72.1.** No CLI surface change, no storage shape change. Non-MySQL targets are entirely unaffected. MySQL operators who used the v0.72.1 workaround see no observable change; the workaround note is now obsolete and the AUTO_INCREMENT path "just works."

## Fixed

- **`fix(engines/mysql): Bug 82 — synthesize supporting UNIQUE KEY when AUTO_INCREMENT is demoted by Shape A rewrite`.** When the rewritten PK contains an `AUTO_INCREMENT` column that doesn't lead, the MySQL DDL emitter now synthesizes a `UNIQUE KEY uq_<table>_<col> (<col>)` inline in the `CREATE TABLE`. MySQL's "every auto column must be a leading key column" rule is satisfied via the secondary unique index instead of the PK lead. The ADR-0048 DP-2 leading-shard invariant (discriminator-first in the composite PK) is preserved — option (a) "engine-specific PK ordering" would have broken DP-2's correctness; option (d) "demote AUTO_INCREMENT on the target" would have removed source-side identity management; option (c) "refuse loudly" would have unnecessarily burdened operators on a routine schema shape. Owner picked (b) (the synthesis path) via the ADR-0048 Amendment 2026-05-22 dialogue. Synthesis is scoped to the in-PK-but-not-leading case — the existing v0.49.0 / GitHub #25 non-PK auto-column behavior (operator-declared supporting index OR loud error) is preserved exactly.

## Tests

- **Unit pins** (`internal/engines/mysql/bug82_autoincrement_pk_demotion_test.go`) — five tests: the synthesis case, the end-to-end `emitTableDef` output including the DP-2 PK-leading invariant, regression guard for the standard `id AUTO_INCREMENT PK` shape (must still return nil; no synthesis), precedence rule (operator-declared index wins over synthesis), scope-narrow guard (no synthesis when auto col is not in PK).

- **Integration pin** (`TestMigrate_MySQL_ShapeA_Bug82_AutoIncrementInPK`) — full MySQL → MySQL Shape A migrate path against the canonical `id BIGINT NOT NULL AUTO_INCREMENT PRIMARY KEY` source. Asserts non-zero rows on the target, the synthesized `uq_widgets_id` UNIQUE KEY is present, and the DP-2 PK-leading invariant holds (target PK leads with `source_shard_id`).

## Who needs this

- **Anyone running MySQL → MySQL Shape A with the canonical `AUTO_INCREMENT PRIMARY KEY` source schema** — pre-v0.72.2 you needed the workaround (non-AUTO_INCREMENT PK on the source); v0.72.2 makes the canonical shape work. The synthesized UNIQUE index is the only behavioral artifact; operators who inspect target schemas will see one additional unique index per Shape-A table.

- **Anyone NOT running MySQL Shape A** sees no change. The synthesis condition is narrowly scoped: MySQL target + Shape A rewrite + AUTO_INCREMENT column in the rewritten PK at a non-leading position. Outside that, behavior is identical to v0.72.1.

## Known follow-ups (informational)

None blocking. Two Shape A Phase-2 surfaces remain demand-gated per ADR-0048 DP-3:
- Shape A add-table mid-stream — currently runs through the drained model (`sync stop --wait` → schema migrate → `sync start --resume`). Live coordination is Phase 2.
- Shape A cross-shard schema-migration coordination — also Phase 2 per DP-3 (lease semantics need their own ADR).

Both will land when concrete operator workloads demand them.
