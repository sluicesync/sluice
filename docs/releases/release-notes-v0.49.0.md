# sluice v0.49.0 — closes #25 + #26 (real-world schema bootstrap fixes)

**Two bug fixes bundled in one release** to reduce per-release CI cost. Both blocked sluice cold-start on real-world MySQL source schemas — #25 against MySQL targets, #26 against PostgreSQL targets. Both reproduced reliably on v0.48.0 via the operator's validation rig; both reproductions now closed by the targeted fixes below.

## Closes GitHub #25 — MySQL `AUTO_INCREMENT` on non-PK column

A source table like:

```sql
CREATE TABLE cell (
  id varchar(50) PRIMARY KEY,
  increment_id int AUTO_INCREMENT,
  UNIQUE KEY uq_cell_increment_id (increment_id),
  ...
);
```

previously failed at sluice cold-start on MySQL/Vitess targets with `Error 1075 (42000): Incorrect table definition; there can be only one auto column and it must be defined as a key`. Pre-v0.49.0 sluice's three-phase apply deferred ALL secondary indexes to phase 2, so the CREATE TABLE landed without the auto column's supporting key.

**Fix**: `inlineAutoIncrementIndex` in `internal/engines/mysql/ddl_emit.go` detects the pattern (AUTO_INCREMENT column not in PK + has a supporting index) and emits the supporting index inline at CREATE TABLE. `CreateIndexes` (phase 2) skips the same index via the same detector to avoid double-create. Unique indexes preferred when both shapes exist — matches the real production `UNIQUE KEY uq_<table>_<col>` pattern from the issue body.

## Closes GitHub #26 — PostgreSQL identifier truncation collision

A source table like:

```sql
CREATE TABLE entity_field_operation_relation (
  ...,
  KEY ix_workflow_block_id_for_op_rel_alpha (workflow_block_id),
  KEY ix_workflow_block_id_for_op_rel_beta (id, workflow_block_id)
);
```

previously failed at sluice's index-creation phase on PG targets with `SQLSTATE 42P07: relation "entity_field_operation_relation_ix_workflow_block_id_for_op_rel" already exists` — both 69-char and 68-char prepended names silently truncated to the same 63-char PG identifier (`NAMEDATALEN-1`) and the second `CREATE INDEX` hit a duplicate.

**Fix**: `pgIndexName` in `internal/engines/postgres/ddl_emit.go` extended with two checks:

1. **Convention-prefix detection** — recognises `ix_<table>_`, `idx_<table>_`, `fk_<table>_`, `uq_<table>_`, `uix_<table>_`, `uidx_<table>_`, `uniq_<table>_`, `pk_<table>_`, `chk_<table>_`, `ck_<table>_` as already-table-scoped names. Covers SQLAlchemy / Alembic / Django / Rails / Hibernate / Diesel conventions plus operator hand-written schemas.

2. **Length-check fallback** — if the explicit `<tableName>_` prepend would exceed 63 chars, emit verbatim instead. Sacrifices the (historical) sibling-table disambiguation for collision-freedom; the same-table self-collision is the more urgent failure mode.

Note: **FK constraint names are NOT affected by this fix.** Empirical testing on the validation rig (two rounds — synthetic + the operator's literal Vitess-renamed examples) showed sluice's PG FK emitter does NOT prepend the table name, so FK names land verbatim from source. No parallel `pgConstraintName` helper needed. See `sluice-validation/BUG-CATALOG.md` entry 3 for the full diagnostic.

## Migration / Compatibility

- **Drop-in upgrade from v0.48.x.** Both fixes are additive at the operator surface; no flag changes, no behaviour change for schemas that don't hit either pattern.
- The supervisor-workaround `--include-table` filters operators were using to skip the failing tables (per the validation rig scripts) are no longer needed for #25/#26.
- Emitted PG index identifiers may differ for long source names: pre-v0.49.0 they silently truncated; post-v0.49.0 they emit verbatim. Existing PG targets that already received the truncated names can either re-bulk via `--reset-target-data` or accept the silent truncation as historical (PG catalog references are by OID, not name).

## Who needs this release

- **Anyone running `sluice sync start` or `sluice migrate` against real-world MySQL source schemas with**: (a) AUTO_INCREMENT on a non-PK column, OR (b) long index names following the `ix_<table>_*` / `idx_<table>_*` convention: **upgrade**. Both shapes blocked cold-start on v0.48.0 and earlier.
- **Anyone bootstrapping a PG-target sync from a legacy / mature MySQL source**: drop-in benefit.
- **Anyone whose schema avoids both patterns**: drop-in, no behaviour change.

## Verification surface

- 5 new test functions / subtests covering:
  - `TestEmitTableDef_AutoIncrementNonPK_GitHub25` — supporting UNIQUE KEY inline at CREATE TABLE
  - `TestInlineAutoIncrementIndex_DetectionTable` — 4 detector cases
  - `TestPgIndexName_GitHub26` — 9 subtests (regression preservation + new behavior shapes)
  - `TestPgIndexName_NoCollisionAcrossLongSiblingNames` — load-bearing pin
- **End-to-end re-verification via the validation rig** at `sluice-validation/`. Both `start_sync_mysql_dest.ps1 -AllTables` (re-triggers #25 pre-fix) and `start_sync_pg_dest.ps1 -AllTables` (re-triggers #26 pre-fix) should now succeed end-to-end with the v0.49.0 binary.

## Issue tracker after v0.49.0

| # | State | Resolution |
|---|---|---|
| 12–17, 19, 21, 22 | ✅ Closed | v0.40.0–v0.48.0 |
| 18 | 🟡 Open (in progress) | Phase 1+2 shipped v0.45.0; Phase 3 (AIMD) pending operator telemetry |
| 20 | 🟡 Open (in progress) | Chunk 14a shipped v0.47.0; 14b–d queued |
| 23 | 🟡 Open (Phase A shipped) | v0.48.0 heartbeat + pprof; Phase B pending operator-collected goroutine dump |
| 24 | 🟡 Open (planned) | PII redaction feature; roadmap entry pending |
| 25 | ✅ Closed | **v0.49.0 — inline supporting index for AUTO_INCREMENT-on-non-PK** |
| 26 | ✅ Closed | **v0.49.0 — pgIndexName convention-prefix + length-check** |
