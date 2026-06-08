# sluice v0.93.0

# sluice v0.93.0 — CDC schema-race family closure (Bugs 112 + 119 + 120)

**Headline:** Closes the three-bug family — RENAME mid-stream silent drop (Bug 112), DROP COLUMN mid-stream silent drift (Bug 119), and DROP+CREATE-same-name mid-stream silent drop (Bug 120). The shared root cause was that the PG applier's `colTypeCache` is keyed by `"schema.table"` (name) and never invalidated, so when the source's relation changed mid-stream the cache held stale shape and dst silently diverged from source. v0.93.0 detects schema-race situations at the CDC reader's RelationMessage handler and refuses loudly with the operator-actionable drained-model recovery hint. **If your PG → PG sync touches tables that may undergo mid-stream DDL, upgrade.**

## Fixed

- **`fix(postgres): refuse loudly on incompatible CDC schema-race situations mid-stream (Bugs 112 + 119 + 120 closure)`** — pre-fix the applier's `colTypeCache` (keyed by `"schema.table"` with no invalidation) silently used stale shape when the source's relation changed mid-stream:
  - **Bug 112 RENAME**: pgoutput's new `RelationMessage` carries the new name; dst still has the old name; apply hits `errUnknownTable` → silently skipped → writes vanish.
  - **Bug 119 DROP COLUMN**: applier's cached column list has the dropped column → new INSERTs land on dst with that column populated as NULL → silent drift.
  - **Bug 120 DROP+CREATE same name**: source DROPs t1 then CREATEs new t1 with a different OID; orphaned old cache entry stays; applier uses old shape → silent drop of writes to the recreated relation.
  
  The shared root cause is the applier's cache having no awareness of relation-OID changes mid-stream. v0.93.0 adds `detectIncompatibleRelationChange` + `checkSchemaRace` in the CDC reader's RelationMessage handler (`internal/engines/postgres/cdc_relations.go`) that compare every incoming RelationMessage against the previously-cached entry for the same OID, and scan the relations map for any orphaned entry with the same `(Schema, Name)` but a different OID. Detected RENAME / DROP COLUMN / RENAME COLUMN / ALTER COLUMN TYPE / DROP+CREATE all surface as a loud stream-killing error naming the table, OID(s), and the drained-model recovery hint:
  
  ```
  sluice does not support this DDL shape mid-stream. Drained-model recovery:
  (1) `sluice sync stop --wait` on every shard,
  (2) apply the schema change via your migration tool on source AND target,
  (3) `sluice sync start --resume` to continue from the last applied LSN.
  For ADD COLUMN only, opt-in to live forwarding via --forward-schema-add-column (ADR-0058).
  ```
  
  **ADD COLUMN appended at the end stays compatible** — the existing ADR-0058 `--forward-schema-add-column` opt-in forwarding path continues to work. Pinned by `TestDetectIncompatibleRelationChange` (9 sub-pins covering each shape including the benign re-send pgoutput emits on reconnect, which MUST NOT false-positive) + `TestCheckSchemaRace_DROPCREATESameNameDifferentOID` + `TestCheckSchemaRace_SameOIDReentryIsBenign` + `TestCheckSchemaRace_ADDColumnIsCompatible`. Per CLAUDE.md's concurrency-chunk rule, the `-race` Integration gate ran green on a push-first-tag-after branch before the tag was cut.

## Compatibility

- **Minor bump (v0.93.0).** Drop-in from v0.92.4 except for the behavior change below.
- **Behavior change:**
  - PG → PG sync now refuses loudly on mid-stream RENAME / DROP COLUMN / RENAME COLUMN / ALTER COLUMN TYPE / DROP+CREATE. Pre-v0.93.0 these silently caused dst-divergence. Operators in the drained-model migration workflow (the documented sluice pattern) are unaffected. Operators relying on the silent-drift behavior must adopt the drained-model workflow OR opt-in to `--forward-schema-add-column` for the supported ADD COLUMN shape.

## Who needs this

- **Anyone running PG → PG sync against a database where mid-stream DDL is possible** — Bug 112 / 119 / 120 silent-loss class is now a loud-refuse class. **Upgrade.**
- **Operators who explicitly rely on silent-drift behavior** — there is no escape hatch beyond `--forward-schema-add-column` for ADD COLUMN. The fix is the drained-model workflow.
- **Everyone else** — no action needed.

## Coming next

The remaining open backlog after v0.93.0 is 8 bugs: 108 (redact YAML/CLI override order), 110 (backup incremental schema-read scope), 113 (PG DOMAIN constraint silent downgrade), 114 (multi-table migrate resume opacity), 115 (PG operator-class silent drop on index), 116 (backup mixed-version restore RLS silent drop), 117 (backup per-chunk passphrase silent rotation), 118 (likely dup of Bug 97 — re-verify). The v0.94.x arc will work through the backup-family (Bugs 110 / 116 / 117) with the Bug 118 re-verify; v0.95.x ships the PG IR-carry additions for DOMAIN / opclass; v0.96.x covers operator-quality-of-life (Bugs 108 / 114). Path to zero open is tractable.
