# Logical Backups Phase 5 — Implementation Design

Supplement to [`design-logical-backups.md`](design-logical-backups.md), [`design-logical-backups-phase-2.md`](design-logical-backups-phase-2.md), [`design-logical-backups-phase-3.md`](design-logical-backups-phase-3.md), [`design-logical-backups-phase-4.md`](design-logical-backups-phase-4.md), and [`design-logical-backups-phase-4-5.md`](design-logical-backups-phase-4-5.md). This file covers Phase 5: **cross-engine chain restore** — `sluice restore --from=<chain-url>` and `sluice sync from-backup` against a chain whose source engine differs from the target engine.

The headline operator outcome: **a PG-rooted chain can restore (and stream-apply) into a MySQL target, and vice versa.** Closes the loud refusal that `chain_restore.go:99` currently raises (`"cross-engine chain restore is a Phase 5+ topic"`). Builds on `internal/translate.RetargetForEngine` which already handles full-backup cross-engine translation (since v0.16.x); Phase 5 extends the translation pass into incrementals' schema deltas + change-event row payloads.

## What's already in Phase 1-4.5 that this builds on

- **Full cross-engine restore** works today (v0.16.x+): `restore --from=<full-only-url> --target-driver=mysql` against a PG full passes through `RetargetForEngine` for schema + `convertRow` for row data. Chain-walker explicitly **refuses** the cross-engine incremental case to keep the contract narrow until Phase 5 (`chain_restore.go:99`).
- **Schema-delta replay** at chain-restore time exists (Phase 3.2): `ir.SchemaDeltaApplier.AlterAddColumn` is implemented on both PG and MySQL.
- **Change-event apply path** (Phase 4.5 broker) routes serialised `ir.Change` events through the engine's existing `ChangeApplier.ApplyBatch`, which already does value translation for live CDC at the applier layer.
- **`internal/translate.RetargetForEngine`** translates IR types (UUID → CHAR(36), INET → VARCHAR(45), JSON → JSON, etc.) — the foundation of cross-engine work.

## Scope

**In scope (Phase 5):**

- Lift the cross-engine refusal in `chain_restore.go:94-103`.
- Route schema deltas in incremental manifests through `RetargetForEngine` before invoking `SchemaDeltaApplier.AlterAddColumn` on the target engine. PG-source `AddColumn UUID` → MySQL target `AddColumn CHAR(36)`.
- Route change-event row payloads through the existing per-engine value translation at chain-replay time. The applier layer already does this for live CDC; thread the same path through `ChainRestore.applyIncremental`.
- Cross-engine `sync from-backup` broker: when target engine differs from chain's source engine, the broker's `--position-from-manifest` path drops the chain's terminal `EndPosition` (engine-specific format won't translate cleanly across engines anyway). Operators continuing CDC from a cross-engine restore must use `--at-chain-id=<terminal-id>` to assert state.
- Loud refusal for unsupportable shapes — same pattern as full cross-engine restore: when `RetargetForEngine` returns an error for a specific column type or DDL feature, the chain refuses with operator-actionable message naming the offending entity.
- Integration tests covering PG-rooted chain → MySQL target + MySQL-rooted chain → PG target.

**Out of scope (Phase 5+ deferred):**

- **Cross-engine CDC handoff with engine-translated `EndPosition`.** Translating a PG LSN to a MySQL GTID set isn't meaningful — they reference different change-log shapes. Operators wanting cross-engine continuous-CDC after restore should set up a new sluice CDC stream against the source's native engine (the chain restore lands the data; sluice sync start handles ongoing replication separately).
- **PG-only types not yet in `RetargetForEngine`'s table** (PostGIS geometry, hstore, custom enums beyond the existing PG enum support). Same refusal pattern as full cross-engine restore — refuse with the offending type named; operator can use `--exclude-table` or `--type-override` per existing escape hatches.
- **MySQL-only DDL** in deltas (e.g. `KEY_BLOCK_SIZE`, `STATS_AUTO_RECALC`). Refuse loudly; not Phase 5 scope.
- **Phase 6 (KMS encryption)** stays unimplemented through Phase 5.

## Open design questions — resolved decisions

### Q1: How to handle `EndPosition` on cross-engine restore?

**Decision: drop the chain's terminal `EndPosition` when target engine differs from chain's source engine.**

The `EndPosition` field is engine-specific (PG: `{slot, lsn}` JSON; MySQL: GTID set string). A PG LSN doesn't translate to a MySQL GTID — they're different change-log abstractions referencing different concepts. Trying to manufacture a synthetic position would be misleading and break operator expectations.

Implementation:
- Cross-engine `restore --from=<chain-url>` succeeds without writing any `sluice_cdc_state` row. Operators using the restored target as a starting point for live replication run `sluice sync start` (no `--position-from-manifest`); the source's CDC pump opens a fresh slot at current LSN/GTID.
- Cross-engine `sync from-backup` is supported at the chain-replay layer, but the broker's resumption-via-`sluice_cdc_state` only works within the same chain (BackupID-keyed). The broker writes its own `_engine="backup-broker"` envelope without the chain's source-engine position field.

**Why not (b) — re-anchor to target's CDC at restore time?**

Tempting (mirror v0.18.0's snapshot-anchor pattern), but cross-engine restore is typically a **one-shot migration** workflow, not a continuous-replication workflow. Operators doing cross-engine restore aren't usually continuing CDC from the chain itself — they're transitioning to a new sluice CDC stream against the source. The added complexity of capturing a target-side position during restore doesn't pay back. If demand emerges, lift in a future minor.

### Q2: Schema-delta translation at apply time vs at chain-walk time?

**Decision: translate at apply time (per-incremental), inside `applyIncremental`.**

Two options:
- (a) Translate the entire chain's schema deltas up-front during `BuildChain`, then pass a target-engine-shaped chain to the apply loop.
- (b) Keep the chain in source-engine form; translate each incremental's deltas inside `applyIncremental` just before invoking `SchemaDeltaApplier`.

(b) is the right call:
- Mirrors how full restore works (`RetargetForEngine` is called inside the schema-write path, not during BuildChain).
- Lazy: a chain with N incrementals only translates the deltas actually present (most incrementals have zero deltas).
- Better error locality: translation failures surface naming the specific incremental + table.
- Doesn't require a new "translated chain" data structure.

### Q3: Change-event value translation — where in the apply path?

**Decision: at the boundary where chunk-stream events feed into `ChangeApplier.ApplyBatch`.**

The change-event chunk format is engine-neutral (`ir.Change` is engine-agnostic; row payloads use IR-typed values). When the chain restore reads a chunk and feeds events to the engine-specific applier, the boundary is the natural translation point.

Translation flow per event:
1. Read `ir.Change` from chunk (source-engine-flavored row payload).
2. If target engine differs: invoke value-translation pass (the same `convertRow` / `prepareValue` machinery the applier already uses for live CDC).
3. Pass the translated event to `ChangeApplier.ApplyBatch` on the target engine.

This avoids duplicating translation logic — the engine appliers already handle "incoming IR-typed values from a different source-engine flavor" because that's the live cross-engine CDC path (PG → MySQL, MySQL → PG sync streams from v0.4.0+).

### Q4: Refusal shapes for unsupportable cross-engine cases?

**Decision: mirror full cross-engine restore's refusal patterns. Same error messages, same recovery hints (`--exclude-table` / `--type-override` / `--skip-views`).**

For each delta entry:
- Pre-translate the `Before`/`After` schema fragments via `RetargetForEngine`.
- If translation succeeds: apply the delta (target-engine-shaped DDL).
- If translation fails: refuse the entire chain restore with a clear error naming the incremental's `BackupID` + table + offending type/DDL. Operator can re-attempt with `--exclude-table=<offending>` to skip the table from the chain's apply.

### Q5: Test coverage?

**Decision: integration tests for both directions on representative type sets.**

Acceptance criteria below cover:
- PG-source chain (with the seed schema's typical types: BIGINT identity, VARCHAR, TIMESTAMPTZ, BOOLEAN, JSON) → MySQL target.
- MySQL-source chain (BIGINT, VARCHAR, DATETIME, TINYINT(1), JSON) → PG target.
- Schema-delta-during-stream cross-engine: ALTER TABLE on source mid-stream → broker on cross-engine target applies translated delta.
- Loud-refusal scenarios: PG-only PostGIS column in chain → MySQL target refusal with operator-actionable message.

## Sub-phasing

| Sub-phase | Scope | LOC est. |
|---|---|---|
| **5.1 — Lift the cross-engine refusal in chain restore** | Remove the early refusal at `chain_restore.go:94-103`. Add cross-engine routing: when `link.manifest.SourceEngine != target.Name()`, route deltas through `RetargetForEngine`. | 50-100 |
| **5.2 — Schema-delta translation per incremental** | In `applyIncremental`, before invoking `SchemaDeltaApplier.AlterAddColumn`, translate the `After`-shape via `RetargetForEngine`. Refuse with clear error on translation failure. Cover `AddTable`, `AlterTable.AddColumn`, `DropTable` (rename refusal already exists). | 150-250 |
| **5.3 — Change-event value translation** | At the chunk-stream → applier boundary, when source-engine ≠ target-engine, route each event's row payload through the existing value-translation machinery (same pattern the live cross-engine CDC stream uses). | 200-300 |
| **5.4 — Cross-engine broker `EndPosition` drop** | When `sync from-backup` detects cross-engine, skip the chain's `EndPosition` write to `sluice_cdc_state`; log INFO note about the operator's responsibility. | 50-100 |
| **5.5 — Integration tests** | PG-rooted chain → MySQL target (full + 2 incrementals); MySQL-rooted chain → PG target; cross-engine schema-evolution (ALTER mid-stream); loud-refusal scenario for PostGIS-on-PG → MySQL. | 250-400 |
| **Total Phase 5** | | ~700-1150 |

## CLI surface

No new CLI surface — Phase 5 is a behavior change to existing commands when target engine differs from chain's source engine.

| Command | Phase 5 work |
|---|---|
| `sluice restore --from=<chain-url> --target-driver=<engine>` | Cross-engine variant now succeeds (was refused). |
| `sluice sync from-backup run --target-driver=<engine>` | Cross-engine variant now succeeds; chain's terminal `EndPosition` is dropped (operator can use `--at-chain-id` for resumption assertions). |
| `sluice backup verify` | Unchanged. |

## Acceptance criteria

A clean Phase 5 must:

1. **PG-rooted chain → MySQL target restore.** PG full + 2 incrementals (with INSERTs and an UPDATE) → restore into fresh MySQL target → all rows land with PG-typed values translated correctly (UUID → CHAR(36), JSONB → JSON, TIMESTAMPTZ → DATETIME with UTC semantics).
2. **MySQL-rooted chain → PG target restore.** MySQL full + 2 incrementals (mix of INSERT/UPDATE/DELETE) → restore into fresh PG target → all rows land (TINYINT(1) → BOOLEAN, JSON → JSONB).
3. **Cross-engine schema evolution.** Stream-write a chain on PG; `ALTER TABLE customers ADD COLUMN tag VARCHAR(64)` on PG mid-stream; broker on MySQL target applies the translated delta + replays the post-ALTER rows.
4. **Loud refusal for unsupportable types.** Take a PG chain that includes a PostGIS geometry column; cross-engine restore to MySQL refuses with a message naming the incremental's BackupID + the offending column + suggesting `--exclude-table` as the recovery path.
5. **Cross-engine `sync from-backup` works without `EndPosition` write.** Broker against cross-engine chain replays incrementals; `sluice_cdc_state` row carries `_engine=backup-broker` envelope but no chain `EndPosition`; operator-facing log line surfaces the resumption-via-`--at-chain-id` guidance.
6. **Same-engine paths regression-clean.** All existing v0.20.x same-engine chain restore tests pass; same-engine broker happy paths unchanged.
7. **`backup verify --from=<chain-url>` unchanged.** Verify is read-only and engine-agnostic; integrity checks pass on cross-engine-target-bound chains identically.

## Tenet check

- **IR-first.** Phase 5 reuses the existing `RetargetForEngine` translation machinery + the per-engine appliers' value-translation. No new translation surface.
- **Contain Postgres complexity.** PG-specific types in deltas surface as loud refusals naming the offending column; same shape as full cross-engine restore. PG slot lifecycle is unchanged (chain-replay doesn't open slots on the target).
- **Validate end-to-end.** Acceptance criteria 1-3 are the load-bearing integration tests on both engines, both directions.
- **Loud failure beats silent corruption.** Translation failures refuse with operator-actionable error; no silent-rewrite of unsupportable shapes.
- **Clean, elegant code.** Reuses existing `RetargetForEngine` + value-translation paths; new code is mostly the wiring at the chain-restore + broker boundaries.

## Risks + mitigations

- **Risk**: change-event value translation has subtle bugs (e.g. timezone handling in TIMESTAMPTZ → DATETIME, NULL-vs-empty-string ambiguity). Mitigated by: reusing the existing live-CDC translation path, which has been battle-tested across the four engine-pair sync directions since v0.4.0+.
- **Risk**: schema-delta translation refusal lands at chain-replay time rather than chain-creation time, so operators won't know their chain is cross-engine-restorable until they try. Mitigated by: documenting the "supported types" list in operator-facing docs; future enhancement could add a `sluice backup verify --target-engine=mysql` mode that pre-validates the chain's translatability.
- **Risk**: the `EndPosition`-drop behavior may surprise operators who expect resume-from-chain to work cross-engine. Mitigated by: clear INFO log line at restore time naming the resumption pattern; release notes explain.

## See also

- [`design-logical-backups.md`](design-logical-backups.md) — original proto-ADR
- [`design-logical-backups-phase-2.md`](design-logical-backups-phase-2.md) — cloud backends
- [`design-logical-backups-phase-3.md`](design-logical-backups-phase-3.md) — incrementals + chain restore (Phase 3.2 has the cross-engine refusal Phase 5 lifts)
- [`design-logical-backups-phase-4.md`](design-logical-backups-phase-4.md) — `backup stream`
- [`design-logical-backups-phase-4-5.md`](design-logical-backups-phase-4-5.md) — `sync from-backup` broker
- ADR-0016 (cross-engine type-policy retargeting) — the foundation `RetargetForEngine` builds on
- `internal/translate/retarget.go` — the type-translation table Phase 5 reuses verbatim
- `internal/translate/notes.go` — translation notes surface (operator-facing warnings on lossy translations)
