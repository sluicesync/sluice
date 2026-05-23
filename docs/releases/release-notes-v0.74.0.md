# sluice v0.74.0 — Two severity-a PG silent-loss closures from the PG-internals research

**Headline:** Closes the two highest-severity findings from the 2026-05-22 PG-internals research run — both silent-loss classes on PG sources that production deployments could hit without any observable signal. Behavior-additive: new refuse-loud paths on previously-silent failure modes. Drop-in upgrade for non-PG-source streams.

## Why this is a minor bump

The two new refuse-loud paths could surface on PG operators upgrading from v0.73.2:

- **F5** (source-identity pinning): operators whose persisted CDC position dates from pre-v0.74.0 sluice will see a one-time INFO log on first reconnect after upgrade ("pin installed lazily from IDENTIFY_SYSTEM"). After that, the source identity is strict-checked on every reconnect. If sluice ever silently switched onto a PITR'd / promoted / wrong-pointed source pre-v0.74.0, the upgrade will surface that via the existing ADR-0022 cold-start fall-through — which is the correct behavior, but it's a behavior change.
- **F7** (synchronous_commit override): operators with `ALTER ROLE … SET synchronous_commit = off` on the sluice apply role will now run apply transactions with `synchronous_commit = on` (via `SET LOCAL`) regardless of the role default. Transparent in correctness terms — but technically a behavior change.

## Fixed

- **`fix(adr-0051): F5 — PG CDC source-identity pinning closes post-PITR / post-promotion silent-loss class`** *(severity a)*. PG's logical-replication LSN reference frame is timeline-scoped: after a source-side PITR, a standby promotion, or sluice being pointed at a different instance with the same DSN host:port shape, the (sysid, timeline) tuple changes and the persisted LSN lives in a different timeline's reference frame than the new source's WAL. Pre-v0.74.0, sluice would silently `START_REPLICATION` from the persisted LSN and stream WAL from "the same LSN" on the new timeline — silent-loss class. v0.74.0 captures `SystemID` + `Timeline` from `IDENTIFY_SYSTEM` on every `StreamChanges` call (cold-start and resume); persists them additively in the position token (`omitempty` so pre-ADR-0051 tokens decode cleanly); compares the live identity against the persisted pin via `checkSourceIdentity` on resume; refuses loudly with `fmt.Errorf("...: %w", ir.ErrPositionInvalid)` on divergence — routing through the existing ADR-0022 cold-start fall-through with the slot-drop recovery hint. Positions from pre-v0.74.0 sluice trigger one-time INFO-level "lazy install" the first time they reconnect. References: PG-internals Ch 10.3, 10.4, 11.1.

- **`fix(engines/postgres): F7 — force synchronous_commit=on inside every apply tx`** *(severity a)*. ADR-0007's "position + data lands durably together" guarantee depends on the COMMIT ACK only returning after the WAL is durably flushed. PG's parameter-precedence chain (Ch 11.2) allows `ALTER ROLE name SET synchronous_commit = off` (or `ALTER DATABASE name SET …`) to pre-apply asynchronous-commit semantics (Ch 9.5) on every login; the sluice apply session would inherit this silently, allowing a COMMIT ACK to return BEFORE the WAL is durably written, so a target crash between ACK and flush silently loses the position+data tx. v0.74.0 emits `SET LOCAL synchronous_commit = on` as the first statement on every apply transaction (the three apply-tx start sites: `applyOne`, `applyOneBatch`, and `WritePosition`). `SET LOCAL` reverts at tx end so non-sluice sessions are unaffected; sessions that already had `synchronous_commit = on` (the PG default) see no behavior change. MySQL applier doesn't need an analogous fix — its sync-commit settings aren't per-role inheritable. ADR-0007 amended with a "Durability hardening for Postgres targets (F7)" section.

## Docs

- **`docs(postgres-source-prep): F6 — WAL volume cost of wal_level=logical`** *(severity b)*. Operator-facing guidance on the 1.2×–1.6× WAL byte-rate multiplier that flipping `wal_level` from `replica` to `logical` introduces (full tuple data carried alongside FPIs per Ch 9.4; `REPLICA IDENTITY FULL` amplifies). Operator-visible consequences on slot-retention disk pressure, WAL-archive volume, and replica bandwidth. Not a sluice bug — a known PG cost that operators should know to expect when sluice flips them to logical.

- **`docs(adr-0007, adr-0054, adr-0022): F8 + F9 — PG-internals research cross-references`** *(severity c)*. ADR-0007 gains a "Related PG-internals research" section pointing at F1/F3 (logical-replication chapter) and F5/F7 (chapters 9–11). ADR-0054 gains the analogous section calling out how each finding interacts with the lease state machine. ADR-0022 documents that `pg_replslot/<slot>/state` on disk (Ch 11.4) is the source of truth behind the slot-missing fall-through, and that ADR-0051's timeline-change refusal extends the same machinery via a different precondition check.

## Tests

End-to-end pins added on both severity-a closures so they can't regress silently:

- **F5 unit pins** (`cdc_reader_test.go`) — position-token round-trip with new SystemID/Timeline fields, pre-ADR-0051 token decode compatibility, the wire-format `omitempty` invariant, every branch of the `checkSourceIdentity` comparator.
- **F5 integration pin** (`cdc_reader_source_identity_integration_test.go`) — happy-path resume, tampered-systemid divergence refusal, legacy-token lazy install.
- **F7 unit pin** (`change_applier_synccommit_test.go`) — in-process recording driver confirms exact emit shape.
- **F7 integration pin** (`change_applier_synccommit_integration_test.go`) — PG container with hostile role default; full end-to-end verification.
- **`waitForSlotInactive` test helper** — polls `pg_replication_slots.active = false` for a 3s grace period, then force-terminates the active backend via `pg_terminate_backend()`. Required because `CDCReader.Close()` is deliberately asynchronous in production.

## Compatibility

- **Drop-in upgrade from v0.73.2.** No CLI surface change. No storage shape change for non-PG-source streams.
- **PG CDC streams** persisted by pre-v0.74.0 sluice trigger a one-time INFO-level "pin installed lazily" log on first reconnect; strict-check engages after.
- **PG apply sessions** running against a role/database with `synchronous_commit = off` now run apply transactions with `synchronous_commit = on` regardless. Transparent. Non-sluice sessions on the same role/database are unaffected.
- **MySQL paths** see no changes.

## Who needs this

- **PG sources behind any operational practice that re-issues an LSN reference frame**: PITR clones, standby promotions, blue/green DB deploys, scripted instance swaps. v0.74.0 catches a wrong-pointer at the next reconnect rather than silently corrupting.
- **PG targets** running on shared infrastructure where role/database-level `synchronous_commit = off` overrides exist. The override hazard is transparent post-v0.74.0.
- **All PG-using sluice deployments** benefit from the hardening even without a specific PITR / role-default condition — cost is one extra `SET LOCAL` per apply tx + one `IDENTIFY_SYSTEM` reply parse per reconnect, both negligible.

## Cross-references

- [ADR-0007 — Position persistence](https://github.com/orware/sluice/blob/main/docs/adr/adr-0007-position-persistence.md) — amended with F7 durability-hardening section
- [ADR-0022 — Slot-missing fall-through](https://github.com/orware/sluice/blob/main/docs/adr/adr-0022-slot-missing-fall-through.md) — F9 cross-reference paragraph
- [ADR-0051 — PG CDC source-identity pinning](https://github.com/orware/sluice/blob/main/docs/adr/adr-0051-pg-cdc-source-identity-pinning.md) — new ADR for F5
- [ADR-0054 — Shape A Phase 2: live cross-shard DDL coordination](https://github.com/orware/sluice/blob/main/docs/adr/adr-0054-shape-a-phase-2-live-cross-shard-ddl-coordination.md) — F8 cross-reference section
- The Internals of PostgreSQL (https://www.interdb.jp/pg/) Ch 9.4, 9.5, 10.3, 10.4, 11.1, 11.2, 11.4
