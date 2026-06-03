# sluice v0.74.1 — F2 spill surface + post-v0.74.0 dev-session bundle

**Headline:** Closes severity-b finding F2 from the PG-internals research (logical-decoding spill visibility in `sync health` + Prometheus), plus the post-v0.74.0 dev-session work — engine-side retry for the Phase 2e pg_type catalog race, pre-commit conflict-marker check (the gap that let v0.74.0's CHANGELOG ship with merge markers), and the pre-public-release housekeeping items #17 (tmp/ → docs/releases/) + #19 (Vultr/AURORA-R11 identifier scrub). All behavior-additive on top of v0.74.0; drop-in upgrade.

## Added

- **`feat(engines/postgres, pipeline, cmd/sluice): F2 — surface PG-14+ logical-decoding spill counters in sync health + Prometheus`** *(severity b)*. PG's `pg_stat_replication_slots` view (PG 14+) exposes `spill_txns` + `spill_bytes` — cumulative counters tracking when CDC-decoded transactions exceed `logical_decoding_work_mem` (default 64 MB) and PG spools un-emitted change records to disk under `pg_replslot/<slot>/snap/`. Pre-v0.74.1 sluice surfaced no signal — operators had to know to query the view themselves, and a slot whose spill directory filled would silently be invalidated by PG (`wal_status` → `lost`), which is silent-loss-class for sluice (the slot has to be dropped and recreated).

  - New optional `ir.SlotSpillReporter` interface implemented by PG's `SchemaReader`. Reads `spill_txns` / `spill_bytes` from `pg_stat_replication_slots`; degrades cleanly on PG < 14 via the `42P01 undefined_table` SQLSTATE (surfaces as "unavailable" rather than a misleading `0`).
  - `sluice sync health` JSON / text output gains `spill_txns` + `spill_bytes` (pointer-omitempty contract — absent when unavailable, not zeroed). New `--slot-name` flag for non-default slot names; defaults to `sluice_slot` when source-driver is postgres.
  - Prometheus `/metrics` endpoint exposes `sluice_pg_slot_spill_txns_total{stream_id,slot}` + `sluice_pg_slot_spill_bytes_total{stream_id,slot}` counters when the streamer is connected to a PG source.
  - Operator action: alert on `rate(sluice_pg_slot_spill_bytes_total[5m]) > 0`, then bump `logical_decoding_work_mem` or split large transactions. New "Logical-decoding spill" section in `docs/postgres-source-prep.md` documents the threat model + recovery playbook.

## Fixed

- **`fix(engines/postgres): retry EnsureControlTable on pg_type / pg_class catalog race`** *(Task #29 — ADR-0054 Phase 2e carryover)*. PG's `CREATE TABLE IF NOT EXISTS` checks `pg_class` for the relation but the table's row type allocates a `pg_type` row independently; concurrent CREATEs for the same name race on `pg_type_typname_nsp_index` (or `pg_class_relname_nsp_index`) and the loser surfaces SQLSTATE 23505. This is the same race the v0.73.0 Phase 2e integration test had to work around with pre-creation. v0.74.1 wraps `EnsureControlTable` in `retryOnCatalogRace`: 3 attempts with 50/100/200 ms backoff, ONLY retrying the narrow pg_type / pg_class catalog-race shape (constraint-name match). Other 23505s (user-table unique violations) stay non-retriable per ADR-0038.

- **`fix(githooks): pre-commit fails on unresolved merge-conflict markers in ANY file`** *(Task #36)*. The pre-commit hook previously only ran Go-file checks; merge-conflict markers in non-Go files (CHANGELOG.md, docs/, configs) slipped through to commits and to main. v0.74.0's F5 cherry-pick landed CHANGELOG.md with live `<<<<<<<` / `=======` / `>>>>>>>` markers because the hook short-circuited on "no Go files staged." Both `.githooks/pre-commit` (Bash) and `scripts/pre-commit.ps1` (PowerShell) now check all staged files for `<<<<<<<` and `>>>>>>>` markers (the unambiguous ones — `=======` alone would false-positive on Setext markdown underlines) BEFORE the Go-only gate.

- **`fix(test): F5 — drain replication slot between stages in source-identity integration tests`** *(post-v0.74.0 follow-up)*. CI on v0.74.0 surfaced SQLSTATE 55006 (`replication slot ... is active for PID N`) in three F5 integration tests when stage 2 raced PG's walsender release after stage 1's `CDCReader.Close()`. `Close()` is deliberately asynchronous (production paths don't re-attach to the same slot immediately). New `waitForSlotInactive` test helper polls `pg_replication_slots.active = false` for a 3s grace period, then force-terminates the active backend via `pg_terminate_backend()` and polls again. Applied to all three between-stage paths in the F5 test file.

## Docs

- **`docs: rename tmp/ → docs/releases/`** *(Task #17)*. `tmp/` implied session-local scratch but the folder held the maintainer's curated GitHub-release-notes files for every released version. 52 tracked files renamed (git detects 100% match — no content changes) + 6 release-notes drafts for v0.73.0-v0.74.0 added at the new path.

- **`docs: public-release scrub — Vultr identifiers + AURORA-R11 hostname`** *(Task #19)*. Privacy-sensitive identifiers redacted from pre-public-release files per the 2026-05-22 audit doc: Vultr public IP (redacted) → `<previous-vultr-IP>` (3 files), Vultr instance ID + SSH key ID redacted, operator's private workstation name `AURORA-R11` → generic placeholder (1 file). Historical references in CHANGELOG.md + ADR-0036 (the bare brand name "Vultr" in past-tense narrative) deliberately left untouched — those are part of the project's documented design-discipline history; scrubbing them would falsify the record.

## Tests

- **`test(engines/postgres): change_applier_catalog_race_test.go`** — unit pins on `isCatalogRaceError` (constraint-name discriminator, wrapping via `errors.As`, non-23505 / non-catalog 23505 negatives) and `retryOnCatalogRace` (immediate success, retry-then-success, exhausted retries, non-race-error immediate return, context cancellation observed between retries).

- **`test(engines/postgres): health_reporter_test.go + slot_spill_integration_test.go`** — F2 pins. Unit: `isUndefinedTableError` correctly matches PG SQLSTATE `42P01` (including via `errors.As` for wrapped errors). Integration: `SlotSpillStats` returns `ok=false` for nonexistent slot, errors on empty slotName, reads non-zero `spill_bytes` end-to-end after a deliberately oversized transaction is decoded through a slot with `logical_decoding_work_mem = '64kB'`.

- **`test(pipeline): metrics_test.go`** — F2 Prometheus surface pins. `emitSpillMetrics` renders both counter lines with the `{stream_id,slot}` label set, including the "zero is real data" case. `MetricsServer.AttachSpillReporter` end-to-end: attaching emits the lines, omitting / detaching / `ok=false` suppresses them, and a reporter error surfaces as a `# error: ...` exposition comment rather than blanking the rest of `/metrics`.

- **`test(cmd/sluice): sync_health_test.go`** — F2 sync-health surface pins. Spill fields render in text output when populated, stay absent (text + JSON) when `nil` (pointer-omitempty contract), and round-trip cleanly through `json.Marshal` / `Unmarshal` when populated.

## Compatibility

- **Drop-in upgrade from v0.74.0.** No CLI surface change beyond the new `--slot-name` flag on `sluice sync health` (defaults to `sluice_slot` for backwards-compatibility). No storage shape change. No behavior change outside the documented F2 surface additions + the Phase 2e retry path (which is invisible to operators in the success case).
- **PG < 14 sources** see the F2 fields as absent (the `pg_stat_replication_slots` view doesn't exist before 14). No error; sluice degrades cleanly to "no spill signal available."
- **MySQL paths** unchanged — F2 is PG-only.

## Cross-references

- [ADR-0007 — Position persistence](https://github.com/orware/sluice/blob/main/docs/adr/adr-0007-position-persistence.md)
- [ADR-0054 — Shape A Phase 2: live cross-shard DDL coordination](https://github.com/orware/sluice/blob/main/docs/adr/adr-0054-shape-a-phase-2-live-cross-shard-ddl-coordination.md) — Task #29 retry closes the Phase 2e race the integration test was working around
- Durable research artifact: `sluice-pg-internals-research-2026-05-22.md` (Ch 12)
- The Internals of PostgreSQL (https://www.interdb.jp/pg/) Ch 9.4 (logical decoding, ReorderBuffer)
