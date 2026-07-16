# sluice v0.99.258

The audit follow-through release: all eleven M1 correctness items from the 2026-07-15 repo audit, roadmap item 71(b), Bug 190, and two fixes live-caught by the first-ever psverify CI dispatch.

## Added

- **`migrate` now handles pre-existing target tables honestly (ADR-0166, item 71b).** Before its CREATE phase it compares each existing same-name table's column shape against what it would create: a match is skipped with an INFO — so a schema pre-created via `sluice deploy-ddl` on a PlanetScale safe-migrations branch now feeds a fresh flagless `migrate` end to end — while a conflict refuses upfront with the new coded `SLUICE-E-TARGET-TABLE-SHAPE-MISMATCH` naming the differing columns, instead of a confusing mid-copy error retried for the full 30-minute wall. `--resume` behavior is unchanged.
- **Two pre-deploy safety gates on the PlanetScale deploy legs (ADR-0167).** The deploy request's computed diff is refused if it touches any object the leg never intended (the stale-base phantom-revert signature), and a review wait longer than 2 minutes re-verifies production's schema against the provisioning baseline before deploying.
- **mydumper torn-dump nets:** the dump's own recorded per-table row counts are cross-checked after every full table read (WARN naming both counts), and non-contiguous chunk numbers WARN at open — surfacing a deleted/lost data chunk that previously streamed silently short. Ground-truthed against real mydumper: chunk numbers are PK-range-derived, so the row-count tripwire is the real net.

## Fixed

- **Slot invalidation no longer reads as "condition cleared" (observed live on PG16).** A `wal_status='lost'` slot — and a slot dropped mid-stream — now pages CRITICAL exactly once and latches; `unreserved` pages immediately but stays clearable. Sustained probe outages page instead of logging DEBUG forever; boundary flapping is damped; a transient sink error can no longer permanently swallow a schema-drift stall's only page.
- **mydumper: multi-assignment `SET` headers now hit the TIME_ZONE/NAMES gates in every position** — `SET SESSION sql_mode='', SESSION time_zone='+05:30'` previously streamed every TIMESTAMP silently shifted 5.5 hours.
- **The control-table exclusion roster now guards every schema-reader door** (mydumper, flat-file, sqlite/D1), a `sluice_cdc_state.csv` refuses loudly, and the live readers log an INFO instead of thinning the table list silently.
- **`sluice verify` no longer exits 0 when a table could not be verified (Bug 190).** Count/sample errors and missing-on-target tables now exit 2 with a "could not be verified" summary (`tables_unverified` in JSON); mismatches keep exit 1; deliberate filter exclusions stay neutral.
- **`--infer-types` no longer promotes JSON columns carrying duplicate object keys to `jsonb`** — PG jsonb keeps the last duplicate while SQLite reads the first, so promotion silently changed which value wins; such columns stay `text`.
- **GeoParquet exports now carry each geometry column's CRS** (PROJJSON for EPSG:4326/3857; explicit null + WARN otherwise), and `--force-overwrite` re-exports delete stale `.parquet` orphans so the cookbook glob can't read a dropped table's old rows.
- **The PlanetScale deploy call retries the timing-dependent 422 "currently validating" settling state** instead of failing hard, and **`expand-contract` on a reused database restarts its backfill when this run's expand leg deployed the schema change** instead of no-oping against a prior cycle's completed marker — both live-caught by the first psverify CI dispatch.

## Changed

- The remaining raw backup integrity refusals are coded (`SLUICE-E-BACKUP-MANIFEST-INVALID` / `SLUICE-E-BACKUP-INCOMPLETE` / new `SLUICE-E-BACKUP-ENCRYPTION-MISMATCH`), and the parquet encoder refusals point at `--exclude-table` instead of a nonexistent flag. The resume-cursor envelope gained MySQL-target, temporal, and chunked-bounds pins.

## Compatibility

- **No breaking changes to data paths.** Three deliberate exit-code shifts, all refuse-louder: verify's unverified class exits 2 (was 0), encryption-mismatch preflights exit 3 (was 1), conflicting pre-existing target tables refuse upfront (was silent IF-NOT-EXISTS tolerance until a mid-copy error). Verify JSON renames `tables_skipped` → `tables_unverified`.

## Who needs this

Anyone running CDC sync with the notify sinks (the paging net is now truthful at exactly the terminal events it exists for), anyone importing mydumper dumps (two new torn-dump nets plus the SET-header fix), anyone scripting `verify` exit codes, and anyone on the PlanetScale deploy-request workflows (the bootstrap → fresh-migrate story now works end to end, and two live-caught races are fixed).

## Install

- Binaries: https://github.com/sluicesync/sluice/releases/tag/v0.99.258
- Homebrew: `brew install sluicesync/tap/sluice`
- Scoop: `scoop bucket add sluicesync https://github.com/sluicesync/scoop-bucket; scoop install sluice`
- winget: `winget install sluicesync.sluice`
- Docker: `docker pull ghcr.io/sluicesync/sluice:v0.99.258`
