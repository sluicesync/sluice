# sluice v0.99.262

The 2026-07-16 confirming audit (grade B+ → A−) found two observed HIGHs that the audit-remediation releases themselves introduced. This release closes them, plus two hardening MEDIUMs.

## Fixed

- **`migrate` no longer false-refuses re-runs and bootstraps on engine pairs the translation layer can't normalize.** The v0.99.258 pre-create shape gate compared MySQL-native types against the lossy Postgres catalog read-back — no mysql→postgres retarget rule exists — so it refused on tables sluice itself created, and its hint led with the data-destroying `--reset-target-data`. The gate now engages only where a storage-shape mapping exists (unmappable pairs keep the pre-gate behavior, with a WARN naming each tolerated table), the PG→MySQL rule now covers the vitess flavor and trigger-variant sources, and the hint leads with `--exclude-table`, naming `--reset-target-data` last with its blast radius spelled out.
- **`backup export-as-parquet --force-overwrite` no longer deletes `.parquet` files it doesn't own.** The stale-orphan sweep was recursive and ungated on a prior export existing — a first forced export into a directory holding foreign datasets deleted them. The sweep is now scoped to top-level names and gated on the prior `parquet_index.json` proving sluice ownership; everything else is WARN-named as unmanaged, never touched.
- **The expand-contract pre-deploy diff gate fails closed on an empty decoded diff** (structurally impossible for a deployable DR — an empty decode signals response-shape drift and refuses coded).
- **CI: an off-pattern psverify test now fails the run-filter guard** instead of silently never running.

## Compatibility

- **No breaking changes.** One false-refusal removed, one silent pass converted to a coded refusal, one destructive sweep fenced to owned files. The v0.99.259 notes carry a same-day correction banner for the two claims its own regression cycle falsified.

## Who needs this

**Anyone who ran `migrate` mysql→postgres on v0.99.258–261 and hit `SLUICE-E-TARGET-TABLE-SHAPE-MISMATCH` on a re-run** — that was the false refusal, and if you followed the old hint's `--reset-target-data` on a multi-shard target, check what it dropped. **Anyone pointing `export-as-parquet --force-overwrite` at a directory shared with other parquet data on v0.99.258–261** — verify nothing foreign was swept (the log INFO-named every deletion).

## Install

- Binaries: https://github.com/sluicesync/sluice/releases/tag/v0.99.262
- Homebrew: `brew install sluicesync/tap/sluice`
- Scoop: `scoop bucket add sluicesync https://github.com/sluicesync/scoop-bucket; scoop install sluice`
- winget: `winget install sluicesync.sluice`
- Docker: `docker pull ghcr.io/sluicesync/sluice:v0.99.262`
