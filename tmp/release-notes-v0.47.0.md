# sluice v0.47.0 — `chain.json` catalog at chain root

**Lands GitHub issue #20 chunk 14a.** Roadmap item 14's keystone. The first step toward bounded backup-chain retention.

Today's chain readers walk the `manifests/` directory and `Get` every result on every chain-state question: chain detection, position-from-manifest lookup, restore startup, resume-detect on backup full / incremental / stream. Fine on local FS for small chains; operationally painful past ~10k incrementals or on object storage where `ListObjects` is the dominant cost. v0.47.0 lands a single `chain.json` at the chain root listing every manifest in order with end-position and file-count metadata pre-extracted — chain readers collapse the per-manifest walk to a single `Get`.

This release is the keystone for the v0.48.0+ rotate-at / prune (chunks 14b–c) and v0.49.0+ compact (chunk 14d) work under the same roadmap item. Those chunks all need O(1) chain-state lookup to build on.

## Added

- **`chain.json` at chain root.** `ChainCatalog` + `ChainCatalogEntry` types serialised as JSON. Per-entry fields: `backup_id`, `kind`, `parent_backup_id`, `manifest_path`, `end_position`, `created_at`, `file_count`, plus a `tombstoned` placeholder reserved for v0.48.0+ compact/prune. `format_version=1`; readers refuse newer versions with an "upgrade sluice" hint.

- **`listAllManifests` fast-path.** When chain.json is present, every chain reader gets O(1) chain-state via a single `Get`. Catalog-absent falls back to today's `List + per-manifest Get` walk — strict backwards compat with pre-v0.47.0 chains.

- **Catalog hooks at every production manifest-write site.** `backup full`, `backup incremental`, and `backup stream run` (both normal-rollover commits and drain-on-ctx-cancel commits) update chain.json after each successful manifest write. Per-chunk / per-table checkpoint writes during the row sweep skip the catalog (BackupID isn't computed yet at that point), so a backup-full run produces exactly one catalog write regardless of chunk count.

- **`sluice backup verify --rebuild-catalog`.** Operator-driven catalog regeneration. Walks every manifest on disk and writes a fresh chain.json from scratch. Useful after manual chain mutation or to seed a catalog on a pre-v0.47.0 legacy chain.

## Fixed

- **Lazy rebuild on first v0.47.0 write to a legacy chain.** When `updateChainCatalog` runs against a chain without chain.json, it performs a one-time rebuild over the existing `manifests/` + the full at the root before appending the new entry. Operator sees a new chain.json appear with all historical entries; subsequent writes pay only one `Get` + one `Put`. **No operator-driven migration step.**

- **Dedup-by-path AND dedup-by-BackupID.** A backup-full re-run that overwrites `manifest.json` produces a new BackupID at the same path. Naive dedup-by-BackupID-only would leave a stale entry pointing at the same path, and chain consumers would read the manifest twice (verify-double-count, ChainRestore double-apply). The dedup filter drops any existing entry whose BackupID OR ManifestPath collides with the new entry before appending.

- **Tombstone forward-compat filter.** A v0.47.0 reader against a v0.48.0+ chain.json with `tombstoned: true` entries skips them during chain iteration — cheap forward-compat insurance so the v0.48.0+ compact/prune work doesn't surface compacted-out manifests in a v0.47.0 restore.

## Migration / Compatibility

- **Drop-in upgrade from v0.46.x.** Existing chains without chain.json keep working through the directory-walk fallback. The first v0.47.0 write into a legacy chain triggers a one-time lazy rebuild; subsequent writes pay only one `Get` + one `Put`.
- **Pre-v0.47.0 sluice reading a v0.47.0-produced chain** ignores `chain.json` entirely and walks `manifests/` as before. **Strict forward AND backward compat** at the chain-root layer.
- **Backup-full Put count is +1** (one chain.json write per backup completion). Operators with strict Put-count budgets / monitoring should adjust expectations; the additional Put is small (~1-10 KB depending on chain length).

## Who needs this release

- **Anyone running `sluice backup stream run` against long-running chains** (especially local-FS chains per the GitHub #20 evidence): drop-in benefit. Chain-state lookups are now O(1).
- **Anyone restoring large chains on object storage**: drop-in benefit. Restore startup eliminates the `ListObjects` walk in favour of a single `Get chain.json`.
- **Operators preparing for the v0.48.0+ rotate-at / prune work**: this is the keystone. The next chunks all build on chain.json.

## Verification surface

- **8 new unit tests in `internal/pipeline/chain_catalog_test.go`** covering load-absent, round-trip, format-version refusal, dedup, lazy-rebuild, tombstone filter, JSON shape, and the listAllManifests fast-path / legacy-walk integration.
- **Existing backup-resume tests retained** with one assertion update (`TestBackup_ResumeSkipsAlreadyCompletedTables`: +1 Put for the chain.json write on flip-to-complete).
- **End-to-end backup-chain validation deferred to operator re-test** — same pattern as prior backup-track releases.

## What's next on this track

- **v0.48.0+: chunks 14b–c** — `--retain-rotate-at=DUR` on `backup stream run` + `sluice backup prune --keep-incrementals N`. Bounded chain length without operator-side cron. Requires careful snapshot/CDC overlap design (the snapshot anchor of the new rotated full must be ≥ the previous stream's last-committed incremental position).
- **v0.49.0+: chunk 14d** — `sluice backup compact --from-dir CHAIN --merge-window DUR`. Naive concat first; smart same-row collapsing deferred to a later release. Restore-side: no code change — merged manifests look like larger incrementals to the existing chain iterator.

## Issue tracker after v0.47.0

| # | State | Resolution |
|---|---|---|
| 12–17, 19 | ✅ Closed | v0.40.0–v0.46.0 (see prior release notes) |
| 18 | 🟡 Open (in progress) | Phase 1+2 shipped v0.45.0; Phase 3 (AIMD controller) pending operator-collected telemetry |
| 20 | 🟡 Open (in progress) | **v0.47.0 — chunk 14a (chain.json catalog) shipped**; chunks 14b–d queued |
