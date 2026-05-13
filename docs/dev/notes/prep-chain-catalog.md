# Prep: `chain.json` catalog (GitHub #20 / roadmap 14a)

Design notes for the chain.json keystone — the v0.47.0 chunk that unblocks every subsequent retention / pruning / compaction chunk under roadmap item 14. Brief by intent; this exists to lock the contract before code lands.

## Why

Today's chain readers (`Restore.storeHasIncrementals`, `ChainRestore.Run`, `listManifestRecords`, `Backup.run` resume detection) all walk the `manifests/` directory and `readManifestAt` each file. Walking is `O(N)` in chain length on every read; on local FS this is fine for small chains and operationally painful past ~50k incrementals (the #20 evidence). On object storage every walk costs `ListObjects` calls. A single `chain.json` at the chain root listing every manifest in order, with end-position and size metadata pre-extracted, collapses every reader to O(1) — and gives prune / compact a structured place to record tombstones without rewriting manifest files in place.

## Schema

JSON file at chain root (same level as `manifest.json`), single object:

```json
{
  "format_version": 1,
  "source_engine": "planetscale",
  "chain_root_backup_id": "abc123…",
  "created_at": "2026-05-13T00:00:00Z",
  "updated_at": "2026-05-13T14:32:47Z",
  "entries": [
    {
      "backup_id": "abc123…",
      "kind": "full",
      "parent_backup_id": "",
      "manifest_path": "manifest.json",
      "end_position": {…opaque ir.Position envelope…},
      "created_at": "2026-05-13T00:00:00Z",
      "size_bytes": 134217728,
      "file_count": 47,
      "tombstoned": false
    },
    {
      "backup_id": "def456…",
      "kind": "incremental",
      "parent_backup_id": "abc123…",
      "manifest_path": "manifests/incr-20260513T0003-def456.json",
      "end_position": {…},
      "created_at": "2026-05-13T00:03:00Z",
      "size_bytes": 786432,
      "file_count": 1,
      "tombstoned": false
    }
    // … in chain order
  ]
}
```

**Field rationale.**

- `format_version` — additive evolution. v1 fields above; v2 might add per-entry encryption metadata or compaction parentage.
- `chain_root_backup_id` — duplicates `entries[0].backup_id` for cheap chain-identity checks without parsing the array.
- `end_position` — copied verbatim from the manifest (opaque `ir.Position` envelope; engine-defined). Lets `--position-from-manifest` and resume code answer "where does this chain reach?" without reading every incremental.
- `size_bytes` / `file_count` — pre-extracted so `sluice backup verify` and operator-facing chain summaries don't have to walk chunks. Recorded at manifest-write time (the writer knows what it just wrote).
- `tombstoned` — placeholder for v0.48.0+ prune / compact. v0.47.0 always writes `false`. Readers in v0.47.0 ignore it; readers from v0.48.0+ skip tombstoned entries during chain iteration.
- `parent_backup_id` — duplicates the underlying manifest's parent reference. Lets the catalog stand alone for chain-graph reasoning without fetching every manifest.

## Atomicity

Single-object write. Two paths:

1. **Local FS / POSIX**: write to `chain.json.tmp` then `os.Rename` to `chain.json`. Atomic on every POSIX filesystem; the existing `LocalStore.Put` already handles temp + rename for any object.
2. **Object storage** (S3 / GCS / Azure): `Put` is atomic at the object level. A `Put(chain.json, newBody)` overwrites the previous object atomically from any reader's perspective — readers either see the old version or the new version, never a partial.

No multi-object transaction needed: chain.json is one object, the underlying manifests are separate objects, and the manifests are written first. If chain.json update fails after a successful manifest write, the next read sees a manifest that isn't in chain.json — handled by the staleness fall-back below.

## Update flow

Touch points (every place a manifest gets written):

- `Backup.run` → `writeManifest(ManifestFileName, manifest)` for the full at chain root.
- `Incremental.run` → `writeManifest(incrementalManifestPath, manifest)` per incremental.
- `Stream.run` → same incremental write inside the rollover loop.

After every successful `writeManifest`, append/update the corresponding entry in chain.json:

```go
// pseudocode
if err := writeManifest(ctx, store, manifest); err != nil { return err }
if err := updateChainCatalog(ctx, store, manifest, manifestPath, sizeBytes, fileCount); err != nil {
    slog.WarnContext(ctx, "chain catalog update failed; chain.json may be stale until next rebuild",
        slog.String("err", err.Error()))
    // NOT a fatal — chain.json is an accelerator; manifests are source of truth
}
```

`updateChainCatalog`:

1. Read existing chain.json (or initialize empty if absent).
2. Append or replace the entry for `manifest.BackupID`.
3. Bump `updated_at`.
4. Write back via `store.Put(chain.json, body)`.

Read-modify-write race window: two concurrent `sluice backup stream run` against the same chain root would step on each other. That case is already operator-error (chain corruption) and not worth a CAS layer; we document the constraint.

## Read flow

New helper `loadChainCatalog(ctx, store) (*ChainCatalog, bool, error)`:

- `(catalog, true, nil)` — chain.json present and parsed.
- `(nil, false, nil)` — chain.json absent (legacy chain, or never-written).
- `(nil, false, err)` — chain.json present but unparseable (real I/O error).

Callers prefer the catalog when present; fall back to today's `listManifestRecords` walk otherwise. New `ChainCatalog.AsManifestRecords()` method returns the same `[]manifestRecord` shape today's walkers consume — minimal disruption to existing call sites.

## Staleness handling

A "stale" chain.json is one where the underlying `manifests/` directory has manifests not listed in `entries` (or vice versa). Causes:

1. v0.47.0+ writer crashed after manifest write, before chain.json update.
2. Operator manually copied / removed manifest files (legitimate for prune today; documented to break chain.json).
3. Chain pre-dates v0.47.0 — no chain.json exists yet; first v0.47.0 write should *create* it lazily including historical entries.

`loadChainCatalog` does NOT verify staleness on every read (would defeat the O(1) goal). Two explicit re-sync paths:

- **`sluice backup verify --rebuild-catalog`** (new flag in v0.47.0) — walks `manifests/`, rewrites chain.json from scratch.
- **Lazy detection** at the start of `Backup.run` / `Incremental.run` / `Stream.run`: if chain.json is absent but `manifests/` is non-empty, rebuild before appending the new entry. Cheap (one-time directory walk) and self-healing.

Restore / verify paths do NOT lazy-rebuild — they're read-only on the chain. They tolerate staleness by falling back to the directory walk when a referenced manifest is missing, or when a chain.json-listed entry has been physically deleted.

## Backwards compatibility

Chains produced by v0.46.0 and earlier have no chain.json. v0.47.0 readers:

- Detect absence → fall back to today's `listManifestRecords` walk (same code path as today, just behind a probe).
- Pre-v0.47.0 backups continue to restore and chain-extend without operator intervention.

v0.47.0 writers extending a legacy chain trigger the lazy-rebuild path: first rollover writes chain.json including all historical entries, then appends the new one. Operator sees no behaviour change beyond a new file appearing at the chain root.

Pre-v0.47.0 sluice reading a v0.47.0-produced chain ignores chain.json entirely (unknown file at chain root) and walks `manifests/` as before. **Strict forward and backward compat.**

## Integration points (code surface)

New file: `internal/pipeline/chain_catalog.go`. Contents:

- Types: `ChainCatalog`, `ChainCatalogEntry`.
- `loadChainCatalog(ctx, store) (*ChainCatalog, bool, error)`.
- `updateChainCatalog(ctx, store, manifest, path, sizeBytes, fileCount) error`.
- `rebuildChainCatalog(ctx, store) (*ChainCatalog, error)` — walks `manifests/` for the lazy / explicit-rebuild path.
- `(c *ChainCatalog) AsManifestRecords() []manifestRecord` — adapter so existing readers can opt in incrementally.

Existing files touched:

- `backup.go` — `Backup.run` after `writeManifest`: call `updateChainCatalog`. Also lazy-rebuild at start.
- `incremental.go` — same: after writing the incremental manifest, update catalog. Lazy-rebuild at start of `Incremental.run`. Reading: `listManifestRecords` grows a fast-path that returns `catalog.AsManifestRecords()` when present.
- `stream.go` — `Stream.run` rollover loop: update catalog after each incremental write. Lazy-rebuild at startup.
- `restore.go` — `Restore.storeHasIncrementals` grows a fast-path: if catalog present, return `len(catalog.entries) > 1` (full + ≥1 incr); else fall back to listing.
- `cmd/sluice` — new `--rebuild-catalog` flag on `backup verify`.

New file: `internal/pipeline/chain_catalog_test.go` — unit coverage for catalog read/write, staleness fall-back, schema version gate, format-version-newer rejection. Plus an integration test in `restore_test.go` or a new `chain_catalog_integration_test.go` that exercises a v0.46.0-style chain (no catalog) being extended in v0.47.0 (catalog gets created lazily, restore reads from it on subsequent runs).

## What this does NOT do

- **Does NOT change manifest shape or chunk shape.** chain.json is purely additive; pre-v0.47.0 readers ignore it.
- **Does NOT change restore behaviour for existing chains.** Same chain order, same per-chunk SHA verification, same idempotent apply.
- **Does NOT implement prune / compact / rotate-at.** Those are 14b–14d. v0.47.0 just lands the catalog so they have an O(1) chain-state lookup to build on.
- **Does NOT include the `tombstoned` filter logic** in iteration. v0.47.0 writers always set `false`; v0.48.0+ readers will honor it. v0.47.0 readers tolerate `true` by silently passing through (one-line check in `AsManifestRecords` is a cheap forward-compat insurance).

## Open questions worth flagging before code

1. **Encryption metadata in chain.json**: should the per-entry encryption envelope (wrapped CEK, Argon2id params for passphrase mode) be duplicated in chain.json for faster `restore` preflight? Pro: avoids reading the chain-root manifest just to detect encryption mode. Con: drift risk if catalog and manifest disagree. **Recommendation: leave out of v0.47.0**; preflight reads the chain root manifest one-time as today.
2. **Size metric — chunk bytes or manifest-JSON bytes?** The #20 issue cares about disk footprint, which is dominated by chunks. `size_bytes` should be the sum of chunk bytes for the manifest's tables. The writer knows this; it sums during `backupTable` calls. Tradeoff: incrementals report change-chunk bytes only, fulls report full-snapshot bytes — operators reading the sum get a meaningful "what does this chain weigh on disk" number.
3. **Object-store `List` cost reduction** beyond chain.json: a single `chain.json` read is one `Get`. Today's `List + Get-per-manifest` for a 1000-entry chain is 1 List + 1000 Gets. Net win on cost. No additional optimization needed for v0.47.0.

## Sizing estimate

- `chain_catalog.go`: ~120-180 LOC (types + load/save/rebuild + adapter).
- Updates across `backup.go` / `incremental.go` / `stream.go`: ~50 LOC total (3 call-site additions + 3 lazy-rebuild hooks).
- `cmd/sluice` `--rebuild-catalog` flag wiring: ~30 LOC.
- Unit tests: ~150 LOC.
- Integration test: ~80 LOC.

**Total: ~430-490 LOC.** Tight v0.47.0 release; pre-commit + integration pass; ships independently of 14b/c/d which build on it.
