// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package pipeline

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"sort"
	"time"

	"github.com/orware/sluice/internal/ir"
)

// # chain.json — backup chain catalog (GitHub #20, roadmap 14a)
//
// chain.json lives at the chain root alongside the full's manifest.json
// and is the single-file O(1) index over the chain's manifests. Pre-
// v0.47.0 every chain consumer (Restore.storeHasIncrementals,
// ChainRestore, position-from-manifest, the resume-detect path on
// backup full / incremental / stream) walked the manifests/ directory
// via store.List and then store.Get every result — fine on local FS
// for small chains, painful past ~10k incrementals or on object
// storage where ListObjects is the dominant cost. chain.json collapses
// the walk to a single Get.
//
// chain.json is an *accelerator*, not the source of truth. The
// underlying manifest files remain authoritative; if chain.json is
// absent / stale / unparseable, readers fall back to the directory
// walk. Writers attempt to keep chain.json in sync but a failed update
// is logged at WARN and does NOT fail the manifest write.
//
// Forward / backward compat:
//   - Pre-v0.47.0 sluice ignores chain.json (unknown file at chain
//     root) and walks manifests/ as before.
//   - v0.47.0+ readers without chain.json fall back to the same walk.
//   - v0.47.0+ writers extending a legacy chain trigger a one-time
//     lazy rebuild before appending the new entry.
//
// See [docs/dev/notes/prep-chain-catalog.md] for the full design.

// ChainCatalogFileName is the filename of the catalog within a backup
// store. Lives at the chain root, sibling to [ManifestFileName].
const ChainCatalogFileName = "chain.json"

// chainCatalogFormatVersion is the integer version of the chain.json
// schema. Bumped only on incompatible field renames; additive fields
// don't require a bump (older sluice ignores unknown fields when
// deserialising).
const chainCatalogFormatVersion = 1

// ChainCatalog is the deserialised content of chain.json. Ordered
// entries[] reflects chain order: entries[0] is the chain root (the
// full), each subsequent entry's ParentBackupID matches the prior
// entry's BackupID.
type ChainCatalog struct {
	FormatVersion     int                 `json:"format_version"`
	SluiceVersion     string              `json:"sluice_version,omitempty"`
	SourceEngine      string              `json:"source_engine,omitempty"`
	ChainRootBackupID string              `json:"chain_root_backup_id,omitempty"`
	CreatedAt         time.Time           `json:"created_at"`
	UpdatedAt         time.Time           `json:"updated_at"`
	Entries           []ChainCatalogEntry `json:"entries"`
}

// ChainCatalogEntry is one manifest's index entry. The catalog
// duplicates a handful of manifest fields (BackupID, Kind, parent ref,
// EndPosition, CreatedAt) so chain readers can answer chain-shape
// questions without parsing every manifest. Restore still reads the
// underlying manifest body when it needs the schema or per-table chunk
// list.
type ChainCatalogEntry struct {
	BackupID       string      `json:"backup_id"`
	Kind           string      `json:"kind"`
	ParentBackupID string      `json:"parent_backup_id,omitempty"`
	ManifestPath   string      `json:"manifest_path"`
	EndPosition    ir.Position `json:"end_position,omitempty"`
	CreatedAt      time.Time   `json:"created_at"`
	FileCount      int         `json:"file_count,omitempty"`

	// Tombstoned is the forward-compat placeholder for the v0.48.0+
	// prune / compact work (roadmap 14b–d). v0.47.0 writers always
	// emit false; v0.47.0 readers MUST filter tombstoned entries when
	// iterating the chain so a future-rebuilt catalog with tombstones
	// doesn't silently include compacted-out manifests in a v0.47.0
	// restore. See [filterActiveEntries].
	Tombstoned bool `json:"tombstoned,omitempty"`
}

// loadChainCatalog reads chain.json from store. Returns (catalog,
// true, nil) when present and parseable; (nil, false, nil) when
// absent; (nil, false, err) for I/O / parse failures. The "absent"
// case is the legacy-chain fall-through that callers handle by
// walking the manifests/ directory.
func loadChainCatalog(ctx context.Context, store ir.BackupStore) (*ChainCatalog, bool, error) {
	exists, err := store.Exists(ctx, ChainCatalogFileName)
	if err != nil {
		return nil, false, fmt.Errorf("inspect %q: %w", ChainCatalogFileName, err)
	}
	if !exists {
		return nil, false, nil
	}
	rc, err := store.Get(ctx, ChainCatalogFileName)
	if err != nil {
		return nil, false, fmt.Errorf("get %q: %w", ChainCatalogFileName, err)
	}
	defer func() { _ = rc.Close() }()
	body, err := io.ReadAll(rc)
	if err != nil {
		return nil, false, fmt.Errorf("read %q: %w", ChainCatalogFileName, err)
	}
	var cat ChainCatalog
	if err := json.Unmarshal(body, &cat); err != nil {
		return nil, false, fmt.Errorf("decode %q: %w", ChainCatalogFileName, err)
	}
	if cat.FormatVersion > chainCatalogFormatVersion {
		return nil, false, fmt.Errorf("%s format version %d is newer than this build supports (%d); upgrade sluice",
			ChainCatalogFileName, cat.FormatVersion, chainCatalogFormatVersion)
	}
	return &cat, true, nil
}

// writeChainCatalog serialises cat as JSON (indented for human
// readability, matching the manifest convention) and writes it to
// store. Single-object Put — atomic at the storage layer from any
// reader's perspective.
func writeChainCatalog(ctx context.Context, store ir.BackupStore, cat *ChainCatalog) error {
	b, err := json.MarshalIndent(cat, "", "  ")
	if err != nil {
		return fmt.Errorf("marshal chain catalog: %w", err)
	}
	return store.Put(ctx, ChainCatalogFileName, bytes.NewReader(b))
}

// updateChainCatalog appends or replaces the entry for manifest in
// store's chain.json. If chain.json is absent (a legacy chain on
// first v0.47.0 write), the function performs a one-time lazy rebuild
// over the existing manifests/ directory + the full at the root
// before appending the new entry — operator sees a new chain.json
// appear with all historical entries the first time v0.47.0 writes
// to the chain.
//
// updateChainCatalog is best-effort by design: callers should log
// failures but NOT propagate them as fatal. The underlying manifest
// files remain authoritative; a missing or stale chain.json triggers
// the directory-walk fallback in [listAllManifests], so a temporary
// catalog inconsistency doesn't break restore.
func updateChainCatalog(
	ctx context.Context,
	store ir.BackupStore,
	manifest *ir.Manifest,
	manifestPath string,
	fileCount int,
) error {
	if manifest == nil {
		return errors.New("chain catalog: nil manifest")
	}
	if manifest.BackupID == "" {
		// Pre-v0.17.0 manifests had no BackupID; v0.47.0+ writers
		// always populate it. Skip the catalog update rather than
		// indexing under an empty key.
		return nil
	}

	cat, present, err := loadChainCatalog(ctx, store)
	if err != nil {
		return err
	}
	if !present {
		cat, err = rebuildChainCatalog(ctx, store)
		if err != nil {
			return fmt.Errorf("lazy rebuild: %w", err)
		}
	}

	entry := newCatalogEntry(manifest, manifestPath, fileCount)

	// Dedup against any existing entry with the same BackupID OR the
	// same ManifestPath, then append the new entry. Two cases:
	//
	//   - Same BackupID — happens on per-chunk-checkpoint writes
	//     within a single backup-full run (orchestrator writes the
	//     same manifest multiple times as chunks complete).
	//   - Same ManifestPath, different BackupID — happens when a
	//     backup full re-runs into the same store: the new run
	//     computes a fresh BackupID (different CreatedAt) but
	//     overwrites the conventional ManifestFileName. The prior
	//     entry's manifest body is gone from disk; leaving the entry
	//     in the catalog would surface two records pointing at the
	//     same file to chain-walk consumers (load it twice → verify
	//     it twice → over-count chunks; see [TestBackup_ResumePerChunkSkipsAlreadyUploadedChunks]
	//     pre-v0.47.0 regression). The path is the structural slot;
	//     the BackupID is the content identifier.
	kept := make([]ChainCatalogEntry, 0, len(cat.Entries)+1)
	for _, e := range cat.Entries {
		if e.BackupID == entry.BackupID || e.ManifestPath == entry.ManifestPath {
			continue
		}
		kept = append(kept, e)
	}
	kept = append(kept, entry)
	cat.Entries = kept

	// Seed catalog-wide fields on first write.
	if cat.ChainRootBackupID == "" && len(cat.Entries) > 0 {
		cat.ChainRootBackupID = cat.Entries[0].BackupID
	}
	if cat.SourceEngine == "" {
		cat.SourceEngine = manifest.SourceEngine
	}
	if cat.CreatedAt.IsZero() {
		cat.CreatedAt = time.Now().UTC()
	}
	cat.UpdatedAt = time.Now().UTC()
	cat.FormatVersion = chainCatalogFormatVersion
	cat.SluiceVersion = manifest.SluiceVersion

	return writeChainCatalog(ctx, store, cat)
}

// newCatalogEntry projects a manifest into its catalog representation.
// Field selection mirrors [ChainCatalogEntry]'s schema; consumers
// reading the entry get enough metadata to answer chain-shape
// questions without parsing the underlying manifest body.
func newCatalogEntry(m *ir.Manifest, path string, fileCount int) ChainCatalogEntry {
	return ChainCatalogEntry{
		BackupID:       m.BackupID,
		Kind:           canonicalKind(m.Kind),
		ParentBackupID: m.ParentBackupID,
		ManifestPath:   path,
		EndPosition:    m.EndPosition,
		CreatedAt:      m.CreatedAt.UTC(),
		FileCount:      fileCount,
		Tombstoned:     false,
	}
}

// canonicalKind normalises empty Kind to BackupKindFull. Mirror of
// the ir.canonicalKind helper used by [ir.ComputeBackupID]; duplicated
// here so the catalog code doesn't pull in unexported ir helpers.
func canonicalKind(kind string) string {
	if kind == "" {
		return ir.BackupKindFull
	}
	return kind
}

// rebuildChainCatalog walks the chain on disk (the full's manifest at
// the legacy path plus every incremental manifest under manifests/)
// and constructs a fresh [ChainCatalog] from scratch. Used by:
//
//  1. [updateChainCatalog]'s lazy-rebuild path on the first v0.47.0
//     write into a legacy chain.
//  2. The `sluice backup verify --rebuild-catalog` operator command
//     for explicit catalog regeneration.
//
// Cost is one List + one Get per manifest — same as today's
// [listAllManifests] walk — incurred once on the first write to a
// legacy chain. Subsequent updates pay only one Get + one Put.
func rebuildChainCatalog(ctx context.Context, store ir.BackupStore) (*ChainCatalog, error) {
	records, err := listAllManifestsViaWalk(ctx, store)
	if err != nil {
		return nil, err
	}
	now := time.Now().UTC()
	cat := &ChainCatalog{
		FormatVersion: chainCatalogFormatVersion,
		CreatedAt:     now,
		UpdatedAt:     now,
		Entries:       make([]ChainCatalogEntry, 0, len(records)),
	}
	for _, rec := range records {
		fc := manifestFileCount(rec.manifest)
		// Pre-v0.17.0 manifests had no BackupID — synthesize via
		// the same helper the chain walker uses so the entry has a
		// stable key.
		id := rec.manifest.BackupID
		if id == "" {
			id = ir.ComputeBackupID(rec.manifest)
		}
		entry := newCatalogEntry(rec.manifest, rec.path, fc)
		entry.BackupID = id
		cat.Entries = append(cat.Entries, entry)
		if cat.SourceEngine == "" {
			cat.SourceEngine = rec.manifest.SourceEngine
		}
		if cat.SluiceVersion == "" {
			cat.SluiceVersion = rec.manifest.SluiceVersion
		}
	}
	// Chain order: stable sort by (kind, manifest path). Full first
	// (kind sorts "full" < "incremental" lexically), then
	// incrementals by the lex-sortable path (which encodes
	// UnixMilli). Same ordering [listAllManifestsViaWalk] produces.
	sort.SliceStable(cat.Entries, func(i, j int) bool {
		ki, kj := cat.Entries[i].Kind, cat.Entries[j].Kind
		if ki != kj {
			return ki == ir.BackupKindFull
		}
		return cat.Entries[i].ManifestPath < cat.Entries[j].ManifestPath
	})
	if len(cat.Entries) > 0 {
		cat.ChainRootBackupID = cat.Entries[0].BackupID
	}
	return cat, nil
}

// filterActiveEntries returns entries from cat with Tombstoned == false.
// Used by [readManifestsFromCatalog] so a v0.48.0+ chain.json with
// tombstones doesn't surface compacted-out manifests to a v0.47.0
// reader.
func filterActiveEntries(entries []ChainCatalogEntry) []ChainCatalogEntry {
	out := entries[:0:len(entries)]
	for _, e := range entries {
		if !e.Tombstoned {
			out = append(out, e)
		}
	}
	return out
}

// readManifestsFromCatalog opens every active (non-tombstoned)
// manifest referenced by cat and returns the [manifestRecord] slice
// in chain order — the same shape today's [listAllManifestsViaWalk]
// produces, so [listAllManifests] can dispatch between the two
// transparently.
//
// Per-manifest Get errors propagate as a fatal: a catalog that
// references a missing manifest is a chain-integrity problem the
// operator needs to know about (manual deletion, partial restore,
// corrupted prune).
func readManifestsFromCatalog(
	ctx context.Context,
	store ir.BackupStore,
	cat *ChainCatalog,
) ([]manifestRecord, error) {
	active := filterActiveEntries(cat.Entries)
	out := make([]manifestRecord, 0, len(active))
	for _, e := range active {
		m, err := readManifestAt(ctx, store, e.ManifestPath)
		if err != nil {
			return nil, fmt.Errorf("chain catalog references %q: %w", e.ManifestPath, err)
		}
		out = append(out, manifestRecord{path: e.ManifestPath, manifest: m})
	}
	return out, nil
}

// manifestFileCount returns the total number of chunk files referenced
// by a manifest — per-table chunks (fulls) plus change chunks
// (incrementals). Exposed so call sites computing the value for
// [updateChainCatalogBestEffort] don't have to duplicate the sum.
func manifestFileCount(m *ir.Manifest) int {
	if m == nil {
		return 0
	}
	fc := 0
	for _, t := range m.Tables {
		fc += len(t.Chunks)
	}
	fc += len(m.ChangeChunks)
	return fc
}

// RebuildChainCatalogAt walks the chain on store, builds a fresh
// [ChainCatalog], and writes it as chain.json. Returns the number of
// entries written. Operator-facing entry point for the `sluice backup
// verify --rebuild-catalog` flag.
func RebuildChainCatalogAt(ctx context.Context, store ir.BackupStore) (int, error) {
	cat, err := rebuildChainCatalog(ctx, store)
	if err != nil {
		return 0, err
	}
	if err := writeChainCatalog(ctx, store, cat); err != nil {
		return 0, err
	}
	return len(cat.Entries), nil
}

// updateChainCatalogBestEffort wraps [updateChainCatalog] and demotes
// errors to a WARN log line. Callers at every writeManifest site use
// this so a catalog hiccup doesn't fail the operator's backup run.
// The next read either uses the still-valid earlier catalog state
// (acceptable — the new manifest is on disk; readers can fall back
// to the walk) or triggers a lazy rebuild on the next write.
func updateChainCatalogBestEffort(
	ctx context.Context,
	store ir.BackupStore,
	manifest *ir.Manifest,
	manifestPath string,
	fileCount int,
) {
	if err := updateChainCatalog(ctx, store, manifest, manifestPath, fileCount); err != nil {
		slog.WarnContext(ctx, "chain catalog update failed; chain.json may be stale until next write",
			slog.String("manifest_path", manifestPath),
			slog.String("err", err.Error()),
		)
	}
}
