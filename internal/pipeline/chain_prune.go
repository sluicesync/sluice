// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package pipeline

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sort"
	"time"

	"github.com/orware/sluice/internal/ir"
)

// # `sluice backup prune` — chain retention pruning (GitHub #20, roadmap 14c)
//
// Pre-v0.50.0 chains grow forever. Operators using local-FS chains
// have no built-in way to expire old incrementals; manual deletion
// works but leaves chain.json out of sync and can break restorability
// in subtle ways (delete a parent but keep a child → child can't
// restore on its own). 14c lands the safe primitive: drop the
// oldest N incrementals (or oldest-than-DUR), advancing the chain's
// "earliest restorable position" forward. The full stays as the
// chain root; only the oldest end gets pruned.
//
// Pre-v0.51.0 (14b rotate-at), pruning ALWAYS narrows the restorable
// window of a chain — pruning incremental 3 means you can no longer
// restore to any GTID/LSN between incremental 2's end and
// incremental 3's end. Operators acknowledge this trade by invoking
// `sluice backup prune` explicitly; there's no implicit pruning.
//
// Restorability invariant: after prune, the chain still consists of
// the full + the remaining (chain-ordered) incrementals, with the
// FIRST surviving incremental's parent being either the full or a
// remaining incremental. The pruner verifies this before deletion.
// If the operator's keep-set would orphan a kept incremental
// (parent dropped), the operation refuses with a clear message.
//
// What pruning physically deletes:
//
//   - Each dropped incremental's change-chunk files (under
//     `chunks/_changes/<rollover-ts>/`)
//   - Each dropped incremental's manifest file (under
//     `manifests/incr-*.json`)
//   - chain.json updated to remove the dropped entries
//
// What pruning preserves:
//
//   - The full manifest (chain root)
//   - The full's data chunks under `chunks/<table>/`
//   - Every kept incremental's manifest + chunks
//   - The remaining chain.json catalog
//
// What pruning does NOT do:
//
//   - Re-anchor the chain to a synthetic full at the new earliest-
//     incremental's start position. That's 14b's rotate-at scope.
//   - Compact the surviving incrementals (14d's job).
//   - Continue against the destination's backup-stream-run state file.
//     The operator is expected to either run prune while no stream is
//     active OR accept that the stream's parent-resolution may
//     reference a now-pruned manifest (the stream will fail on next
//     restart with a clear "parent not found" message).

// PruneOpts configures [PruneChain]. Exactly one of KeepIncrementals
// or KeepDuration is required; specifying both is an error.
type PruneOpts struct {
	// KeepIncrementals retains the N most-recent incrementals.
	// The full is always kept regardless of this value. Zero means
	// "use KeepDuration instead" (and KeepDuration must be set).
	KeepIncrementals int

	// KeepDuration retains incrementals whose CreatedAt is newer
	// than (now - duration). Zero means "use KeepIncrementals
	// instead" (and KeepIncrementals must be set).
	KeepDuration time.Duration

	// DryRun: report what would be pruned without deleting anything
	// or updating chain.json. Returns the would-prune list in the
	// PruneResult.
	DryRun bool

	// Now overrides the wall-clock time source for KeepDuration math.
	// Default time.Now; tests inject a stub.
	Now func() time.Time
}

// PruneResult summarises a [PruneChain] run.
type PruneResult struct {
	// Pruned is the list of manifest paths that were deleted (or
	// would be deleted under DryRun).
	Pruned []string

	// Kept is the list of manifest paths that were preserved.
	Kept []string

	// ChunksDeleted is the count of chunk files deleted across all
	// pruned manifests. 0 on DryRun.
	ChunksDeleted int

	// EarliestRestorableBackupID identifies the first kept entry
	// after the full. After prune, restore-to-position-X requires
	// X >= this entry's StartPosition (or full's EndPosition if no
	// incrementals are kept). Empty when no incrementals survived.
	EarliestRestorableBackupID string
}

// PruneChain executes a retention prune against the chain in store.
// See package doc for semantics + invariants. Returns the resulting
// summary or a wrapped error on any pre-flight refusal / I/O failure.
//
// Pre-flight refusal cases (no mutation occurs):
//
//   - chain.json absent OR unparseable (operator must rebuild via
//     `sluice backup verify --rebuild-catalog` first)
//   - Both KeepIncrementals and KeepDuration set
//   - Neither KeepIncrementals nor KeepDuration set
//   - KeepIncrementals >= incremental count (nothing to prune)
//   - Keep-set would orphan a surviving incremental (its parent
//     would be dropped)
func PruneChain(ctx context.Context, store ir.BackupStore, opts PruneOpts) (*PruneResult, error) {
	if (opts.KeepIncrementals > 0) == (opts.KeepDuration > 0) {
		return nil, errors.New("prune: exactly one of KeepIncrementals or KeepDuration is required")
	}
	now := opts.Now
	if now == nil {
		now = time.Now
	}

	cat, present, err := loadChainCatalog(ctx, store)
	if err != nil {
		return nil, fmt.Errorf("prune: load chain catalog: %w", err)
	}
	if !present {
		return nil, errors.New("prune: chain.json not found; run `sluice backup verify --rebuild-catalog` first")
	}

	// Split the catalog into full(s) + incrementals (chain order).
	var fulls, incrs []ChainCatalogEntry
	for _, e := range cat.Entries {
		if e.Tombstoned {
			continue
		}
		switch e.Kind {
		case ir.BackupKindFull:
			fulls = append(fulls, e)
		case ir.BackupKindIncremental:
			incrs = append(incrs, e)
		}
	}
	if len(fulls) == 0 {
		return nil, errors.New("prune: chain has no full backup; refusing to prune")
	}
	// Incrementals are chain-ordered by CreatedAt; sort defensively.
	sort.SliceStable(incrs, func(i, j int) bool {
		return incrs[i].CreatedAt.Before(incrs[j].CreatedAt)
	})

	// Compute the drop-set: oldest end of incrs.
	dropN := 0
	switch {
	case opts.KeepIncrementals > 0:
		if opts.KeepIncrementals >= len(incrs) {
			return &PruneResult{
				Kept:                       collectPaths(fulls, incrs),
				EarliestRestorableBackupID: firstID(incrs),
			}, nil
		}
		dropN = len(incrs) - opts.KeepIncrementals
	case opts.KeepDuration > 0:
		threshold := now().Add(-opts.KeepDuration)
		for _, e := range incrs {
			if e.CreatedAt.Before(threshold) {
				dropN++
				continue
			}
			break
		}
		if dropN == 0 {
			return &PruneResult{
				Kept:                       collectPaths(fulls, incrs),
				EarliestRestorableBackupID: firstID(incrs),
			}, nil
		}
	}
	dropped := incrs[:dropN]
	kept := incrs[dropN:]

	// Internal-orphan check: after pruning a contiguous oldest-prefix,
	// the remaining kept incrementals must form a continuous chain
	// among themselves (each kept incremental's parent is either the
	// full, a dropped incremental, or another kept incremental).
	//
	// The first-kept incremental's parent (when in the drop set) gets
	// re-stitched to point at the full below — that's the documented
	// "advance the earliest restorable position" semantic from
	// roadmap 14c. But interior orphans (a kept incremental whose
	// parent is in the drop set AND it's not the first kept) indicate
	// the catalog is structurally broken (parent links aren't a
	// linear chain) and the operator must intervene manually.
	keptIDs := make(map[string]struct{}, len(fulls)+len(kept))
	for _, e := range fulls {
		keptIDs[e.BackupID] = struct{}{}
	}
	for _, e := range kept {
		keptIDs[e.BackupID] = struct{}{}
	}
	droppedIDs := make(map[string]struct{}, len(dropped))
	for _, e := range dropped {
		droppedIDs[e.BackupID] = struct{}{}
	}
	for i, e := range kept {
		if e.ParentBackupID == "" {
			// Pre-v0.17.0 chain shape with no parent links; tolerate.
			continue
		}
		if _, ok := keptIDs[e.ParentBackupID]; ok {
			continue // parent is kept — fine
		}
		if i == 0 {
			continue // first-kept's parent is in drop set — fine, gets re-stitched below
		}
		// Interior kept entry whose parent isn't kept and isn't a
		// drop-set member to bridge over — structural break.
		if _, inDrop := droppedIDs[e.ParentBackupID]; !inDrop {
			return nil, fmt.Errorf("prune: kept incremental %q has parent %q which is neither in the catalog nor in the drop set; chain is structurally broken — run `sluice backup verify --rebuild-catalog` to inspect",
				e.BackupID, e.ParentBackupID)
		}
		// Interior kept whose parent is in drop set means the chain
		// would need MORE than one re-stitch — not supported by v1.
		// Operator must lower --keep-incrementals or use 14b
		// rotate-at to create a new chain root.
		return nil, fmt.Errorf("prune: refusing to break chain — interior kept incremental %q has parent %q in the drop set (only the first-kept can re-stitch). Lower --keep-incrementals so the drop set doesn't include interior parents",
			e.BackupID, e.ParentBackupID)
	}

	// Build the result + execute the drops.
	res := &PruneResult{
		Pruned:                     make([]string, 0, len(dropped)),
		Kept:                       collectPaths(fulls, kept),
		EarliestRestorableBackupID: firstID(kept),
	}
	for _, e := range dropped {
		res.Pruned = append(res.Pruned, e.ManifestPath)
	}

	if opts.DryRun {
		return res, nil
	}

	// Re-stitch the first kept incremental's manifest if its parent
	// was in the drop set. This narrows the chain's restorable range
	// (the dropped incrementals' event windows are LOST from the
	// chain — operator opts into this) but keeps chain restore's
	// parent-link walk + StartPosition validation passing.
	if len(kept) > 0 && len(fulls) > 0 {
		first := kept[0]
		if _, inDrop := droppedIDs[first.ParentBackupID]; inDrop {
			fullEntry := fulls[0]
			if err := restitchManifest(ctx, store, first.ManifestPath, fullEntry); err != nil {
				return nil, fmt.Errorf("prune: re-stitch first-kept manifest %q: %w", first.ManifestPath, err)
			}
			// Update the catalog entry so subsequent reads see the
			// correct parent link.
			for i := range cat.Entries {
				if cat.Entries[i].BackupID == first.BackupID {
					cat.Entries[i].ParentBackupID = fullEntry.BackupID
					// Mirror the update into the kept slice we just
					// captured (it's a copy, but EarliestRestorable
					// already records first.BackupID).
					break
				}
			}
		}
	}

	// Actually delete + rewrite catalog.
	for _, e := range dropped {
		// Read the manifest to enumerate chunk files (defensive — we
		// could also walk chunks/_changes/<run-namespace>/ but the
		// manifest's ChangeChunks list is authoritative).
		m, err := readManifestAt(ctx, store, e.ManifestPath)
		if err != nil {
			slog.WarnContext(ctx, "prune: failed to read manifest before delete; continuing",
				slog.String("manifest_path", e.ManifestPath),
				slog.String("err", err.Error()),
			)
			continue
		}
		for _, ch := range m.ChangeChunks {
			if err := store.Delete(ctx, ch.File); err != nil {
				slog.WarnContext(ctx, "prune: failed to delete chunk; continuing",
					slog.String("chunk", ch.File),
					slog.String("err", err.Error()),
				)
				continue
			}
			res.ChunksDeleted++
		}
		// Delete the manifest itself.
		if err := store.Delete(ctx, e.ManifestPath); err != nil {
			return nil, fmt.Errorf("prune: delete manifest %q: %w", e.ManifestPath, err)
		}
	}

	// Rebuild the catalog entries: keep fulls + kept incrementals.
	newEntries := make([]ChainCatalogEntry, 0, len(fulls)+len(kept))
	newEntries = append(newEntries, fulls...)
	newEntries = append(newEntries, kept...)
	cat.Entries = newEntries
	cat.UpdatedAt = now().UTC()
	if err := writeChainCatalog(ctx, store, cat); err != nil {
		return nil, fmt.Errorf("prune: rewrite chain catalog: %w", err)
	}

	return res, nil
}

// restitchManifest rewrites the manifest at path so its ParentBackupID
// points at fullEntry.BackupID and its StartPosition matches the
// full's EndPosition. Used by [PruneChain] to re-anchor the first
// surviving incremental directly to the chain root when the original
// parent has been pruned.
//
// The manifest's change-chunk list + EndPosition + every other field
// stays intact — only the parent-link fields change. Chain restore's
// parent-link walk + StartPosition validation pass on the rewritten
// manifest; the event-data delta between full.EndPosition and the
// rewritten StartPosition is implicitly "no events" from the chain's
// perspective (it's a documented data-loss window per [PruneChain]
// semantics).
func restitchManifest(ctx context.Context, store ir.BackupStore, path string, fullEntry ChainCatalogEntry) error {
	m, err := readManifestAt(ctx, store, path)
	if err != nil {
		return fmt.Errorf("read manifest: %w", err)
	}
	m.ParentBackupID = fullEntry.BackupID
	m.StartPosition = fullEntry.EndPosition
	return writeManifestAt(ctx, store, path, m)
}

// collectPaths returns the ManifestPath slice across two entry lists,
// fulls first, then incrementals in chain order.
func collectPaths(fulls, incrs []ChainCatalogEntry) []string {
	out := make([]string, 0, len(fulls)+len(incrs))
	for _, e := range fulls {
		out = append(out, e.ManifestPath)
	}
	for _, e := range incrs {
		out = append(out, e.ManifestPath)
	}
	return out
}

// firstID returns the BackupID of entries[0] or "" if empty.
func firstID(entries []ChainCatalogEntry) string {
	if len(entries) == 0 {
		return ""
	}
	return entries[0].BackupID
}
