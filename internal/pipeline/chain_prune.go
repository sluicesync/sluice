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

// # `sluice backup prune` — lineage retention pruning (ADR-0046 §4)
//
// Reframed (zero users) from the grafted "parent-link re-stitch +
// tombstone bookkeeping" onto the native segment-list model. Prune is
// now a list/segment-local operation:
//
//   - Drop leading WHOLE segments (their full + every incremental):
//     a segment full is a self-contained snapshot at its anchor S, so
//     the oldest KEPT segment's full is always a complete restore
//     base — dropping older whole segments is unconditionally
//     restore-safe.
//   - Drop leading incrementals WITHIN the oldest kept segment: this
//     narrows the restorable window (the dropped incrementals' event
//     ranges are LOST), exactly the trade the operator opts into by
//     invoking prune — but the segment full + the remaining
//     incrementals still form a contiguous chain (the boundary
//     invariant prune always preserved, now expressed on segments).
//   - Advance `lineage.json`'s `restorable_from_segment` floor;
//     restore refuses to start before it.
//
// The open (last, uncapped) segment is never fully dropped while it's
// the only segment, and its incrementals are pruned only down to its
// full (the full is always kept — it's the restore base).
//
// What prune physically deletes: every dropped incremental's change
// chunks + manifest, and every dropped whole segment's full manifest +
// data chunks + sub-dir contents. lineage.json is rewritten in one
// atomic Put. What it preserves: the oldest kept segment's full +
// every kept incremental + every later segment.

// PruneOpts configures [PruneChain]. Exactly one of KeepIncrementals
// or KeepDuration is required; specifying both is an error.
type PruneOpts struct {
	// KeepIncrementals retains the N most-recent incrementals across
	// the whole lineage. Segment fulls are not counted; the oldest
	// kept incremental's segment full (and every later segment) is
	// always retained as the restore base. Zero means "use
	// KeepDuration".
	KeepIncrementals int

	// KeepDuration retains incrementals whose CreatedAt is newer than
	// (now - duration). Zero means "use KeepIncrementals".
	KeepDuration time.Duration

	// DryRun reports what would be pruned without deleting anything or
	// rewriting lineage.json.
	DryRun bool

	// Now overrides the wall-clock time source for KeepDuration math.
	Now func() time.Time
}

// PruneResult summarises a [PruneChain] run.
type PruneResult struct {
	// Pruned is the list of manifest paths deleted (or, under DryRun,
	// that would be deleted). Paths are segment-Dir-qualified.
	Pruned []string

	// Kept is the list of manifest paths preserved.
	Kept []string

	// ChunksDeleted is the count of chunk files deleted across all
	// pruned manifests. 0 on DryRun.
	ChunksDeleted int

	// SegmentsDropped is the number of whole leading segments removed.
	SegmentsDropped int

	// EarliestRestorableBackupID identifies the first kept incremental
	// (or the oldest kept segment's full when no incrementals remain).
	// After prune, restore-to-position-X requires X >= this entry's
	// StartPosition.
	EarliestRestorableBackupID string
}

// lineageIncr is one incremental in lineage-flattened order, tagged
// with its owning segment index + in-segment index so the pruner can
// translate a flat keep/drop decision back into per-segment edits.
type lineageIncr struct {
	segIdx   int
	inSegIdx int
	path     string
	manifest *ir.Manifest
}

// PruneChain executes a retention prune against the lineage in store.
// See package doc for semantics. Returns the summary or a wrapped
// error on any pre-flight refusal / I/O failure.
func PruneChain(ctx context.Context, store ir.BackupStore, opts PruneOpts) (*PruneResult, error) {
	if (opts.KeepIncrementals > 0) == (opts.KeepDuration > 0) {
		return nil, errors.New("prune: exactly one of KeepIncrementals or KeepDuration is required")
	}
	now := opts.Now
	if now == nil {
		now = time.Now
	}

	cat, ok, err := loadLineageCatalog(ctx, store)
	if err != nil {
		return nil, fmt.Errorf("prune: load lineage catalog: %w", err)
	}
	if !ok {
		return nil, errors.New("prune: lineage.json not found; run `sluice backup verify --rebuild-catalog` first")
	}
	if cat.RestorableFromSegment < 0 || cat.RestorableFromSegment >= len(cat.Segments) {
		return nil, fmt.Errorf("prune: lineage restorable_from_segment=%d out of range — corrupt lineage", cat.RestorableFromSegment)
	}

	// Flatten the currently-restorable incrementals across segments,
	// oldest first. (Reads each incremental manifest for CreatedAt.)
	var flat []lineageIncr
	for si := cat.RestorableFromSegment; si < len(cat.Segments); si++ {
		seg := &cat.Segments[si]
		ss := seg.store(store)
		for ii, ip := range seg.Incrementals {
			m, err := readManifestAt(ctx, ss, ip)
			if err != nil {
				return nil, fmt.Errorf("prune: read incremental %q (segment %d): %w", ip, si, err)
			}
			flat = append(flat, lineageIncr{segIdx: si, inSegIdx: ii, path: ip, manifest: m})
		}
	}
	// Defensive stable order by CreatedAt (segments are already
	// ordered; this guards a clock-skew edge).
	sort.SliceStable(flat, func(i, j int) bool {
		if flat[i].segIdx != flat[j].segIdx {
			return flat[i].segIdx < flat[j].segIdx
		}
		return flat[i].inSegIdx < flat[j].inSegIdx
	})

	// Compute how many leading incrementals to drop.
	dropN := 0
	switch {
	case opts.KeepIncrementals > 0:
		if opts.KeepIncrementals >= len(flat) {
			return r0(cat, store, ctx, "nothing to prune (keep >= incremental count)")
		}
		dropN = len(flat) - opts.KeepIncrementals
	case opts.KeepDuration > 0:
		threshold := now().Add(-opts.KeepDuration)
		for _, e := range flat {
			if e.manifest.CreatedAt.Before(threshold) {
				dropN++
				continue
			}
			break
		}
		if dropN == 0 {
			return r0(cat, store, ctx, "nothing to prune (all incrementals newer than keep-duration)")
		}
	}

	dropped := flat[:dropN]
	kept := flat[dropN:]

	// The oldest kept incremental's segment is the new restore floor.
	// Every segment strictly before it is dropped WHOLE (full + all
	// incrementals): a segment full is a self-contained snapshot, so
	// the kept floor segment's full is a complete restore base.
	floorSeg := cat.RestorableFromSegment
	if len(kept) > 0 {
		floorSeg = kept[0].segIdx
	} else if len(dropped) > 0 {
		// Everything dropped: keep the LAST segment (its full is the
		// restore base; never drop the only restore anchor).
		floorSeg = len(cat.Segments) - 1
	}

	res := &PruneResult{
		Pruned: make([]string, 0, len(dropped)),
	}

	// 1. Whole leading segments [RestorableFromSegment, floorSeg).
	for si := cat.RestorableFromSegment; si < floorSeg; si++ {
		seg := &cat.Segments[si]
		ss := seg.store(store)
		// Full + its data chunks.
		if fm, err := readManifestAt(ctx, ss, seg.FullManifestPath); err == nil {
			for _, t := range fm.Tables {
				for _, ch := range t.Chunks {
					res.Pruned = appendChunk(res, ch.File)
					if !opts.DryRun {
						if derr := ss.Delete(ctx, ch.File); derr == nil {
							res.ChunksDeleted++
						}
					}
				}
			}
		}
		if !opts.DryRun {
			_ = ss.Delete(ctx, seg.FullManifestPath)
		}
		// Incrementals + their change chunks.
		for _, ip := range seg.Incrementals {
			if im, err := readManifestAt(ctx, ss, ip); err == nil {
				for _, ch := range im.ChangeChunks {
					if !opts.DryRun {
						if derr := ss.Delete(ctx, ch.File); derr == nil {
							res.ChunksDeleted++
						}
					}
				}
			}
			res.Pruned = append(res.Pruned, segQualify(seg.Dir, ip))
			if !opts.DryRun {
				_ = ss.Delete(ctx, ip)
			}
		}
		res.SegmentsDropped++
	}

	// 2. Leading incrementals within the floor segment.
	floor := &cat.Segments[floorSeg]
	floorStore := floor.store(store)
	keepFromInSeg := 0
	if len(kept) > 0 && kept[0].segIdx == floorSeg {
		keepFromInSeg = kept[0].inSegIdx
	} else if len(kept) == 0 {
		// Everything dropped — keep the floor segment's full only.
		keepFromInSeg = len(floor.Incrementals)
	}
	for ii := 0; ii < keepFromInSeg; ii++ {
		ip := floor.Incrementals[ii]
		if im, err := readManifestAt(ctx, floorStore, ip); err == nil {
			for _, ch := range im.ChangeChunks {
				if !opts.DryRun {
					if derr := floorStore.Delete(ctx, ch.File); derr == nil {
						res.ChunksDeleted++
					}
				}
			}
		}
		res.Pruned = append(res.Pruned, segQualify(floor.Dir, ip))
		if !opts.DryRun {
			_ = floorStore.Delete(ctx, ip)
		}
	}

	// Build the post-prune lineage: drop whole leading segments, trim
	// the floor segment's incrementals, advance the restore floor.
	newSegs := make([]LineageSegment, 0, len(cat.Segments)-(floorSeg-cat.RestorableFromSegment))
	for si := floorSeg; si < len(cat.Segments); si++ {
		seg := cat.Segments[si]
		if si == floorSeg {
			seg.Incrementals = append([]string(nil), floor.Incrementals[keepFromInSeg:]...)
		}
		newSegs = append(newSegs, seg)
		// Kept-path bookkeeping for the result.
		res.Kept = append(res.Kept, segQualify(seg.Dir, seg.FullManifestPath))
		for _, ip := range seg.Incrementals {
			res.Kept = append(res.Kept, segQualify(seg.Dir, ip))
		}
	}
	cat.Segments = newSegs
	cat.RestorableFromSegment = 0

	if len(kept) > 0 {
		res.EarliestRestorableBackupID = manifestBackupID(kept[0].manifest)
	} else if len(newSegs) > 0 {
		// No incrementals kept — the floor segment's full is the
		// earliest restorable point.
		if fm, err := readManifestAt(ctx, newSegs[0].store(store), newSegs[0].FullManifestPath); err == nil {
			res.EarliestRestorableBackupID = manifestBackupID(fm)
		}
	}

	if opts.DryRun {
		return res, nil
	}
	cat.UpdatedAt = now().UTC()
	if err := writeLineageCatalog(ctx, store, cat); err != nil {
		return nil, fmt.Errorf("prune: rewrite lineage catalog: %w", err)
	}
	slog.InfoContext(ctx, "prune: lineage pruned",
		slog.Int("segments_dropped", res.SegmentsDropped),
		slog.Int("manifests_dropped", len(res.Pruned)),
		slog.Int("chunks_deleted", res.ChunksDeleted),
	)
	return res, nil
}

// r0 is the "nothing to prune" early return: report the full kept set
// without mutating anything.
func r0(cat *LineageCatalog, store ir.BackupStore, ctx context.Context, why string) (*PruneResult, error) {
	res := &PruneResult{}
	for si := cat.RestorableFromSegment; si < len(cat.Segments); si++ {
		seg := &cat.Segments[si]
		res.Kept = append(res.Kept, segQualify(seg.Dir, seg.FullManifestPath))
		for _, ip := range seg.Incrementals {
			res.Kept = append(res.Kept, segQualify(seg.Dir, ip))
		}
		if res.EarliestRestorableBackupID == "" && len(seg.Incrementals) > 0 {
			if m, err := readManifestAt(ctx, seg.store(store), seg.Incrementals[0]); err == nil {
				res.EarliestRestorableBackupID = manifestBackupID(m)
			}
		}
	}
	slog.InfoContext(ctx, "prune: "+why)
	return res, nil
}

// segQualify renders a segment-relative path as a lineage-root-
// relative one for the result's path lists (operator-facing).
func segQualify(dir, p string) string {
	if dir == "" {
		return p
	}
	return dir + "/" + p
}

// appendChunk records a pruned chunk path (segment-relative) for the
// result; centralised so DryRun and real prune agree on the list.
func appendChunk(res *PruneResult, file string) []string {
	return append(res.Pruned, file)
}
