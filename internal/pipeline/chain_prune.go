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

	"sluicesync.dev/sluice/internal/ir"
	irbackup "sluicesync.dev/sluice/internal/ir/backup"
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
	manifest *irbackup.Manifest
}

// PruneChain executes a retention prune against the lineage in store.
// See package doc for semantics. Returns the summary or a wrapped
// error on any pre-flight refusal / I/O failure.
func PruneChain(ctx context.Context, store irbackup.Store, opts PruneOpts) (*PruneResult, error) {
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
	// ADR-0067: if prune trimmed the floor segment's leading incrementals,
	// its earliest incremental coverage moved forward; resync the floor
	// segment's IncrementalCoverageStart so the restore-time within-segment
	// integrity check (validateFirstIncrementalBoundary) stays valid. An
	// untrimmed floor segment keeps its recorded value.
	if len(newSegs) > 0 {
		switch {
		case len(kept) > 0 && kept[0].segIdx == floorSeg && keepFromInSeg > 0:
			// Floor segment's leading incrementals were trimmed; the new
			// first incremental is kept[0] — its StartPosition is the new
			// earliest coverage.
			newSegs[0].IncrementalCoverageStart = kept[0].manifest.StartPosition
		case len(kept) == 0:
			// Everything dropped: the floor segment retains only its full,
			// so there is no incremental coverage — clear the field (it
			// resolves to StartPosition, the full anchor).
			newSegs[0].IncrementalCoverageStart = ir.Position{}
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
	slog.InfoContext(
		ctx, "prune: lineage pruned",
		slog.Int("segments_dropped", res.SegmentsDropped),
		slog.Int("manifests_dropped", len(res.Pruned)),
		slog.Int("chunks_deleted", res.ChunksDeleted),
	)
	return res, nil
}

// r0 is the "nothing to prune" early return: report the full kept set
// without mutating anything.
func r0(cat *LineageCatalog, store irbackup.Store, ctx context.Context, why string) (*PruneResult, error) {
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

// SchemaHistoryRetentionFloor returns the combined floor below which
// ADR-0049 schema-history rows are safe to compact (DP-2):
//
//	min(liveSafePoint, oldest retained backup chain's resume position)
//
// "Min" is defined by the engine's [ir.PositionOrderer]: the older of
// the two, where "older" = the one the other is at-or-after but the
// reverse is false. Two incomparable positions (partial order, e.g. two
// MySQL GTID sets that diverge) yield a loud error — compaction can't
// pick a safe floor when the candidates can't be ordered, and a guess
// is the Bug-74 silent-loss class this ADR exists to kill.
//
// liveSafePoint is the live persisted ADR-0007 CDC safe-point on the
// target (sluice_cdc_state.source_position for the active stream).
// Callers extract it from the target before invoking this helper —
// chain_prune.go has no target connection. An empty live position
// (no active stream) reduces the floor to the backup-side alone.
//
// Returns the chosen floor and ok=true on success; ok=false when there
// is NO floor to apply (no backups in store and an empty live position
// — the caller must skip compaction in that case, since deleting
// everything would defeat the loud-floor sentinel).
//
// ADR-0049 Chunk D one-shot wiring: the caller passes the resulting
// floor to [ir.SchemaHistoryCompactor.CompactSchemaHistoryBelow] on the
// target's [ir.ChangeApplier]. Compaction is conservative by default
// (locked DP-2 "retain generously"); this helper is the safety wrapper
// that ensures the floor is never above the backup-resume needs.
func SchemaHistoryRetentionFloor(
	ctx context.Context,
	store irbackup.Store,
	liveSafePoint ir.Position,
	orderer ir.PositionOrderer,
) (floor ir.Position, ok bool, err error) {
	if orderer == nil {
		return ir.Position{}, false, errors.New("schema-history floor: orderer is nil; ordering is a correctness primitive (loud-failure tenet)")
	}
	backupFloor, backupOK, err := oldestRetainedBackupResumePosition(ctx, store)
	if err != nil {
		return ir.Position{}, false, fmt.Errorf("schema-history floor: load lineage: %w", err)
	}
	liveOK := liveSafePoint.Engine != "" || liveSafePoint.Token != ""

	switch {
	case !backupOK && !liveOK:
		return ir.Position{}, false, nil
	case !backupOK:
		return liveSafePoint, true, nil
	case !liveOK:
		return backupFloor, true, nil
	}
	// Both present: pick the OLDER under the partial order.
	liveAfter, err := orderer.PositionAtOrAfter(liveSafePoint, backupFloor)
	if err != nil {
		return ir.Position{}, false, fmt.Errorf("schema-history floor: order live vs backup: %w", err)
	}
	backupAfter, err := orderer.PositionAtOrAfter(backupFloor, liveSafePoint)
	if err != nil {
		return ir.Position{}, false, fmt.Errorf("schema-history floor: order backup vs live: %w", err)
	}
	switch {
	case liveAfter && !backupAfter:
		// live > backup → backup is the older (smaller) → it is the
		// safe min-floor.
		return backupFloor, true, nil
	case backupAfter && !liveAfter:
		return liveSafePoint, true, nil
	case liveAfter && backupAfter:
		// Equal positions — either is fine.
		return liveSafePoint, true, nil
	default:
		// Neither is at-or-after the other: incomparable. Loud refuse
		// (Bug-74 class — never guess on a partial order).
		return ir.Position{}, false, fmt.Errorf(
			"schema-history floor: live safe-point %+v and oldest backup resume %+v are incomparable under the engine's partial order; cannot pick a single retention floor (loud-failure tenet)",
			liveSafePoint, backupFloor,
		)
	}
}

// oldestRetainedBackupResumePosition returns the resume position the
// OLDEST currently-retained backup chain in store would resume CDC
// from after restore. For a chain root [full, incr_1, …, incr_N]:
//
//   - If the full has a recorded EndPosition (v0.18.0+), that IS the
//     resume position (snapshot's anchor LSN/GTID + the chain's events
//     forward).
//   - Else falls back to the OLDEST incremental's StartPosition (the
//     pre-v0.18.0 shape, where fulls didn't record EndPosition).
//   - If neither is populated, no usable floor → ok=false.
//
// The "oldest retained" chain is the one at the lineage catalog's
// [LineageCatalog.RestorableFromSegment] index (the first segment the
// lineage considers restorable post-prune).
func oldestRetainedBackupResumePosition(ctx context.Context, store irbackup.Store) (ir.Position, bool, error) {
	cat, ok, err := loadLineageCatalog(ctx, store)
	if err != nil {
		return ir.Position{}, false, fmt.Errorf("load lineage: %w", err)
	}
	if !ok || len(cat.Segments) == 0 {
		return ir.Position{}, false, nil
	}
	if cat.RestorableFromSegment < 0 || cat.RestorableFromSegment >= len(cat.Segments) {
		return ir.Position{}, false, fmt.Errorf("lineage restorable_from_segment=%d out of range", cat.RestorableFromSegment)
	}
	seg := &cat.Segments[cat.RestorableFromSegment]
	segStore := seg.store(store)
	fm, err := readManifestAt(ctx, segStore, seg.FullManifestPath)
	if err != nil {
		return ir.Position{}, false, fmt.Errorf("read oldest full %q: %w", seg.FullManifestPath, err)
	}
	if fm.EndPosition.Engine != "" || fm.EndPosition.Token != "" {
		return fm.EndPosition, true, nil
	}
	// Pre-v0.18.0 fall-back: the oldest incremental's StartPosition.
	if len(seg.Incrementals) == 0 {
		return ir.Position{}, false, nil
	}
	im, err := readManifestAt(ctx, segStore, seg.Incrementals[0])
	if err != nil {
		return ir.Position{}, false, fmt.Errorf("read oldest incremental %q: %w", seg.Incrementals[0], err)
	}
	if im.StartPosition.Engine == "" && im.StartPosition.Token == "" {
		return ir.Position{}, false, nil
	}
	return im.StartPosition, true, nil
}
