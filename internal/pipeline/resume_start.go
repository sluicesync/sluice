// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package pipeline

import (
	"context"
	"fmt"
	"log/slog"

	"sluicesync.dev/sluice/internal/ir"
	irbackup "sluicesync.dev/sluice/internal/ir/backup"
	"sluicesync.dev/sluice/internal/pipeline/lineage"
)

// resumeStartFromParent decides where a chain extension (`backup
// incremental`, `backup stream`) resumes the source's change stream from,
// given the parent link it chains onto.
//
// The ordinary answer is the parent's EndPosition. Two link shapes record
// an EMPTY one on purpose: a DDL-only window (the schema snapshot rides
// the manifest envelope, not the chunk stream, so nothing moves
// EndPosition — pinned by TestIncrementalWindow_SchemaSnapshotDoesNotMoveEndPosition)
// and a QUIET window in which nothing arrived at all. Before v0.138.0 both
// fell into the legacy branch meant for v0.16.x FULLS with no recorded
// position — "start from the source's CURRENT position" — which silently
// skips every change between the link's real position and now. Measured
// on the real Vitess cluster rig (audit 2026-09-01 SLM-2's VStream arm,
// 2026-09-02): a foreign resume that vtgate stalled produced exactly such
// a link, and the next `backup incremental` restarted from "current" on an
// unrelated cluster at exit 0. The shape is engine-independent: any quiet
// window followed by writes and another incremental lost those writes.
//
// The rule now: an INCREMENTAL parent with no EndPosition resumes from the
// nearest ANCESTOR that recorded one — the position the empty link itself
// started from, since a link that recorded nothing ended where it began.
// Re-streaming the empty link's window re-delivers at most its schema
// snapshot, which the schema history absorbs. Only a FULL with no
// EndPosition (the genuine v0.16.x shape) still reaches the legacy branch,
// and only when no ancestor exists to walk to.
func resumeStartFromParent(ctx context.Context, store irbackup.Store, parent *irbackup.Manifest, parentPath string) (ir.Position, error) {
	if !positionEmpty(parent.EndPosition) || parent.Kind != irbackup.BackupKindIncremental {
		return parent.EndPosition, nil
	}
	chain, err := lineage.BuildLineageChain(ctx, store, nil)
	if err != nil {
		return ir.Position{}, fmt.Errorf("parent %s records no EndPosition and the chain could not be walked to find one: %w", parentPath, err)
	}
	start, ancestor, ok := nearestAncestorPosition(chain, lineage.ManifestBackupID(parent))
	if !ok {
		return ir.Position{}, fmt.Errorf("parent incremental %s (%s) records no EndPosition and no ancestor in the chain "+
			"records one either, so this chain cannot be extended without silently skipping every change between its "+
			"last real position and the source's current one; take a fresh full backup and start a new chain: %w",
			lineage.ManifestBackupID(parent), parentPath, ir.ErrPositionInvalid)
	}
	slog.InfoContext(
		ctx, "chain: parent link recorded no EndPosition (a quiet or DDL-only window); resuming from the "+
			"nearest ancestor that did, which is where that link itself started",
		slog.String("parent", lineage.ManifestBackupID(parent)),
		slog.String("ancestor", ancestor),
		slog.String("start_position", start.Token),
	)
	return start, nil
}

// nearestAncestorPosition walks the ordered chain back from the link with
// the given BackupID to the nearest link recording a non-empty EndPosition.
// ok is false when the link is not in the chain or no ancestor records one
// (a chain whose root is a legacy full with no position, or a corrupt one).
func nearestAncestorPosition(chain []lineage.SegmentRecord, parentID string) (pos ir.Position, ancestorID string, ok bool) {
	at := -1
	for i, rec := range chain {
		if rec.Manifest != nil && lineage.ManifestBackupID(rec.Manifest) == parentID {
			at = i
			break
		}
	}
	if at < 0 {
		return ir.Position{}, "", false
	}
	for i := at - 1; i >= 0; i-- {
		m := chain[i].Manifest
		if m == nil {
			continue
		}
		if !positionEmpty(m.EndPosition) {
			return m.EndPosition, lineage.ManifestBackupID(m), true
		}
	}
	return ir.Position{}, "", false
}

func positionEmpty(p ir.Position) bool {
	return p.Engine == "" && p.Token == ""
}
