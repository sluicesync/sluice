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
// snapshot, which the schema history absorbs.
//
// A FULL with no EndPosition is REFUSED (v0.153.1, POSITIONLESS-FULL-ROOT)
// on every source whose CDC reader resumes from a recorded position. The
// legacy "start from the source's current position" branch it used to
// take was written for v0.16.x fulls, which nothing has produced since
// v0.17.2 — but the population it admits kept growing: a full FROM a
// PlanetScale Neki (no WAL position to record, v0.153.1), a MySQL full
// taken with the binlog off, and the unrecognised shapes to come. Every
// one of them extended "from now" at exit 0 after a WARN, which is the
// silent chain gap the Bug 260 door in backup.go refused to trade a loud
// failure for. The safety argument that no incremental could reach this
// branch off a Neki full rested on the router refusing replication
// connections — and the same release measured that a SHARD-TARGETED
// replication connection is accepted, so the branch was one DSN option
// away from starting a chain on one shard's changes only. A written
// invariant nobody checks is indistinguishable from one that holds; this
// is the check.
//
// The one exemption, stated: a trigger-CDC source ([ir.CDCTriggers]).
// Its fulls carry no position BY CONSTRUCTION — no trigger engine
// implements [irbackup.PositionCapturer], its reader anchors at the
// change log's current MAX(id) when handed an empty position — so the
// from-now branch is the only chain shape those engines have today.
// That is the same gap class (the window between the full's read and the
// incremental's anchor is uncovered), pre-existing, and filed as its own
// item rather than closed here by refusing every trigger-engine chain.
func resumeStartFromParent(ctx context.Context, store irbackup.Store, src ir.Engine, parent *irbackup.Manifest, parentPath string) (ir.Position, error) {
	if !positionEmpty(parent.EndPosition) {
		return parent.EndPosition, nil
	}
	if parent.Kind != irbackup.BackupKindIncremental {
		if src.Capabilities().CDC == ir.CDCTriggers {
			return ir.Position{}, nil
		}
		return ir.Position{}, fmt.Errorf("%s: parent full %s (%s) records no EndPosition, so there is no position for this "+
			"chain to resume from; extending it would start the chain from the source's CURRENT position and silently "+
			"skip every change between the full's read and now. A full recorded without a position cannot root a "+
			"chain on this source (a PlanetScale Neki router, a MySQL server whose binlog was off, or a pre-v0.17.2 "+
			"full): take a fresh `backup full` on a source that records one and start a new chain: %w",
			positionlessFullRootMarker, lineage.ManifestBackupID(parent), parentPath, ir.ErrPositionInvalid)
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

// positionlessFullRootMarker is the grep-stable handle on the refusal above
// (same convention as POSITION-MODE / FOREIGN-LINEAGE-REFUSED).
const positionlessFullRootMarker = "POSITIONLESS-FULL-ROOT"

func positionEmpty(p ir.Position) bool {
	return p.Engine == "" && p.Token == ""
}
