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
// on EVERY source. The legacy "start from the source's current position"
// branch it used to take was written for v0.16.x fulls, which nothing has
// produced since v0.17.2 — but the population it admits kept growing: a
// full FROM a PlanetScale Neki (no WAL position to record, v0.153.1), a
// MySQL full taken with the binlog off, and the unrecognised shapes to
// come. Every one of them extended "from now" at exit 0 after a WARN,
// which is the silent chain gap the Bug 260 door in backup.go refused to
// trade a loud failure for. The safety argument that no incremental could
// reach this branch off a Neki full rested on the router refusing
// replication connections — and the same release measured that a
// SHARD-TARGETED replication connection is accepted, so the branch was one
// DSN option away from starting a chain on one shard's changes only. A
// written invariant nobody checks is indistinguishable from one that
// holds; this is the check.
//
// Until v0.154.0 a trigger-CDC source ([ir.CDCTriggers]) was the ONE
// exemption: no trigger engine recorded a position on a full, and their
// readers anchored at the change log's current MAX(id) when handed an
// empty one — so the from-now branch was the only chain shape those
// engines had, and the window between the full's sweep and that anchor
// was silently uncovered (roadmap item 163; measured at 100 of 100 rows
// by the v0.153.1 regression cycle). Every trigger engine now records the
// change log's anchor inside the full's own read (their
// [irbackup.SnapshotOpener] implementations), so the exemption is gone:
// a trigger full with no EndPosition is a pre-v0.154.0 full, or one whose
// snapshot-anchored open refused (its capturer then records nothing on
// purpose), and it is refused here like every other positionless root.
func resumeStartFromParent(ctx context.Context, store irbackup.Store, parent *irbackup.Manifest, parentPath string) (ir.Position, error) {
	if !positionEmpty(parent.EndPosition) {
		return parent.EndPosition, nil
	}
	if parent.Kind != irbackup.BackupKindIncremental {
		return ir.Position{}, fmt.Errorf("%s: parent full %s (%s) records no EndPosition, so there is no position for this "+
			"chain to resume from; extending it would start the chain from the source's CURRENT position and silently "+
			"skip every change between the full's read and now. A full recorded without a position cannot root a "+
			"chain on this source (a PlanetScale Neki router, a MySQL server whose binlog was off, a trigger-CDC "+
			"full taken before v0.154.0 or whose snapshot-anchored open refused, or a pre-v0.17.2 full): take a fresh "+
			"`backup full` on a source that records one and start a new chain: %w",
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

// The chain's seat in a trigger-CDC source's change-log CONSUMER REGISTRY
// (roadmap item 163, the prune-floor half). A trigger source's change log
// is pruned at the MIN durably-applied frontier over the registry
// (roadmap item 115) — every sync reading it registers its own. A backup
// chain reads the same change log, from the position its last link
// recorded, and until now registered nothing: a peer sync's auto-prune,
// or an operator `sluice trigger prune`, could reap the rows between the
// chain's resume point and the peer's frontier, and the next `backup
// incremental` would open above a hole it cannot see.
//
// So both chain extenders register the chain as a consumer at the
// position they RESUME from, and refresh it to each committed link's
// EndPosition — the same interface, token and engine codec the streamer
// uses ([ir.ChangeLogConsumerRegistry]). A non-trigger source implements
// nothing and every call is a no-op.
//
// STATED RESIDUAL — the window between the full and its FIRST incremental
// is NOT covered. Registration needs the chain's identity, which is the
// root full's BackupID, and that is computed from the full's EndPosition
// AFTER the engine's snapshot open — so the engine cannot register at
// anchor time and the orchestrator (backup.go) does not thread an identity
// into [irbackup.SnapshotOptions]. Failure shape: a trigger full at anchor
// A; before the first `backup incremental`, a peer sync applies past A and
// its auto-prune (or an operator prune) cuts at `its frontier − keep` > A;
// the incremental then resumes at A, and the rows in (A, cut] are in no
// link of the chain, at exit 0 on all three engines — on pgtrigger the
// poller's contiguity walk proves the pruned range "aborted" and skips it
// with an INFO line; on sqlite-trigger / d1-trigger the `id > last_id`
// scan simply never sees the deleted ids, with no signal at all. Closing it needs
// either a chain identity threaded through SnapshotOptions so the engine
// registers at anchor time, or a `pruned_through` watermark the prune
// writes to the change-log meta table that every reader refuses to resume
// below; both are filed with the item. Until then, take the first
// incremental promptly after the full on a source with peer syncs.
//
// chainConsumerIDPrefix distinguishes a chain's registry row from a sync's
// (`<stream-id> -> <target>`, see ChangeLogConsumerID) so the staleness
// WARN a prune emits names what is holding it back.
const chainConsumerIDPrefix = "backup-chain:"

// chainConsumerID derives the chain's registry identity from the lineage
// root full's BackupID — stable across incrementals, rotations (the root
// stays at segment 0) and both extenders, and unique per chain, which is
// what keeps two chains off one source from sharing a row and the later
// full's anchor from unblocking a prune past the earlier chain's resume
// point. ok is false when the store has no readable root (the caller
// registers nothing and says so).
func chainConsumerID(ctx context.Context, store irbackup.Store) (id string, ok bool, err error) {
	root, err := lineage.ReadRootManifest(ctx, store)
	if err != nil {
		return "", false, err
	}
	if root == nil {
		return "", false, nil
	}
	return chainConsumerIDPrefix + lineage.ManifestBackupID(root), true, nil
}

// registerChainConsumer publishes the chain's resume position into the
// source's consumer registry when the source reader implements it. Called
// by both extenders once the reader is open (BEFORE StreamChanges, so the
// registration precedes any read) and again after every committed link
// with that link's EndPosition. An empty position leaves the previous
// registration in place — a link that recorded nothing ended where it
// began (see resumeStartFromParent), and the row must never move above
// the chain's real resume point.
//
// Failure-isolated like the streamer's tick: a registry write that fails
// WARNs and continues, never failing the backup — but the WARN is blunt,
// because a chain that cannot register is invisible to the pruner, which
// is the silent-loss shape this exists to close.
func registerChainConsumer(ctx context.Context, cdc ir.CDCReader, store irbackup.Store, pos ir.Position, command string) {
	registry, ok := cdc.(ir.ChangeLogConsumerRegistry)
	if !ok || positionEmpty(pos) {
		return
	}
	consumerID, found, err := chainConsumerID(ctx, store)
	if err != nil || !found {
		slog.WarnContext(
			ctx, command+": could not derive this chain's consumer identity from the lineage root, so the chain is NOT "+
				"registered in the source's change-log consumer registry; a peer sync's prune cannot see it and may "+
				"delete rows the next link needs (roadmap item 163)",
			slog.Any("error", err),
		)
		return
	}
	if err := registry.RegisterChangeLogConsumer(ctx, consumerID, pos.Token); err != nil {
		slog.WarnContext(
			ctx, command+": FAILED to publish this chain's resume position to the source's change-log consumer "+
				"registry. Until it succeeds, a sync pruning this shared change log cannot see the chain and may delete "+
				"rows the next `backup incremental` needs (roadmap item 163). The backup itself is unaffected",
			slog.String("consumer_id", consumerID),
			slog.String("position_token", pos.Token),
			slog.String("error", err.Error()),
		)
		return
	}
	slog.DebugContext(
		ctx, command+": chain registered as a change-log consumer at its resume position",
		slog.String("consumer_id", consumerID),
		slog.String("position_token", pos.Token),
	)
}
