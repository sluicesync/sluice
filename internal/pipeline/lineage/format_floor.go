// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package lineage

import (
	"context"
	"log/slog"

	irbackup "sluicesync.dev/sluice/internal/ir/backup"
)

// A chain's readability floor is its NEWEST link, not its root.
//
// Every manifest records the format version its own chunks were sealed
// under, and readers recompute at the version the artifact records — the
// dual-version rule of ADR-0154 and ADR-0181. That rule is what makes
// restoring an old chain with a new binary work forever, and it is
// deliberately per-link.
//
// The consequence is not per-link. A restore walks the whole chain, so a
// binary that refuses ANY segment restores NOTHING from that chain —
// including segments it wrote itself, under a version it understands. So
// the moment a newer binary extends an older chain, the chain's floor rises
// to the new segment's version and every older binary loses the whole thing.
//
// That is correct behaviour and there is no in-product alternative worth
// having: keeping the extension at the chain's old version would seal fresh
// chunks under an encoding a format bump exists to retire. What was missing
// is that nothing told the operator it had happened. A per-link decision
// with a whole-chain consequence had only its per-link half implemented,
// which is how it reached v0.104.0's release notes as "existing chains are
// unaffected" — true only of a chain nobody extends, and taking an
// incremental is the most ordinary thing an operator does to a chain.
//
// Roadmap item 90 / Bug 212, found by the v0.104.0 regression cycle.

// WarnChainFormatVersionRaise logs a warning when a freshly stamped segment
// records a HIGHER format version than the chain link it extends, naming the
// chain's new readability floor and the release an operator needs to reach
// it.
//
// It is a no-op when the segment does not raise the floor, which is the
// overwhelmingly common case: a chain written entirely by one version never
// warns, and a chain whose floor has already been raised does not warn again
// on subsequent extensions (the parent is by then the already-raised
// segment). Callers pass the version they are about to stamp, AFTER any
// inherit-the-chain's-shape adjustment, so the value logged is the one that
// actually lands on the store.
func WarnChainFormatVersionRaise(ctx context.Context, chainRef string, parentVersion, segmentVersion int) {
	if segmentVersion <= parentVersion {
		return
	}
	attrs := []any{
		slog.String("chain", chainRef),
		slog.Int("previous_format_version", parentVersion),
		slog.Int("new_format_version", segmentVersion),
	}
	if release := irbackup.MinimumReaderVersion(segmentVersion); release != "" {
		attrs = append(attrs, slog.String("minimum_sluice_version", release))
		slog.WarnContext(ctx, "backup: this segment raises the chain's format version, so sluice releases older than "+release+
			" can no longer restore ANY part of this chain — including the segments they wrote themselves. "+
			"If a binary that must be able to restore is still on an older release, upgrade it before taking further backups here",
			attrs...)
		return
	}
	slog.WarnContext(ctx, "backup: this segment raises the chain's format version, so older sluice releases "+
		"can no longer restore ANY part of this chain — including the segments they wrote themselves. "+
		"If a binary that must be able to restore is still on an older release, upgrade it before taking further backups here",
		attrs...)
}
