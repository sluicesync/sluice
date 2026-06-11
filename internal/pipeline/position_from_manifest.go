// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package pipeline

// Phase 3.3.B helper: load the chain's terminal CDC position from a
// backup chain stored in an [irbackup.Store]. Used by `sluice sync
// start --position-from-manifest=<chain-url>` so a sync stream that
// just had a chain restored into its target can resume CDC from the
// chain's terminal position without re-bulking.
//
// The returned position is the last manifest's [irbackup.Manifest.EndPosition]
// — the terminal incremental's end-of-window cursor, or (when no
// incrementals exist in the chain) the full's end-of-backup cursor
// recorded by Phase 3.3.A. Empty EndPosition surfaces as a clear error
// rather than the silent "from now" fall-through; the operator
// expected position-from-manifest, and an empty position indicates
// either a pre-Phase-3.3 full or a malformed chain.

import (
	"context"
	"errors"
	"fmt"

	"sluicesync.dev/sluice/internal/ir"
	irbackup "sluicesync.dev/sluice/internal/ir/backup"
)

// LoadChainTerminalPosition walks the lineage, validates its shape
// via [buildLineageChain] (the single boundary-monotonicity
// invariant, intra- and inter-segment), and returns the [ir.Position]
// at the lineage's terminal manifest (the open segment's last
// committed incremental). Used as the position source by
// `sluice sync start --position-from-manifest`.
//
// Errors loudly when:
//
//   - the store contains no manifests
//   - the chain's shape is invalid (no full, branching, cycles, missing
//     parent — same loud-failure surface as restore)
//   - the chain's terminal manifest carries an empty EndPosition (a
//     pre-Phase-3.3 v0.16.x or v0.17.0 full with no recorded position;
//     the chain handoff path has nowhere to start)
func LoadChainTerminalPosition(ctx context.Context, store irbackup.Store) (ir.Position, error) {
	// nil comparator: position-from-manifest only reads the terminal
	// position; the structural + write-time guarantees suffice (no
	// source engine instance available here without breaking the
	// pipeline's no-engine-registry layering).
	chain, err := buildLineageChain(ctx, store, nil)
	if err != nil {
		return ir.Position{}, fmt.Errorf("position-from-manifest: build lineage: %w", err)
	}
	if len(chain) == 0 {
		return ir.Position{}, errors.New("position-from-manifest: store contains no manifests")
	}
	terminal := chain[len(chain)-1].manifest
	if terminal.EndPosition.Engine == "" && terminal.EndPosition.Token == "" {
		return ir.Position{}, fmt.Errorf(
			"position-from-manifest: terminal manifest %q has no EndPosition recorded "+
				"(pre-Phase-3.3 full backup or malformed chain). Take a fresh full backup "+
				"with sluice v0.17.2+ to populate EndPosition automatically",
			manifestBackupID(terminal),
		)
	}
	return terminal.EndPosition, nil
}
