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
// recorded by Phase 3.3.A. An empty EndPosition falls back to the
// terminal link's StartPosition when it has one (Bug 275: an incremental
// whose window captured nothing ends where it began), and surfaces as a
// clear error only when BOTH are empty — a pre-Phase-3.3 full or a
// genuinely malformed chain — rather than the silent "from now"
// fall-through the operator did not ask for.

import (
	"context"
	"errors"
	"fmt"
	"log/slog"

	"sluicesync.dev/sluice/internal/ir"
	irbackup "sluicesync.dev/sluice/internal/ir/backup"
	"sluicesync.dev/sluice/internal/pipeline/lineage"
)

// LoadChainTerminalPosition walks the lineage, validates its shape
// via [lineage.BuildLineageChain] (the single boundary-monotonicity
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
//   - the chain's terminal manifest carries an empty EndPosition AND an
//     empty StartPosition (a pre-Phase-3.3 v0.16.x or v0.17.0 full with no
//     recorded position; the chain handoff path has nowhere to start). An
//     empty EndPosition with a populated StartPosition RESUMES from the
//     latter — see the fallback's comment for why the two are different
//
// It returns the terminal manifest's REDACTION MARKER alongside the
// position, and that second return is not a convenience. This function
// builds the whole lineage and reads the terminal manifest, so it has
// always held the marker — and it returned only the position, which is
// how `--position-from-manifest` became the one redaction-posture door
// reaching no guard (audit 2026-09-06, found by the pre-tag review).
// Resuming CDC off a redacted chain under a sync that is not redacting
// overwrites the restore's redacted values with plaintext on a LIVE
// target. Handing the marker back rather than exposing a separate
// lookup is deliberate: the defect was not a missing check, it was a
// function that held the evidence and dropped it, and a caller cannot
// drop this one without an unused-variable or a visible `_`.
// [backup.RefusePositionFromRedactedChain] is what grades it.
func LoadChainTerminalPosition(ctx context.Context, store irbackup.Store) (ir.Position, *irbackup.RedactionInfo, error) {
	// nil comparator: position-from-manifest only reads the terminal
	// position; the structural + write-time guarantees suffice (no
	// source engine instance available here without breaking the
	// pipeline's no-engine-registry layering).
	chain, err := lineage.BuildLineageChain(ctx, store, nil)
	if err != nil {
		return ir.Position{}, nil, fmt.Errorf("position-from-manifest: build lineage: %w", err)
	}
	if len(chain) == 0 {
		return ir.Position{}, nil, errors.New("position-from-manifest: store contains no manifests")
	}
	terminal := chain[len(chain)-1].Manifest
	if terminal.EndPosition.Engine == "" && terminal.EndPosition.Token == "" {
		// FALL BACK TO StartPosition when the terminal link has one (Bug
		// 275). The two empty-EndPosition populations are distinguishable
		// and this refusal used to conflate them:
		//
		//   - a pre-Phase-3.3 FULL has no EndPosition and no StartPosition
		//     either (StartPosition is documented empty on fulls), so there
		//     is genuinely nothing to resume from. Still refuses.
		//   - an INCREMENTAL whose window captured nothing wrote an empty
		//     EndPosition while its StartPosition holds the exact resume
		//     point — it equals the parent's EndPosition by construction,
		//     and a window that observed nothing ends where it began.
		//   - a DDL-ONLY incremental lands here too, and that is fine rather
		//     than incidental. Its empty EndPosition is LOAD-BEARING on the
		//     restore side, which uses it to tell a schema-only window from
		//     a data window whose rows were emptied — so the fix belongs
		//     here and NOT in the writer. (The first attempt did stamp the
		//     writer and was caught by
		//     TestIncrementalWindow_SchemaSnapshotDoesNotMoveEndPosition.)
		//     Resuming such a chain from StartPosition re-reads the window
		//     carrying the DDL, which the restore has already applied as a
		//     schema delta.
		//
		// Nothing here rewrites a manifest: only the position handed to the
		// resuming stream changes, so every restore-side guard still sees
		// the same empty EndPosition it reads today. And re-reading is the
		// safe direction — a resume that re-observes a span cannot skip one.
		if terminal.StartPosition.Engine != "" || terminal.StartPosition.Token != "" {
			slog.WarnContext(
				ctx,
				"position-from-manifest: the terminal manifest records no EndPosition, so its "+
					"StartPosition is being used as the resume point. That is correct for an "+
					"incremental whose window captured nothing — it ends where it began — and it is "+
					"what a writer before v0.146.0 left behind for such a window. The resume may "+
					"re-read a span that produced no changes; it cannot skip one",
				slog.String("backup_id", lineage.ManifestBackupID(terminal)),
				slog.String("start_position_engine", terminal.StartPosition.Engine),
			)
			return terminal.StartPosition, terminal.Redaction, nil
		}
		return ir.Position{}, nil, fmt.Errorf(
			"position-from-manifest: terminal manifest %q has no EndPosition and no StartPosition "+
				"recorded (pre-Phase-3.3 full backup or malformed chain). Take a fresh full backup "+
				"with sluice v0.17.2+ to populate EndPosition automatically",
			lineage.ManifestBackupID(terminal),
		)
	}
	return terminal.EndPosition, terminal.Redaction, nil
}
