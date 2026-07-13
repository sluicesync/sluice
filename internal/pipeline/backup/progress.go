// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package backup

import "sluicesync.dev/sluice/internal/progress"

// ADR-0155 phase 2 pretty-view specs for backup + restore. These commands
// keep their historical direct-slog output on the non-TTY path (the sink
// is [progress.Nop] there, so the log stream is byte-identical); the phase
// checklist drives only the interactive TTY view. Per-table live bars are
// deliberately NOT wired here — the copy/apply loops are concurrent and
// the checklist + rich summary is the phase-2 deliverable (see ADR-0155).

// sinkOrNop defaults a nil presentation sink to the no-op sink so every
// emit call-site stays nil-free.
func sinkOrNop(s progress.Sink) progress.Sink {
	if s == nil {
		return progress.Nop{}
	}
	return s
}

// Backup-full phases.
var (
	backupPhaseSchema   = progress.Phase{Key: "schema", Label: "Schema"}
	backupPhaseCopy     = progress.Phase{Key: "copy", Label: "Copy"}
	backupPhaseFinalize = progress.Phase{Key: "finalize", Label: "Finalize"}
)

// BackupFullProgressSpec is the pretty-view spec for `sluice backup full`.
var BackupFullProgressSpec = progress.Spec{
	Title:      "sluice backup full",
	Phases:     []progress.Phase{backupPhaseSchema, backupPhaseCopy, backupPhaseFinalize},
	LabelWidth: 12,
}

// Restore phases (shared by the single-manifest and chain-restore paths).
var (
	restorePhaseSchema      = progress.Phase{Key: "schema", Label: "Schema"}
	restorePhaseData        = progress.Phase{Key: "data", Label: "Data"}
	restorePhaseConstraints = progress.Phase{Key: "constraints", Label: "Constraints"}
)

// RestoreProgressSpec is the pretty-view spec for `sluice restore`.
var RestoreProgressSpec = progress.Spec{
	Title:      "sluice restore",
	Phases:     []progress.Phase{restorePhaseSchema, restorePhaseData, restorePhaseConstraints},
	LabelWidth: 12,
}

// Backup-verify phases. `backup verify` is CLI-orchestrated (no pipeline
// Run), so the CLI drives this checklist directly; the spec lives here
// with the other backup specs.
var (
	VerifyPhaseLoad  = progress.Phase{Key: "load", Label: "Load"}
	VerifyPhaseCheck = progress.Phase{Key: "verify", Label: "Verify"}
)

// VerifyChainProgressSpec is the pretty-view spec for `sluice backup verify`.
var VerifyChainProgressSpec = progress.Spec{
	Title:      "sluice backup verify",
	Phases:     []progress.Phase{VerifyPhaseLoad, VerifyPhaseCheck},
	LabelWidth: 14,
}
