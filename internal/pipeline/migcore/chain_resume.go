// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package migcore

import (
	"context"

	"sluicesync.dev/sluice/internal/ir"
	irbackup "sluicesync.dev/sluice/internal/ir/backup"
)

// PreflightChainResume runs the engine's [irbackup.ChainResumePreflighter]
// (when implemented) against the chain's resume position before any CDC
// stream opens. Shared by the backup-chain orchestrators (IncrementalBackup,
// BackupStream, the carved-out backup domain) and root's streaming layer —
// the refusal semantics are identical: a slot that is missing or has advanced
// past `from` cannot serve the chain gap-free, and starting the stream anyway
// would silently skip the WAL in between. The zero position (a "from now"
// chain start) skips the check; engines without the surface (MySQL) skip it
// too.
func PreflightChainResume(ctx context.Context, source ir.Engine, dsn string, from ir.Position) error {
	pf, ok := source.(irbackup.ChainResumePreflighter)
	if !ok || (from.Engine == "" && from.Token == "") {
		return nil
	}
	return pf.PreflightChainResume(ctx, dsn, from)
}
