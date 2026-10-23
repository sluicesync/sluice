// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package migcore

import "sluicesync.dev/sluice/internal/ir"

// PreflightSnapshotScope returns the refusal source's snapshot open would
// raise for tables, before anything is opened or written
// ([ir.SnapshotScopeChecker]); nil for a source without the surface. Today
// that is the VStream family's VSTREAM-SINGLE-STREAM-MULTI-TABLE: a copy of
// more than one table under `vstream_copy_single_stream=true` loses deletes.
//
// It has three callers, each reaching the refusal earlier than the open
// would: the `sync` cold start (before it clears the stream's apply marks),
// the `sync` dry run (so the plan does not promise a copy the run refuses)
// and `backup full` (whose snapshot-open error falls back to the
// non-snapshot copy with a WARN instead of failing). tables is the copy
// scope the caller hands the snapshot opener.
func PreflightSnapshotScope(source ir.Engine, dsn string, tables []string) error {
	checker, ok := source.(ir.SnapshotScopeChecker)
	if !ok {
		return nil
	}
	return checker.CheckSnapshotScope(dsn, tables)
}
