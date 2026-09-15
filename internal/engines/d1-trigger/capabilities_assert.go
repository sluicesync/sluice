// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package d1trigger

import (
	sqlitetrigger "sluicesync.dev/sluice/internal/engines/sqlite-trigger"
	"sluicesync.dev/sluice/internal/ir"
	irbackup "sluicesync.dev/sluice/internal/ir/backup"
)

// Compile-time declarations of the ir interfaces this engine's concrete type
// intentionally implements. The orchestrator discovers optional surfaces by
// runtime type-assertion, so a method-set break wouldn't fail the build — the
// assertion would quietly stop matching and the pipeline would silently
// downgrade. These blank-var assertions turn that silent downgrade into a
// compile error here.
//
// The engine's surface is intentionally NARROW: it composes the `d1` engine by
// delegation (see the Engine doc) precisely so it does NOT inherit any optional
// opener the orchestrator type-asserts on that D1 cannot honour (the writer /
// target surfaces). D1-trigger is a CDC SOURCE only; do NOT "fix" a
// missing-interface error by widening this engine — that narrowness is
// load-bearing. The CDC reader type itself lives in the sqlite-trigger package
// (the shared implementation), where its [ir.CDCReader] conformance is pinned.
var (
	_ ir.Engine = Engine{}

	// Roadmap item 163: the two halves of "a trigger full can root a chain".
	// The snapshot opener is the gap-free primary path; losing it silently
	// drops the orchestrator to the v0.17.x post-sweep fallback, where the
	// capturer records nothing on purpose and the chain refuses. The reader
	// this engine hands back is the sqlite-trigger package's D1 wrapper; the
	// pin lives HERE (asserting the delegated type) so the registry-derived
	// roster (docsync's TestEveryCDCEngineRecordsABackupPosition) attributes
	// the surface to `d1-trigger`, matching the runtime truth.
	_ irbackup.SnapshotOpener   = Engine{}
	_ irbackup.PositionCapturer = (*sqlitetrigger.D1SchemaReader)(nil)
)
