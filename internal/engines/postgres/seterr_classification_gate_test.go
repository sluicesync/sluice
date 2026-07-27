// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package postgres

import (
	"testing"

	"sluicesync.dev/sluice/internal/errclassgate"
)

// TestSetErrSitesClassify is this engine's instance of the shared Bug-207
// gate, and it exists because the MySQL-only version could not see this
// package.
//
// The defect it was built for had already happened here: the CDC pump parked
// dispatchWAL's error raw (audit 2026-07-26 SL-2), and dispatchWAL is not a
// pure decode — it runs live catalog queries on the reader's own connection
// (resolveColumnStableIDs → pg_attribute on every RelationMessage,
// resolveIdentityKeyCols → pg_index, and the extension/enum OID resolvers).
// pgoutput re-sends Relation on first touch after each reconnect, i.e. exactly
// when a pooler is least healthy, so a routine fault there was terminal on the
// shape nettransient exists to ride out. Three independent audit workers
// converged on the line; the gate designed to prevent that class structurally
// could not report it.
func TestSetErrSitesClassify(t *testing.T) {
	errclassgate.Assert(t, errclassgate.Config{
		Dir:    ".",
		Method: "setErr",
		Classifiers: map[string]bool{
			"classifyReaderError":  true,
			"classifyApplierError": true,
		},
		// Deliberate exceptions, each judged individually rather than
		// bulk-annotated. Keyed "file.go:argument-source".
		Allowed: map[string]string{
			"row_reader.go:ctx.Err()": "the caller's own cancellation, never a source fault",
			`row_reader.go:fmt.Errorf("postgres: column %q: %w", col.Name, err)`: "a value decode fault is a data-fidelity error: no retry can change the bytes, and classifying it risks routing corruption to a retry loop",
			// Same structural argument as the MySQL twin, and ground-truthed on
			// a real server rather than carried over by analogy: see
			// TestRowReader_MidStreamConnectionDrop_IsClassifiedRetriable in
			// this package, which kills a live connection mid-stream and
			// observes which exit fires.
			`row_reader.go:fmt.Errorf("postgres: scan: %w", err)`:                 "a mid-stream connection drop surfaces at the classified rows.Err() exit, never here (database/sql records the driver failure in lasterr, so Next() returns false and the loop exits without re-entering Scan); the destinations are *any, which convertAssign cannot fail on",
			`cdc_reader.go:fmt.Errorf("postgres: cdc: parse keepalive: %w", err)`: "a parse failure over bytes ALREADY received is terminal by construction: the frame is malformed, and no reconnect changes bytes already in hand. Unlike dispatchWAL this path performs no I/O",
		},
		MinSites: 5,
	})
}
