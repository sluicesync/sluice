// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package mysql

import (
	"testing"

	"sluicesync.dev/sluice/internal/errclassgate"
)

// TestSetErrSitesClassify instantiates the shared Bug-207 gate for this
// package. The walker itself now lives in internal/errclassgate — it was
// originally written here, and being package-scoped is precisely how the class
// recurred on Postgres unseen (audit 2026-07-26 SL-2). The rationale for the
// gate and for each allowlist entry is preserved below; the mechanics moved.
func TestSetErrSitesClassify(t *testing.T) {
	errclassgate.Assert(t, errclassgate.Config{
		Dir:    ".",
		Method: "setErr",
		// Either a classifier, or a constructor returning a typed error the
		// streamer matches structurally (the liveness and progress timeouts
		// implement ir.LivenessProgressTimeoutError and are handled by their
		// own arm, so routing them through a text classifier would be strictly
		// worse).
		Classifiers: map[string]bool{
			"classifyReaderError":         true,
			"classifyApplierError":        true,
			"vstreamLivenessTimeoutError": true,
			"vstreamProgressTimeoutError": true,
		},
		// Deliberate exceptions. Keep this list SMALL — a gate whose
		// exceptions are bulk-annotated away is decoration.
		Allowed: map[string]string{
			"cdc_snapshot_concurrent_resume.go:err":                           "already classified — this arm is guarded by !isRetriableReadErr(err), so the value has been judged terminal by a dedicated predicate before it is parked",
			"cdc_snapshot_concurrent_resume.go:rerr":                          "recoverFromDrop's failure is terminal by construction (retry budget exhausted / binlog purged); it exists to abort the copy loudly so restart-from-scratch takes over",
			"cdc_snapshot_concurrent_resume.go:ctx.Err()":                     "the caller's own cancellation, never a source fault",
			"row_reader.go:ctx.Err()":                                         "the caller's own cancellation, never a source fault",
			`row_reader.go:fmt.Errorf("mysql: column %q: %w", col.Name, err)`: "a value decode / zero-date-policy fault is a data-fidelity error: no retry can change the bytes, and classifying it risks routing corruption to a retry loop",
			"row_reader.go:rerr":                                              "the TINYINT(1)-out-of-range refusal (SLUICE-E-VALUE-TINYINT1-RANGE) is the same data-fidelity class as the decode-fault sibling above: no retry can bring the value into {0,1}, so it is terminal by construction — classifying a coded refusal would risk routing it into a retry loop",
			// SETTLED (roadmap item 83) by killing a real connection mid-stream
			// in TestRowReader_MidStreamConnectionDrop_IsClassifiedRetriable:
			// the drop surfaces at the SIBLING rows.Err() exit, which IS
			// classified. This exit is not on the transient path — database/sql
			// records a driver failure in lasterr and Next() returns false, so
			// the loop exits without ever re-entering Scan — and its
			// destinations are *any, which convertAssign cannot fail on.
			`row_reader.go:fmt.Errorf("mysql: scan: %w", err)`: "settled by the mid-stream-drop probe (item 83): a real connection drop surfaces at the classified rows.Err() exit, never here; this exit is not on the transient path",
		},
		MinSites: 10,
	})
}
