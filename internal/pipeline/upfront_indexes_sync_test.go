// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package pipeline

import (
	"context"
	"strings"
	"sync"
	"testing"

	"sluicesync.dev/sluice/internal/ir"
)

// TestUpfrontIndexesSyncColdStartOrdering pins the item-111-phase-2 sync
// cold-start behavior: [bulkCopyOpts.UpfrontIndexes] flips the index build from
// AFTER the bulk copy (the deferred default) to BEFORE it, mirroring the
// Migrator's --upfront-indexes reorder. The sync cold-start drives the copy
// through runBulkCopyWithOpts directly (streamer_coldstart.go /
// streamer_multidb.go), so this exercises the exact code path both callers hit.
//
// Two invariants, both load-bearing:
//   - the phase ORDER flips with the flag (CreateIndexes before vs after the
//     WriteRows copy phase), and
//   - CreateIndexes runs EXACTLY ONCE either way — the upfront branch must SKIP
//     the post-copy build, never double-build (the guard on migrate.go's
//     post-copy CreateIndexes). verifyBuiltIndexes stays unconditional but is a
//     no-op here (recordingSchemaWriter is not an ir.IndexVerifier), so it does
//     not appear in the phase log.
func TestUpfrontIndexesSyncColdStartOrdering(t *testing.T) {
	twoTableSchema := func() *ir.Schema {
		return &ir.Schema{
			Tables: []*ir.Table{
				{Name: "users", Columns: []*ir.Column{{Name: "id", Type: ir.Integer{Width: 64}}}},
				{Name: "orders", Columns: []*ir.Column{{Name: "id", Type: ir.Integer{Width: 64}}}},
			},
		}
	}

	// runCopy drives runBulkCopyWithOpts with recording writers sharing one
	// phase log and returns the ordered phase names.
	runCopy := func(t *testing.T, upfront bool) []string {
		t.Helper()
		var (
			phaseLog []string
			mu       sync.Mutex
		)
		sw := &recordingSchemaWriter{phaseLog: &phaseLog}
		rw := &recordingRowWriter{phaseLog: &phaseLog, mu: &mu}
		reader := &recordingRowReader{}
		if err := runBulkCopyWithOpts(context.Background(), twoTableSchema(), reader, sw, rw, bulkCopyOpts{
			UpfrontIndexes: upfront,
		}); err != nil {
			t.Fatalf("runBulkCopyWithOpts(upfront=%v): %v", upfront, err)
		}
		return phaseLog
	}

	// firstIdx / countPrefix are small helpers over the phase log.
	firstIdx := func(log []string, want string) int {
		for i, p := range log {
			if p == want {
				return i
			}
		}
		return -1
	}
	firstPrefix := func(log []string, prefix string) int {
		for i, p := range log {
			if strings.HasPrefix(p, prefix) {
				return i
			}
		}
		return -1
	}
	lastPrefix := func(log []string, prefix string) int {
		idx := -1
		for i, p := range log {
			if strings.HasPrefix(p, prefix) {
				idx = i
			}
		}
		return idx
	}
	count := func(log []string, want string) int {
		n := 0
		for _, p := range log {
			if p == want {
				n++
			}
		}
		return n
	}

	t.Run("upfront builds indexes BEFORE the copy", func(t *testing.T) {
		log := runCopy(t, true)
		idxAt := firstIdx(log, "CreateIndexes")
		firstCopy := firstPrefix(log, "WriteRows:")
		if idxAt < 0 {
			t.Fatalf("CreateIndexes never ran; phase log = %v", log)
		}
		if firstCopy < 0 {
			t.Fatalf("no WriteRows copy phase ran; phase log = %v", log)
		}
		if idxAt > firstCopy {
			t.Errorf("upfront: CreateIndexes at %d must precede the first WriteRows at %d; phase log = %v", idxAt, firstCopy, log)
		}
		if n := count(log, "CreateIndexes"); n != 1 {
			t.Errorf("upfront: CreateIndexes ran %d times; want exactly 1 (no post-copy double-build); phase log = %v", n, log)
		}
	})

	t.Run("deferred (default) builds indexes AFTER the copy", func(t *testing.T) {
		log := runCopy(t, false)
		idxAt := firstIdx(log, "CreateIndexes")
		lastCopy := lastPrefix(log, "WriteRows:")
		if idxAt < 0 {
			t.Fatalf("CreateIndexes never ran; phase log = %v", log)
		}
		if lastCopy < 0 {
			t.Fatalf("no WriteRows copy phase ran; phase log = %v", log)
		}
		if idxAt < lastCopy {
			t.Errorf("deferred: CreateIndexes at %d must follow the last WriteRows at %d; phase log = %v", idxAt, lastCopy, log)
		}
		if n := count(log, "CreateIndexes"); n != 1 {
			t.Errorf("deferred: CreateIndexes ran %d times; want exactly 1; phase log = %v", n, log)
		}
	})
}
