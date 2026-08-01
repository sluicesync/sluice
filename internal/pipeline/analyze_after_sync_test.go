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

// recordingAnalyzeSchemaWriter is a recordingSchemaWriter that ALSO implements
// the optional [ir.TableAnalyzer] surface, logging each per-table ANALYZE into
// the shared phase log so the sync cold-start's --analyze-after phase is
// observable in order. (recordingSchemaWriter deliberately does not implement
// TableAnalyzer — the unsupported-engine WARN path is pinned separately.)
type recordingAnalyzeSchemaWriter struct {
	recordingSchemaWriter
}

func (w *recordingAnalyzeSchemaWriter) AnalyzeTable(_ context.Context, table *ir.Table) error {
	*w.phaseLog = append(*w.phaseLog, "AnalyzeTable:"+table.Name)
	return nil
}

// TestAnalyzeAfterSyncColdStartOrdering pins the item-111 sync-cold-start
// AnalyzeAfter parity: [bulkCopyOpts.AnalyzeAfter] runs the advisory per-table
// ANALYZE AFTER the bulk copy AND after the constraints/views DDL, mirroring the
// Migrator's --analyze-after phase; with the flag off, no ANALYZE runs at all.
// The sync cold-start drives the copy through runBulkCopyWithOpts directly
// (streamer_coldstart.go / streamer_multidb.go), so this exercises the exact
// path both callers hit.
func TestAnalyzeAfterSyncColdStartOrdering(t *testing.T) {
	oneTableSchema := func() *ir.Schema {
		return &ir.Schema{
			Tables: []*ir.Table{
				{Name: "users", Columns: []*ir.Column{{Name: "id", Type: ir.Integer{Width: 64}}}},
			},
		}
	}

	// runCopy drives runBulkCopyWithOpts with an analyzer-capable recording
	// writer sharing one phase log and returns the ordered phase names.
	runCopy := func(t *testing.T, analyze bool) []string {
		t.Helper()
		var (
			phaseLog []string
			mu       sync.Mutex
		)
		sw := &recordingAnalyzeSchemaWriter{recordingSchemaWriter: recordingSchemaWriter{phaseLog: &phaseLog}}
		rw := &recordingRowWriter{phaseLog: &phaseLog, mu: &mu}
		reader := &recordingRowReader{}
		if err := runBulkCopyWithOpts(context.Background(), oneTableSchema(), reader, sw, rw, bulkCopyOpts{
			AnalyzeAfter: analyze,
		}); err != nil {
			t.Fatalf("runBulkCopyWithOpts(analyze=%v): %v", analyze, err)
		}
		return phaseLog
	}

	idxOf := func(log []string, want string) int {
		for i, p := range log {
			if p == want {
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

	t.Run("analyze-after runs ANALYZE after the copy and constraints", func(t *testing.T) {
		log := runCopy(t, true)
		analyzeAt := idxOf(log, "AnalyzeTable:users")
		lastCopy := lastPrefix(log, "WriteRows:")
		constraintsAt := idxOf(log, "CreateConstraints")
		if analyzeAt < 0 {
			t.Fatalf("AnalyzeTable never ran with AnalyzeAfter set; phase log = %v", log)
		}
		if lastCopy < 0 {
			t.Fatalf("no WriteRows copy phase ran; phase log = %v", log)
		}
		if analyzeAt < lastCopy {
			t.Errorf("AnalyzeTable at %d must follow the last WriteRows at %d; phase log = %v", analyzeAt, lastCopy, log)
		}
		if constraintsAt >= 0 && analyzeAt < constraintsAt {
			t.Errorf("AnalyzeTable at %d must follow CreateConstraints at %d; phase log = %v", analyzeAt, constraintsAt, log)
		}
	})

	t.Run("default off runs no ANALYZE", func(t *testing.T) {
		log := runCopy(t, false)
		if i := idxOf(log, "AnalyzeTable:users"); i >= 0 {
			t.Errorf("AnalyzeTable ran without AnalyzeAfter set (at %d); phase log = %v", i, log)
		}
	})
}
