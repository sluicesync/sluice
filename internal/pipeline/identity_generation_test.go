// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package pipeline

import (
	"context"
	"log/slog"
	"strings"
	"testing"

	"sluicesync.dev/sluice/internal/ir"
	"sluicesync.dev/sluice/internal/logcapture"
)

// identityAlwaysSchema is sampleSchema plus one GENERATED ALWAYS identity
// column — the shape that makes the GC-3 restore phase dispatch.
func identityAlwaysSchema() *ir.Schema {
	return &ir.Schema{
		Tables: []*ir.Table{{
			Name: "users",
			Columns: []*ir.Column{
				{Name: "id", Type: ir.Integer{Width: 64, AutoIncrement: true}, Identity: &ir.IdentityOptions{Always: true, Start: 1, Increment: 1, MinValue: 1, MaxValue: 1<<63 - 1, Cache: 1}},
			},
		}},
	}
}

// TestRunRestoresIdentityGenerationLast pins the GC-3 phase: with an
// ALWAYS identity in the schema, RestoreIdentityGeneration runs AFTER
// every row-writing and DDL phase of the migrate ladder — strictly after
// the copy and after CreateConstraints — and exactly once.
func TestRunRestoresIdentityGenerationLast(t *testing.T) {
	src := newRecordingEngine("source")
	src.schema = identityAlwaysSchema()
	tgt := newRecordingEngine("target")

	m := &Migrator{Source: src, Target: tgt, SourceDSN: "src", TargetDSN: "tgt"}
	if err := m.Run(context.Background()); err != nil {
		t.Fatalf("Run: %v", err)
	}

	wantPhases := []string{
		"CreateTablesWithoutConstraints",
		"WriteRows:users",
		"SyncIdentitySequences",
		"CreateIndexes",
		"CreateConstraints",
		"RestoreIdentityGeneration",
	}
	if len(tgt.phaseLog) != len(wantPhases) {
		t.Fatalf("got %d phases (%v); want %d (%v)", len(tgt.phaseLog), tgt.phaseLog, len(wantPhases), wantPhases)
	}
	for i, want := range wantPhases {
		if tgt.phaseLog[i] != want {
			t.Errorf("phase[%d] = %q; want %q", i, tgt.phaseLog[i], want)
		}
	}
	// Relative-order guards that survive a future phase insertion: the
	// restore is the LAST phase, after the copy and after constraints.
	restoreAt := indexOf(tgt.phaseLog, "RestoreIdentityGeneration")
	if restoreAt != len(tgt.phaseLog)-1 {
		t.Errorf("RestoreIdentityGeneration at %d of %d; want last (a row write after it would hit SQLSTATE 428C9)", restoreAt, len(tgt.phaseLog))
	}
	if copyAt := indexOf(tgt.phaseLog, "WriteRows:users"); copyAt > restoreAt {
		t.Errorf("copy (%d) ran after the ALWAYS restore (%d)", copyAt, restoreAt)
	}
}

// TestRunSkipsIdentityRestoreWithoutAlwaysColumns is the anti-vacuity
// half of the pin above: the same recording writer implements the
// surface, and the phase is NOT dispatched when no column is ALWAYS —
// which is what keeps every existing exact-phase-list test unchanged.
func TestRunSkipsIdentityRestoreWithoutAlwaysColumns(t *testing.T) {
	src := newRecordingEngine("source")
	src.schema = sampleSchema()
	tgt := newRecordingEngine("target")
	m := &Migrator{Source: src, Target: tgt, SourceDSN: "src", TargetDSN: "tgt"}
	if err := m.Run(context.Background()); err != nil {
		t.Fatalf("Run: %v", err)
	}
	if indexOf(tgt.phaseLog, "RestoreIdentityGeneration") >= 0 {
		t.Errorf("RestoreIdentityGeneration dispatched for a schema with no ALWAYS column: %v", tgt.phaseLog)
	}
}

// TestRunKeepsByDefaultForShardConsolidationAndForceColdStart pins the two
// migrate shapes that are NOT the last explicit-id writer: the phase
// WARNs (IDENTITY-ALWAYS-DOWNGRADED) and does not restore.
func TestRunKeepsByDefaultForShardConsolidationAndForceColdStart(t *testing.T) {
	for _, tc := range []struct {
		name string
		set  func(m *Migrator)
	}{
		{"--force-cold-start", func(m *Migrator) { m.ForceColdStart = true }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			buf := &logcapture.Buffer{}
			prev := slog.Default()
			slog.SetDefault(slog.New(slog.NewJSONHandler(buf, &slog.HandlerOptions{Level: slog.LevelWarn})))
			t.Cleanup(func() { slog.SetDefault(prev) })

			src := newRecordingEngine("source")
			src.schema = identityAlwaysSchema()
			tgt := newRecordingEngine("target")
			m := &Migrator{Source: src, Target: tgt, SourceDSN: "src", TargetDSN: "tgt"}
			tc.set(m)
			if err := m.Run(context.Background()); err != nil {
				t.Fatalf("Run: %v", err)
			}
			if indexOf(tgt.phaseLog, "RestoreIdentityGeneration") >= 0 {
				t.Errorf("%s restored ALWAYS on a target that will take more explicit-id loads: %v", tc.name, tgt.phaseLog)
			}
			if !strings.Contains(buf.String(), "IDENTITY-ALWAYS-DOWNGRADED") {
				t.Errorf("%s: no IDENTITY-ALWAYS-DOWNGRADED WARN; log: %s", tc.name, buf.String())
			}
		})
	}
}

// TestRestoreIdentityGeneration_KeepByDefaultWarnsAndSkips drives the
// helper directly for the shard-consolidation arm (a Migrator with
// InjectShardColumn engaged needs the whole shard-injection harness; the
// Run-level pin above covers --force-cold-start, and both flags feed the
// same keepByDefault argument).
func TestRestoreIdentityGeneration_KeepByDefaultWarnsAndSkips(t *testing.T) {
	buf := &logcapture.Buffer{}
	prev := slog.Default()
	slog.SetDefault(slog.New(slog.NewJSONHandler(buf, &slog.HandlerOptions{Level: slog.LevelWarn})))
	t.Cleanup(func() { slog.SetDefault(prev) })

	var log []string
	w := &recordingSchemaWriter{phaseLog: &log}
	if err := restoreIdentityGeneration(context.Background(), w, identityAlwaysSchema(), true); err != nil {
		t.Fatalf("restoreIdentityGeneration: %v", err)
	}
	if len(log) != 0 {
		t.Errorf("keepByDefault=true dispatched %v", log)
	}
	if !strings.Contains(buf.String(), "IDENTITY-ALWAYS-DOWNGRADED") {
		t.Errorf("no WARN; log: %s", buf.String())
	}
	if err := restoreIdentityGeneration(context.Background(), w, identityAlwaysSchema(), false); err != nil {
		t.Fatalf("restoreIdentityGeneration: %v", err)
	}
	if len(log) != 1 || log[0] != "RestoreIdentityGeneration" {
		t.Errorf("keepByDefault=false dispatched %v; want one RestoreIdentityGeneration", log)
	}
}
