// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package pipeline

import (
	"context"
	"errors"
	"strings"
	"testing"

	"sluicesync.dev/sluice/internal/ir"
)

// TestRunAnalyzeAfterRunsLast pins the --analyze-after phase position:
// per-table AnalyzeTable calls run AFTER every DDL phase (constraints
// are the last one in the phase log for a view-less schema), once per
// migrated table, and only when the flag is set.
func TestRunAnalyzeAfterRunsLast(t *testing.T) {
	src := newRecordingEngine("source")
	src.schema = sampleSchema()
	tgt := newRecordingEngine("target")

	m := &Migrator{
		Source: src, Target: tgt,
		SourceDSN: "src", TargetDSN: "tgt",
		AnalyzeAfter: true,
	}
	if err := m.Run(context.Background()); err != nil {
		t.Fatalf("Run: %v", err)
	}

	wantPhases := []string{
		"CreateTablesWithoutConstraints",
		"WriteRows:users",
		"SyncIdentitySequences",
		"CreateIndexes",
		"CreateConstraints",
		"AnalyzeTable:users",
	}
	if len(tgt.phaseLog) != len(wantPhases) {
		t.Fatalf("got %d phases (%v); want %d (%v)", len(tgt.phaseLog), tgt.phaseLog, len(wantPhases), wantPhases)
	}
	for i, want := range wantPhases {
		if tgt.phaseLog[i] != want {
			t.Errorf("phase[%d] = %q; want %q", i, tgt.phaseLog[i], want)
		}
	}

	// Relative-order guard independent of the exact list: analyze strictly
	// after constraints, so a future phase insertion can't silently move it
	// ahead of the DDL it must postdate.
	analyzeAt, constraintsAt := indexOf(tgt.phaseLog, "AnalyzeTable:users"), indexOf(tgt.phaseLog, "CreateConstraints")
	if analyzeAt < constraintsAt {
		t.Errorf("AnalyzeTable (at %d) must follow CreateConstraints (at %d): %v", analyzeAt, constraintsAt, tgt.phaseLog)
	}
}

// TestRunAnalyzeAfterDefaultOff pins the zero-value default: no
// AnalyzeTable call happens unless the operator opts in.
func TestRunAnalyzeAfterDefaultOff(t *testing.T) {
	src := newRecordingEngine("source")
	src.schema = sampleSchema()
	tgt := newRecordingEngine("target")

	m := &Migrator{
		Source: src, Target: tgt,
		SourceDSN: "src", TargetDSN: "tgt",
	}
	if err := m.Run(context.Background()); err != nil {
		t.Fatalf("Run: %v", err)
	}
	for _, p := range tgt.phaseLog {
		if strings.HasPrefix(p, "AnalyzeTable:") {
			t.Errorf("AnalyzeTable ran without AnalyzeAfter set: %v", tgt.phaseLog)
		}
	}
}

// failingAnalyzer is a schema writer whose AnalyzeTable always errors,
// recording each attempt — the advisory-phase contract says every table
// is still attempted and the run never fails.
type failingAnalyzer struct {
	recordingSchemaWriter
	attempts int
}

func (f *failingAnalyzer) AnalyzeTable(_ context.Context, table *ir.Table) error {
	f.attempts++
	return errors.New("analyze blew up for " + table.Name)
}

// TestAnalyzeAfterPhaseFailureWarnsAndContinues pins the loud-but-
// advisory contract: a per-table analyze failure WARNs (naming the
// table), the phase moves on to the next table, and nothing escalates.
func TestAnalyzeAfterPhaseFailureWarnsAndContinues(t *testing.T) {
	logs := captureSlog(t)
	var phaseLog []string
	fa := &failingAnalyzer{recordingSchemaWriter: recordingSchemaWriter{phaseLog: &phaseLog}}
	schema := &ir.Schema{Tables: []*ir.Table{{Name: "users"}, {Name: "orders"}}}

	runAnalyzeAfterPhase(context.Background(), schema, fa)

	if fa.attempts != 2 {
		t.Errorf("attempts = %d; want 2 (a failure must not stop the sweep)", fa.attempts)
	}
	out := logs.String()
	if !strings.Contains(out, "analyze-after failed for table") || !strings.Contains(out, "users") {
		t.Errorf("expected a WARN naming the failed table; got:\n%s", out)
	}
}

// noAnalyzeSchemaWriter implements ir.SchemaWriter WITHOUT the optional
// TableAnalyzer surface, for the unsupported-engine warn path.
type noAnalyzeSchemaWriter struct{}

func (noAnalyzeSchemaWriter) CreateTablesWithoutConstraints(context.Context, *ir.Schema) error {
	return nil
}
func (noAnalyzeSchemaWriter) CreateIndexes(context.Context, *ir.Schema) error         { return nil }
func (noAnalyzeSchemaWriter) CreateConstraints(context.Context, *ir.Schema) error     { return nil }
func (noAnalyzeSchemaWriter) SyncIdentitySequences(context.Context, *ir.Schema) error { return nil }
func (noAnalyzeSchemaWriter) CreateViews(context.Context, *ir.Schema) error           { return nil }

// TestAnalyzeAfterPhaseUnsupportedWriterWarns pins that a target engine
// without ir.TableAnalyzer surfaces ONE loud WARN instead of silently
// skipping the phase the operator explicitly requested.
func TestAnalyzeAfterPhaseUnsupportedWriterWarns(t *testing.T) {
	logs := captureSlog(t)
	schema := &ir.Schema{Tables: []*ir.Table{{Name: "users"}}}

	runAnalyzeAfterPhase(context.Background(), schema, noAnalyzeSchemaWriter{})

	if !strings.Contains(logs.String(), "does not support per-table ANALYZE") {
		t.Errorf("expected the unsupported-engine WARN; got:\n%s", logs.String())
	}
}

// TestDryRunPlanCarriesAnalyzeAfter pins the plan surface: the dry-run
// plan mirrors the flag so `--dry-run` output shows the phase.
func TestDryRunPlanCarriesAnalyzeAfter(t *testing.T) {
	src := newRecordingEngine("source")
	src.schema = sampleSchema()
	tgt := newRecordingEngine("target")

	var got *MigrationPlan
	m := &Migrator{
		Source: src, Target: tgt,
		SourceDSN: "src", TargetDSN: "tgt",
		DryRun:       true,
		AnalyzeAfter: true,
		PlanSink:     func(p *MigrationPlan) { got = p },
	}
	if err := m.Run(context.Background()); err != nil {
		t.Fatalf("Run: %v", err)
	}
	if got == nil {
		t.Fatal("PlanSink never received a plan")
	}
	if !got.AnalyzeAfter {
		t.Error("plan.AnalyzeAfter = false; want true when Migrator.AnalyzeAfter is set")
	}
}
