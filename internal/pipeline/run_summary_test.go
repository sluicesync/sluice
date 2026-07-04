// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package pipeline

import (
	"context"
	"strings"
	"testing"

	"sluicesync.dev/sluice/internal/ir"
)

// TestRunSummaryNilSafe pins the zero-plumbing contract: every method
// on a nil *RunSummary is a no-op, so orchestrator call sites record
// unconditionally and text mode pays nothing.
func TestRunSummaryNilSafe(t *testing.T) {
	var s *RunSummary
	s.RecordTable("app", "users")
	s.RecordTableRows("app", "users", 5)
	if got := s.Tables(); got != nil {
		t.Fatalf("nil summary Tables() = %v, want nil", got)
	}
}

// TestRunSummaryAccumulatesAndOrders pins the accumulation semantics
// (repeat RecordTableRows sums — chain restores re-apply a table per
// segment), the unknown-vs-zero distinction (nil Rows vs *0), and the
// first-recorded ordering.
func TestRunSummaryAccumulatesAndOrders(t *testing.T) {
	s := &RunSummary{}
	s.RecordTable("", "no_rows_known")
	s.RecordTableRows("app", "orders", 2)
	s.RecordTableRows("app", "orders", 3)
	s.RecordTableRows("", "empty", 0)
	s.RecordTable("", "no_rows_known") // repeat is a no-op

	got := s.Tables()
	if len(got) != 3 {
		t.Fatalf("Tables() len = %d, want 3 (%v)", len(got), got)
	}
	if got[0].Name != "no_rows_known" || got[0].Rows != nil {
		t.Errorf("got[0] = %+v; want no_rows_known with nil Rows", got[0])
	}
	if got[1].Schema != "app" || got[1].Name != "orders" || got[1].Rows == nil || *got[1].Rows != 5 {
		t.Errorf("got[1] = %+v; want app.orders rows=5", got[1])
	}
	if got[2].Name != "empty" || got[2].Rows == nil || *got[2].Rows != 0 {
		t.Errorf("got[2] = %+v; want empty with a REAL 0 (not unknown)", got[2])
	}

	// The returned Rows pointers are copies — mutating them must not
	// write through into the collector.
	*got[1].Rows = 99
	if again := s.Tables(); *again[1].Rows != 5 {
		t.Errorf("Tables() returned a live pointer; internal total mutated to %d", *again[1].Rows)
	}
}

// TestMigratorDryRunPlanSink pins the `--dry-run --format json`
// pipeline contract: with a PlanSink attached the dry-run hands the
// built plan to the sink INSTEAD of rendering, touches no writer, and
// fills the plan from the translated schema (row counts -1 when the
// source reader implements no RowCounter).
func TestMigratorDryRunPlanSink(t *testing.T) {
	src := newRecordingEngine("source")
	src.schema = sampleSchema()
	tgt := newRecordingEngine("target")

	var plan *MigrationPlan
	m := &Migrator{
		Source: src, Target: tgt,
		SourceDSN: "src", TargetDSN: "tgt",
		DryRun:   true,
		PlanSink: func(p *MigrationPlan) { plan = p },
	}
	if err := m.Run(context.Background()); err != nil {
		t.Fatalf("Run: %v", err)
	}
	if plan == nil {
		t.Fatal("PlanSink never received a plan")
	}
	if plan.SourceEngine != "source" || plan.TargetEngine != "target" {
		t.Errorf("plan engines = %s/%s", plan.SourceEngine, plan.TargetEngine)
	}
	if len(plan.Tables) != 1 || plan.Tables[0].Name != "users" || plan.Tables[0].Columns != 1 {
		t.Errorf("plan tables = %+v", plan.Tables)
	}
	if plan.Tables[0].RowCount != -1 {
		t.Errorf("row count = %d; want -1 (source has no RowCounter — unknown must not read as 0)", plan.Tables[0].RowCount)
	}
	if tgt.openSchemaWriterCalls != 0 || tgt.openRowWriterCalls != 0 {
		t.Errorf("dry-run touched the target: schemaWriter=%d rowWriter=%d",
			tgt.openSchemaWriterCalls, tgt.openRowWriterCalls)
	}
}

// TestStreamerDryRunPlan pins the stream-plan build for both
// branches: warm resume carries the truncated persisted token and
// never touches the source; cold start summarises the filtered
// source schema with row counts pinned to -1 (never probed).
func TestStreamerDryRunPlan(t *testing.T) {
	src := newRecordingEngine("source")
	src.schema = sampleSchema()
	tgt := newRecordingEngine("target")
	s := &Streamer{
		Source: src, Target: tgt,
		SourceDSN: "postgres://u:pw@src.example.com:5432/db",
		TargetDSN: "u:pw@tcp(dst.example.com:3306)/db",
	}

	t.Run("warm resume", func(t *testing.T) {
		plan, err := s.buildDryRunPlan(context.Background(), "stream-1", ir.Position{Token: "tok"}, true)
		if err != nil {
			t.Fatalf("buildDryRunPlan: %v", err)
		}
		if !plan.WarmResume || plan.PositionToken != "tok" || plan.StreamID != "stream-1" {
			t.Errorf("plan = %+v", plan)
		}
		if len(plan.Tables) != 0 {
			t.Errorf("warm resume must not read the source schema: %+v", plan.Tables)
		}
		for _, host := range []string{plan.SourceHost, plan.TargetHost} {
			if host == "" || strings.Contains(host, "pw") {
				t.Errorf("host not redacted: %q", host)
			}
		}
	})

	t.Run("cold start", func(t *testing.T) {
		plan, err := s.buildDryRunPlan(context.Background(), "stream-1", ir.Position{}, false)
		if err != nil {
			t.Fatalf("buildDryRunPlan: %v", err)
		}
		if plan.WarmResume || plan.PositionToken != "" {
			t.Errorf("plan = %+v", plan)
		}
		if len(plan.Tables) != 1 || plan.Tables[0].Name != "users" || plan.Tables[0].RowCount != -1 {
			t.Errorf("plan tables = %+v", plan.Tables)
		}
	})
}
