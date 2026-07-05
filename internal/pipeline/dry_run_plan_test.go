// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package pipeline

import (
	"context"
	"strings"
	"testing"

	"sluicesync.dev/sluice/internal/ir"
)

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
