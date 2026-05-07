// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package pipeline

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"strings"
	"testing"

	"github.com/orware/sluice/internal/ir"
)

// verifyStubEngine is a minimal Engine for verify-orchestrator unit
// tests. The reader it returns satisfies both ir.SchemaReader and
// ir.Verifier — verify needs both surfaces. Other Engine methods
// panic so any pipeline regression that reaches them surfaces loud.
type verifyStubEngine struct {
	name           string
	schema         *ir.Schema
	counts         map[string]int64
	countErr       map[string]error
	noVerifier     bool // when true, the reader does NOT implement ir.Verifier
	readSchemaFail error
}

func (e *verifyStubEngine) Name() string                  { return e.name }
func (e *verifyStubEngine) Capabilities() ir.Capabilities { return ir.Capabilities{} }

func (e *verifyStubEngine) OpenSchemaReader(_ context.Context, _ string) (ir.SchemaReader, error) {
	if e.noVerifier {
		return &verifyStubReaderNoVerifier{schema: e.schema, fail: e.readSchemaFail}, nil
	}
	return &verifyStubReader{
		schema:   e.schema,
		counts:   e.counts,
		countErr: e.countErr,
		fail:     e.readSchemaFail,
	}, nil
}

func (e *verifyStubEngine) OpenSchemaWriter(_ context.Context, _ string) (ir.SchemaWriter, error) {
	panic("verify should not open schema writer")
}

func (e *verifyStubEngine) OpenRowReader(_ context.Context, _ string) (ir.RowReader, error) {
	panic("verify should not open row reader")
}

func (e *verifyStubEngine) OpenRowWriter(_ context.Context, _ string) (ir.RowWriter, error) {
	panic("verify should not open row writer")
}

func (e *verifyStubEngine) OpenCDCReader(_ context.Context, _ string) (ir.CDCReader, error) {
	panic("verify should not open CDC reader")
}

func (e *verifyStubEngine) OpenChangeApplier(_ context.Context, _ string) (ir.ChangeApplier, error) {
	panic("verify should not open change applier")
}

func (e *verifyStubEngine) OpenSnapshotStream(_ context.Context, _ string) (*ir.SnapshotStream, error) {
	panic("verify should not open snapshot stream")
}

type verifyStubReader struct {
	schema   *ir.Schema
	counts   map[string]int64
	countErr map[string]error
	fail     error
}

func (r *verifyStubReader) ReadSchema(_ context.Context) (*ir.Schema, error) {
	if r.fail != nil {
		return nil, r.fail
	}
	return r.schema, nil
}

func (r *verifyStubReader) ExactRowCount(_ context.Context, table *ir.Table) (int64, error) {
	if err, ok := r.countErr[table.Name]; ok {
		return 0, err
	}
	return r.counts[table.Name], nil
}

// verifyStubReaderNoVerifier omits ExactRowCount so the test that
// covers "engine doesn't support Verifier" exercises the right fall-
// through.
type verifyStubReaderNoVerifier struct {
	schema *ir.Schema
	fail   error
}

func (r *verifyStubReaderNoVerifier) ReadSchema(_ context.Context) (*ir.Schema, error) {
	if r.fail != nil {
		return nil, r.fail
	}
	return r.schema, nil
}

func verifySchema(tableNames ...string) *ir.Schema {
	tables := make([]*ir.Table, len(tableNames))
	for i, n := range tableNames {
		tables[i] = &ir.Table{
			Name: n,
			Columns: []*ir.Column{
				{Name: "id", Type: ir.Integer{Width: 64}, Nullable: false},
			},
		}
	}
	return &ir.Schema{Tables: tables}
}

func TestVerifier_Run_AllClean(t *testing.T) {
	src := &verifyStubEngine{name: "postgres", schema: verifySchema("users", "orders"), counts: map[string]int64{"users": 100, "orders": 250}}
	tgt := &verifyStubEngine{name: "postgres", schema: verifySchema("users", "orders"), counts: map[string]int64{"users": 100, "orders": 250}}

	var buf bytes.Buffer
	v := &Verifier{Source: src, Target: tgt, SourceDSN: "src", TargetDSN: "tgt", Out: &buf}

	r, err := v.Run(context.Background())
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	if r.HasMismatch() {
		t.Errorf("expected no mismatch; got summary %+v", r.Summary)
	}
	if r.Summary.TablesChecked != 2 || r.Summary.TablesClean != 2 {
		t.Errorf("expected 2 checked / 2 clean; got %+v", r.Summary)
	}
	out := buf.String()
	if !strings.Contains(out, "OK rows=100") || !strings.Contains(out, "OK rows=250") {
		t.Errorf("expected per-table OK lines; got:\n%s", out)
	}
}

func TestVerifier_Run_Mismatch(t *testing.T) {
	src := &verifyStubEngine{name: "postgres", schema: verifySchema("users"), counts: map[string]int64{"users": 100}}
	tgt := &verifyStubEngine{name: "postgres", schema: verifySchema("users"), counts: map[string]int64{"users": 95}}

	var buf bytes.Buffer
	v := &Verifier{Source: src, Target: tgt, SourceDSN: "src", TargetDSN: "tgt", Out: &buf}

	r, err := v.Run(context.Background())
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	if !r.HasMismatch() {
		t.Errorf("expected mismatch; got %+v", r.Summary)
	}
	if r.Tables[0].SourceRowCount != 100 || r.Tables[0].TargetRowCount != 95 {
		t.Errorf("expected 100/95; got %+v", r.Tables[0])
	}
	out := buf.String()
	if !strings.Contains(out, "MISMATCH source=100 target=95 (delta=-5)") {
		t.Errorf("expected MISMATCH line with delta; got:\n%s", out)
	}
}

func TestVerifier_Run_TableMissingOnTarget(t *testing.T) {
	src := &verifyStubEngine{name: "postgres", schema: verifySchema("users", "orphan_table"), counts: map[string]int64{"users": 10, "orphan_table": 5}}
	tgt := &verifyStubEngine{name: "postgres", schema: verifySchema("users"), counts: map[string]int64{"users": 10}}

	var buf bytes.Buffer
	v := &Verifier{Source: src, Target: tgt, SourceDSN: "src", TargetDSN: "tgt", Out: &buf}

	r, err := v.Run(context.Background())
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	if r.Summary.TablesSkipped != 1 {
		t.Errorf("expected 1 skipped; got %+v", r.Summary)
	}
	if r.HasMismatch() {
		t.Errorf("missing-on-target should be SKIPPED, not mismatch")
	}
}

func TestVerifier_Run_EngineWithoutVerifierFails(t *testing.T) {
	src := &verifyStubEngine{name: "postgres", schema: verifySchema("users"), noVerifier: true}
	tgt := &verifyStubEngine{name: "postgres", schema: verifySchema("users"), counts: map[string]int64{"users": 1}}

	var buf bytes.Buffer
	v := &Verifier{Source: src, Target: tgt, SourceDSN: "src", TargetDSN: "tgt", Out: &buf}

	_, err := v.Run(context.Background())
	if err == nil {
		t.Fatal("expected error when engine doesn't implement ir.Verifier")
	}
	if !strings.Contains(err.Error(), "does not support data verification") {
		t.Errorf("error should mention verification support; got %v", err)
	}
}

func TestVerifier_Run_JSONOutput(t *testing.T) {
	src := &verifyStubEngine{name: "postgres", schema: verifySchema("users"), counts: map[string]int64{"users": 7}}
	tgt := &verifyStubEngine{name: "postgres", schema: verifySchema("users"), counts: map[string]int64{"users": 7}}

	var buf bytes.Buffer
	v := &Verifier{Source: src, Target: tgt, SourceDSN: "src", TargetDSN: "tgt", Format: "json", Out: &buf}
	if _, err := v.Run(context.Background()); err != nil {
		t.Fatalf("Run: %v", err)
	}
	var got VerifyResult
	if err := json.Unmarshal(buf.Bytes(), &got); err != nil {
		t.Fatalf("decode JSON: %v\noutput:\n%s", err, buf.String())
	}
	if got.Summary.TablesChecked != 1 || got.Summary.TablesClean != 1 {
		t.Errorf("JSON shape: got %+v", got.Summary)
	}
}

func TestVerifier_Run_PerTableCountErrorTreatedAsSkipped(t *testing.T) {
	src := &verifyStubEngine{
		name: "postgres", schema: verifySchema("users"),
		counts:   map[string]int64{"users": 5},
		countErr: map[string]error{"users": errors.New("simulated count failure")},
	}
	tgt := &verifyStubEngine{name: "postgres", schema: verifySchema("users"), counts: map[string]int64{"users": 5}}

	var buf bytes.Buffer
	v := &Verifier{Source: src, Target: tgt, SourceDSN: "src", TargetDSN: "tgt", Out: &buf}
	r, err := v.Run(context.Background())
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	if r.Summary.TablesSkipped != 1 {
		t.Errorf("expected count error → 1 skipped; got %+v", r.Summary)
	}
}

// TestVerifier_Run_ExtraOnTarget pins the v0.12.x enhancement:
// tables present on target but absent on source surface in the
// ExtraOnTarget slice (informational), do NOT contribute to
// TablesMismatch. Operators get visibility without false-positive
// drift signal.
func TestVerifier_Run_ExtraOnTarget(t *testing.T) {
	src := &verifyStubEngine{name: "postgres", schema: verifySchema("users"), counts: map[string]int64{"users": 5}}
	tgt := &verifyStubEngine{name: "postgres", schema: verifySchema("users", "audit_log", "ops_metrics"), counts: map[string]int64{"users": 5, "audit_log": 100, "ops_metrics": 50}}

	var buf bytes.Buffer
	v := &Verifier{Source: src, Target: tgt, SourceDSN: "src", TargetDSN: "tgt", Out: &buf}
	r, err := v.Run(context.Background())
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	if r.HasMismatch() {
		t.Errorf("extra-on-target should not flag mismatch; got %+v", r.Summary)
	}
	if r.Summary.TablesExtraOnTarget != 2 {
		t.Errorf("expected 2 extra; got %d", r.Summary.TablesExtraOnTarget)
	}
	if len(r.ExtraOnTarget) != 2 || r.ExtraOnTarget[0] != "audit_log" || r.ExtraOnTarget[1] != "ops_metrics" {
		t.Errorf("ExtraOnTarget should be sorted [audit_log, ops_metrics]; got %v", r.ExtraOnTarget)
	}
	out := buf.String()
	if !strings.Contains(out, "tables present on target but absent on source") {
		t.Errorf("text output should announce extra-on-target section; got:\n%s", out)
	}
	if !strings.Contains(out, "audit_log") || !strings.Contains(out, "ops_metrics") {
		t.Errorf("text output should list extra table names; got:\n%s", out)
	}
}

func TestVerifier_Run_UnsupportedDepthRejected(t *testing.T) {
	src := &verifyStubEngine{name: "postgres", schema: verifySchema("users"), counts: map[string]int64{"users": 1}}
	tgt := &verifyStubEngine{name: "postgres", schema: verifySchema("users"), counts: map[string]int64{"users": 1}}

	var buf bytes.Buffer
	v := &Verifier{Source: src, Target: tgt, SourceDSN: "src", TargetDSN: "tgt", Depth: "sample", Out: &buf}
	_, err := v.Run(context.Background())
	if err == nil || !strings.Contains(err.Error(), "not supported in v0.12.0") {
		t.Errorf("expected unsupported-depth error; got %v", err)
	}
}
