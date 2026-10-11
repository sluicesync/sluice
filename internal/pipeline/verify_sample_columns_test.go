// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package pipeline

import (
	"bytes"
	"context"
	"reflect"
	"strings"
	"testing"

	"sluicesync.dev/sluice/internal/ir"
)

// The stub must keep satisfying the real interface, or every sample-mode
// unit test silently degrades to the "no ir.SampleVerifier" skip path.
var _ ir.SampleVerifier = (*verifyStubReader)(nil)

// sampleColsTable builds a table whose columns are declared in the order
// given, keyed on its first column.
func sampleColsTable(name string, cols ...string) *ir.Table {
	t := &ir.Table{Name: name, PrimaryKey: &ir.Index{Columns: []ir.IndexColumn{{Column: cols[0]}}}}
	for _, c := range cols {
		t.Columns = append(t.Columns, &ir.Column{Name: c, Type: ir.Text{}})
	}
	return t
}

// TestPlanSampleColumns_MatchesByNameNotPosition pins the
// VERIFY-SAMPLE-COLUMN-ORDER fix at the planning layer: the hashed order
// is the source names sorted, so a target that declares the same names
// in another order gets the SAME plan — values are then compared under
// their names, never by position.
func TestPlanSampleColumns_MatchesByNameNotPosition(t *testing.T) {
	src := sampleColsTable("kd", "id", "c", "b")
	tgt := sampleColsTable("kd", "id", "b", "c")

	p, err := planSampleColumns(src, tgt)
	if err != nil {
		t.Fatalf("planSampleColumns: %v", err)
	}
	want := ir.SampleColumns{Key: []string{"id"}, Hashed: []string{"b", "c", "id"}}
	if !reflect.DeepEqual(p.cols, want) {
		t.Errorf("plan = %+v; want %+v", p.cols, want)
	}
	if len(p.missing) != 0 || len(p.extra) != 0 {
		t.Errorf("same column set reported missing=%v extra=%v", p.missing, p.extra)
	}
	// The plan must not depend on which side declared what: planning
	// from the reordered target as "source" yields the same hashed order.
	q, err := planSampleColumns(tgt, src)
	if err != nil {
		t.Fatalf("planSampleColumns (reversed): %v", err)
	}
	if !reflect.DeepEqual(p.cols.Hashed, q.cols.Hashed) {
		t.Errorf("hashed order depends on declaration order: %v vs %v", p.cols.Hashed, q.cols.Hashed)
	}
}

// TestPlanSampleColumns_MissingAndExtra pins the one-side-only columns:
// each is named, sorted, and left out of the hash.
func TestPlanSampleColumns_MissingAndExtra(t *testing.T) {
	src := sampleColsTable("kd", "id", "c", "b", "a")
	tgt := sampleColsTable("kd", "id", "b", "z", "d")

	p, err := planSampleColumns(src, tgt)
	if err != nil {
		t.Fatalf("planSampleColumns: %v", err)
	}
	if want := []string{"a", "c"}; !reflect.DeepEqual(p.missing, want) {
		t.Errorf("missing = %v; want %v", p.missing, want)
	}
	if want := []string{"d", "z"}; !reflect.DeepEqual(p.extra, want) {
		t.Errorf("extra = %v; want %v", p.extra, want)
	}
	if want := []string{"b", "id"}; !reflect.DeepEqual(p.cols.Hashed, want) {
		t.Errorf("hashed = %v; want only the shared columns %v", p.cols.Hashed, want)
	}
}

// TestPlanSampleColumns_KeyIsTheSourcePK pins that the row identity is
// the SOURCE key, in key order, whatever the target declares.
func TestPlanSampleColumns_KeyIsTheSourcePK(t *testing.T) {
	src := sampleColsTable("kd", "a", "b", "c")
	src.PrimaryKey = &ir.Index{Columns: []ir.IndexColumn{{Column: "c"}, {Column: "a"}}}
	tgt := sampleColsTable("kd", "b", "a", "c") // keyed on b

	p, err := planSampleColumns(src, tgt)
	if err != nil {
		t.Fatalf("planSampleColumns: %v", err)
	}
	if want := []string{"c", "a"}; !reflect.DeepEqual(p.cols.Key, want) {
		t.Errorf("key = %v; want the source PK in key order %v", p.cols.Key, want)
	}
}

// TestPlanSampleColumns_NoSourcePKRefused pins the deterministic-sampling
// precondition: a keyless source is refused, not sampled arbitrarily.
func TestPlanSampleColumns_NoSourcePKRefused(t *testing.T) {
	src := sampleColsTable("kd", "id", "b")
	src.PrimaryKey = nil
	if _, err := planSampleColumns(src, sampleColsTable("kd", "id", "b")); err == nil ||
		!strings.Contains(err.Error(), "no primary key") {
		t.Fatalf("err = %v; want the no-primary-key refusal", err)
	}
}

// TestVerifier_Run_SampleMode_BothSidesGetOnePlan pins the orchestrator
// half: source and target receive the IDENTICAL column plan, derived from
// both tables, even when they declare the columns in different orders.
// Passing each side its own declared order is exactly the defect.
func TestVerifier_Run_SampleMode_BothSidesGetOnePlan(t *testing.T) {
	var srcSeen, tgtSeen []ir.SampleColumns
	hashes := []ir.SampledRowHash{{PrimaryKey: "1", Hash: "h"}}
	src := &verifyStubEngine{
		name: "postgres", schema: &ir.Schema{Tables: []*ir.Table{sampleColsTable("kd", "id", "c", "b")}},
		counts:       map[string]int64{"kd": 1},
		sampleHashes: map[string][]ir.SampledRowHash{"kd": hashes},
		colsSeen:     &srcSeen,
	}
	tgt := &verifyStubEngine{
		name: "postgres", schema: &ir.Schema{Tables: []*ir.Table{sampleColsTable("kd", "id", "b", "c")}},
		counts:       map[string]int64{"kd": 1},
		sampleHashes: map[string][]ir.SampledRowHash{"kd": hashes},
		colsSeen:     &tgtSeen,
	}
	var buf bytes.Buffer
	v := &Verifier{Source: src, Target: tgt, SourceDSN: "src", TargetDSN: "tgt", Depth: VerifyDepthSample, Out: &buf}
	r, err := v.Run(context.Background())
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	if r.HasMismatch() || r.HasUnverified() {
		t.Fatalf("summary = %+v; want clean", r.Summary)
	}
	want := ir.SampleColumns{Key: []string{"id"}, Hashed: []string{"b", "c", "id"}}
	if len(srcSeen) != 1 || len(tgtSeen) != 1 {
		t.Fatalf("plans seen: source %d, target %d; want one each", len(srcSeen), len(tgtSeen))
	}
	if !reflect.DeepEqual(srcSeen[0], want) || !reflect.DeepEqual(tgtSeen[0], want) {
		t.Errorf("source plan %+v, target plan %+v; want both %+v", srcSeen[0], tgtSeen[0], want)
	}
}

// TestVerifier_Run_SampleMode_MissingColumnIsAMismatch pins the loud
// half: a source column the target lacks fails the table (exit 1) and is
// named, even though every count and every sampled hash would agree.
func TestVerifier_Run_SampleMode_MissingColumnIsAMismatch(t *testing.T) {
	var seen []ir.SampleColumns
	hashes := []ir.SampledRowHash{{PrimaryKey: "1", Hash: "h"}}
	src := &verifyStubEngine{
		name: "mysql", schema: &ir.Schema{Tables: []*ir.Table{sampleColsTable("kd", "id", "b", "c")}},
		counts:       map[string]int64{"kd": 1},
		sampleHashes: map[string][]ir.SampledRowHash{"kd": hashes},
		colsSeen:     &seen,
	}
	tgt := &verifyStubEngine{
		name: "mysql", schema: &ir.Schema{Tables: []*ir.Table{sampleColsTable("kd", "id", "b", "d")}},
		counts:       map[string]int64{"kd": 1},
		sampleHashes: map[string][]ir.SampledRowHash{"kd": hashes},
		colsSeen:     &seen,
	}
	var buf bytes.Buffer
	v := &Verifier{Source: src, Target: tgt, SourceDSN: "src", TargetDSN: "tgt", Depth: VerifyDepthSample, Out: &buf}
	r, err := v.Run(context.Background())
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	if !r.HasMismatch() || r.Summary.TablesMismatch != 1 {
		t.Fatalf("summary = %+v; want the table mismatched", r.Summary)
	}
	tr := r.Tables[0]
	if !reflect.DeepEqual(tr.ColumnsMissingOnTarget, []string{"c"}) || !reflect.DeepEqual(tr.ColumnsExtraOnTarget, []string{"d"}) {
		t.Errorf("missing=%v extra=%v; want [c] / [d]", tr.ColumnsMissingOnTarget, tr.ColumnsExtraOnTarget)
	}
	if len(seen) != 0 {
		t.Errorf("rows were sampled for a table missing columns: %+v", seen)
	}
	out := buf.String()
	for _, want := range []string{"COLUMN-MISMATCH", "column(s) missing on target: c", "column(s) on target only (informational; not hashed): d"} {
		if !strings.Contains(out, want) {
			t.Errorf("text output lacks %q:\n%s", want, out)
		}
	}
}

// TestVerifier_Run_SampleMode_ExtraColumnIsInformational pins the other
// half: a target-only column is named but neither hashed nor failed —
// it holds nothing the source can be missing.
func TestVerifier_Run_SampleMode_ExtraColumnIsInformational(t *testing.T) {
	var seen []ir.SampleColumns
	hashes := []ir.SampledRowHash{{PrimaryKey: "1", Hash: "h"}}
	src := &verifyStubEngine{
		name: "mysql", schema: &ir.Schema{Tables: []*ir.Table{sampleColsTable("kd", "id", "b")}},
		counts:       map[string]int64{"kd": 1},
		sampleHashes: map[string][]ir.SampledRowHash{"kd": hashes},
		colsSeen:     &seen,
	}
	tgt := &verifyStubEngine{
		name: "mysql", schema: &ir.Schema{Tables: []*ir.Table{sampleColsTable("kd", "id", "b", "shard")}},
		counts:       map[string]int64{"kd": 1},
		sampleHashes: map[string][]ir.SampledRowHash{"kd": hashes},
		colsSeen:     &seen,
	}
	var buf bytes.Buffer
	v := &Verifier{Source: src, Target: tgt, SourceDSN: "src", TargetDSN: "tgt", Depth: VerifyDepthSample, Out: &buf}
	r, err := v.Run(context.Background())
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	if r.HasMismatch() || r.HasUnverified() {
		t.Fatalf("summary = %+v; an extra target column must not fail the table", r.Summary)
	}
	if !reflect.DeepEqual(r.Tables[0].ColumnsExtraOnTarget, []string{"shard"}) {
		t.Errorf("extra = %v; want [shard]", r.Tables[0].ColumnsExtraOnTarget)
	}
	for _, p := range seen {
		for _, c := range p.Hashed {
			if c == "shard" {
				t.Errorf("target-only column hashed: %+v", p)
			}
		}
	}
	if !strings.Contains(buf.String(), "on target only (informational; not hashed): shard") {
		t.Errorf("text output lacks the extra-column note:\n%s", buf.String())
	}
}
