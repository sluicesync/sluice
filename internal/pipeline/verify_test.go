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
	// Sample-mode per-table data; nil for count-mode tests.
	sampleHashes map[string][]ir.SampledRowHash
	sampleErr    map[string]error
}

func (e *verifyStubEngine) Name() string                  { return e.name }
func (e *verifyStubEngine) Capabilities() ir.Capabilities { return ir.Capabilities{} }

func (e *verifyStubEngine) OpenSchemaReader(_ context.Context, _ string) (ir.SchemaReader, error) {
	if e.noVerifier {
		return &verifyStubReaderNoVerifier{schema: e.schema, fail: e.readSchemaFail}, nil
	}
	return &verifyStubReader{
		schema:       e.schema,
		counts:       e.counts,
		countErr:     e.countErr,
		fail:         e.readSchemaFail,
		sampleHashes: e.sampleHashes,
		sampleErr:    e.sampleErr,
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
	// Per-table sampled-row hashes for sample-mode tests. Each entry
	// is a slice of (pk, hash) pairs that the stub returns verbatim
	// from SampleRowHashes. Tests populate this map directly.
	sampleHashes map[string][]ir.SampledRowHash
	// sampleErr forces SampleRowHashes to fail per-table.
	sampleErr map[string]error
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

// SampleRowHashes implements ir.SampleVerifier so the stub can drive
// sample-mode tests. Returns the pre-populated slice; tests assemble
// matched / mismatched pairs by setting source vs. target hashes.
func (r *verifyStubReader) SampleRowHashes(_ context.Context, table *ir.Table, _ int, _ int64) ([]ir.SampledRowHash, error) {
	if err, ok := r.sampleErr[table.Name]; ok {
		return nil, err
	}
	return r.sampleHashes[table.Name], nil
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
	v := &Verifier{Source: src, Target: tgt, SourceDSN: "src", TargetDSN: "tgt", Depth: "full", Out: &buf}
	_, err := v.Run(context.Background())
	if err == nil || !strings.Contains(err.Error(), "not supported") {
		t.Errorf("expected unsupported-depth error; got %v", err)
	}
}

// TestVerifier_Run_SampleMode_Clean pins the v0.14.0 sample-mode
// happy path: identical sampled hashes on both sides → no mismatch,
// per-table SampleSize populated, text output reports "clean".
func TestVerifier_Run_SampleMode_Clean(t *testing.T) {
	hashes := []ir.SampledRowHash{
		{PrimaryKey: "1", Hash: "abc"},
		{PrimaryKey: "2", Hash: "def"},
		{PrimaryKey: "3", Hash: "ghi"},
	}
	src := &verifyStubEngine{
		name: "postgres", schema: verifySchema("users"),
		counts:       map[string]int64{"users": 3},
		sampleHashes: map[string][]ir.SampledRowHash{"users": hashes},
	}
	tgt := &verifyStubEngine{
		name: "postgres", schema: verifySchema("users"),
		counts:       map[string]int64{"users": 3},
		sampleHashes: map[string][]ir.SampledRowHash{"users": hashes},
	}

	var buf bytes.Buffer
	v := &Verifier{Source: src, Target: tgt, SourceDSN: "src", TargetDSN: "tgt", Depth: VerifyDepthSample, Out: &buf}
	r, err := v.Run(context.Background())
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	if r.HasMismatch() {
		t.Errorf("identical hashes should be clean; got %+v", r.Summary)
	}
	if r.Tables[0].SampleSize != 3 {
		t.Errorf("expected SampleSize=3; got %d", r.Tables[0].SampleSize)
	}
	if !strings.Contains(buf.String(), "sampled=3 clean") {
		t.Errorf("text output should announce 'sampled=3 clean'; got:\n%s", buf.String())
	}
}

// TestVerifier_Run_SampleMode_HashMismatch covers the case where
// counts match but row content differs. Crucial for catching silent
// data drift that count-mode would miss entirely.
func TestVerifier_Run_SampleMode_HashMismatch(t *testing.T) {
	srcHashes := []ir.SampledRowHash{
		{PrimaryKey: "1", Hash: "abc"},
		{PrimaryKey: "2", Hash: "def"},
		{PrimaryKey: "3", Hash: "ghi"},
	}
	tgtHashes := []ir.SampledRowHash{
		{PrimaryKey: "1", Hash: "abc"},
		{PrimaryKey: "2", Hash: "WRONG"}, // drift on PK=2
		{PrimaryKey: "3", Hash: "ghi"},
	}
	src := &verifyStubEngine{
		name: "postgres", schema: verifySchema("users"),
		counts:       map[string]int64{"users": 3},
		sampleHashes: map[string][]ir.SampledRowHash{"users": srcHashes},
	}
	tgt := &verifyStubEngine{
		name: "postgres", schema: verifySchema("users"),
		counts:       map[string]int64{"users": 3},
		sampleHashes: map[string][]ir.SampledRowHash{"users": tgtHashes},
	}

	var buf bytes.Buffer
	v := &Verifier{Source: src, Target: tgt, SourceDSN: "src", TargetDSN: "tgt", Depth: VerifyDepthSample, Out: &buf}
	r, err := v.Run(context.Background())
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	if !r.HasMismatch() {
		t.Errorf("hash drift should surface as mismatch; got %+v", r.Summary)
	}
	if r.Tables[0].SampleMismatch != 1 {
		t.Errorf("expected SampleMismatch=1; got %d", r.Tables[0].SampleMismatch)
	}
	if len(r.Tables[0].SampleMismatchPKs) != 1 || r.Tables[0].SampleMismatchPKs[0] != "2" {
		t.Errorf("expected mismatch PK=[2]; got %v", r.Tables[0].SampleMismatchPKs)
	}
	if !strings.Contains(buf.String(), "SAMPLE-MISMATCH") {
		t.Errorf("text output should announce SAMPLE-MISMATCH; got:\n%s", buf.String())
	}
}

// TestVerifier_Run_SampleMode_PKsDiffer covers the case where one
// side has a row the other side doesn't (PK present on one only).
// The merge-walk in compareSampleHashes catches this distinct from
// pure hash drift.
func TestVerifier_Run_SampleMode_PKsDiffer(t *testing.T) {
	src := &verifyStubEngine{
		name: "postgres", schema: verifySchema("users"),
		counts: map[string]int64{"users": 3},
		sampleHashes: map[string][]ir.SampledRowHash{"users": {
			{PrimaryKey: "1", Hash: "h1"},
			{PrimaryKey: "2", Hash: "h2"},
			{PrimaryKey: "3", Hash: "h3"},
		}},
	}
	tgt := &verifyStubEngine{
		name: "postgres", schema: verifySchema("users"),
		counts: map[string]int64{"users": 3},
		sampleHashes: map[string][]ir.SampledRowHash{"users": {
			{PrimaryKey: "1", Hash: "h1"},
			// missing PK=2
			{PrimaryKey: "3", Hash: "h3"},
			{PrimaryKey: "4", Hash: "h4"}, // extra PK=4
		}},
	}

	var buf bytes.Buffer
	v := &Verifier{Source: src, Target: tgt, SourceDSN: "src", TargetDSN: "tgt", Depth: VerifyDepthSample, Out: &buf}
	r, err := v.Run(context.Background())
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	if !r.HasMismatch() {
		t.Errorf("PK divergence should surface as mismatch")
	}
	// 2 mismatches: PK=2 (source-only) and PK=4 (target-only).
	if r.Tables[0].SampleMismatch != 2 {
		t.Errorf("expected SampleMismatch=2; got %d", r.Tables[0].SampleMismatch)
	}
	pks := r.Tables[0].SampleMismatchPKs
	if len(pks) != 2 || pks[0] != "2" || pks[1] != "4" {
		t.Errorf("expected mismatch PKs=[2, 4]; got %v", pks)
	}
}

// TestVerifier_Run_SampleMode_CrossEngineRejected pins the same-
// engine constraint: cross-engine sample-mode produces silent false-
// positive mismatches due to text-rendering differences, so the
// orchestrator refuses upfront with a clear error.
func TestVerifier_Run_SampleMode_CrossEngineRejected(t *testing.T) {
	src := &verifyStubEngine{name: "postgres", schema: verifySchema("users"), counts: map[string]int64{"users": 1}}
	tgt := &verifyStubEngine{name: "mysql", schema: verifySchema("users"), counts: map[string]int64{"users": 1}}

	var buf bytes.Buffer
	v := &Verifier{Source: src, Target: tgt, SourceDSN: "src", TargetDSN: "tgt", Depth: VerifyDepthSample, Out: &buf}
	_, err := v.Run(context.Background())
	if err == nil || !strings.Contains(err.Error(), "requires same source and target engine") {
		t.Errorf("expected cross-engine rejection; got %v", err)
	}
}
