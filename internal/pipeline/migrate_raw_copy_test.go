// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package pipeline

import (
	"context"
	"errors"
	"io"
	"strings"
	"testing"

	"sluicesync.dev/sluice/internal/config"
	"sluicesync.dev/sluice/internal/ir"
	"sluicesync.dev/sluice/internal/redact"
)

// fakeRawExporter / fakeRawImporter satisfy the raw-copy surfaces so the
// pure negotiation logic can be unit-tested without a real database.
type fakeRawExporter struct {
	major    int
	majorErr error
}

func (fakeRawExporter) ReadRows(context.Context, *ir.Table) (<-chan ir.Row, error) {
	return nil, errors.New("not used")
}
func (fakeRawExporter) Err() error { return nil }
func (f fakeRawExporter) ServerMajorVersion(context.Context) (int, error) {
	return f.major, f.majorErr
}

func (fakeRawExporter) ExportRawCopy(context.Context, *ir.Table, *ir.RawCopyChunk, ir.RawCopyFormat, io.Writer) error {
	return nil
}

type fakeRawImporter struct {
	major    int
	majorErr error
}

func (fakeRawImporter) WriteRows(context.Context, *ir.Table, <-chan ir.Row) error { return nil }
func (f fakeRawImporter) ServerMajorVersion(context.Context) (int, error) {
	return f.major, f.majorErr
}

func (fakeRawImporter) ImportRawCopy(context.Context, *ir.Table, ir.RawCopyFormat, io.Reader) (int64, error) {
	return 0, nil
}

// TestNegotiateRawCopyFormat pins the format negotiation: text request
// stays text; a binary request engages binary only when both majors
// match; a major mismatch or a probe error downgrades to text (loudly —
// never silently engaging binary across majors).
func TestNegotiateRawCopyFormat(t *testing.T) {
	ctx := context.Background()
	tests := []struct {
		name      string
		requested ir.RawCopyFormat
		srcMajor  int
		dstMajor  int
		srcErr    error
		dstErr    error
		want      ir.RawCopyFormat
	}{
		{"text request stays text", ir.RawCopyText, 16, 16, nil, nil, ir.RawCopyText},
		{"binary matched majors -> binary", ir.RawCopyBinary, 16, 16, nil, nil, ir.RawCopyBinary},
		{"binary mismatched majors -> text", ir.RawCopyBinary, 16, 17, nil, nil, ir.RawCopyText},
		{"binary src probe error -> text", ir.RawCopyBinary, 0, 16, errors.New("boom"), nil, ir.RawCopyText},
		{"binary dst probe error -> text", ir.RawCopyBinary, 16, 0, nil, errors.New("boom"), ir.RawCopyText},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			exp := fakeRawExporter{major: tc.srcMajor, majorErr: tc.srcErr}
			imp := fakeRawImporter{major: tc.dstMajor, majorErr: tc.dstErr}
			if got := negotiateRawCopyFormat(ctx, tc.requested, exp, imp); got != tc.want {
				t.Errorf("negotiateRawCopyFormat = %v; want %v", got, tc.want)
			}
		})
	}
}

// TestRawCopyGate is the load-bearing correctness matrix: each value
// transform present MUST force ok=false (so the byte-pipe can't silently
// skip it — a silent-loss class), plus the all-clear positive. The gate
// is the single auditable predicate the value-fidelity reviewer audits.
func TestRawCopyGate(t *testing.T) {
	pgSrc := newRecordingEngine("postgres")
	pgDst := newRecordingEngine("postgres")

	// allClear is the baseline same-engine, no-transform Migrator the
	// negative cases each mutate one field of.
	allClear := func() *Migrator {
		return &Migrator{Source: pgSrc, Target: pgDst}
	}

	redactorWithRule := func() *redact.Registry {
		r := redact.New()
		r.Set("public", "users", "email", redact.Hash{Algo: "sha256"})
		return r
	}

	tests := []struct {
		name    string
		migr    *Migrator
		wantOK  bool
		wantSub string // substring the reason must contain (negatives only)
	}{
		{
			name:   "all clear positive",
			migr:   allClear(),
			wantOK: true,
		},
		{
			name:    "cross engine (different names)",
			migr:    &Migrator{Source: newRecordingEngine("postgres"), Target: newRecordingEngine("mysql")},
			wantOK:  false,
			wantSub: "cross-engine",
		},
		{
			name:    "nil source",
			migr:    &Migrator{Source: nil, Target: pgDst},
			wantOK:  false,
			wantSub: "cross-engine",
		},
		{
			name: "redaction configured",
			migr: func() *Migrator {
				m := allClear()
				m.Redactor = redactorWithRule()
				return m
			}(),
			wantOK:  false,
			wantSub: "redaction",
		},
		{
			name: "empty redactor is allowed",
			migr: func() *Migrator {
				m := allClear()
				m.Redactor = redact.New() // no rules => Empty()
				return m
			}(),
			wantOK: true,
		},
		{
			name: "type override (Mappings) configured",
			migr: func() *Migrator {
				m := allClear()
				m.Mappings = []config.Mapping{{Table: "t", Column: "c", TargetType: "text"}}
				return m
			}(),
			wantOK:  false,
			wantSub: "type override",
		},
		{
			name: "expression override configured",
			migr: func() *Migrator {
				m := allClear()
				m.ExpressionMappings = []config.ExpressionMapping{{Table: "t", Column: "c", Expression: "1"}}
				return m
			}(),
			wantOK:  false,
			wantSub: "expression override",
		},
		{
			name: "shard injection engaged",
			migr: func() *Migrator {
				m := allClear()
				m.InjectShardColumn = ShardColumnSpec{Name: "shard_id", Value: "us-east-1"}
				return m
			}(),
			wantOK:  false,
			wantSub: "shard-column injection",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			ok, reason := rawCopyGate(rawCopyConfigForMigrator(tc.migr))
			if ok != tc.wantOK {
				t.Fatalf("rawCopyGate ok = %v (reason %q); want %v", ok, reason, tc.wantOK)
			}
			if !tc.wantOK {
				if reason == "" {
					t.Fatalf("negative gate must give a reason; got empty")
				}
				if tc.wantSub != "" && !strings.Contains(reason, tc.wantSub) {
					t.Errorf("reason %q does not contain %q", reason, tc.wantSub)
				}
			}
		})
	}
}

// TestRawCopyGate_StreamerParity asserts the SAME gate governs the sync
// cold-start path (ADR-0079): a Streamer-populated config routes through
// the identical predicate. The matrix above already pins every transform
// negative on the migrate projection; here we confirm (a) the all-clear
// same-engine Streamer config is eligible and (b) each transform the
// Streamer can carry forces the IR fallback through the shared gate, so
// the raw lane can never silently skip a transform on the sync path.
func TestRawCopyGate_StreamerParity(t *testing.T) {
	allClear := func() *Streamer {
		return &Streamer{Source: newRecordingEngine("postgres"), Target: newRecordingEngine("postgres")}
	}

	tests := []struct {
		name    string
		stream  *Streamer
		wantOK  bool
		wantSub string
	}{
		{
			name:   "all clear positive",
			stream: allClear(),
			wantOK: true,
		},
		{
			name:    "cross engine",
			stream:  &Streamer{Source: newRecordingEngine("postgres"), Target: newRecordingEngine("mysql")},
			wantOK:  false,
			wantSub: "cross-engine",
		},
		{
			name: "redaction configured",
			stream: func() *Streamer {
				s := allClear()
				r := redact.New()
				r.Set("public", "users", "email", redact.Hash{Algo: "sha256"})
				s.Redactor = r
				return s
			}(),
			wantOK:  false,
			wantSub: "redaction",
		},
		{
			name: "type override configured",
			stream: func() *Streamer {
				s := allClear()
				s.Mappings = []config.Mapping{{Table: "t", Column: "c", TargetType: "text"}}
				return s
			}(),
			wantOK:  false,
			wantSub: "type override",
		},
		{
			name: "expression override configured",
			stream: func() *Streamer {
				s := allClear()
				s.ExpressionMappings = []config.ExpressionMapping{{Table: "t", Column: "c", Expression: "1"}}
				return s
			}(),
			wantOK:  false,
			wantSub: "expression override",
		},
		{
			name: "shard injection engaged",
			stream: func() *Streamer {
				s := allClear()
				s.InjectShardColumn = ShardColumnSpec{Name: "shard_id", Value: "us-east-1"}
				return s
			}(),
			wantOK:  false,
			wantSub: "shard-column injection",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			ok, reason := rawCopyGate(rawCopyConfigForStreamer(tc.stream))
			if ok != tc.wantOK {
				t.Fatalf("rawCopyGate ok = %v (reason %q); want %v", ok, reason, tc.wantOK)
			}
			if !tc.wantOK && tc.wantSub != "" && !strings.Contains(reason, tc.wantSub) {
				t.Errorf("reason %q does not contain %q", reason, tc.wantSub)
			}
		})
	}
}

// TestIdentityProjection pins the per-table re-check: a generated column
// is still identity-safe (it is excluded from BOTH the source projection
// and the target column list by the same helper, so it never desyncs),
// while every OID/wire-format-sensitive type family (extension /
// verbatim / bit / geometry) routes the table to the IR path in v1.
func TestIdentityProjection(t *testing.T) {
	plain := func() *ir.Column { return col("c", false) } // ir.Varchar

	generated := func() *ir.Column {
		c := col("g", false)
		c.GeneratedExpr = "c || 'x'"
		c.GeneratedStored = true
		return c
	}

	typed := func(name string, ty ir.Type) *ir.Column {
		return &ir.Column{Name: name, Type: ty}
	}

	tests := []struct {
		name  string
		table *ir.Table
		want  bool
	}{
		{
			name:  "nil table",
			table: nil,
			want:  false,
		},
		{
			name:  "plain columns only",
			table: &ir.Table{Name: "t", Columns: []*ir.Column{plain(), plain()}},
			want:  true,
		},
		{
			name:  "generated column present is still identity (excluded both sides)",
			table: &ir.Table{Name: "t", Columns: []*ir.Column{plain(), generated()}},
			want:  true,
		},
		{
			name:  "extension type column excluded",
			table: &ir.Table{Name: "t", Columns: []*ir.Column{plain(), typed("v", ir.ExtensionType{Extension: "vector", Name: "vector"})}},
			want:  false,
		},
		{
			name:  "verbatim type column excluded",
			table: &ir.Table{Name: "t", Columns: []*ir.Column{typed("x", ir.VerbatimType{})}},
			want:  false,
		},
		{
			name:  "bit type column excluded",
			table: &ir.Table{Name: "t", Columns: []*ir.Column{typed("b", ir.Bit{})}},
			want:  false,
		},
		{
			name:  "geometry type column excluded",
			table: &ir.Table{Name: "t", Columns: []*ir.Column{typed("geom", ir.Geometry{})}},
			want:  false,
		},
		{
			name:  "array type column excluded (Bug 73/74 element-family matrix)",
			table: &ir.Table{Name: "t", Columns: []*ir.Column{typed("arr", ir.Array{})}},
			want:  false,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if got := identityProjection(tc.table); got != tc.want {
				t.Errorf("identityProjection = %v; want %v", got, tc.want)
			}
		})
	}
}

// TestFirstPKBound pins the single-column chunk-bound projection used by
// copyChunkRaw: an empty/nil PK tuple is an open bound (nil); a 1-wide
// tuple yields its lone element.
func TestFirstPKBound(t *testing.T) {
	if got := firstPKBound(nil); got != nil {
		t.Errorf("firstPKBound(nil) = %v; want nil", got)
	}
	if got := firstPKBound([]any{}); got != nil {
		t.Errorf("firstPKBound(empty) = %v; want nil", got)
	}
	if got := firstPKBound([]any{int64(7)}); got != int64(7) {
		t.Errorf("firstPKBound([7]) = %v; want 7", got)
	}
}
