// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package postgres

import (
	"strings"
	"testing"

	"sluicesync.dev/sluice/internal/ir"
)

func rawCopyTestTable() *ir.Table {
	gen := &ir.Column{Name: "full_name", Type: ir.Varchar{Length: 255}, GeneratedExpr: "first || ' ' || last", GeneratedStored: true}
	return &ir.Table{
		Name: "users",
		Columns: []*ir.Column{
			{Name: "id", Type: ir.Integer{Width: 64}},
			{Name: "first", Type: ir.Varchar{Length: 64}},
			{Name: "last", Type: ir.Varchar{Length: 64}},
			gen,
		},
	}
}

// TestBuildRawCopyToStmt_ProjectionExcludesGenerated is the CRUX
// invariant pin (ADR-0078): the exporter projects an explicit SELECT of
// the source-readable columns — generated columns EXCLUDED — never a bare
// `COPY tbl TO STDOUT`. A regression to the bare form would include the
// generated column and desync the importer's column list.
func TestBuildRawCopyToStmt_ProjectionExcludesGenerated(t *testing.T) {
	stmt, err := buildRawCopyToStmt("public", rawCopyTestTable(), nil, ir.RawCopyText)
	if err != nil {
		t.Fatalf("buildRawCopyToStmt: %v", err)
	}
	if !strings.HasPrefix(stmt, "COPY (SELECT ") {
		t.Fatalf("exporter must use an explicit SELECT projection, got: %s", stmt)
	}
	for _, want := range []string{`"id"`, `"first"`, `"last"`} {
		if !strings.Contains(stmt, want) {
			t.Errorf("projection missing %s: %s", want, stmt)
		}
	}
	if strings.Contains(stmt, `"full_name"`) {
		t.Errorf("generated column must be excluded from the export projection: %s", stmt)
	}
	if !strings.Contains(stmt, `FROM "public"."users"`) {
		t.Errorf("missing schema-qualified table ref: %s", stmt)
	}
	if !strings.HasSuffix(stmt, "TO STDOUT WITH (FORMAT text)") {
		t.Errorf("missing TO STDOUT + format clause: %s", stmt)
	}
}

// TestBuildRawCopyToStmt_ChunkPredicate pins the chunk WHERE bounds:
// (pk > lower AND pk <= upper), with open ends for nil bounds.
func TestBuildRawCopyToStmt_ChunkPredicate(t *testing.T) {
	tests := []struct {
		name  string
		chunk *ir.RawCopyChunk
		want  string // substring expected in the WHERE, "" => no WHERE
	}{
		{"both bounds", &ir.RawCopyChunk{PKColumn: "id", LowerPK: int64(10), UpperPK: int64(20)}, `WHERE "id" > 10 AND "id" <= 20`},
		{"lower only", &ir.RawCopyChunk{PKColumn: "id", LowerPK: int64(10)}, `WHERE "id" > 10`},
		{"upper only", &ir.RawCopyChunk{PKColumn: "id", UpperPK: int64(20)}, `WHERE "id" <= 20`},
		{"open both", &ir.RawCopyChunk{PKColumn: "id"}, ""},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			stmt, err := buildRawCopyToStmt("public", rawCopyTestTable(), tc.chunk, ir.RawCopyBinary)
			if err != nil {
				t.Fatalf("buildRawCopyToStmt: %v", err)
			}
			if tc.want == "" {
				if strings.Contains(stmt, "WHERE") {
					t.Errorf("open-both chunk must have no WHERE: %s", stmt)
				}
			} else if !strings.Contains(stmt, tc.want) {
				t.Errorf("want predicate %q in %s", tc.want, stmt)
			}
			if !strings.Contains(stmt, "WITH (FORMAT binary)") {
				t.Errorf("binary format clause missing: %s", stmt)
			}
		})
	}
}

// TestBuildRawCopyToStmt_RejectsNonIntegerBound pins the loud refusal: a
// non-integer chunk bound is a programming error (the gate routes
// non-integer PKs away from the raw lane), never a silently malformed
// predicate.
func TestBuildRawCopyToStmt_RejectsNonIntegerBound(t *testing.T) {
	chunk := &ir.RawCopyChunk{PKColumn: "id", LowerPK: "not-an-int"}
	if _, err := buildRawCopyToStmt("public", rawCopyTestTable(), chunk, ir.RawCopyText); err == nil {
		t.Fatal("expected error for non-integer bound")
	}
}

// TestBuildRawCopyFromStmt_ColumnListMatchesExportProjection pins the
// other half of the CRUX invariant: the importer's COPY column list is
// the non-generated columns — the SAME set the exporter projects — so the
// two line up by construction.
func TestBuildRawCopyFromStmt_ColumnListMatchesExportProjection(t *testing.T) {
	stmt := buildRawCopyFromStmt("public", rawCopyTestTable(), ir.RawCopyText)
	if !strings.HasPrefix(stmt, `COPY "public"."users" (`) {
		t.Fatalf("unexpected target COPY prefix: %s", stmt)
	}
	for _, want := range []string{`"id"`, `"first"`, `"last"`} {
		if !strings.Contains(stmt, want) {
			t.Errorf("import column list missing %s: %s", want, stmt)
		}
	}
	if strings.Contains(stmt, `"full_name"`) {
		t.Errorf("generated column must be excluded from the import column list: %s", stmt)
	}
	if !strings.HasSuffix(stmt, "FROM STDIN WITH (FORMAT text)") {
		t.Errorf("missing FROM STDIN + format clause: %s", stmt)
	}
}

// TestRawCopyFormatString pins the COPY format token rendering.
func TestRawCopyFormatString(t *testing.T) {
	if got := ir.RawCopyText.String(); got != "text" {
		t.Errorf("RawCopyText.String() = %q; want text", got)
	}
	if got := ir.RawCopyBinary.String(); got != "binary" {
		t.Errorf("RawCopyBinary.String() = %q; want binary", got)
	}
}
