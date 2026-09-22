// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

// The generated-column STORAGE-CLASS axis (gap census 2026-09-22 S1/D5,
// GC-6): the reader classifies pg_attribute.attgenerated into
// ir.Column.GeneratedStored, and the writer renders STORED / VIRTUAL from
// it under the target-version gate. Both halves of the class are pinned
// here — every attgenerated value the catalog can carry on the read side,
// and every {storage class} × {target supports VIRTUAL} × {indexed} cell
// on the emit side — because a green test on one cell says nothing about
// the others (the Bug 74 lesson applied to a two-valued family).

package postgres

import (
	"errors"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"sluicesync.dev/sluice/internal/ir"
	"sluicesync.dev/sluice/internal/logcapture"
)

// TestColumnFromRow_GeneratedStorageClass pins the reader's
// classification of attgenerated: 's' → STORED, 'v' → VIRTUAL, and the
// empty-string fallback (a pre-12 server, where the select expression is a
// constant empty string) → STORED. Before GC-6 every generated column read as
// STORED regardless of what the catalog said.
func TestColumnFromRow_GeneratedStorageClass(t *testing.T) {
	cases := []struct {
		name         string
		attGenerated string
		wantStored   bool
	}{
		{"attgenerated 's' is STORED", pgAttGeneratedStored, true},
		{"attgenerated 'v' is VIRTUAL", pgAttGeneratedVirtual, false},
		// The defaulting branch. Unreachable for a real generated column
		// on PG 12+ (the catalog always writes 's' or 'v'), and the only
		// spelling below 12 — where no generated column exists to reach
		// it. STORED is the choice that can never mint a VIRTUAL column
		// the source did not declare.
		{"attgenerated '' (version-gated out) defaults to STORED", "", true},
	}
	r := &SchemaReader{schema: "public"}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			col, err := r.columnFromRow(columnRow{
				tableName:    "t",
				colName:      "g",
				isNullable:   "YES",
				dataType:     "integer",
				udtName:      "int4",
				isIdentity:   "NO",
				isGenerated:  "ALWAYS",
				genExpr:      "(c * 2)",
				attTypmod:    -1,
				attGenerated: tc.attGenerated,
			}, columnLookups{})
			if err != nil {
				t.Fatalf("columnFromRow: %v", err)
			}
			if !col.IsGenerated() {
				t.Fatalf("column read as not generated: %+v", col)
			}
			if col.GeneratedStored != tc.wantStored {
				t.Errorf("GeneratedStored = %v; want %v for attgenerated=%q", col.GeneratedStored, tc.wantStored, tc.attGenerated)
			}
		})
	}

	// Anti-vacuity: a plain column must not become generated whatever
	// attgenerated says — the storage class rides ONLY on a generated
	// column.
	t.Run("a plain column ignores attgenerated", func(t *testing.T) {
		col, err := r.columnFromRow(columnRow{
			tableName: "t", colName: "c", isNullable: "YES",
			dataType: "integer", udtName: "int4", isIdentity: "NO",
			isGenerated: "NEVER", attTypmod: -1, attGenerated: pgAttGeneratedVirtual,
		}, columnLookups{})
		if err != nil {
			t.Fatalf("columnFromRow: %v", err)
		}
		if col.IsGenerated() || col.GeneratedStored {
			t.Errorf("plain column came back generated: %+v", col)
		}
	})
}

// TestSchemaReaderFloorIsDocumented pins the reader's stated floor to the
// operator doc: the column read requires PG 12 catalog columns
// unconditionally, and docs/production-readiness.md must say so.
func TestSchemaReaderFloorIsDocumented(t *testing.T) {
	if pgVersionSchemaReaderFloor != 120000 {
		t.Fatalf("pgVersionSchemaReaderFloor = %d; the column read references PG 12 catalog columns", pgVersionSchemaReaderFloor)
	}
	doc, err := os.ReadFile(filepath.Join("..", "..", "..", "docs", "production-readiness.md"))
	if err != nil {
		t.Fatalf("read doc: %v", err)
	}
	if !strings.Contains(string(doc), "PostgreSQL 12 or newer") {
		t.Error("docs/production-readiness.md does not state the PostgreSQL 12 schema-read floor")
	}
}

// TestVirtualBlockedBy pins the static half of the PG 18 VIRTUAL
// predicate across every shape PostgreSQL refuses: plain index,
// expression index, primary key, unique index, foreign key, and each
// user-defined column type — and the two shapes it must NOT block (a
// plain built-in column; an expression index over a DIFFERENT column).
func TestVirtualBlockedBy(t *testing.T) {
	gen := func(typ ir.Type) *ir.Column {
		return &ir.Column{Name: "v", Type: typ, Nullable: true, GeneratedExpr: "(c * 3)"}
	}
	intCol := gen(ir.Integer{Width: 32})
	cases := []struct {
		name  string
		table *ir.Table
		col   *ir.Column
		want  string
		// skipEmit: the type needs a real definition / enabled extension to
		// render; the predicate is what this cell grades.
		skipEmit bool
	}{
		{"plain column, nothing on it", &ir.Table{Name: "t"}, intCol, "", false},
		{"expression index over another column", &ir.Table{Name: "t", Indexes: []*ir.Index{{Columns: []ir.IndexColumn{{Expression: "(vv + 1)"}}}}}, intCol, "", false},
		{"plain index", &ir.Table{Name: "t", Indexes: []*ir.Index{{Columns: []ir.IndexColumn{{Column: "v"}}}}}, intCol, "indexed", false},
		{"expression index", &ir.Table{Name: "t", Indexes: []*ir.Index{{Columns: []ir.IndexColumn{{Expression: "(v + 1)"}}}}}, intCol, "indexed", false},
		{"expression index, upper-cased identifier", &ir.Table{Name: "t", Indexes: []*ir.Index{{Columns: []ir.IndexColumn{{Expression: "UPPER(V)"}}}}}, intCol, "indexed", false},
		{"unique index", &ir.Table{Name: "t", Indexes: []*ir.Index{{Unique: true, Columns: []ir.IndexColumn{{Column: "v"}}}}}, intCol, "unique index/constraint member", false},
		{"primary key", &ir.Table{Name: "t", PrimaryKey: &ir.Index{Columns: []ir.IndexColumn{{Column: "v"}}}}, intCol, "primary key member", false},
		{"foreign key", &ir.Table{Name: "t", ForeignKeys: []*ir.ForeignKey{{Columns: []string{"v"}, ReferencedTable: "p"}}}, intCol, "foreign key member", false},
		{"enum type", &ir.Table{Name: "t"}, gen(ir.Enum{Values: []string{"a"}}), "user-defined column type", false},
		{"domain type", &ir.Table{Name: "t"}, gen(ir.Domain{Name: "posint", BaseType: ir.Integer{Width: 32}}), "user-defined column type", false},
		{"extension type", &ir.Table{Name: "t"}, gen(ir.ExtensionType{}), "user-defined column type", true},
		{"verbatim type", &ir.Table{Name: "t"}, gen(ir.VerbatimType{}), "user-defined column type", true},
		{"nil table", nil, intCol, "", false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := virtualBlockedBy(tc.table, tc.col); got != tc.want {
				t.Errorf("virtualBlockedBy = %q; want %q", got, tc.want)
			}
			if tc.skipEmit {
				return
			}
			// The predicate is what the emitter promotes on: every
			// blocked shape must render STORED on a VIRTUAL-capable target.
			got, err := emitColumnDef(tc.table, tc.col, emitOpts{VirtualGeneratedColumns: true})
			if err != nil {
				t.Fatalf("emitColumnDef: %v", err)
			}
			wantTail := ") VIRTUAL"
			if tc.want != "" {
				wantTail = ") STORED"
			}
			if _, isEnum := tc.col.Type.(ir.Enum); isEnum {
				wantTail = ") STORED" // Bug 25 renders enum generated columns as TEXT; still STORED
			}
			if !strings.HasSuffix(got, wantTail) {
				t.Errorf("emitted %q; want the %q tail", got, wantTail)
			}
		})
	}
}

// TestIsVirtualGeneratedRefusal pins the server-string matcher behind
// the CREATE TABLE retry: every listed refusal matches, an unrelated
// error does not, and promoteVirtualColumns copies rather than mutates.
func TestIsVirtualGeneratedRefusal(t *testing.T) {
	for _, s := range pgVirtualGeneratedRefusals {
		if _, ok := isVirtualGeneratedRefusal(errors.New("ERROR: " + s + " (SQLSTATE 0A000)")); !ok {
			t.Errorf("%q not recognised", s)
		}
	}
	if _, ok := isVirtualGeneratedRefusal(errors.New("ERROR: relation \"t\" already exists")); ok {
		t.Error("an unrelated error matched the VIRTUAL refusal set")
	}
	if _, ok := isVirtualGeneratedRefusal(nil); ok {
		t.Error("nil matched")
	}

	tbl := &ir.Table{Name: "t", Columns: []*ir.Column{
		{Name: "c", Type: ir.Integer{Width: 32}},
		{Name: "v", Type: ir.Integer{Width: 32}, GeneratedExpr: "(c*2)", GeneratedStored: false},
		{Name: "s", Type: ir.Integer{Width: 32}, GeneratedExpr: "(c*3)", GeneratedStored: true},
	}}
	promoted, had := promoteVirtualColumns(tbl)
	if !had {
		t.Fatal("promoteVirtualColumns reported no VIRTUAL column")
	}
	if !promoted.Columns[1].GeneratedStored || tbl.Columns[1].GeneratedStored {
		t.Errorf("promotion: copy STORED=%v, input STORED=%v; want true/false (input never mutated)", promoted.Columns[1].GeneratedStored, tbl.Columns[1].GeneratedStored)
	}
	if _, had := promoteVirtualColumns(&ir.Table{Name: "t", Columns: tbl.Columns[:1]}); had {
		t.Error("a table with no VIRTUAL column reported one")
	}
}

// captureGeneratedWarns installs a WARN-level JSON slog handler for the
// test's duration so the promotion WARNs can be asserted on.
func captureGeneratedWarns(t *testing.T) *logcapture.Buffer {
	t.Helper()
	buf := &logcapture.Buffer{}
	prev := slog.Default()
	slog.SetDefault(slog.New(slog.NewJSONHandler(buf, &slog.HandlerOptions{Level: slog.LevelWarn})))
	t.Cleanup(func() { slog.SetDefault(prev) })
	return buf
}

// TestEmitColumnDef_GeneratedStorageClassMatrix pins the emit side:
// {STORED, VIRTUAL} × {target accepts VIRTUAL, target does not} ×
// {indexed, not indexed}. Only two cells render VIRTUAL — an unindexed
// VIRTUAL column on a VIRTUAL-capable target — and every promotion
// carries the grep-stable marker; a STORED source never WARNs.
func TestEmitColumnDef_GeneratedStorageClassMatrix(t *testing.T) {
	col := func(stored bool) *ir.Column {
		return &ir.Column{
			Name: "g", Type: ir.Integer{Width: 32}, Nullable: true,
			GeneratedExpr: "(c * 2)", GeneratedStored: stored,
		}
	}
	plain := &ir.Table{Name: "t"}
	indexed := &ir.Table{Name: "t", Indexes: []*ir.Index{{Name: "t_g_idx", Columns: []ir.IndexColumn{{Column: "g"}}}}}
	pkIndexed := &ir.Table{Name: "t", PrimaryKey: &ir.Index{Columns: []ir.IndexColumn{{Column: "g"}}}}

	cases := []struct {
		name     string
		table    *ir.Table
		stored   bool
		virtual  bool // opts.VirtualGeneratedColumns
		wantTail string
		wantWarn bool
	}{
		{"STORED, pre-18 target", plain, true, false, ") STORED", false},
		{"STORED, PG 18 target", plain, true, true, ") STORED", false},
		{"STORED indexed, PG 18 target", indexed, true, true, ") STORED", false},
		{"VIRTUAL, pre-18 target: promoted", plain, false, false, ") STORED", true},
		{"VIRTUAL, PG 18 target: faithful", plain, false, true, ") VIRTUAL", false},
		{"VIRTUAL indexed, PG 18 target: promoted (PG 18 cannot index it)", indexed, false, true, ") STORED", true},
		{"VIRTUAL in the primary key, PG 18 target: promoted", pkIndexed, false, true, ") STORED", true},
		{"VIRTUAL indexed, pre-18 target: promoted (version reason first)", indexed, false, false, ") STORED", true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			buf := captureGeneratedWarns(t)
			got, err := emitColumnDef(tc.table, col(tc.stored), emitOpts{VirtualGeneratedColumns: tc.virtual})
			if err != nil {
				t.Fatalf("emitColumnDef: %v", err)
			}
			if !strings.HasSuffix(got, tc.wantTail) {
				t.Errorf("emitted %q; want the %q tail", got, tc.wantTail)
			}
			if strings.Contains(got, "STORED") && strings.Contains(got, "VIRTUAL") {
				t.Errorf("emitted both storage keywords: %q", got)
			}
			warned := strings.Contains(buf.String(), generatedVirtualPromotedMarker)
			if warned != tc.wantWarn {
				t.Errorf("promotion WARN present = %v; want %v (log: %s)", warned, tc.wantWarn, buf.String())
			}
		})
	}
}

// TestColumnIsIndexed pins the index-membership probe the PG 18
// promotion keys on, primary key included and nil-safe.
func TestColumnIsIndexed(t *testing.T) {
	tbl := &ir.Table{
		Name:       "t",
		PrimaryKey: &ir.Index{Columns: []ir.IndexColumn{{Column: "id"}}},
		Indexes:    []*ir.Index{nil, {Columns: []ir.IndexColumn{{Column: "a"}, {Column: "b"}}}},
	}
	for col, want := range map[string]bool{"id": true, "a": true, "b": true, "c": false} {
		if got := columnIsIndexed(tbl, col); got != want {
			t.Errorf("columnIsIndexed(%q) = %v; want %v", col, got, want)
		}
	}
	if columnIsIndexed(nil, "id") {
		t.Error("nil table reported an indexed column")
	}
}
