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
	"log/slog"
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

// TestGeneratedStorageClassExpr pins the version gate on the catalog
// read: the real column on PG 12+, a constant below it.
func TestGeneratedStorageClassExpr(t *testing.T) {
	if got := generatedStorageClassExpr(pgVersionGeneratedColumns); !strings.Contains(got, "attgenerated") {
		t.Errorf("PG 12 expression = %q; want the attgenerated read", got)
	}
	if got := generatedStorageClassExpr(pgVersionGeneratedColumns - 1); got != "''" {
		t.Errorf("PG 11 expression = %q; want the constant ''", got)
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
