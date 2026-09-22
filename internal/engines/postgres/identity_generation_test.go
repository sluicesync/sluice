// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

// GC-3 (gap census 2026-09-22 S4 + S5) unit pins: the reader's
// ALWAYS / BY DEFAULT classification and its refusal on an identity
// column with no backing sequence, the emitter's sequence-options clause
// across every option, the once-per-run IDENTITY-ALWAYS-DOWNGRADED WARN,
// and the restore statement.

package postgres

import (
	"context"
	"math"
	"strings"
	"testing"

	"sluicesync.dev/sluice/internal/ir"
)

func identityRow(gen string) columnRow {
	return columnRow{
		tableName: "t", colName: "id", isNullable: "NO",
		dataType: "bigint", udtName: "int8", isIdentity: "YES",
		isGenerated: "NEVER", attTypmod: -1, identityGeneration: gen,
	}
}

// TestColumnFromRow_IdentityGeneration: ALWAYS and BY DEFAULT both land
// on Column.Identity with the sequence options from the identity map;
// before GC-3 identity_generation was never read and the two were
// indistinguishable.
func TestColumnFromRow_IdentityGeneration(t *testing.T) {
	r := &SchemaReader{schema: "public"}
	seqOpts := ir.IdentityOptions{Start: 3, Increment: 10, MinValue: 1, MaxValue: 1000, Cache: 50, Cycle: true}
	lk := columnLookups{identitySeqs: map[pgSequenceOwner]ir.IdentityOptions{{table: "t", column: "id"}: seqOpts}}

	for gen, wantAlways := range map[string]bool{"ALWAYS": true, "BY DEFAULT": false} {
		col, err := r.columnFromRow(identityRow(gen), lk)
		if err != nil {
			t.Fatalf("%s: columnFromRow: %v", gen, err)
		}
		intT, ok := col.Type.(ir.Integer)
		if !ok || !intT.AutoIncrement {
			t.Fatalf("%s: Type = %#v; want an AutoIncrement Integer (the pre-GC-3 carry must be untouched)", gen, col.Type)
		}
		if col.Identity == nil {
			t.Fatalf("%s: Identity nil", gen)
		}
		want := seqOpts
		want.Always = wantAlways
		if *col.Identity != want {
			t.Errorf("%s: Identity = %+v; want %+v", gen, *col.Identity, want)
		}
	}

	t.Run("an identity column with no backing sequence is refused", func(t *testing.T) {
		_, err := r.columnFromRow(identityRow("ALWAYS"), columnLookups{})
		if err == nil || !strings.Contains(err.Error(), "no identity-backed sequence") {
			t.Fatalf("err = %v; want the loud no-backing-sequence refusal", err)
		}
	})

	t.Run("a plain column carries no Identity", func(t *testing.T) {
		row := identityRow("")
		row.isIdentity = "NO"
		col, err := r.columnFromRow(row, columnLookups{})
		if err != nil {
			t.Fatalf("columnFromRow: %v", err)
		}
		if col.Identity != nil {
			t.Errorf("Identity = %+v; want nil", *col.Identity)
		}
	})
}

// TestIdentityOptionsClause pins the S5 emit across every option and both
// integer widths: factory defaults render nothing, each non-default
// option renders, a descending identity renders every option that
// differs, and a narrowing type-override omits the source ceiling.
func TestIdentityOptionsClause(t *testing.T) {
	col := func(width int8, id *ir.IdentityOptions) *ir.Column {
		return &ir.Column{Name: "id", Type: ir.Integer{Width: width, AutoIncrement: true}, Identity: id}
	}
	cases := []struct {
		name string
		col  *ir.Column
		want string
	}{
		{"nil Identity (MySQL source / pre-field manifest)", col(64, nil), ""},
		{"bigint factory defaults", col(64, &ir.IdentityOptions{Start: 1, Increment: 1, MinValue: 1, MaxValue: math.MaxInt64, Cache: 1}), ""},
		{"integer factory defaults", col(32, &ir.IdentityOptions{Start: 1, Increment: 1, MinValue: 1, MaxValue: math.MaxInt32, Cache: 1}), ""},
		{"sharded scheme", col(64, &ir.IdentityOptions{Start: 3, Increment: 10, MinValue: 1, MaxValue: math.MaxInt64, Cache: 1}), " (INCREMENT BY 10 START WITH 3)"},
		{"every option", col(64, &ir.IdentityOptions{Always: true, Start: 5, Increment: 2, MinValue: 0, MaxValue: 1000, Cache: 50, Cycle: true}), " (INCREMENT BY 2 MINVALUE 0 MAXVALUE 1000 START WITH 5 CACHE 50 CYCLE)"},
		{"descending", col(64, &ir.IdentityOptions{Start: -1, Increment: -1, MinValue: math.MinInt64, MaxValue: -1, Cache: 1}), " (INCREMENT BY -1 MINVALUE -9223372036854775808 MAXVALUE -1 START WITH -1)"},
		// The source was bigint (ceiling MaxInt64); a --type-override
		// narrowed the emitted type to integer. The bigint ceiling is
		// the source's factory default, so it is omitted rather than
		// rendered as a MAXVALUE the integer column cannot hold — but
		// it is the SOURCE type's ceiling, not the emitted one, so it
		// differs from integer's and renders. That is the honest
		// outcome: CREATE TABLE refuses loudly instead of silently
		// re-capping the sequence.
		{"narrowing override keeps the explicit source ceiling", col(32, &ir.IdentityOptions{Start: 1, Increment: 1, MinValue: 1, MaxValue: math.MaxInt64, Cache: 1}), " (MAXVALUE 9223372036854775807)"},
		{"not an identity", &ir.Column{Name: "n", Type: ir.Integer{Width: 32}, Identity: &ir.IdentityOptions{Increment: 7}}, ""},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := identityOptionsClause(tc.col); got != tc.want {
				t.Errorf("identityOptionsClause = %q; want %q", got, tc.want)
			}
		})
	}

	// Through emitColumnDef: the clause sits directly after AS IDENTITY.
	got, err := emitColumnDef(&ir.Table{Name: "t"}, col(64, &ir.IdentityOptions{Start: 3, Increment: 10, MinValue: 1, MaxValue: math.MaxInt64, Cache: 1}), emitOpts{})
	if err != nil {
		t.Fatalf("emitColumnDef: %v", err)
	}
	if want := `"id" BIGINT GENERATED BY DEFAULT AS IDENTITY (INCREMENT BY 10 START WITH 3) NOT NULL`; got != want {
		t.Errorf("emitColumnDef = %q; want %q", got, want)
	}
}

// TestWarnIdentityAlwaysDowngraded pins the once-per-run WARN: one line,
// carrying the marker, naming every ALWAYS column and none of the BY
// DEFAULT ones — and no line at all when there is nothing to say.
func TestWarnIdentityAlwaysDowngraded(t *testing.T) {
	always := &ir.IdentityOptions{Always: true, Start: 1, Increment: 1, MinValue: 1, MaxValue: math.MaxInt64, Cache: 1}
	byDefault := &ir.IdentityOptions{Start: 1, Increment: 1, MinValue: 1, MaxValue: math.MaxInt64, Cache: 1}
	s := &ir.Schema{Tables: []*ir.Table{
		{Name: "orders", Columns: []*ir.Column{{Name: "id", Type: ir.Integer{Width: 64, AutoIncrement: true}, Identity: always}}},
		{Name: "events", Columns: []*ir.Column{{Name: "seq", Type: ir.Integer{Width: 64, AutoIncrement: true}, Identity: byDefault}}},
		{Name: "audit", Columns: []*ir.Column{{Name: "n", Type: ir.Integer{Width: 32, AutoIncrement: true}, Identity: always}}},
	}}

	buf := captureGeneratedWarns(t)
	// PreviewDDL is the DB-free path that renders the schema; the apply
	// path calls the same helper at the same point.
	if _, err := (&SchemaWriter{schema: "public"}).PreviewDDL(context.Background(), s); err != nil {
		t.Fatalf("PreviewDDL: %v", err)
	}
	out := buf.String()
	if n := strings.Count(out, identityAlwaysDowngradedMarker); n != 1 {
		t.Fatalf("marker appears %d times; want exactly 1 (one WARN per run):\n%s", n, out)
	}
	for _, want := range []string{"orders.id", "audit.n"} {
		if !strings.Contains(out, want) {
			t.Errorf("WARN does not name %s:\n%s", want, out)
		}
	}
	if strings.Contains(out, "events.seq") {
		t.Errorf("WARN names the BY DEFAULT column events.seq:\n%s", out)
	}

	buf2 := captureGeneratedWarns(t)
	warnIdentityAlwaysDowngraded(context.Background(), &ir.Schema{Tables: s.Tables[1:2]})
	if strings.Contains(buf2.String(), identityAlwaysDowngradedMarker) {
		t.Errorf("WARN fired for a schema with only BY DEFAULT identities:\n%s", buf2.String())
	}
}
