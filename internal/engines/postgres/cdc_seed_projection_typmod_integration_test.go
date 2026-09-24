// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

//go:build integration

package postgres

import (
	"context"
	"reflect"
	"testing"
	"time"

	"sluicesync.dev/sluice/internal/ir"
	"sluicesync.dev/sluice/internal/pipeline"
)

// TestSeedAgreesWithPgoutputProjection_TypmodFamilies is the real-server
// leg of TestNormalizeForCDCComparison_PG_SeedAgreesWithBoundaryProjection
// (GC-36): the unit pin encodes each typmod itself, this one lets Postgres
// supply BOTH sides — the SchemaReader's read of a table carrying every
// typmod-capable family declared and bare, scalar and array, and the
// pgoutput RelationMessage the live CDC reader projects for the same table.
// So a wrong belief about what the wire sends (a typmod encoding, "an array
// column's typmod is its element's") cannot green both.
//
// Scalars must agree RAW (the forwarded ADD COLUMN is emitted from the raw
// projection — GC-36's NUMERIC(0,0)); every column must agree through the
// comparison lens, or the first boundary phantom-alters.
func TestSeedAgreesWithPgoutputProjection_TypmodFamilies(t *testing.T) {
	dsn, cleanup := newSharedPGDB(t, "typmod_parity")
	defer cleanup()
	applyPGSQL(t, dsn, `
		CREATE TABLE w (
			id INT PRIMARY KEY,
			n_decl numeric(10,2), n_bare numeric,
			vc_decl varchar(10), vc_bare varchar,
			bp_decl char(10), bp_bare bpchar,
			bit_decl bit(3), vb_decl varbit(3), vb_bare varbit,
			t_decl time(3), t_bare time,
			ttz_decl timetz(3), ttz_bare timetz,
			ts_decl timestamp(3), ts_bare timestamp,
			tstz_decl timestamptz(3), tstz_bare timestamptz,
			iv_decl interval(3), iv_bare interval,
			an_decl numeric(10,2)[], an_bare numeric[],
			avc_decl varchar(10)[], avc_bare varchar[],
			abp_decl char(10)[], abp_bare bpchar[],
			at_decl time(3)[], at_bare time[],
			attz_decl timetz(3)[], attz_bare timetz[],
			ats_decl timestamp(3)[], ats_bare timestamp[],
			atstz_decl timestamptz(3)[], atstz_bare timestamptz[]
		);
	`)

	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()
	eng := Engine{}

	sr, err := eng.OpenSchemaReader(ctx, dsn)
	if err != nil {
		t.Fatalf("OpenSchemaReader: %v", err)
	}
	schema, err := sr.ReadSchema(ctx)
	if c, ok := sr.(interface{ Close() error }); ok {
		_ = c.Close()
	}
	if err != nil {
		t.Fatalf("ReadSchema: %v", err)
	}
	var seed *ir.Table
	for _, tbl := range schema.Tables {
		if tbl.Name == "w" {
			seed = tbl
		}
	}
	if seed == nil {
		t.Fatal("schema reader did not return table w")
	}

	rdr, err := eng.OpenCDCReader(ctx, dsn)
	if err != nil {
		t.Fatalf("OpenCDCReader: %v", err)
	}
	defer func() {
		if c, ok := rdr.(interface{ Close() error }); ok {
			_ = c.Close()
		}
	}()
	changes, err := rdr.StreamChanges(ctx, ir.Position{})
	if err != nil {
		t.Fatalf("StreamChanges: %v", err)
	}
	time.Sleep(200 * time.Millisecond)
	applyPGSQL(t, dsn, `INSERT INTO w (id) VALUES (1)`)

	var proj *ir.Table
	deadline := time.After(60 * time.Second)
	for proj == nil {
		select {
		case c, ok := <-changes:
			if !ok {
				t.Fatal("change stream closed before the relation snapshot")
			}
			if s, isSnap := c.(ir.SchemaSnapshot); isSnap && s.Table == "w" {
				proj = s.IR
			}
		case <-deadline:
			t.Fatal("no SchemaSnapshot for w within 60s")
		}
	}

	if len(proj.Columns) != len(seed.Columns) || len(seed.Columns) < 34 {
		t.Fatalf("seed has %d columns, projection %d; want the same 34 (id + 19 scalar + 14 array shapes; anti-vacuity)", len(seed.Columns), len(proj.Columns))
	}
	for i, sc := range seed.Columns {
		pc := proj.Columns[i]
		if sc.Name != pc.Name {
			t.Fatalf("ordinal %d: seed %q vs projection %q", i, sc.Name, pc.Name)
		}
		if _, isArray := sc.Type.(ir.Array); isArray {
			continue
		}
		if !reflect.DeepEqual(sc.Type, pc.Type) {
			t.Errorf("%s: raw split — schema reader %#v, pgoutput projection %#v", sc.Name, sc.Type, pc.Type)
		}
	}
	// Named in the unit pin's words: the unconstrained numeric is the GC-36
	// cell, and it must come off the wire as Unconstrained, never {0,0}.
	for _, c := range proj.Columns {
		if c.Name == "n_bare" && c.Type != (ir.Decimal{Unconstrained: true}) {
			t.Errorf("n_bare projected as %#v; want ir.Decimal{Unconstrained:true}", c.Type)
		}
	}

	// Classified ONE COLUMN AT A TIME: ClassifyShape's altered-column diff
	// reports an alter only when exactly one column moved (two or more read
	// as no alter at all), so a whole-table compare with several phantom
	// columns would pass vacuously — measured: it did, under the
	// array-lens mutant.
	normSeed, normProj := eng.NormalizeForCDCComparison(seed), eng.NormalizeForCDCComparison(proj)
	for i := 1; i < len(normSeed.Columns); i++ {
		pair := func(tb *ir.Table) *ir.Table {
			return &ir.Table{Schema: tb.Schema, Name: tb.Name, Columns: []*ir.Column{tb.Columns[0], tb.Columns[i]}}
		}
		shape, err := pipeline.ClassifyShape(pair(normSeed), pair(normProj))
		if err != nil || shape.Kind != pipeline.ShapeKindNone {
			t.Errorf("%s: seed vs live projection = %v (err %v); want None — a phantom alter at the first boundary",
				normSeed.Columns[i].Name, shape.Kind, err)
		}
	}
}
