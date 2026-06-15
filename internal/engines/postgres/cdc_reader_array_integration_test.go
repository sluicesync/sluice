//go:build integration

// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

// Bug 144 CDC array pin. Before this, the pgoutput path's oidToType had no
// array-OID cases, so a PG source table with ANY array column wedged the sync
// stream on the first array DML ("unsupported column type OID 1007/1009/1231…")
// — arrays cold-start (COPY) fine but could not be continuous-synced. This pin
// drives real pgoutput INSERTs of array columns across the Bug-74 element-family
// matrix (native / string-leaf / temporal) × shapes (1-D, multi-dim, NULL
// element) through the live replication stream and asserts the decoded ir.Row
// value: arrays resolve (no refusal), multi-dimensional arrays are NOT flattened
// (the Bug-74 core failure), and NULL elements survive as nil slots. numeric[]
// (the literal Bug-74 victim family) is exercised in every shape.

package postgres

import (
	"context"
	"fmt"
	"testing"
	"time"

	"sluicesync.dev/sluice/internal/ir"
)

// asAnySlice fails the test unless v is a non-nil []any, returning it.
func asAnySlice(t *testing.T, label string, v any) []any {
	t.Helper()
	s, ok := v.([]any)
	if !ok {
		t.Fatalf("%s: decoded value is %T (%#v); want []any", label, v, v)
	}
	return s
}

func TestCDCReader_Arrays_Bug144(t *testing.T) {
	dsn, cleanup := startPostgresForCDC(t)
	defer cleanup()

	const seedDDL = `
		CREATE TABLE arr (
			id   BIGINT PRIMARY KEY,
			i4   int4[],
			f8   float8[],
			bl   bool[],
			txt  text[],
			num  numeric[],
			uu   uuid[],
			ip   inet[],
			tstz timestamptz[],
			dt   date[]
		);
		ALTER TABLE arr REPLICA IDENTITY FULL;
	`
	applyPGSQL(t, dsn, seedDDL)

	eng := Engine{}
	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()

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

	// row 1: 1-D scalar across every family.
	// row 2: multi-dimensional (2-D) for the families that nest naturally.
	// row 3: 1-D with a NULL element (incl. numeric — the Bug-74 victim).
	const dml = `
		INSERT INTO arr (id,i4,f8,bl,txt,num,uu,ip,tstz,dt) VALUES
			(1,
			 '{1,2,3}', '{1.5,2.5}', '{t,f}', '{a,b}', '{1.50,2.25}',
			 '{11111111-1111-1111-1111-111111111111}', '{10.0.0.1}',
			 '{2026-01-01 00:00:00+00}', '{2026-01-02}');
		INSERT INTO arr (id,i4,f8,bl,txt,num,uu,ip,tstz,dt) VALUES
			(2,
			 '{{1,2},{3,4}}', '{1.5,2.5}', '{t}', '{{a,b},{c,d}}',
			 '{{1.1,2.2},{3.3,4.4}}',
			 '{11111111-1111-1111-1111-111111111111}', '{10.0.0.1}',
			 '{2026-01-01 00:00:00+00}', '{2026-01-02}');
		INSERT INTO arr (id,i4,f8,bl,txt,num,uu,ip,tstz,dt) VALUES
			(3,
			 '{1,NULL,3}', '{1.5,2.5}', '{t,f}', '{a,NULL,c}',
			 '{1.5,NULL,3.5}',
			 '{11111111-1111-1111-1111-111111111111}', '{10.0.0.1}',
			 '{2026-01-01 00:00:00+00}', '{2026-01-02}');
	`
	applyPGSQL(t, dsn, dml)

	got := drainChanges(t, ctx, changes, 3, 30*time.Second)
	if len(got) != 3 {
		if cdcRdr, ok := rdr.(*CDCReader); ok {
			if streamErr := cdcRdr.Err(); streamErr != nil {
				t.Fatalf("got %d changes; want 3 (stream error: %v)", len(got), streamErr)
			}
		}
		t.Fatalf("got %d changes; want 3 (arrays must not wedge the stream — Bug 144)", len(got))
	}

	rows := map[int64]ir.Row{}
	for _, c := range got {
		ins, ok := c.(ir.Insert)
		if !ok {
			t.Fatalf("change = %T; want ir.Insert", c)
		}
		id, _ := ins.Row["id"].(int64)
		rows[id] = ins.Row
	}

	// --- row 1: 1-D scalar — every family decodes to a flat []any. ---
	r1 := rows[1]
	if r1 == nil {
		t.Fatal("row id=1 missing")
	}
	for _, col := range []struct {
		name string
		n    int
	}{{"i4", 3}, {"f8", 2}, {"bl", 2}, {"txt", 2}, {"num", 2}, {"uu", 1}, {"ip", 1}, {"tstz", 1}, {"dt", 1}} {
		s := asAnySlice(t, "row1."+col.name, r1[col.name])
		if len(s) != col.n {
			t.Errorf("row1.%s: len=%d; want %d (%#v)", col.name, len(s), col.n, s)
		}
	}
	// numeric value fidelity (Bug-74 victim family), rendered to text — the
	// scale-preserving "1.50" (not "1.5") confirms the element decoded through
	// the Decimal path, not a lossy float.
	if got := fmt.Sprintf("%v", r1["num"]); got != "[1.50 2.25]" {
		t.Errorf("row1.num = %#v (rendered %q); want [1.50 2.25]", r1["num"], got)
	}

	// --- row 2: multi-dim must NOT flatten (the Bug-74 core). ---
	r2 := rows[2]
	if r2 == nil {
		t.Fatal("row id=2 missing")
	}
	for _, col := range []string{"i4", "txt", "num"} {
		outer := asAnySlice(t, "row2."+col, r2[col])
		if len(outer) != 2 {
			t.Fatalf("row2.%s: 2-D outer len=%d; want 2 (flattened? %#v)", col, len(outer), outer)
		}
		for i, inner := range outer {
			row := asAnySlice(t, fmt.Sprintf("row2.%s[%d]", col, i), inner)
			if len(row) != 2 {
				t.Errorf("row2.%s[%d]: inner len=%d; want 2 (2-D not preserved: %#v)", col, i, len(row), r2[col])
			}
		}
	}

	// --- row 3: NULL elements survive as nil slots. ---
	r3 := rows[3]
	if r3 == nil {
		t.Fatal("row id=3 missing")
	}
	for _, col := range []string{"i4", "txt", "num"} {
		s := asAnySlice(t, "row3."+col, r3[col])
		if len(s) != 3 {
			t.Fatalf("row3.%s: len=%d; want 3 (%#v)", col, len(s), s)
		}
		if s[1] != nil {
			t.Errorf("row3.%s[1] = %#v; want nil (NULL element dropped/altered)", col, s[1])
		}
		if s[0] == nil || s[2] == nil {
			t.Errorf("row3.%s: non-NULL elements became nil: %#v", col, s)
		}
	}
}
