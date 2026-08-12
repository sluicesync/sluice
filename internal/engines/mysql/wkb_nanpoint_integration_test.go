//go:build integration

// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package mysql

import (
	"context"
	"encoding/binary"
	"encoding/hex"
	"fmt"
	"math"
	"testing"
	"time"

	"sluicesync.dev/sluice/internal/ir"
	"sluicesync.dev/sluice/internal/sluicecode"
)

// The SPAT-1 real-server leg (audit 2026-08-11). The unit matrix
// (wkb_scan_test.go) owns scan breadth; this leg proves, per family, against
// a real MySQL:
//
//   - the PREMISE the client-side refusal is load-bearing on: MySQL still
//     ACCEPTS a raw NaN-coordinate point silently and produces a value it
//     cannot itself create (ST_AsText NULL) — if MySQL ever starts refusing,
//     this pin fires and the scan can be reconsidered;
//   - the WIRING: a NaN-bearing value refuses through the real batched write
//     core with SLUICE-E-VALUE-UNREPRESENTABLE and lands NOTHING;
//   - the DICHOTOMY per family × {clean, NaN, EMPTY}: every cell is either
//     faithful or loud, never silent-poison. EMPTY differs by family — POINT
//     EMPTY is WKB NaN/NaN (our refusal), empty LINESTRING/POLYGON/MULTI*
//     draw the server's own 1416, and GEOMETRYCOLLECTION EMPTY is a real,
//     storable value that must keep landing (over-refusal control).

// wkbFamilies is the 7-family matrix: MySQL column type, a clean WKB value,
// and a NaN-poisoned sibling. Builders come from wkb_scan_test.go.
var wkbFamilies = []struct {
	name    string
	colType string
	clean   []byte
	poison  []byte
	empty   []byte // count-0 form for counted families; NaN/NaN for Point; nil = no arm
}{
	{
		"Point", "POINT",
		point(true, 1, 2),
		point(true, math.NaN(), math.NaN()),
		point(true, math.NaN(), math.NaN()), // POINT EMPTY *is* the NaN form
	},
	{
		"LineString", "LINESTRING",
		newWKBW(true).header(2).u32(2).f64(0).f64(0).f64(1).f64(1).buf,
		newWKBW(true).header(2).u32(2).f64(0).f64(0).f64(math.NaN()).f64(1).buf,
		newWKBW(true).header(2).u32(0).buf,
	},
	{
		"Polygon", "POLYGON",
		newWKBW(true).header(3).u32(1).u32(4).f64(0).f64(0).f64(1).f64(0).f64(1).f64(1).f64(0).f64(0).buf,
		newWKBW(true).header(3).u32(1).u32(4).f64(0).f64(0).f64(1).f64(0).f64(math.NaN()).f64(1).f64(0).f64(0).buf,
		newWKBW(true).header(3).u32(0).buf,
	},
	{
		"MultiPoint", "MULTIPOINT",
		newWKBW(true).header(4).u32(1).raw(point(true, 5, 6)).buf,
		newWKBW(true).header(4).u32(1).raw(point(true, math.NaN(), 6)).buf,
		newWKBW(true).header(4).u32(0).buf,
	},
	{
		"MultiLineString", "MULTILINESTRING",
		newWKBW(true).header(5).u32(1).raw(newWKBW(true).header(2).u32(2).f64(0).f64(0).f64(1).f64(1).buf).buf,
		newWKBW(true).header(5).u32(1).raw(newWKBW(true).header(2).u32(2).f64(0).f64(0).f64(math.Inf(1)).f64(1).buf).buf,
		newWKBW(true).header(5).u32(0).buf,
	},
	{
		"MultiPolygon", "MULTIPOLYGON",
		newWKBW(true).header(6).u32(1).raw(newWKBW(true).header(3).u32(1).u32(4).
			f64(0).f64(0).f64(1).f64(0).f64(1).f64(1).f64(0).f64(0).buf).buf,
		newWKBW(true).header(6).u32(1).raw(newWKBW(true).header(3).u32(1).u32(4).
			f64(0).f64(0).f64(1).f64(0).f64(1).f64(math.NaN()).f64(0).f64(0).buf).buf,
		newWKBW(true).header(6).u32(0).buf,
	},
	{
		"GeometryCollection", "GEOMETRYCOLLECTION",
		newWKBW(true).header(7).u32(1).raw(point(true, 1, 2)).buf,
		newWKBW(true).header(7).u32(1).raw(point(true, math.NaN(), 2)).buf,
		newWKBW(true).header(7).u32(0).buf, // GEOMETRYCOLLECTION() is a real, storable value
	},
}

// TestMySQLAcceptsRawNaNPointPremise pins the environmental premise: with the
// client-side scan bypassed (raw bytes inserted directly), MySQL accepts the
// NaN point silently and stores a value it cannot itself produce. This is
// what makes refuseNonFiniteWKB load-bearing rather than a duplicate of a
// server refusal. If this pin fires, MySQL grew coordinate validation and the
// scan's role should be re-derived.
func TestMySQLAcceptsRawNaNPointPremise(t *testing.T) {
	dsn, cleanup := newSharedDB(t, "spat1_premise")
	defer cleanup()
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	db, err := openDB(ctx, mustParseDSN(t, dsn), nil)
	if err != nil {
		t.Fatalf("openDB: %v", err)
	}
	defer db.Close()

	if _, err := db.ExecContext(ctx, "CREATE TABLE p (id INT PRIMARY KEY, loc POINT)"); err != nil {
		t.Fatalf("CREATE: %v", err)
	}
	// MySQL internal geometry layout: 4-byte LE SRID prefix + WKB.
	inner := point(true, math.NaN(), math.NaN())
	raw := make([]byte, 4+len(inner))
	binary.LittleEndian.PutUint32(raw[:4], 0)
	copy(raw[4:], inner)
	if _, err := db.ExecContext(ctx,
		fmt.Sprintf("INSERT INTO p (id, loc) VALUES (1, x'%s')", hex.EncodeToString(raw))); err != nil {
		t.Fatalf("PREMISE CHANGED: MySQL now refuses a raw NaN-coordinate point (%v) — the client-side "+
			"SPAT-1 scan is no longer the only guard; re-derive its role before weakening it", err)
	}
	var txt *string
	if err := db.QueryRowContext(ctx, "SELECT ST_AsText(loc) FROM p WHERE id=1").Scan(&txt); err != nil {
		t.Fatalf("read back: %v", err)
	}
	if txt != nil {
		t.Fatalf("PREMISE CHANGED: ST_AsText on the stored NaN point = %q; the audit observed NULL "+
			"(a value MySQL cannot itself produce)", *txt)
	}
}

// TestGeometryNonFinite_FamilyMatrix drives every family × {clean, NaN,
// EMPTY} through the real batched write core. Mutation: disabling the
// refuseNonFiniteWKB call in prepareValue flips every NaN cell from
// client-refused to silently-landed.
func TestGeometryNonFinite_FamilyMatrix(t *testing.T) {
	dsn, cleanup := newSharedDB(t, "spat1_matrix")
	defer cleanup()
	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()
	db, err := openDB(ctx, mustParseDSN(t, dsn), nil)
	if err != nil {
		t.Fatalf("openDB: %v", err)
	}
	defer db.Close()

	for i, fam := range wkbFamilies {
		tblName := fmt.Sprintf("g%d", i)
		if _, err := db.ExecContext(ctx, fmt.Sprintf(
			"CREATE TABLE %s (id INT PRIMARY KEY, loc %s)", tblName, fam.colType,
		)); err != nil {
			t.Fatalf("CREATE %s: %v", fam.name, err)
		}
		tbl := &ir.Table{
			Name: tblName,
			Columns: []*ir.Column{
				{Name: "id", Type: ir.Integer{Width: 32}},
				{Name: "loc", Type: ir.Geometry{}},
			},
		}
		w := &RowWriter{db: db}
		write := func(id int64, wkb []byte) error {
			ch := make(chan ir.Row, 1)
			ch <- ir.Row{"id": id, "loc": wkb}
			close(ch)
			return w.writeBatched(ctx, tbl, ch)
		}
		count := func() int {
			var n int
			if err := db.QueryRowContext(ctx, "SELECT COUNT(*) FROM "+tblName).Scan(&n); err != nil {
				t.Fatalf("%s count: %v", fam.name, err)
			}
			return n
		}

		t.Run(fam.name+"/clean lands", func(t *testing.T) {
			if err := write(1, fam.clean); err != nil {
				t.Fatalf("clean %s refused: %v — over-refusal breaks working migrations", fam.name, err)
			}
			if count() != 1 {
				t.Fatalf("clean %s landed %d rows; want 1", fam.name, count())
			}
		})
		t.Run(fam.name+"/NaN refuses client-side, nothing lands", func(t *testing.T) {
			before := count()
			err := write(2, fam.poison)
			if err == nil {
				t.Fatalf("NaN %s landed silently — the SPAT-1 poison shape", fam.name)
			}
			if ce, ok := sluicecode.FromError(err); !ok || ce.Code != sluicecode.CodeValueUnrepresentable {
				t.Errorf("refusal should carry %s structurally (client-side, before the wire); got: %v",
					sluicecode.CodeValueUnrepresentable, err)
			}
			if count() != before {
				t.Fatalf("NaN %s refusal still landed a row (%d -> %d)", fam.name, before, count())
			}
		})
		t.Run(fam.name+"/EMPTY is faithful-or-loud, never silent poison", func(t *testing.T) {
			before := count()
			err := write(3, fam.empty)
			isOurs := func(err error) bool {
				ce, ok := sluicecode.FromError(err)
				return ok && ce.Code == sluicecode.CodeValueUnrepresentable
			}
			switch fam.name {
			case "Point":
				// POINT EMPTY is the NaN form — ours, client-side.
				if err == nil || !isOurs(err) {
					t.Fatalf("POINT EMPTY: err = %v; want the client-side refusal", err)
				}
			case "GeometryCollection":
				// GEOMETRYCOLLECTION() is a real storable value: must keep landing.
				if err != nil {
					t.Fatalf("GEOMETRYCOLLECTION EMPTY refused (%v); it is a valid MySQL value — over-refusal", err)
				}
				if count() != before+1 {
					t.Fatalf("GEOMETRYCOLLECTION EMPTY did not land")
				}
			default:
				// The server's own 1416 — loud, not ours, and not silent.
				if err == nil {
					t.Fatalf("empty %s landed silently; the audit measured the server refusing these loudly (1416) — "+
						"if MySQL became permissive this cell just caught it", fam.name)
				}
				if isOurs(err) {
					t.Errorf("empty %s was refused CLIENT-side; expected the server's own refusal (scope creep in the scan)", fam.name)
				}
			}
		})
	}
}
