// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package mysql

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"vitess.io/vitess/go/vt/proto/query"

	"sluicesync.dev/sluice/internal/ir"
)

// Per-family zero/partial-date shapes. Date columns only ever carry the
// date-only shapes; datetime/timestamp columns carry the datetime shapes; the
// fractional-second precision families add the .ffffff micros shape. This is
// the Bug-74 "pin the class, not the representative" matrix for ADR-0127: the
// per-sync mode threads through the SAME decode path for every temporal family,
// but a green pin on one family does not prove the others (the wire/codec path
// differs by family, especially on the VStream side).
var (
	zeroDateOnlyShapes = []string{"0000-00-00", "2026-00-15", "2026-06-00", "2026-00-00"}
	zeroDateTimeShapes = []string{"0000-00-00 00:00:00", "2026-00-15 01:02:03", "2026-06-00 01:02:03", "2026-00-00 00:00:00"}
	zeroDateTime6Shape = []string{"0000-00-00 00:00:00.000000", "2026-00-15 01:02:03.123456", "2026-06-00 01:02:03.000000"}
)

// assertZeroDateOutcome checks the three policy outcomes for one already-decoded
// sentinel against a column: refuse → loud error naming the column; null → SQL
// NULL on a nullable column but a loud refusal on a NOT NULL column; epoch → the
// representable floor. The mode is passed EXPLICITLY (not via the global), which
// is exactly how a per-sync reader threads its own mode.
func assertZeroDateOutcome(t *testing.T, zd *zeroDateValueError, nullable, notNull *ir.Column) {
	t.Helper()

	if _, err := applyZeroDatePolicy(zd, nullable, zeroDateRefuse); err == nil {
		t.Errorf("refuse: err = nil; want a loud refusal")
	} else if !strings.Contains(err.Error(), "zero/partial date") {
		t.Errorf("refuse: err = %q; want it to name the zero/partial date", err)
	}

	v, err := applyZeroDatePolicy(zd, nullable, zeroDateAsNull)
	if err != nil {
		t.Errorf("null on nullable: err = %v; want nil", err)
	} else if v != nil {
		t.Errorf("null on nullable: v = %#v; want nil (SQL NULL)", v)
	}

	if _, err := applyZeroDatePolicy(zd, notNull, zeroDateAsNull); err == nil {
		t.Errorf("null on NOT NULL: err = nil; want a loud refusal")
	} else if !strings.Contains(err.Error(), "NOT NULL") {
		t.Errorf("null on NOT NULL: err = %q; want it to name the NOT NULL conflict", err)
	}

	ev, err := applyZeroDatePolicy(zd, notNull, zeroDateAsEpoch)
	if err != nil {
		t.Errorf("epoch: err = %v; want nil", err)
	} else if got, ok := ev.(time.Time); !ok || !got.Equal(zeroDateEpochValue) {
		t.Errorf("epoch: v = %#v; want %v", ev, zeroDateEpochValue)
	}
}

// TestZeroDatePerSync_VanillaMatrix pins the per-sync zero-date resolution
// across the WHOLE temporal family (DATE / DATETIME / DATETIME(6) / TIMESTAMP /
// TIMESTAMP(6)) × every zero/partial shape × every policy, on the vanilla
// row-decode path (decodeValue → applyZeroDatePolicy with an explicit mode).
func TestZeroDatePerSync_VanillaMatrix(t *testing.T) {
	families := []struct {
		name   string
		typ    ir.Type
		shapes []string
	}{
		{"DATE", ir.Date{}, zeroDateOnlyShapes},
		{"DATETIME", ir.DateTime{}, zeroDateTimeShapes},
		{"DATETIME(6)", ir.DateTime{Precision: 6}, zeroDateTime6Shape},
		{"TIMESTAMP", ir.Timestamp{}, zeroDateTimeShapes},
		{"TIMESTAMP(6)", ir.Timestamp{Precision: 6}, zeroDateTime6Shape},
	}
	for _, fam := range families {
		nullable := &ir.Column{Name: "d", Type: fam.typ, Nullable: true}
		notNull := &ir.Column{Name: "d", Type: fam.typ, Nullable: false}
		for _, shape := range fam.shapes {
			t.Run(fam.name+"/"+shape, func(t *testing.T) {
				// Both the string and []byte forms reach decodeTime.
				for _, raw := range []any{shape, []byte(shape)} {
					_, derr := decodeValue(raw, fam.typ)
					var zd *zeroDateValueError
					if !errors.As(derr, &zd) {
						t.Fatalf("decodeValue(%q as %s) err = %v; want *zeroDateValueError", shape, fam.name, derr)
					}
					assertZeroDateOutcome(t, zd, nullable, notNull)
				}
			})
		}
	}
}

// TestZeroDatePerSync_VStreamMatrix pins the SAME family × shape × policy matrix
// on the VStream decode path (decodeVStreamRow with an explicit per-sync mode).
// VStream cells dispatch on query.Type (DATE/DATETIME/TIMESTAMP) and parse the
// textual wire value, so the codec path differs from the vanilla one and must be
// pinned independently (Bug-74). The ColumnType string carries the precision.
func TestZeroDatePerSync_VStreamMatrix(t *testing.T) {
	families := []struct {
		name       string
		qt         query.Type
		columnType string
		shapes     []string
	}{
		{"DATE", query.Type_DATE, "date", zeroDateOnlyShapes},
		{"DATETIME", query.Type_DATETIME, "datetime", zeroDateTimeShapes},
		{"DATETIME(6)", query.Type_DATETIME, "datetime(6)", zeroDateTime6Shape},
		{"TIMESTAMP", query.Type_TIMESTAMP, "timestamp", zeroDateTimeShapes},
		{"TIMESTAMP(6)", query.Type_TIMESTAMP, "timestamp(6)", zeroDateTime6Shape},
	}
	mkRow := func(raw string) *query.Row {
		return &query.Row{Lengths: []int64{int64(len(raw))}, Values: []byte(raw)}
	}
	for _, fam := range families {
		nullableFields := []*query.Field{{Name: "d", Type: fam.qt, ColumnType: fam.columnType}}
		notNullFields := []*query.Field{{Name: "d", Type: fam.qt, ColumnType: fam.columnType, Flags: mysqlNotNullFlag}}
		for _, shape := range fam.shapes {
			t.Run(fam.name+"/"+shape, func(t *testing.T) {
				// Sanity: the cell must surface the shared sentinel first.
				if got := decodeVStreamCell(nullableFields[0], []byte(shape)); func() bool { _, ok := got.(*zeroDateValueError); return !ok }() {
					t.Fatalf("decodeVStreamCell(%s, %q) = %T; want *zeroDateValueError", fam.name, shape, got)
				}

				// refuse → loud, naming the column.
				if _, _, err := decodeVStreamRow(mkRow(shape), nullableFields, "t", newBoolRangeWarner(), zeroDateRefuse); err == nil {
					t.Errorf("refuse: err = nil; want a loud refusal")
				} else if !strings.Contains(err.Error(), `"d"`) {
					t.Errorf("refuse: err = %q; want it to name column d", err)
				}

				// null on nullable → SQL NULL.
				out, _, err := decodeVStreamRow(mkRow(shape), nullableFields, "t", newBoolRangeWarner(), zeroDateAsNull)
				if err != nil {
					t.Errorf("null on nullable: err = %v; want nil", err)
				} else if out["d"] != nil {
					t.Errorf("null on nullable: d = %#v; want nil", out["d"])
				}

				// null on NOT NULL → loud refusal.
				if _, _, err := decodeVStreamRow(mkRow(shape), notNullFields, "t", newBoolRangeWarner(), zeroDateAsNull); err == nil {
					t.Errorf("null on NOT NULL: err = nil; want a loud refusal")
				} else if !strings.Contains(err.Error(), "NOT NULL") {
					t.Errorf("null on NOT NULL: err = %q; want it to name the NOT NULL conflict", err)
				}

				// epoch → representable floor.
				out, _, err = decodeVStreamRow(mkRow(shape), notNullFields, "t", newBoolRangeWarner(), zeroDateAsEpoch)
				if err != nil {
					t.Errorf("epoch: err = %v; want nil", err)
				} else if got, ok := out["d"].(time.Time); !ok || !got.Equal(zeroDateEpochValue) {
					t.Errorf("epoch: d = %#v; want %v", out["d"], zeroDateEpochValue)
				}
			})
		}
	}
}

// TestReaderZeroDateMode pins the DSN-param parser (ADR-0127): absent →
// inherit, the three valid values, and a loud refusal (naming the param + the
// valid set) for a bogus value.
func TestReaderZeroDateMode(t *testing.T) {
	cases := []struct {
		dsn  string
		want zeroDateMode
	}{
		{"u:p@tcp(h:3306)/db", zeroDateInherit},
		{"u:p@tcp(h:3306)/db?zero_date=error", zeroDateRefuse},
		{"u:p@tcp(h:3306)/db?zero_date=null", zeroDateAsNull},
		{"u:p@tcp(h:3306)/db?zero_date=epoch", zeroDateAsEpoch},
	}
	for _, c := range cases {
		cfg, err := parseDSN(c.dsn)
		if err != nil {
			t.Fatalf("parseDSN(%q): %v", c.dsn, err)
		}
		got, err := readerZeroDateMode(cfg)
		if err != nil {
			t.Fatalf("readerZeroDateMode(%q): %v", c.dsn, err)
		}
		if got != c.want {
			t.Errorf("readerZeroDateMode(%q) = %v; want %v", c.dsn, got, c.want)
		}
	}

	cfg, err := parseDSN("u:p@tcp(h:3306)/db?zero_date=bogus")
	if err != nil {
		t.Fatalf("parseDSN: %v", err)
	}
	if _, err := readerZeroDateMode(cfg); err == nil {
		t.Error("readerZeroDateMode(bogus) err = nil; want a loud refusal")
	} else if !strings.Contains(err.Error(), "zero_date") || !strings.Contains(err.Error(), "error, null, epoch") {
		t.Errorf("err = %q; want it to name the zero_date param + valid set", err)
	}
}

// TestOpenRowReader_InvalidZeroDateRefuses pins that an invalid zero_date DSN
// param is refused LOUDLY at reader construction — before a connection is even
// opened — naming the param. (The resolution happens before openDB, so this
// needs no live MySQL.)
func TestOpenRowReader_InvalidZeroDateRefuses(t *testing.T) {
	_, err := Engine{}.OpenRowReader(context.Background(), "u:p@tcp(h:3306)/db?zero_date=bogus")
	if err == nil {
		t.Fatal("OpenRowReader(zero_date=bogus) err = nil; want a loud refusal")
	}
	if !strings.Contains(err.Error(), "zero_date") {
		t.Errorf("err = %q; want it to name the zero_date param", err)
	}
}

// TestZeroDatePerSyncIsolation is the load-bearing per-sync property this ADR
// adds (task 2.5 per-instance form): two readers in ONE process with DIFFERENT
// modes must not interfere, a per-source DSN override beats the engine default,
// and an unset reader falls back to the engine's --zero-date default (folded at
// construction) — no shared process global.
func TestZeroDatePerSyncIsolation(t *testing.T) {
	zd := &zeroDateValueError{raw: "0000-00-00"}
	nullable := &ir.Column{Name: "d", Type: ir.Date{}, Nullable: true}

	readerNull := &RowReader{zeroDate: zeroDateAsNull}   // ?zero_date=null
	readerEpoch := &RowReader{zeroDate: zeroDateAsEpoch} // ?zero_date=epoch
	readerDefault := &RowReader{}                        // no param → inherit

	// Two readers, different modes, SAME zero date → DIFFERENT results (no
	// shared-global interference).
	vNull, errNull := applyZeroDatePolicy(zd, nullable, readerNull.zeroDate)
	if errNull != nil || vNull != nil {
		t.Fatalf("readerNull: v=%#v err=%v; want nil,nil (NULL)", vNull, errNull)
	}
	vEpoch, errEpoch := applyZeroDatePolicy(zd, nullable, readerEpoch.zeroDate)
	if errEpoch != nil {
		t.Fatalf("readerEpoch: err=%v; want nil", errEpoch)
	}
	if got, ok := vEpoch.(time.Time); !ok || !got.Equal(zeroDateEpochValue) {
		t.Fatalf("readerEpoch: v=%#v; want %v", vEpoch, zeroDateEpochValue)
	}

	// An unset (inherit) reader whose engine has no --zero-date default resolves
	// to the loud refuse default.
	if _, errDefault := applyZeroDatePolicy(zd, nullable, readerDefault.zeroDate); errDefault == nil {
		t.Error("readerDefault (inherit, no engine default): err = nil; want the refuse default")
	}

	// The engine default is folded at CONSTRUCTION: an engine carrying
	// --zero-date=epoch folds onto an inherit-DSN reader, so that reader
	// substitutes the floor — while the per-source ?zero_date=null reader is
	// untouched (override beats the engine default).
	eEpoch, err := Engine{}.WithZeroDate("epoch")
	if err != nil {
		t.Fatalf("WithZeroDate(epoch): %v", err)
	}
	en := eEpoch.(Engine)
	foldedDefault := en.resolveReaderZeroDate(zeroDateInherit) // DSN unset → engine epoch
	foldedNull := en.resolveReaderZeroDate(zeroDateAsNull)     // DSN null wins
	vDef, errDef := applyZeroDatePolicy(zd, nullable, foldedDefault)
	if errDef != nil {
		t.Fatalf("readerDefault (inherit, engine=epoch): err=%v; want nil", errDef)
	}
	if got, ok := vDef.(time.Time); !ok || !got.Equal(zeroDateEpochValue) {
		t.Errorf("readerDefault (inherit, engine=epoch): v=%#v; want %v", vDef, zeroDateEpochValue)
	}
	if vN, errN := applyZeroDatePolicy(zd, nullable, foldedNull); errN != nil || vN != nil {
		t.Errorf("readerNull (?zero_date=null over engine=epoch): v=%#v err=%v; want nil,nil", vN, errN)
	}
}
