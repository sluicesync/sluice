// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package mysql

import (
	"fmt"
	"testing"
	"time"

	"vitess.io/vitess/go/vt/proto/query"

	"sluicesync.dev/sluice/internal/ir"
)

// TestFloatPatchKeyFamilies_ReadersRenderEqualGoValues binds the SECOND
// premise of backup float repair's patch-key argument (invariant sweep
// 2026-08-22; audit M-4's key bridges TWO readers).
//
// internal/pipeline/backup's floatPatchKey joins rows across two readers by
// rendering each PK component with `%v`: the exact-scan side decodes through
// THIS package's decodeValue (the RowReader / database-sql shapes) and the
// map-lookup side through decodeVStreamCell (the VStream COPY wire shapes).
// Its comment argues the two sides "render to the same decimal text, so they
// MATCH" — which is a claim about EVERY PK family, and until this gate only
// the unsigned-integer cell was pinned
// (TestFloatExactPatchReader_UnsignedPKCrossReaderTypesStillPatch). A family
// whose two decoders render differently makes every row of such a table miss
// its exact-float patch; the total-miss shape is caught by the SL-F2
// zero-patched tripwire (WARN by default, refusal under --strict-float), so
// this gate is what turns "signalled at run time" into "fails the build".
//
// Independent expected value: there is none here beyond the two decoders
// themselves — this is deliberately an AGREEMENT gate, not a fidelity gate.
// The fidelity of each side against a real server is owned by the existing
// per-decoder pins (value_decode_test.go, TestDecodeVStreamCell, and the
// integration matrices); what none of those held together, and this does, is
// the `%v` RENDER EQUALITY the patch key depends on.
//
// The driver-raw shapes fed to decodeValue are the binary-protocol shapes
// those pins record (int64/uint64 for integers, []byte for text-ish columns,
// time.Time-or-text for temporals under parseTime=true&loc=UTC — see
// finishParseDSN). Mutation-verified: re-typing decodeVStreamCell's DECIMAL
// arm to return []byte fails the decimal cell; dropping the tinyint(1) bool
// collapse on either side fails the boolean cell.
func TestFloatPatchKeyFamilies_ReadersRenderEqualGoValues(t *testing.T) {
	utc := func(s string) time.Time {
		tv, err := time.Parse("2006-01-02 15:04:05.999999", s)
		if err != nil {
			t.Fatalf("fixture time %q: %v", s, err)
		}
		return tv
	}

	cells := []struct {
		name string

		// exact-scan side (RowReader → decodeValue)
		irType    ir.Type
		driverRaw any

		// COPY side (VStream → decodeVStreamCell)
		fieldType  query.Type
		columnType string
		wire       string
	}{
		{
			"signed int",
			ir.Integer{Width: 64},
			int64(-42),
			query.Type_INT64, "bigint", "-42",
		},
		{
			"unsigned int ≤2^63 (int64 vs uint64)",
			ir.Integer{Width: 64, Unsigned: true},
			int64(5),
			query.Type_UINT64, "bigint unsigned", "5",
		},
		{
			"unsigned int >2^63",
			ir.Integer{Width: 64, Unsigned: true},
			uint64(18446744073709551615),
			query.Type_UINT64, "bigint unsigned", "18446744073709551615",
		},
		{
			"tinyint(1) boolean",
			ir.Boolean{},
			int64(1),
			query.Type_INT8, "tinyint(1)", "1",
		},
		{
			"varchar",
			ir.Varchar{Length: 20},
			[]byte("o'brien"),
			query.Type_VARCHAR, "varchar(20)", "o'brien",
		},
		{
			"char",
			ir.Char{Length: 5},
			[]byte("fixed"),
			query.Type_CHAR, "char(5)", "fixed",
		},
		{
			"decimal",
			ir.Decimal{Precision: 10, Scale: 2},
			[]byte("1234.56"),
			query.Type_DECIMAL, "decimal(10,2)", "1234.56",
		},
		{
			"date (driver time.Time)",
			ir.Date{},
			utc("2026-08-22 00:00:00"),
			query.Type_DATE, "date", "2026-08-22",
		},
		{
			"datetime with fraction (driver time.Time)",
			ir.DateTime{Precision: 6},
			utc("2026-08-22 10:11:12.123456"),
			query.Type_DATETIME, "datetime(6)", "2026-08-22 10:11:12.123456",
		},
		{
			"datetime from text (driver string)",
			ir.DateTime{},
			"2026-08-22 10:11:12",
			query.Type_DATETIME, "datetime", "2026-08-22 10:11:12",
		},
		{
			"timestamp (driver time.Time)",
			ir.Timestamp{},
			utc("2026-08-22 10:11:12"),
			query.Type_TIMESTAMP, "timestamp", "2026-08-22 10:11:12",
		},
		{
			"time-of-day stays text",
			ir.Time{},
			[]byte("12:34:56"),
			query.Type_TIME, "time", "12:34:56",
		},
		{
			"enum label",
			ir.Enum{Values: []string{"a", "b", "c"}},
			[]byte("b"),
			query.Type_ENUM, "enum('a','b','c')", "b",
		},
		{
			"varbinary",
			ir.Varbinary{Length: 8},
			[]byte{0x01, 0xfe},
			query.Type_VARBINARY, "varbinary(8)", "\x01\xfe",
		},
		{
			"bit",
			ir.Bit{Length: 3},
			[]byte{0x05},
			query.Type_BIT, "bit(3)", "\x05",
		},
	}

	const familyFloor = 12 // anti-vacuity: the matrix must stay a matrix
	if len(cells) < familyFloor {
		t.Fatalf("only %d family cells; floor is %d — a trimmed matrix passes on whatever is left", len(cells), familyFloor)
	}

	for _, c := range cells {
		c := c
		t.Run(c.name, func(t *testing.T) {
			rowVal, err := decodeValue(c.driverRaw, c.irType)
			if err != nil {
				t.Fatalf("decodeValue(%#v, %T): %v", c.driverRaw, c.irType, err)
			}
			vsVal := decodeVStreamCell(&query.Field{Type: c.fieldType, ColumnType: c.columnType}, []byte(c.wire))

			rowKey := fmt.Sprintf("%v", rowVal)
			vsKey := fmt.Sprintf("%v", vsVal)
			if rowKey != vsKey {
				t.Errorf("patch-key render diverges for the %s family:\n"+
					"  RowReader (exact scan): %q  (Go %T)\n"+
					"  VStream   (COPY side):  %q  (Go %T)\n"+
					"every row of a table with this PK family would miss its exact-FLOAT patch "+
					"(silent display-rounding, SL-F2 tripwire WARN at run time)", c.name, rowKey, rowVal, vsKey, vsVal)
			}
		})
	}
}
