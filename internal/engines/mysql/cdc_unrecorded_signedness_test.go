// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package mysql

import (
	"math"
	"reflect"
	"strings"
	"testing"

	gomysql "github.com/go-mysql-org/go-mysql/mysql"
	"github.com/go-mysql-org/go-mysql/replication"

	"sluicesync.dev/sluice/internal/ir"
	"sluicesync.dev/sluice/internal/sluicecode"
)

// TestReinterpretUnsigned_EveryIntegerCode pins the reinterpretation for
// each of the five integer codes at the values that tell signed from
// unsigned: the unsigned maximum (a signed -1), the signed maximum plus one
// (the signed minimum), the signed maximum, zero and NULL. Each input is the
// Go value go-mysql produces for those bytes when it decodes them as signed
// (MEDIUMINT sign-extended into an int32); the expected value is the
// literal, not a decode.
func TestReinterpretUnsigned_EveryIntegerCode(t *testing.T) {
	cases := []struct {
		code byte
		in   any
		want uint64
	}{
		{gomysql.MYSQL_TYPE_TINY, int8(-1), math.MaxUint8},
		{gomysql.MYSQL_TYPE_TINY, int8(math.MinInt8), 1 << 7},
		{gomysql.MYSQL_TYPE_TINY, int8(math.MaxInt8), 1<<7 - 1},
		{gomysql.MYSQL_TYPE_TINY, int8(0), 0},
		{gomysql.MYSQL_TYPE_SHORT, int16(-1), math.MaxUint16},
		{gomysql.MYSQL_TYPE_SHORT, int16(math.MinInt16), 1 << 15},
		{gomysql.MYSQL_TYPE_SHORT, int16(math.MaxInt16), 1<<15 - 1},
		{gomysql.MYSQL_TYPE_INT24, int32(-1), 1<<24 - 1},
		{gomysql.MYSQL_TYPE_INT24, int32(-1 << 23), 1 << 23},
		{gomysql.MYSQL_TYPE_INT24, int32(1<<23 - 1), 1<<23 - 1},
		{gomysql.MYSQL_TYPE_LONG, int32(-1), math.MaxUint32},
		{gomysql.MYSQL_TYPE_LONG, int32(math.MinInt32), 1 << 31},
		{gomysql.MYSQL_TYPE_LONG, int32(math.MaxInt32), 1<<31 - 1},
		{gomysql.MYSQL_TYPE_LONGLONG, int64(-1), math.MaxUint64},
		{gomysql.MYSQL_TYPE_LONGLONG, int64(math.MinInt64), 1 << 63},
		{gomysql.MYSQL_TYPE_LONGLONG, int64(math.MaxInt64), 1<<63 - 1},
		{gomysql.MYSQL_TYPE_LONGLONG, int64(0), 0},
	}
	for _, c := range cases {
		got, err := reinterpretUnsigned(c.in, c.code)
		if err != nil || got != c.want {
			t.Errorf("code %d, %#v: got %#v (%v); want uint64(%d)", c.code, c.in, got, err, c.want)
		}
	}
	for _, code := range []byte{gomysql.MYSQL_TYPE_TINY, gomysql.MYSQL_TYPE_SHORT, gomysql.MYSQL_TYPE_INT24, gomysql.MYSQL_TYPE_LONG, gomysql.MYSQL_TYPE_LONGLONG} {
		if got, err := reinterpretUnsigned(nil, code); got != nil || err != nil {
			t.Errorf("code %d NULL: got %#v (%v); want nil", code, got, err)
		}
	}
}

// TestReinterpretUnsigned_RefusesAValueItsCodeDoesNotProduce: a value whose
// Go type is not the one the code decodes to is not a signed integer of that
// width, and reinterpreting it would invent a number.
func TestReinterpretUnsigned_RefusesAValueItsCodeDoesNotProduce(t *testing.T) {
	for _, c := range []struct {
		code byte
		in   any
	}{
		{gomysql.MYSQL_TYPE_TINY, int16(-1)},
		{gomysql.MYSQL_TYPE_SHORT, int32(-1)},
		{gomysql.MYSQL_TYPE_INT24, int64(-1)},
		{gomysql.MYSQL_TYPE_LONG, int16(-1)},
		{gomysql.MYSQL_TYPE_LONGLONG, int32(-1)},
		{gomysql.MYSQL_TYPE_LONGLONG, uint64(1)},
		{gomysql.MYSQL_TYPE_LONG, "4294967295"},
	} {
		if got, err := reinterpretUnsigned(c.in, c.code); err == nil {
			t.Errorf("code %d, %#v (%T): got %#v; want a refusal", c.code, c.in, c.in, got)
		}
	}
}

// signednessTable is a proven shape with one column per integer width,
// UNSIGNED and signed alternating, plus a non-integer unsigned DECIMAL.
func signednessTable() (tbl *tableSchema, codes []byte) {
	tbl = &tableSchema{
		Schema: "app",
		Name:   "t",
		Columns: []*ir.Column{
			{Name: "u8", Type: ir.Integer{Width: 8, Unsigned: true}},
			{Name: "s8", Type: ir.Integer{Width: 8}},
			{Name: "u16", Type: ir.Integer{Width: 16, Unsigned: true}},
			{Name: "u24", Type: ir.Integer{Width: 24, Unsigned: true}},
			{Name: "u32", Type: ir.Integer{Width: 32, Unsigned: true}},
			{Name: "u64", Type: ir.Integer{Width: 64, Unsigned: true}},
			{Name: "d", Type: ir.Decimal{Precision: 20}},
		},
		proven: true,
	}
	codes = []byte{
		gomysql.MYSQL_TYPE_TINY, gomysql.MYSQL_TYPE_TINY, gomysql.MYSQL_TYPE_SHORT, gomysql.MYSQL_TYPE_INT24,
		gomysql.MYSQL_TYPE_LONG, gomysql.MYSQL_TYPE_LONGLONG, gomysql.MYSQL_TYPE_NEWDECIMAL,
	}
	return tbl, codes
}

func signednessTableMap(codes, signedness []byte, names []string) *replication.TableMapEvent {
	tm := &replication.TableMapEvent{
		ColumnCount:      uint64(len(codes)),
		ColumnType:       codes,
		ColumnMeta:       make([]uint16, len(codes)),
		SignednessBitmap: signedness,
	}
	for _, n := range names {
		tm.ColumnName = append(tm.ColumnName, []byte(n))
	}
	return tm
}

// TestBinlogUnrecordedUnsigned_ProvenShapeNoRecord: with no signedness
// record (MariaDB NO_LOG), every UNSIGNED integer column is marked with its
// code and nothing else is.
func TestBinlogUnrecordedUnsigned_ProvenShapeNoRecord(t *testing.T) {
	tbl, codes := signednessTable()
	got, err := binlogUnrecordedUnsigned(tbl, signednessTableMap(codes, nil, nil))
	if err != nil {
		t.Fatalf("err = %v", err)
	}
	want := unrecordedUnsigned{codes[0], 0, codes[2], codes[3], codes[4], codes[5], 0}
	if !reflect.DeepEqual(got, want) {
		t.Errorf("got %v; want %v", got, want)
	}
}

// TestBinlogUnrecordedUnsigned_RecordedSignednessWins: with the record
// present (MINIMAL/FULL) go-mysql already decoded by it, so nothing is
// reinterpreted — even where the record says signed and the shape says
// unsigned (that disagreement is the TABLE_MAP/look-ahead guards' to refuse).
func TestBinlogUnrecordedUnsigned_RecordedSignednessWins(t *testing.T) {
	tbl, codes := signednessTable()
	for _, bitmap := range [][]byte{{0b10111110}, {0}} {
		got, err := binlogUnrecordedUnsigned(tbl, signednessTableMap(codes, bitmap, nil))
		if err != nil || got != nil {
			t.Errorf("bitmap %08b: got %v, %v; want nil, nil", bitmap[0], got, err)
		}
	}
}

// TestBinlogUnrecordedUnsigned_RefusesWithoutProof: an integer column the
// event records no signedness for refuses when the shape is not proven to
// hold at the event, and when the event names its columns yet omits their
// signedness — a SIGNED one too, whose signed reading is then a guess as
// well. A proven signed-only shape decodes as go-mysql read it.
func TestBinlogUnrecordedUnsigned_RefusesWithoutProof(t *testing.T) {
	tbl, codes := signednessTable()
	tbl.proven = false
	_, err := binlogUnrecordedUnsigned(tbl, signednessTableMap(codes, nil, nil))
	assertSignednessRefusal(t, "unproven shape", "u8", err)

	tbl, codes = signednessTable()
	names := []string{"u8", "s8", "u16", "u24", "u32", "u64", "d"}
	_, err = binlogUnrecordedUnsigned(tbl, signednessTableMap(codes, nil, names))
	assertSignednessRefusal(t, "names without signedness", "u8", err)

	signedOnly := func(proven bool) *tableSchema {
		return &tableSchema{Schema: "app", Name: "t", Columns: []*ir.Column{{Name: "s", Type: ir.Integer{Width: 32}}}, proven: proven}
	}
	long := []byte{gomysql.MYSQL_TYPE_LONG}
	_, err = binlogUnrecordedUnsigned(signedOnly(false), signednessTableMap(long, nil, nil))
	assertSignednessRefusal(t, "unproven signed-only shape", "s", err)
	if got, err := binlogUnrecordedUnsigned(signedOnly(true), signednessTableMap(long, nil, nil)); got != nil || err != nil {
		t.Errorf("proven signed-only shape: got %v, %v; want nil, nil", got, err)
	}
}

func assertSignednessRefusal(t *testing.T, what, column string, err error) {
	t.Helper()
	ce, ok := sluicecode.FromError(err)
	if !ok || ce.Code != sluicecode.CodeCDCSchemaReplayMismatch || !strings.Contains(err.Error(), `integer column "`+column+`"`) {
		t.Errorf("%s: err = %v; want %s naming column %s", what, err, sluicecode.CodeCDCSchemaReplayMismatch, column)
	}
}

// TestDecodeBinlogRow_ReinterpretsUnrecordedUnsigned: the decoder applies the
// marks — every width at its maximum (the bytes go-mysql read as -1) — and
// leaves the signed column and the unmarked columns alone.
func TestDecodeBinlogRow_ReinterpretsUnrecordedUnsigned(t *testing.T) {
	tbl, codes := signednessTable()
	us, err := binlogUnrecordedUnsigned(tbl, signednessTableMap(codes, nil, nil))
	if err != nil {
		t.Fatal(err)
	}
	raw := []any{int8(-1), int8(-1), int16(-1), int32(-1), int32(-1), int64(-1), "18446744073709551615"}
	row, err := decodeBinlogRow(raw, tbl.Columns, nil, FlavorMariaDB, "t", zeroDateInherit, binlogLabelGuard{}, nil, us)
	if err != nil {
		t.Fatal(err)
	}
	want := ir.Row{
		"u8": uint64(math.MaxUint8), "s8": int64(-1), "u16": uint64(math.MaxUint16), "u24": uint64(1<<24 - 1),
		"u32": uint64(math.MaxUint32), "u64": uint64(math.MaxUint64), "d": "18446744073709551615",
	}
	if !reflect.DeepEqual(row, want) {
		t.Errorf("got %#v; want %#v", row, want)
	}
}

// TestRecordedColumnMeta_IntegerWithoutSignednessRefuses: typing a column
// from a TABLE_MAP that names it but records no signedness must not default
// to signed.
func TestRecordedColumnMeta_IntegerWithoutSignednessRefuses(t *testing.T) {
	for _, code := range []byte{gomysql.MYSQL_TYPE_TINY, gomysql.MYSQL_TYPE_SHORT, gomysql.MYSQL_TYPE_INT24, gomysql.MYSQL_TYPE_LONG, gomysql.MYSQL_TYPE_LONGLONG} {
		tm := signednessTableMap([]byte{code}, nil, []string{"c"})
		if _, typ, err := recordedColumnMeta(tm, 0, FlavorMariaDB, nil); err == nil || !strings.Contains(err.Error(), "does not record whether it was signed") {
			t.Errorf("code %d: got %v, %v; want the unrecorded-signedness refusal", code, typ, err)
		}
	}
}
