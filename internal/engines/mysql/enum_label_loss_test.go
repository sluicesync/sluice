// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package mysql

import (
	"reflect"
	"strings"
	"testing"

	"vitess.io/vitess/go/vt/proto/query"

	"sluicesync.dev/sluice/internal/ir"
)

func TestLostEnumSetLabels(t *testing.T) {
	enum := ir.Enum{Values: []string{"?b", "x", "é"}}
	cases := []struct {
		name    string
		charset string
		typ     ir.Type
		want    []bool
	}{
		{"utf8mb4 enum with '?'", "utf8mb4", enum, []bool{true, false, false}},
		{"utf16 set with '?'", "utf16", ir.Set{Values: []string{"y", "?"}}, []bool{false, true}},
		{"UTF32 upper-case charset", "UTF32", enum, []bool{true, false, false}},
		{"latin1 cannot hold a lost character", "latin1", enum, nil},
		{"utf8mb3 cannot hold a lost character", "utf8mb3", enum, nil},
		{"no '?' anywhere", "utf8mb4", ir.Enum{Values: []string{"a", "é"}}, nil},
		{"not an enum", "utf8mb4", ir.Varchar{Length: 3}, nil},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			if got := lostEnumSetLabels(c.charset, c.typ); !reflect.DeepEqual(got, c.want) {
				t.Errorf("lostEnumSetLabels(%q, %v) = %v; want %v", c.charset, c.typ, got, c.want)
			}
		})
	}
}

func TestLabelAgreesWithCatalog(t *testing.T) {
	cases := []struct {
		tableMap, catalog string
		want              bool
	}{
		{"😀b", "?b", true},
		{"?b", "?b", true}, // a genuine '?' agrees with itself
		{"é😀?", "é??", true},
		{"😀b", "?c", false},
		{"😀😀", "?", false},  // one '?' per lost character, not per label
		{"xb", "?b", false}, // a BMP character is never written as '?'
		{"\xff\xfe", "?", false},
	}
	for _, c := range cases {
		if got := labelAgreesWithCatalog(c.tableMap, c.catalog); got != c.want {
			t.Errorf("labelAgreesWithCatalog(%q, %q) = %v; want %v", c.tableMap, c.catalog, got, c.want)
		}
	}
}

// TestDecodeBinlogRow_LostEnumSetLabels pins the binlog decode of an ENUM
// index / SET bitmask through labels the catalog wrote as '?': refused
// without the TABLE_MAP's labels, recovered with them, and untouched for a
// label the catalog did not lose.
func TestDecodeBinlogRow_LostEnumSetLabels(t *testing.T) {
	cols := []*ir.Column{
		{Name: "id", Type: ir.Integer{Width: 32}},
		{Name: "e", Type: ir.Enum{Values: []string{"?b", "x", "é"}}, Nullable: true},
		{Name: "s", Type: ir.Set{Values: []string{"?", "y", "é"}}, Nullable: true},
	}
	lost := [][]bool{nil, {true, false, false}, {true, false, false}}
	tableMap := [][]string{nil, {"😀b", "x", "é"}, {"😀", "y", "é"}}

	decode := func(g binlogLabelGuard, e, s any) (ir.Row, error) {
		t.Helper()
		return decodeBinlogRow([]any{int32(1), e, s}, cols, nil, FlavorVanilla, "t", zeroDateInherit, g, nil)
	}

	t.Run("no TABLE_MAP labels: an index onto a lost label refuses", func(t *testing.T) {
		_, err := decode(binlogLabelGuard{lost: lost}, int64(1), int64(2))
		if err == nil || !strings.Contains(err.Error(), enumLabelNotRecoverableMarker) {
			t.Fatalf("err = %v; want %s", err, enumLabelNotRecoverableMarker)
		}
		if !strings.Contains(err.Error(), `"?b"`) || !strings.Contains(err.Error(), "t.e") {
			t.Errorf("refusal does not name the column and label: %v", err)
		}
	})
	t.Run("no TABLE_MAP labels: a set bit on a lost label refuses", func(t *testing.T) {
		_, err := decode(binlogLabelGuard{lost: lost}, int64(2), int64(3))
		if err == nil || !strings.Contains(err.Error(), enumLabelNotRecoverableMarker) {
			t.Fatalf("err = %v; want %s", err, enumLabelNotRecoverableMarker)
		}
	})
	t.Run("no TABLE_MAP labels: labels the catalog kept decode normally", func(t *testing.T) {
		row, err := decode(binlogLabelGuard{lost: lost}, int64(3), int64(6))
		if err != nil {
			t.Fatal(err)
		}
		if row["e"] != "é" || !reflect.DeepEqual(row["s"], []string{"y", "é"}) {
			t.Errorf("row = %v; want e=é s=[y é]", row)
		}
	})
	t.Run("TABLE_MAP labels recover the lost label", func(t *testing.T) {
		row, err := decode(binlogLabelGuard{lost: lost, tableMap: tableMap}, int64(1), int64(3))
		if err != nil {
			t.Fatal(err)
		}
		if row["e"] != "😀b" || !reflect.DeepEqual(row["s"], []string{"😀", "y"}) {
			t.Errorf("row = %v; want e=😀b s=[😀 y]", row)
		}
	})
	t.Run("TABLE_MAP confirms a genuine '?' label", func(t *testing.T) {
		genuine := [][]string{nil, {"?b", "x", "é"}, {"?", "y", "é"}}
		row, err := decode(binlogLabelGuard{lost: lost, tableMap: genuine}, int64(1), int64(1))
		if err != nil {
			t.Fatal(err)
		}
		if row["e"] != "?b" || !reflect.DeepEqual(row["s"], []string{"?"}) {
			t.Errorf("row = %v; want e=?b s=[?]", row)
		}
	})
	t.Run("a TABLE_MAP label that disagrees with the catalog refuses", func(t *testing.T) {
		wrong := [][]string{nil, {"😀c", "x", "é"}, {"😀", "y", "é"}}
		_, err := decode(binlogLabelGuard{lost: lost, tableMap: wrong}, int64(1), nil)
		if err == nil || !strings.Contains(err.Error(), "does not match the catalog") {
			t.Fatalf("err = %v; want a catalog-mismatch refusal", err)
		}
	})
	t.Run("a label carried as TEXT is not the index path", func(t *testing.T) {
		row, err := decode(binlogLabelGuard{lost: lost}, "😀b", "😀")
		if err != nil {
			t.Fatal(err)
		}
		if row["e"] != "😀b" {
			t.Errorf("e = %v; want the carried text", row["e"])
		}
	})
	t.Run("the zero guard changes nothing", func(t *testing.T) {
		row, err := decode(binlogLabelGuard{}, int64(1), int64(1))
		if err != nil {
			t.Fatal(err)
		}
		if row["e"] != "?b" {
			t.Errorf("e = %v; want the catalog label with no guard", row["e"])
		}
	})
}

// TestRefuseVStreamLostLabel pins the VStream arm: vttablet hands over the
// label TEXT its own lossy catalog holds, so a cell equal to a label the
// catalog may have lost refuses, and nothing else does.
func TestRefuseVStreamLostLabel(t *testing.T) {
	const utf8mb4, latin1 = 255, 8 // utf8mb4_0900_ai_ci, latin1_swedish_ci
	enumField := func(ct string, cs uint32) *query.Field {
		return &query.Field{Name: "e", Type: query.Type_ENUM, ColumnType: ct, Charset: cs}
	}
	setField := func(ct string, cs uint32) *query.Field {
		return &query.Field{Name: "s", Type: query.Type_SET, ColumnType: ct, Charset: cs}
	}
	cases := []struct {
		name   string
		f      *query.Field
		v      any
		refuse bool
	}{
		{"enum cell on a lost label", enumField("enum('?b','x','é')", utf8mb4), "?b", true},
		{"enum cell on a kept label", enumField("enum('?b','x','é')", utf8mb4), "é", false},
		{"enum true text from the copy phase", enumField("enum('?b','x','é')", utf8mb4), "😀b", false},
		{"latin1 '?' is genuine", enumField("enum('?b','x')", latin1), "?b", false},
		{"set member on a lost label", setField("set('?','y')", utf8mb4), []string{"y", "?"}, true},
		{"set members all kept", setField("set('?','y')", utf8mb4), []string{"y"}, false},
		{"no label list, utf8mb4, '?' cell", enumField("", utf8mb4), "?b", true},
		{"not an enum", &query.Field{Name: "v", Type: query.Type_VARCHAR, Charset: utf8mb4}, "?b", false},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			err := refuseVStreamLostLabel("t", c.f, c.v)
			if c.refuse != (err != nil) {
				t.Fatalf("refuseVStreamLostLabel(%v) = %v; want refuse=%v", c.v, err, c.refuse)
			}
			if err != nil && !strings.Contains(err.Error(), enumLabelNotRecoverableMarker) {
				t.Errorf("refusal lacks %s: %v", enumLabelNotRecoverableMarker, err)
			}
		})
	}
}
