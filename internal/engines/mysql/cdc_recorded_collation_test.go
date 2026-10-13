// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package mysql

import (
	"context"
	"testing"

	gomysql "github.com/go-mysql-org/go-mysql/mysql"
	"github.com/go-mysql-org/go-mysql/replication"

	"sluicesync.dev/sluice/internal/ir"
)

const (
	collIDBin = 46  // utf8mb4_bin
	collIDCI  = 255 // utf8mb4_0900_ai_ci
	collIDASC = 65  // ascii_bin
)

func recordedCollationReader() *CDCReader {
	return &CDCReader{
		schema:                 "d",
		serverCollationIDNames: map[uint64]string{collIDBin: "utf8mb4_bin", collIDCI: "utf8mb4_0900_ai_ci", collIDASC: "ascii_bin"},
		serverCollations:       map[uint64]string{collIDBin: "utf8mb4", collIDCI: "utf8mb4", collIDASC: "ascii"},
	}
}

func kdShape(collation string) *tableSchema {
	return &tableSchema{Schema: "d", Name: "kd", Columns: []*ir.Column{
		{Name: "id", Type: ir.Integer{Width: 32}},
		{Name: "k", Type: ir.Varchar{Length: 20, Charset: "utf8mb4", Collation: collation}},
	}}
}

func kdTableMap(kCollationID uint64) *replication.TableMapEvent {
	return &replication.TableMapEvent{
		ColumnCount:   2,
		ColumnType:    []byte{gomysql.MYSQL_TYPE_LONG, gomysql.MYSQL_TYPE_VARCHAR},
		ColumnMeta:    []uint16{0, 80},
		ColumnCharset: []uint64{kCollationID},
	}
}

func kCollation(tbl *tableSchema) (charset, collation string) {
	v := tbl.Columns[1].Type.(ir.Varchar)
	return v.Charset, v.Collation
}

// TestStampRecordedCollations pins KEY-COLLATION-REVERTED-WINDOW-RESTORE and
// KEY-COLLATION-LOOSENED-WINDOW at the reader: a rows event read behind the
// head under a catalog that has since changed the key's collation is
// decoded and reported under the collation it RECORDED, so the boundary the
// stream emits for it carries the shape in force at that row.
func TestStampRecordedCollations(t *testing.T) {
	ctx := context.Background()
	r := recordedCollationReader()
	cases := []struct {
		name                  string
		declared              string
		recorded              uint64
		wantCharset, wantColl string
		same                  bool
	}{
		{"reverted: written under _bin, catalog back on _ci", "utf8mb4_0900_ai_ci", collIDBin, "utf8mb4", "utf8mb4_bin", false},
		{"loosened: written under _bin, catalog loosened to _ci", "utf8mb4_0900_ai_ci", collIDBin, "utf8mb4", "utf8mb4_bin", false},
		{"tightened: written under _ci, catalog now _bin", "utf8mb4_bin", collIDCI, "utf8mb4", "utf8mb4_0900_ai_ci", false},
		{"character set too", "utf8mb4_bin", collIDASC, "ascii", "ascii_bin", false},
		{"at the head: recorded equals the catalog", "utf8mb4_0900_ai_ci", collIDCI, "utf8mb4", "utf8mb4_0900_ai_ci", true},
		{"binary column", "utf8mb4_0900_ai_ci", binaryCollationID, "utf8mb4", "utf8mb4_0900_ai_ci", true},
		{"unnamed ID", "utf8mb4_0900_ai_ci", 999, "utf8mb4", "utf8mb4_0900_ai_ci", true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			in := kdShape(tc.declared)
			got := r.stampRecordedCollations(ctx, in, kdTableMap(tc.recorded))
			if (got == in) != tc.same {
				t.Fatalf("returned the input shape = %v; want %v", got == in, tc.same)
			}
			if cs, coll := kCollation(got); cs != tc.wantCharset || coll != tc.wantColl {
				t.Fatalf("k = %s/%s; want %s/%s", cs, coll, tc.wantCharset, tc.wantColl)
			}
			if _, coll := kCollation(in); coll != tc.declared {
				t.Fatalf("the input shape was modified: k = %s", coll)
			}
		})
	}
	// No TABLE_MAP collations at all (MariaDB NO_LOG): the shape is the catalog's.
	in := kdShape("utf8mb4_bin")
	noLog := &replication.TableMapEvent{ColumnCount: 2, ColumnType: []byte{gomysql.MYSQL_TYPE_LONG, gomysql.MYSQL_TYPE_VARCHAR}, ColumnMeta: []uint16{0, 80}}
	if got := r.stampRecordedCollations(ctx, in, noLog); got != in {
		t.Fatal("a TABLE_MAP recording no collation changed the shape")
	}
}
