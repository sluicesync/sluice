// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package mysql

import (
	"context"
	"errors"
	"testing"

	gomysql "github.com/go-mysql-org/go-mysql/mysql"
	"github.com/go-mysql-org/go-mysql/replication"

	"sluicesync.dev/sluice/internal/ir"
)

const (
	collIDBin = 46  // utf8mb4_bin
	collIDCI  = 255 // utf8mb4_0900_ai_ci
)

func recordedCollationReader(baseline []*ir.Table) *CDCReader {
	r := &CDCReader{schema: "d", serverCollationIDNames: map[uint64]string{collIDBin: "utf8mb4_bin", collIDCI: "utf8mb4_0900_ai_ci"}}
	if baseline != nil {
		r.SetDecodedShapeBaseline(baseline)
	}
	return r
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

func kdBaseline(collation string) []*ir.Table {
	s := kdShape(collation)
	return []*ir.Table{{Schema: "d", Name: "kd", Columns: s.Columns}}
}

// TestHoldRecordedCollations pins KEY-COLLATION-REVERTED-WINDOW-RESTORE at
// the reader: a row recorded under a collation that neither the shape
// decoding it nor the stream's last known one declares refuses; a forward
// change replayed behind the head (rows recorded under the basis, then under
// the catalog's current collation) and a live stream pass.
func TestHoldRecordedCollations(t *testing.T) {
	ctx := context.Background()
	const qn = "d.kd"
	type event struct {
		declared string
		recorded uint64
		refuse   bool
	}
	cases := []struct {
		name     string
		baseline []*ir.Table
		events   []event
	}{
		{"reverted window replayed: _bin rows under a _ci catalog and basis", kdBaseline("utf8mb4_0900_ai_ci"), []event{
			{"utf8mb4_0900_ai_ci", collIDCI, false},
			{"utf8mb4_0900_ai_ci", collIDBin, true},
		}},
		{"forward change replayed: rows under the basis, then under the catalog", kdBaseline("utf8mb4_0900_ai_ci"), []event{
			{"utf8mb4_bin", collIDCI, false},
			{"utf8mb4_bin", collIDCI, false},
			{"utf8mb4_bin", collIDBin, false},
		}},
		{"forward then back: the last recorded collation is the basis", kdBaseline("utf8mb4_0900_ai_ci"), []event{
			{"utf8mb4_bin", collIDBin, false},
			{"utf8mb4_0900_ai_ci", collIDCI, false},
		}},
		{"live: recorded equals declared", kdBaseline("utf8mb4_0900_ai_ci"), []event{
			{"utf8mb4_0900_ai_ci", collIDCI, false},
		}},
		{"armed, table not in the baseline", []*ir.Table{}, []event{
			{"utf8mb4_0900_ai_ci", collIDBin, true},
		}},
		{"unarmed reader with no basis compares nothing", nil, []event{
			{"utf8mb4_0900_ai_ci", collIDBin, false},
		}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			r := recordedCollationReader(tc.baseline)
			for i, ev := range tc.events {
				err := r.holdRecordedCollations(ctx, qn, kdShape(ev.declared), kdTableMap(ev.recorded))
				if got := errors.Is(err, ir.ErrDeferredSchemaChange); got != ev.refuse {
					t.Fatalf("event %d (declared %s, recorded ID %d): err = %v; refuse = %v, want %v",
						i, ev.declared, ev.recorded, err, got, ev.refuse)
				}
				if err != nil {
					return
				}
				// What holdDecodedOrder does after the hold: the shape becomes the basis.
				if err := r.holdDecodedOrder(qn, kdShape(ev.declared)); err != nil {
					t.Fatalf("event %d: holdDecodedOrder: %v", i, err)
				}
			}
		})
	}
}

// TestHoldRecordedCollations_SkipsWhatItCannotCompare: a binary column, an
// ID the source does not name, and a TABLE_MAP that records no collation
// (MariaDB NO_LOG) pass untouched.
func TestHoldRecordedCollations_SkipsWhatItCannotCompare(t *testing.T) {
	ctx := context.Background()
	r := recordedCollationReader(kdBaseline("utf8mb4_0900_ai_ci"))
	for _, tm := range []*replication.TableMapEvent{
		kdTableMap(binaryCollationID),
		kdTableMap(999), // unnamed
		{ColumnCount: 2, ColumnType: []byte{gomysql.MYSQL_TYPE_LONG, gomysql.MYSQL_TYPE_VARCHAR}, ColumnMeta: []uint16{0, 80}},
	} {
		if err := r.holdRecordedCollations(ctx, "d.kd", kdShape("utf8mb4_0900_ai_ci"), tm); err != nil {
			t.Fatalf("err = %v; want nil", err)
		}
	}
}
