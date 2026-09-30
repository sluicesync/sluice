// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package mysql

import (
	"fmt"
	"strings"
	"testing"

	gomysql "github.com/go-sql-driver/mysql"

	"sluicesync.dev/sluice/internal/applymarks"
	"sluicesync.dev/sluice/internal/ir"
)

// TestUpsertSpelling_VitessFamilyNeverUsesValuesFunc pins the premise the
// sharded-target apply rests on (GC-41 (e)): vtgate refuses an ON DUPLICATE
// KEY UPDATE assignment to a primary-vindex column UNLESS it is spelled
// literally `col = VALUES(col)` — and that one spelling it ACCEPTS and
// executes as an insert on the new value's shard, measured on vttestserver as
// a silent cross-shard duplicate primary key ({10,1,a} on -80, {10,4,moved} on
// 80-). The row-alias spelling (`col = new.col`) is refused loudly, which is
// what keeps a vindex-moving change from corrupting a sharded target. So every
// flavor that talks to vtgate must render the row alias; the flavor set is
// derived from usesVStream, not listed, and a floor keeps the walk honest.
// The real-vtgate pin is TestVStream_ShardedTarget_VindexMoveRefusesLoudly.
func TestUpsertSpelling_VitessFamilyNeverUsesValuesFunc(t *testing.T) {
	vitessFamily := 0
	for f := FlavorVanilla; f <= FlavorMariaDB; f++ {
		if !f.usesVStream() {
			continue
		}
		vitessFamily++
		if f.upsertSpelling() == upsertValuesFunc {
			t.Errorf("flavor %s renders ON DUPLICATE KEY UPDATE col = VALUES(col): vtgate accepts that spelling for a primary-vindex column and duplicates the row across shards", f)
		}
		stmt, _, err := buildInsertSQL("ks", "t", ir.Row{"id": int64(1), "cust": int64(4)}, []string{"id"}, nil, f.upsertSpelling())
		if err != nil {
			t.Fatalf("buildInsertSQL: %v", err)
		}
		if strings.Contains(strings.ToUpper(stmt), "VALUES(") {
			t.Errorf("flavor %s upsert contains VALUES(): %s", f, stmt)
		}
	}
	if vitessFamily < 2 {
		t.Fatalf("found %d vtgate flavors; want planetscale and vitess at least — the walk is not reaching them", vitessFamily)
	}
}

// TestClassifyApplierError_VindexUpdateRefusalIsMarkedTerminal pins the
// SHARDED-TARGET-VINDEX-UPDATE classification on vtgate's two measured
// VT12001 spellings (an UPDATE, and an ON DUPLICATE KEY UPDATE), and that an
// unrelated 1235 stays the bare terminal it was.
func TestClassifyApplierError_VindexUpdateRefusalIsMarkedTerminal(t *testing.T) {
	for _, msg := range []string{
		"VT12001: unsupported: you cannot UPDATE primary vindex columns; invalid update on vindex: hash",
		"VT12001: unsupported: DML cannot update vindex column",
	} {
		err := classifyApplierError(fmt.Errorf("mysql: applier: update `test`.`t_np`: %w", &gomysql.MySQLError{Number: 1235, Message: msg}))
		if !strings.Contains(err.Error(), shardedTargetVindexUpdateMarker) || !strings.Contains(err.Error(), "t_np") {
			t.Errorf("%q classified as %v; want the %s marker naming the table", msg, err, shardedTargetVindexUpdateMarker)
		}
		if !ir.IsTerminal(err) || applymarks.Transient(err) {
			t.Errorf("%q: want terminal, not retriable", msg)
		}
	}
	other := classifyApplierError(&gomysql.MySQLError{Number: 1235, Message: "This version of MySQL doesn't yet support 'x'"})
	if strings.Contains(other.Error(), shardedTargetVindexUpdateMarker) {
		t.Errorf("an unrelated 1235 was marked: %v", other)
	}
}
