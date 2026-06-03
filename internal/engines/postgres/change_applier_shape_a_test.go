// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package postgres

import (
	"testing"

	"sluicesync.dev/sluice/internal/ir"
)

// Postgres-side mirror of the MySQL applier's Shape-A applier
// pin. Both engines must stamp the same scope; testing each
// independently keeps the parity from silently slipping.

func TestSetShardColumn_StampsAllRowBearingChanges(t *testing.T) {
	a := &ChangeApplier{}
	a.SetShardColumn("source_shard_id", "us-east-1")

	insert := ir.Insert{Row: ir.Row{"customer_id": int64(1)}}
	a.stampShardChange(insert)
	if got := insert.Row["source_shard_id"]; got != "us-east-1" {
		t.Errorf("Insert.Row stamp = %v; want us-east-1", got)
	}

	update := ir.Update{
		Before: ir.Row{"customer_id": int64(1)},
		After:  ir.Row{"customer_id": int64(1), "email": "x@y"},
	}
	a.stampShardChange(update)
	if got := update.Before["source_shard_id"]; got != "us-east-1" {
		t.Errorf("Update.Before stamp = %v; want us-east-1", got)
	}
	if got := update.After["source_shard_id"]; got != "us-east-1" {
		t.Errorf("Update.After stamp = %v; want us-east-1", got)
	}

	del := ir.Delete{Before: ir.Row{"customer_id": int64(1)}}
	a.stampShardChange(del)
	if got := del.Before["source_shard_id"]; got != "us-east-1" {
		t.Errorf("Delete.Before stamp = %v; want us-east-1", got)
	}
}

func TestSetShardColumn_EmptyNameNoOp(t *testing.T) {
	a := &ChangeApplier{}
	row := ir.Row{"customer_id": int64(1)}
	a.stampShardChange(ir.Insert{Row: row})
	if _, ok := row["source_shard_id"]; ok {
		t.Errorf("expected no stamp when shardColumn empty; got %v", row)
	}
}

func TestSetShardColumn_NonRowChangesPassThrough(_ *testing.T) {
	a := &ChangeApplier{}
	a.SetShardColumn("source_shard_id", "us-east-1")
	a.stampShardChange(ir.TxBegin{})
	a.stampShardChange(ir.TxCommit{})
	a.stampShardChange(ir.Truncate{Schema: "s", Table: "t"})
	a.stampShardChange(ir.Insert{Row: nil})
	a.stampShardChange(ir.Update{Before: nil, After: nil})
	a.stampShardChange(ir.Delete{Before: nil})
}

func TestSetShardColumn_Idempotent(t *testing.T) {
	a := &ChangeApplier{}
	a.SetShardColumn("shard", "v1")
	a.SetShardColumn("shard", "v2")
	row := ir.Row{}
	a.stampShardChange(ir.Insert{Row: row})
	if got := row["shard"]; got != "v2" {
		t.Errorf("second SetShardColumn = %v; want v2", got)
	}
}

func TestApplierImplementsShardColumnSetter(_ *testing.T) {
	var _ ir.ShardColumnSetter = (*ChangeApplier)(nil)
}
