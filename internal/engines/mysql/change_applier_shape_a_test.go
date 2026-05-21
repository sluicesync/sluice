// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package mysql

import (
	"testing"

	"github.com/orware/sluice/internal/ir"
)

// TestSetShardColumn_StampsAllRowBearingChanges pins the applier
// half of ADR-0048 Shape A's two-surface split (DP-1 option (a)):
// SetShardColumn wires the discriminator, stampShardChange
// stamps it onto Insert.Row, Update.Before+After, and
// Delete.Before. Non-row changes (TxBegin / TxCommit / Truncate /
// SchemaSnapshot) pass through verbatim.
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

// TestSetShardColumn_EmptyNameNoOp: when the operator hasn't set
// --inject-shard-column, the apply path stamps nothing — the
// hot path stays zero-cost on single-source streams.
func TestSetShardColumn_EmptyNameNoOp(t *testing.T) {
	a := &ChangeApplier{}
	// SetShardColumn never called → shardColumn == "".
	row := ir.Row{"customer_id": int64(1)}
	a.stampShardChange(ir.Insert{Row: row})
	if _, ok := row["source_shard_id"]; ok {
		t.Errorf("expected no stamp when shardColumn empty; got %v", row)
	}
}

// TestSetShardColumn_NonRowChangesPassThrough: TxBegin, TxCommit,
// Truncate, SchemaSnapshot carry no row data — the stamper must
// pass them through without panicking and without trying to
// dereference a nil row.
func TestSetShardColumn_NonRowChangesPassThrough(_ *testing.T) {
	a := &ChangeApplier{}
	a.SetShardColumn("source_shard_id", "us-east-1")
	// No assertions beyond "doesn't panic" — the cases below all
	// drop into the default branch of stampShardChange's type
	// switch and return cleanly.
	a.stampShardChange(ir.TxBegin{})
	a.stampShardChange(ir.TxCommit{})
	a.stampShardChange(ir.Truncate{Schema: "s", Table: "t"})
	// nil row inside an Insert/Update/Delete must also pass
	// through (defensive — engines that hand a nil row shouldn't
	// crash the stamper).
	a.stampShardChange(ir.Insert{Row: nil})
	a.stampShardChange(ir.Update{Before: nil, After: nil})
	a.stampShardChange(ir.Delete{Before: nil})
}

// TestSetShardColumn_Idempotent: calling SetShardColumn twice
// with different values keeps the latter — the streamer's
// re-arm-on-Run pattern is the source of truth for the
// per-applier discriminator.
func TestSetShardColumn_Idempotent(t *testing.T) {
	a := &ChangeApplier{}
	a.SetShardColumn("shard", "v1")
	a.SetShardColumn("shard", "v2")
	row := ir.Row{}
	a.stampShardChange(ir.Insert{Row: row})
	if got := row["shard"]; got != "v2" {
		t.Errorf("second SetShardColumn = %v; want v2", got)
	}
	a.SetShardColumn("", nil)
	row2 := ir.Row{}
	a.stampShardChange(ir.Insert{Row: row2})
	if _, ok := row2["shard"]; ok {
		t.Errorf("empty name should clear wiring; got %v", row2)
	}
}

// TestApplierImplementsShardColumnSetter is the compile-time
// witness that the applier's surface matches
// [ir.ShardColumnSetter] — the orchestrator's
// `applyShardColumn` (added in a follow-up phase) consults this
// via type-assertion, so a typo in the method signature would
// silently de-wire the feature.
func TestApplierImplementsShardColumnSetter(_ *testing.T) {
	var _ ir.ShardColumnSetter = (*ChangeApplier)(nil)
}
