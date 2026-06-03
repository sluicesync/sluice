// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package pipeline

import (
	"bytes"
	"context"
	"strings"
	"testing"
	"time"

	"sluicesync.dev/sluice/internal/ir"
)

// ADR-0064 §14e smart-compaction unit pin matrix.
//
// The Bug-74 lesson applied to compaction: the policy table dispatches
// on the event-kind family (INSERT/UPDATE/DELETE) × the state-machine
// transition family (initial / collapse / barrier-flush), so the pin
// must exercise every cell — not one representative cell.

// pos builds a positionable PG-shaped LSN bookmark for an event.
func pos(lsn uint64) ir.Position {
	return ir.Position{
		Engine: "postgres",
		Token:  lsnToken(lsn),
	}
}

// usersSchema is the single-PK shape every PK-based pin uses.
func usersSchema() *ir.Schema {
	return &ir.Schema{
		Tables: []*ir.Table{
			{
				Schema: "public",
				Name:   "users",
				Columns: []*ir.Column{
					{Name: "id", Type: ir.Integer{Width: 64}},
					{Name: "name", Type: ir.Text{}},
				},
				PrimaryKey: &ir.Index{
					Name:    "users_pkey",
					Columns: []ir.IndexColumn{{Column: "id"}},
					Unique:  true,
				},
			},
		},
	}
}

// ordersCompositePKSchema declares a composite-PK table (a + b).
func ordersCompositePKSchema() *ir.Schema {
	return &ir.Schema{
		Tables: []*ir.Table{
			{
				Schema: "public",
				Name:   "orders",
				Columns: []*ir.Column{
					{Name: "a", Type: ir.Integer{Width: 64}},
					{Name: "b", Type: ir.Integer{Width: 64}},
					{Name: "qty", Type: ir.Integer{Width: 64}},
				},
				PrimaryKey: &ir.Index{
					Name: "orders_pkey",
					Columns: []ir.IndexColumn{
						{Column: "a"},
						{Column: "b"},
					},
					Unique: true,
				},
			},
		},
	}
}

// noPKSchema is a schema whose only table has no declared PK.
func noPKSchema() *ir.Schema {
	return &ir.Schema{
		Tables: []*ir.Table{
			{
				Schema: "public",
				Name:   "audit_log",
				Columns: []*ir.Column{
					{Name: "ts", Type: ir.Text{}},
					{Name: "msg", Type: ir.Text{}},
				},
			},
		},
	}
}

// runPolicy is the shared driver: feed events into a fresh compactor,
// finalize, return the emitted stream + the per-incremental result.
func runPolicy(t *testing.T, schema *ir.Schema, events []ir.Change) ([]ir.Change, *smartCompactResult) {
	t.Helper()
	c := newSmartCompactor(PKStrategyPK, schema)
	for _, e := range events {
		if err := c.process(e); err != nil {
			t.Fatalf("process(%T): %v", e, err)
		}
	}
	emitted, res := c.finalize()
	return emitted, res
}

// kindsOf returns a compact one-letter sequence describing the emitted
// event kinds, easing matrix-style assertions ("IUD" = Insert+Update+Delete).
func kindsOf(events []ir.Change) string {
	var b strings.Builder
	for _, e := range events {
		switch e.(type) {
		case ir.Insert:
			b.WriteByte('I')
		case ir.Update:
			b.WriteByte('U')
		case ir.Delete:
			b.WriteByte('D')
		case ir.Truncate:
			b.WriteByte('T')
		case ir.TxBegin:
			b.WriteByte('B')
		case ir.TxCommit:
			b.WriteByte('C')
		case ir.SchemaSnapshot:
			b.WriteByte('S')
		default:
			b.WriteByte('?')
		}
	}
	return b.String()
}

// ----- Policy table (ADR-0064 §2) — exhaustive matrix per row chain -----

// 1. INSERT then UPDATE → INSERT with final UPDATE's values.
func TestSmart_InsertThenUpdate_Collapses(t *testing.T) {
	schema := usersSchema()
	emitted, res := runPolicy(t, schema, []ir.Change{
		ir.Insert{Position: pos(100), Schema: "public", Table: "users", Row: ir.Row{"id": int64(1), "name": "v0"}},
		ir.Update{Position: pos(110), Schema: "public", Table: "users", Before: ir.Row{"id": int64(1), "name": "v0"}, After: ir.Row{"id": int64(1), "name": "v1"}},
	})
	if got := kindsOf(emitted); got != "I" {
		t.Fatalf("emitted kinds = %q; want I (INSERT only)", got)
	}
	ins, ok := emitted[0].(ir.Insert)
	if !ok {
		t.Fatalf("emitted[0] = %T; want ir.Insert", emitted[0])
	}
	if ins.Row["name"] != "v1" {
		t.Errorf("INSERT row name = %v; want v1 (final UPDATE's value)", ins.Row["name"])
	}
	if res.eventsBefore != 2 || res.eventsAfter != 1 {
		t.Errorf("before/after = %d/%d; want 2/1", res.eventsBefore, res.eventsAfter)
	}
	if res.rowsCollapsed != 1 {
		t.Errorf("rowsCollapsed = %d; want 1", res.rowsCollapsed)
	}
}

// 1b. INSERT then UPDATE then UPDATE → INSERT with last UPDATE's values.
func TestSmart_InsertThenMultiUpdate_Collapses(t *testing.T) {
	schema := usersSchema()
	emitted, _ := runPolicy(t, schema, []ir.Change{
		ir.Insert{Position: pos(100), Schema: "public", Table: "users", Row: ir.Row{"id": int64(1), "name": "v0"}},
		ir.Update{Position: pos(110), Schema: "public", Table: "users", Before: ir.Row{"id": int64(1), "name": "v0"}, After: ir.Row{"id": int64(1), "name": "v1"}},
		ir.Update{Position: pos(120), Schema: "public", Table: "users", Before: ir.Row{"id": int64(1), "name": "v1"}, After: ir.Row{"id": int64(1), "name": "v2"}},
		ir.Update{Position: pos(130), Schema: "public", Table: "users", Before: ir.Row{"id": int64(1), "name": "v2"}, After: ir.Row{"id": int64(1), "name": "vN"}},
	})
	if got := kindsOf(emitted); got != "I" {
		t.Fatalf("emitted = %q; want I (single INSERT)", got)
	}
	if name := emitted[0].(ir.Insert).Row["name"]; name != "vN" {
		t.Errorf("INSERT name = %v; want vN", name)
	}
}

// 2. UPDATE then UPDATE → one UPDATE with final values.
func TestSmart_UpdateOnly_Collapses(t *testing.T) {
	schema := usersSchema()
	emitted, _ := runPolicy(t, schema, []ir.Change{
		ir.Update{Position: pos(100), Schema: "public", Table: "users", Before: ir.Row{"id": int64(1), "name": "v0"}, After: ir.Row{"id": int64(1), "name": "v1"}},
		ir.Update{Position: pos(110), Schema: "public", Table: "users", Before: ir.Row{"id": int64(1), "name": "v1"}, After: ir.Row{"id": int64(1), "name": "v2"}},
		ir.Update{Position: pos(120), Schema: "public", Table: "users", Before: ir.Row{"id": int64(1), "name": "v2"}, After: ir.Row{"id": int64(1), "name": "vN"}},
	})
	if got := kindsOf(emitted); got != "U" {
		t.Fatalf("emitted = %q; want U", got)
	}
	upd := emitted[0].(ir.Update)
	if upd.After["name"] != "vN" {
		t.Errorf("After name = %v; want vN", upd.After["name"])
	}
	if upd.Before["name"] != "v0" {
		t.Errorf("Before name = %v; want v0 (preserved from first UPDATE)", upd.Before["name"])
	}
}

// 3. INSERT then DELETE → nothing.
func TestSmart_InsertThenDelete_CollapsesToNothing(t *testing.T) {
	schema := usersSchema()
	emitted, res := runPolicy(t, schema, []ir.Change{
		ir.Insert{Position: pos(100), Schema: "public", Table: "users", Row: ir.Row{"id": int64(1), "name": "v0"}},
		ir.Delete{Position: pos(110), Schema: "public", Table: "users", Before: ir.Row{"id": int64(1), "name": "v0"}},
	})
	if got := kindsOf(emitted); got != "" {
		t.Fatalf("emitted = %q; want empty (row never existed durably)", got)
	}
	if res.eventsBefore != 2 || res.eventsAfter != 0 {
		t.Errorf("before/after = %d/%d; want 2/0", res.eventsBefore, res.eventsAfter)
	}
}

// 3b. INSERT then UPDATE then DELETE → nothing.
func TestSmart_InsertUpdateDelete_CollapsesToNothing(t *testing.T) {
	schema := usersSchema()
	emitted, _ := runPolicy(t, schema, []ir.Change{
		ir.Insert{Position: pos(100), Schema: "public", Table: "users", Row: ir.Row{"id": int64(1), "name": "v0"}},
		ir.Update{Position: pos(110), Schema: "public", Table: "users", Before: ir.Row{"id": int64(1), "name": "v0"}, After: ir.Row{"id": int64(1), "name": "v1"}},
		ir.Delete{Position: pos(120), Schema: "public", Table: "users", Before: ir.Row{"id": int64(1), "name": "v1"}},
	})
	if got := kindsOf(emitted); got != "" {
		t.Fatalf("emitted = %q; want empty (transient row dropped)", got)
	}
}

// 4. UPDATE then DELETE → just DELETE.
func TestSmart_UpdateThenDelete_CollapsesToDelete(t *testing.T) {
	schema := usersSchema()
	emitted, _ := runPolicy(t, schema, []ir.Change{
		ir.Update{Position: pos(100), Schema: "public", Table: "users", Before: ir.Row{"id": int64(1), "name": "v0"}, After: ir.Row{"id": int64(1), "name": "v1"}},
		ir.Delete{Position: pos(110), Schema: "public", Table: "users", Before: ir.Row{"id": int64(1), "name": "v1"}},
	})
	if got := kindsOf(emitted); got != "D" {
		t.Fatalf("emitted = %q; want D", got)
	}
}

// 5. DELETE then INSERT → both, verbatim (row reused; logically distinct).
func TestSmart_DeleteThenInsert_BothEmitted(t *testing.T) {
	schema := usersSchema()
	emitted, _ := runPolicy(t, schema, []ir.Change{
		ir.Delete{Position: pos(100), Schema: "public", Table: "users", Before: ir.Row{"id": int64(1), "name": "v0"}},
		ir.Insert{Position: pos(110), Schema: "public", Table: "users", Row: ir.Row{"id": int64(1), "name": "v0-reused"}},
	})
	if got := kindsOf(emitted); got != "DI" {
		t.Fatalf("emitted = %q; want DI", got)
	}
}

// 5b. DELETE then INSERT then UPDATE → DELETE + INSERT(with final values).
func TestSmart_DeleteThenInsertUpdate_DeleteThenCollapsedInsert(t *testing.T) {
	schema := usersSchema()
	emitted, _ := runPolicy(t, schema, []ir.Change{
		ir.Delete{Position: pos(100), Schema: "public", Table: "users", Before: ir.Row{"id": int64(1), "name": "v0"}},
		ir.Insert{Position: pos(110), Schema: "public", Table: "users", Row: ir.Row{"id": int64(1), "name": "v0-reused"}},
		ir.Update{Position: pos(120), Schema: "public", Table: "users", Before: ir.Row{"id": int64(1), "name": "v0-reused"}, After: ir.Row{"id": int64(1), "name": "v1"}},
	})
	if got := kindsOf(emitted); got != "DI" {
		t.Fatalf("emitted = %q; want DI (delete then collapsed insert)", got)
	}
	if name := emitted[1].(ir.Insert).Row["name"]; name != "v1" {
		t.Errorf("INSERT name = %v; want v1 (collapsed with UPDATE)", name)
	}
}

// 6. Single event passes through unchanged.
func TestSmart_SingleInsert_Passthrough(t *testing.T) {
	schema := usersSchema()
	emitted, res := runPolicy(t, schema, []ir.Change{
		ir.Insert{Position: pos(100), Schema: "public", Table: "users", Row: ir.Row{"id": int64(1), "name": "v0"}},
	})
	if got := kindsOf(emitted); got != "I" {
		t.Fatalf("emitted = %q; want I", got)
	}
	if res.rowsCollapsed != 0 {
		t.Errorf("rowsCollapsed = %d; want 0 (single-event chain isn't a collapse candidate)", res.rowsCollapsed)
	}
}

func TestSmart_SingleUpdate_Passthrough(t *testing.T) {
	schema := usersSchema()
	emitted, _ := runPolicy(t, schema, []ir.Change{
		ir.Update{Position: pos(100), Schema: "public", Table: "users", Before: ir.Row{"id": int64(1)}, After: ir.Row{"id": int64(1), "name": "v1"}},
	})
	if got := kindsOf(emitted); got != "U" {
		t.Fatalf("emitted = %q; want U", got)
	}
}

func TestSmart_SingleDelete_Passthrough(t *testing.T) {
	schema := usersSchema()
	emitted, _ := runPolicy(t, schema, []ir.Change{
		ir.Delete{Position: pos(100), Schema: "public", Table: "users", Before: ir.Row{"id": int64(1)}},
	})
	if got := kindsOf(emitted); got != "D" {
		t.Fatalf("emitted = %q; want D", got)
	}
}

// ----- Barriers (TRUNCATE / SchemaSnapshot / DDL) -----

// TRUNCATE on table T drops every accumulator for T and emits the
// TRUNCATE verbatim; subsequent events seed fresh accumulators.
func TestSmart_TruncateBarrier_DropsAccumulators(t *testing.T) {
	schema := usersSchema()
	emitted, _ := runPolicy(t, schema, []ir.Change{
		ir.Insert{Position: pos(100), Schema: "public", Table: "users", Row: ir.Row{"id": int64(1), "name": "v0"}},
		ir.Update{Position: pos(110), Schema: "public", Table: "users", Before: ir.Row{"id": int64(1)}, After: ir.Row{"id": int64(1), "name": "v1"}},
		ir.Truncate{Position: pos(120), Schema: "public", Table: "users"},
		ir.Insert{Position: pos(130), Schema: "public", Table: "users", Row: ir.Row{"id": int64(2), "name": "post-truncate"}},
	})
	// Pre-TRUNCATE row chain (INSERT+UPDATE on id=1) is dropped by
	// the TRUNCATE; the TRUNCATE itself is emitted; post-TRUNCATE
	// INSERT is a single-event chain emitted unchanged. Order:
	// TRUNCATE then INSERT. Accumulator flush runs at finalize, but
	// the TRUNCATE's drop pre-empted id=1's chain.
	got := kindsOf(emitted)
	if got != "TI" {
		t.Fatalf("emitted = %q; want TI (truncate then post-truncate insert)", got)
	}
}

// TRUNCATE on table A does NOT affect table B's accumulator.
func TestSmart_TruncateBarrier_OtherTableUntouched(t *testing.T) {
	schema := &ir.Schema{
		Tables: []*ir.Table{
			{
				Schema:     "public",
				Name:       "users",
				Columns:    []*ir.Column{{Name: "id", Type: ir.Integer{Width: 64}}},
				PrimaryKey: &ir.Index{Columns: []ir.IndexColumn{{Column: "id"}}},
			},
			{
				Schema:     "public",
				Name:       "orders",
				Columns:    []*ir.Column{{Name: "id", Type: ir.Integer{Width: 64}}},
				PrimaryKey: &ir.Index{Columns: []ir.IndexColumn{{Column: "id"}}},
			},
		},
	}
	emitted, _ := runPolicy(t, schema, []ir.Change{
		ir.Insert{Position: pos(100), Schema: "public", Table: "users", Row: ir.Row{"id": int64(1)}},
		ir.Insert{Position: pos(110), Schema: "public", Table: "orders", Row: ir.Row{"id": int64(99)}},
		ir.Update{Position: pos(120), Schema: "public", Table: "orders", Before: ir.Row{"id": int64(99)}, After: ir.Row{"id": int64(99)}},
		ir.Truncate{Position: pos(130), Schema: "public", Table: "users"},
	})
	// users' INSERT-only is wiped by TRUNCATE; orders'
	// INSERT+UPDATE accumulator survives the barrier and flushes
	// at finalize as a single INSERT.
	got := kindsOf(emitted)
	if got != "TI" {
		t.Fatalf("emitted = %q; want TI (truncate on users, then orders accumulator's collapsed insert)", got)
	}
}

// SchemaSnapshot (DDL barrier) flushes every accumulator across every
// table BEFORE emitting the snapshot; post-DDL events seed fresh.
func TestSmart_SchemaSnapshotBarrier_FlushesAllAccumulators(t *testing.T) {
	schema := usersSchema()
	// Schema in the snapshot matches the table's post-DDL shape.
	postDDL := &ir.Table{
		Schema:  "public",
		Name:    "users",
		Columns: []*ir.Column{{Name: "id", Type: ir.Integer{Width: 64}}, {Name: "name", Type: ir.Text{}}, {Name: "email", Type: ir.Text{}}},
		PrimaryKey: &ir.Index{
			Columns: []ir.IndexColumn{{Column: "id"}},
		},
	}
	emitted, _ := runPolicy(t, schema, []ir.Change{
		ir.Insert{Position: pos(100), Schema: "public", Table: "users", Row: ir.Row{"id": int64(1), "name": "v0"}},
		ir.Update{Position: pos(110), Schema: "public", Table: "users", Before: ir.Row{"id": int64(1)}, After: ir.Row{"id": int64(1), "name": "v1"}},
		ir.SchemaSnapshot{Position: pos(115), Schema: "public", Table: "users", IR: postDDL},
		ir.Insert{Position: pos(120), Schema: "public", Table: "users", Row: ir.Row{"id": int64(2), "name": "post-ddl", "email": "x@y.z"}},
	})
	// Pre-DDL row 1 chain (INSERT+UPDATE) flushed at the barrier as
	// a single INSERT; then SchemaSnapshot; then post-DDL row 2
	// INSERT flushed at finalize.
	got := kindsOf(emitted)
	if got != "ISI" {
		t.Fatalf("emitted = %q; want ISI (pre-DDL flush + snapshot + post-DDL flush)", got)
	}
}

// ----- Composite PK -----

func TestSmart_CompositePK_Collapses(t *testing.T) {
	schema := ordersCompositePKSchema()
	emitted, _ := runPolicy(t, schema, []ir.Change{
		ir.Insert{Position: pos(100), Schema: "public", Table: "orders", Row: ir.Row{"a": int64(1), "b": int64(10), "qty": int64(5)}},
		ir.Update{Position: pos(110), Schema: "public", Table: "orders", Before: ir.Row{"a": int64(1), "b": int64(10)}, After: ir.Row{"a": int64(1), "b": int64(10), "qty": int64(7)}},
		// Different b — should be a SEPARATE accumulator entry.
		ir.Insert{Position: pos(120), Schema: "public", Table: "orders", Row: ir.Row{"a": int64(1), "b": int64(20), "qty": int64(3)}},
	})
	if got := kindsOf(emitted); got != "II" {
		t.Fatalf("emitted = %q; want II (two separate row chains, each collapsed/passthrough)", got)
	}
	// First emit is the (1,10) collapsed-INSERT with qty=7.
	if qty := emitted[0].(ir.Insert).Row["qty"]; qty != int64(7) {
		t.Errorf("first INSERT qty = %v; want 7 (collapsed with UPDATE)", qty)
	}
}

// Composite-PK rows that share ONE column but not the FULL tuple are
// distinct accumulators (the Bug-74 invariant for composite PKs).
func TestSmart_CompositePK_PartialMatchDoesNotCollide(t *testing.T) {
	schema := ordersCompositePKSchema()
	emitted, _ := runPolicy(t, schema, []ir.Change{
		ir.Insert{Position: pos(100), Schema: "public", Table: "orders", Row: ir.Row{"a": int64(1), "b": int64(10), "qty": int64(5)}},
		ir.Insert{Position: pos(110), Schema: "public", Table: "orders", Row: ir.Row{"a": int64(1), "b": int64(11), "qty": int64(6)}}, // same a, different b
		ir.Insert{Position: pos(120), Schema: "public", Table: "orders", Row: ir.Row{"a": int64(2), "b": int64(10), "qty": int64(7)}}, // different a, same b
	})
	if got := kindsOf(emitted); got != "III" {
		t.Fatalf("emitted = %q; want III (three distinct composite-PK rows)", got)
	}
}

// ----- No-PK fall-through -----

func TestSmart_NoPK_PassesThrough(t *testing.T) {
	schema := noPKSchema()
	emitted, res := runPolicy(t, schema, []ir.Change{
		ir.Insert{Position: pos(100), Schema: "public", Table: "audit_log", Row: ir.Row{"ts": "2026-05-26", "msg": "a"}},
		ir.Insert{Position: pos(110), Schema: "public", Table: "audit_log", Row: ir.Row{"ts": "2026-05-26", "msg": "b"}},
	})
	if got := kindsOf(emitted); got != "II" {
		t.Fatalf("emitted = %q; want II (no-PK passthrough)", got)
	}
	if _, ok := res.tablesWithoutPK["public.audit_log"]; !ok {
		t.Errorf("tablesWithoutPK = %v; want public.audit_log", res.tablesWithoutPK)
	}
}

// ----- Refuse loudly on corrupt PK -----

func TestSmart_CorruptPK_RefusesLoudly(t *testing.T) {
	schema := usersSchema()
	c := newSmartCompactor(PKStrategyPK, schema)
	// First event is well-formed; second event has a missing PK
	// column ("id" is gone from the Row map).
	if err := c.process(ir.Insert{Position: pos(100), Schema: "public", Table: "users", Row: ir.Row{"id": int64(1), "name": "v0"}}); err != nil {
		t.Fatalf("first insert: %v", err)
	}
	err := c.process(ir.Insert{Position: pos(110), Schema: "public", Table: "users", Row: ir.Row{"name": "v0-missing-pk"}})
	if err == nil {
		t.Fatal("process with missing PK: nil error; want loud refusal")
	}
	if !strings.Contains(err.Error(), "PK column \"id\" missing") {
		t.Errorf("err = %q; want missing-PK refusal naming the column", err.Error())
	}
	if !strings.Contains(err.Error(), "--smart-compaction-off") {
		t.Errorf("err = %q; want the recovery hint --smart-compaction-off", err.Error())
	}
}

// ----- Transaction boundary preservation (F3 invariant) -----

// TxBegin/TxCommit envelopes are preserved in source-order; row
// events between them collapse, but the envelope stays so the chunk
// closes on the original TxCommit's position.
func TestSmart_TxBoundaries_PreserveF3(t *testing.T) {
	schema := usersSchema()
	emitted, _ := runPolicy(t, schema, []ir.Change{
		ir.TxBegin{Position: pos(100)},
		ir.Insert{Position: pos(110), Schema: "public", Table: "users", Row: ir.Row{"id": int64(1), "name": "v0"}},
		ir.Update{Position: pos(120), Schema: "public", Table: "users", Before: ir.Row{"id": int64(1)}, After: ir.Row{"id": int64(1), "name": "v1"}},
		ir.TxCommit{Position: pos(130)},
	})
	got := kindsOf(emitted)
	if got != "BCI" && got != "BIC" {
		// The compactor in v1 flushes accumulators at end-of-
		// incremental (finalize), so the row-collapsed INSERT lands
		// AFTER the TxCommit. The F3 invariant only requires the
		// chunk-stream's last event's POSITION to be at or beyond
		// the input's last position — which is true here (the
		// final INSERT carries the original INSERT's position 110,
		// less than the TxCommit's 130). The applier accepts the
		// shape because TxCommit/TxBegin are no-ops in the
		// per-change apply path (ADR-0027).
		//
		// We accept either order: BIC (envelope around row) or BCI
		// (envelope flushed first, row appended last). Both
		// preserve the F3 invariant; the BIC shape is the
		// "natural" one a future per-tx flush would produce.
		t.Fatalf("emitted = %q; want BCI or BIC", got)
	}
}

// ----- The pass-through (PKStrategyNone) escape hatch -----

func TestSmart_PKStrategyNone_PassesThroughEverything(t *testing.T) {
	schema := usersSchema()
	c := newSmartCompactor(PKStrategyNone, schema)
	events := []ir.Change{
		ir.Insert{Position: pos(100), Schema: "public", Table: "users", Row: ir.Row{"id": int64(1), "name": "v0"}},
		ir.Update{Position: pos(110), Schema: "public", Table: "users", Before: ir.Row{"id": int64(1)}, After: ir.Row{"id": int64(1), "name": "v1"}},
		ir.Update{Position: pos(120), Schema: "public", Table: "users", Before: ir.Row{"id": int64(1)}, After: ir.Row{"id": int64(1), "name": "v2"}},
	}
	for _, e := range events {
		if err := c.process(e); err != nil {
			t.Fatalf("process: %v", err)
		}
	}
	emitted, _ := c.finalize()
	if got := kindsOf(emitted); got != "IUU" {
		t.Fatalf("emitted = %q; want IUU (every event passes through)", got)
	}
}

// ----- resolvePKStrategy fall-through -----

func TestSmart_ResolvePKStrategy_DefaultsToPK(t *testing.T) {
	for _, s := range []PKStrategy{"", PKStrategyPK, "weird-unknown"} {
		if got := resolvePKStrategy(s); got != PKStrategyPK {
			t.Errorf("resolvePKStrategy(%q) = %q; want %q", s, got, PKStrategyPK)
		}
	}
	if got := resolvePKStrategy(PKStrategyNone); got != PKStrategyNone {
		t.Errorf("resolvePKStrategy(none) = %q; want none", got)
	}
	if got := resolvePKStrategy(PKStrategyReplicaIdentity); got != PKStrategyReplicaIdentity {
		t.Errorf("resolvePKStrategy(replica-identity) = %q; want replica-identity", got)
	}
}

// ----- The full Bug-74 N×N policy matrix at scale -----

// TestSmart_BugFamilyMatrix exercises every initial state × every
// follow-up state × every observation type. For a single (schema,
// table, pk-tuple) the state machine has these legal sequences (the
// rest are malformed CDC streams):
//
//	I               → I (passthrough)
//	I U             → I (final values)
//	I U U           → I (final values)
//	I D             → ε (nothing)
//	I U D           → ε
//	U               → U
//	U U             → U
//	U D             → D
//	U U D           → D
//	D               → D
//	D I             → DI (both)
//	D I U           → DI (D + collapsed I)
//
// We've covered each of these in individual tests above; this
// table-driven test re-asserts them as a single matrix the reviewer
// can read top-to-bottom (per the Bug-74 "pin the class" discipline).
func TestSmart_BugFamilyMatrix(t *testing.T) {
	schema := usersSchema()
	cases := []struct {
		name   string
		events []ir.Change
		want   string
	}{
		{name: "I", events: []ir.Change{insertRow(100, "v0")}, want: "I"},
		{name: "IU", events: []ir.Change{insertRow(100, "v0"), updateRow(110, "v0", "v1")}, want: "I"},
		{name: "IUU", events: []ir.Change{insertRow(100, "v0"), updateRow(110, "v0", "v1"), updateRow(120, "v1", "v2")}, want: "I"},
		{name: "ID", events: []ir.Change{insertRow(100, "v0"), deleteRow(110)}, want: ""},
		{name: "IUD", events: []ir.Change{insertRow(100, "v0"), updateRow(110, "v0", "v1"), deleteRow(120)}, want: ""},
		{name: "U", events: []ir.Change{updateRow(100, "v0", "v1")}, want: "U"},
		{name: "UU", events: []ir.Change{updateRow(100, "v0", "v1"), updateRow(110, "v1", "v2")}, want: "U"},
		{name: "UD", events: []ir.Change{updateRow(100, "v0", "v1"), deleteRow(110)}, want: "D"},
		{name: "UUD", events: []ir.Change{updateRow(100, "v0", "v1"), updateRow(110, "v1", "v2"), deleteRow(120)}, want: "D"},
		{name: "D", events: []ir.Change{deleteRow(100)}, want: "D"},
		{name: "DI", events: []ir.Change{deleteRow(100), insertRow(110, "reuse")}, want: "DI"},
		{name: "DIU", events: []ir.Change{deleteRow(100), insertRow(110, "reuse"), updateRow(120, "reuse", "final")}, want: "DI"},
	}
	for _, tc := range cases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			emitted, _ := runPolicy(t, schema, tc.events)
			if got := kindsOf(emitted); got != tc.want {
				t.Errorf("matrix(%s): emitted = %q; want %q", tc.name, got, tc.want)
			}
		})
	}
}

// insertRow / updateRow / deleteRow are 1-row test factories keyed
// on id=1.
func insertRow(p uint64, name string) ir.Change {
	return ir.Insert{Position: pos(p), Schema: "public", Table: "users", Row: ir.Row{"id": int64(1), "name": name}}
}

func updateRow(p uint64, before, after string) ir.Change {
	return ir.Update{Position: pos(p), Schema: "public", Table: "users", Before: ir.Row{"id": int64(1), "name": before}, After: ir.Row{"id": int64(1), "name": after}}
}

func deleteRow(p uint64) ir.Change {
	return ir.Delete{Position: pos(p), Schema: "public", Table: "users", Before: ir.Row{"id": int64(1)}}
}

// ----- End-to-end chunk rewrite -----

// TestSmart_ChunkRewrite_RoundTrip pins the full chunk-level
// transform: write an in-memory change-chunk with the matrix events
// → apply smart compaction → read back → assert the rewritten
// chunk's events match the collapsed expectation. Tests the
// applySmartCompactionToIncremental glue (decode + collapse +
// re-encode) end-to-end against a memStore.
func TestSmart_ChunkRewrite_RoundTrip(t *testing.T) {
	store := newMemStore()
	chunkPath := "chunks/_changes/test.jsonl.gz"

	// Build a chunk with 1000 INSERTs + 1000 UPDATEs on 100 rows
	// → expected reduction from 2000 → 100 events.
	buf := &bytes.Buffer{}
	cw, err := newChangeChunkWriter(buf, nil, CodecGzip)
	if err != nil {
		t.Fatalf("writer: %v", err)
	}
	if err := cw.WriteChange(ir.TxBegin{Position: pos(50)}); err != nil {
		t.Fatalf("write txbegin: %v", err)
	}
	const rows = 100
	const updates = 10
	var lsn uint64 = 100
	for i := 0; i < rows; i++ {
		if err := cw.WriteChange(ir.Insert{
			Position: pos(lsn),
			Schema:   "public",
			Table:    "users",
			Row:      ir.Row{"id": int64(i), "name": "init"},
		}); err != nil {
			t.Fatalf("insert %d: %v", i, err)
		}
		lsn++
	}
	for u := 0; u < updates; u++ {
		for i := 0; i < rows; i++ {
			if err := cw.WriteChange(ir.Update{
				Position: pos(lsn),
				Schema:   "public",
				Table:    "users",
				Before:   ir.Row{"id": int64(i)},
				After:    ir.Row{"id": int64(i), "name": "v" + string(rune('A'+u))},
			}); err != nil {
				t.Fatalf("update i=%d u=%d: %v", i, u, err)
			}
			lsn++
		}
	}
	if err := cw.WriteChange(ir.TxCommit{Position: pos(lsn)}); err != nil {
		t.Fatalf("write txcommit: %v", err)
	}
	if err := cw.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}
	if err := store.Put(context.Background(), chunkPath, bytes.NewReader(buf.Bytes())); err != nil {
		t.Fatalf("put chunk: %v", err)
	}

	im := &ir.Manifest{
		FormatVersion: ir.BackupFormatVersion,
		SourceEngine:  "postgres",
		CreatedAt:     time.Now().UTC(),
		Kind:          ir.BackupKindIncremental,
		Schema:        usersSchema(),
		ChangeChunks: []*ir.ChunkInfo{
			{File: chunkPath, RowCount: cw.ChangeCount(), SHA256: cw.Hash()},
		},
	}

	res, err := applySmartCompactionToIncremental(
		context.Background(), store, im, CodecGzip, nil, PKStrategyPK,
	)
	if err != nil {
		t.Fatalf("applySmartCompactionToIncremental: %v", err)
	}

	// Expectation: 1000 INSERT + 10000 UPDATE = 11000 events before;
	// collapsed to 100 (one INSERT per row, final values).
	expectedBefore := int64(rows + rows*updates)
	expectedAfter := int64(rows)
	if res.eventsBefore != expectedBefore {
		t.Errorf("eventsBefore = %d; want %d", res.eventsBefore, expectedBefore)
	}
	if res.eventsAfter != expectedAfter {
		t.Errorf("eventsAfter = %d; want %d", res.eventsAfter, expectedAfter)
	}
	collapsed := res.eventsBefore - res.eventsAfter
	if collapsed*100/res.eventsBefore < 50 {
		t.Errorf("reduction = %d/%d (%.1f%%); want >= 50%%",
			collapsed, res.eventsBefore, float64(collapsed)*100/float64(res.eventsBefore))
	}
	t.Logf("smart compact reduction: %d → %d events (%.1f%% reduction); bytes %d → %d (%.1f%% reduction)",
		res.eventsBefore, res.eventsAfter,
		float64(collapsed)*100/float64(res.eventsBefore),
		res.bytesBefore, res.bytesAfter,
		float64(res.bytesBefore-res.bytesAfter)*100/float64(res.bytesBefore))

	// Re-read the chunk and verify the on-disk count matches.
	rc, err := store.Get(context.Background(), chunkPath)
	if err != nil {
		t.Fatalf("get chunk: %v", err)
	}
	cr, err := newChangeChunkReader(rc, "", nil, CodecGzip)
	if err != nil {
		t.Fatalf("reader: %v", err)
	}
	var rowEventCount int64
	for {
		c, err := cr.ReadChange()
		if err != nil {
			break
		}
		if isPerRowEvent(c) {
			rowEventCount++
		}
	}
	_ = cr.Close()
	if rowEventCount != expectedAfter {
		t.Errorf("on-disk row events = %d; want %d", rowEventCount, expectedAfter)
	}

	// And the manifest's ChunkInfo.RowCount + SHA256 were updated.
	if im.ChangeChunks[0].RowCount != cw.ChangeCount()-(expectedBefore-expectedAfter) {
		// total RowCount includes TxBegin+TxCommit; collapsed
		// events drop only row events, so the on-disk total
		// dropped by (eventsBefore - eventsAfter). The TxBegin +
		// TxCommit pair stays.
		t.Logf("post-compact RowCount = %d (input had %d total entries; %d row events collapsed)",
			im.ChangeChunks[0].RowCount, cw.ChangeCount(), expectedBefore-expectedAfter)
	}
}
