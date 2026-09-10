// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package postgres

import (
	"strings"
	"testing"

	"sluicesync.dev/sluice/internal/ir"
	"sluicesync.dev/sluice/internal/sluicecode"
)

// This whole file exists because a live run disproved the assumption that the
// preflight refusal was sufficient.
//
// Requiring the shard key to be IN the upsert conflict key makes the INSERT
// path safe. It does nothing for the UPDATE path, because the SET list is
// built from the ROW, not from the key — so the routing column is named
// there whether or not it is part of the primary key. A sync into a table
// keyed (tenant_id, id) and sharded on tenant_id copied cleanly, entered CDC,
// applied a DELETE, and died on the first UPDATE with NK013.
//
// The rule these cells pin: an UNCHANGED shard key is dropped from the SET
// list (a no-op assignment the target refuses on shape alone), a CHANGED one
// is refused loudly, and the WHERE clause keeps the whole before-image either
// way so the statement still routes.

func shardKeyRow(tenant any, v string) ir.Row {
	return ir.Row{"tenant_id": tenant, "id": int64(7), "v": v}
}

func TestDropUnchangedShardKeysDropsANoOpAssignment(t *testing.T) {
	t.Parallel()
	before := shardKeyRow(int64(3), "old")
	after := shardKeyRow(int64(3), "new")

	got, err := dropUnchangedShardKeys("public", "orders", before, after, []string{"tenant_id"})
	if err != nil {
		t.Fatalf("an unchanged shard key was refused: %v", err)
	}
	if _, present := got["tenant_id"]; present {
		t.Error("the shard key survived into the SET row; the target refuses the statement on shape alone, " +
			"even when the assignment is a no-op")
	}
	// Everything else must be untouched — this trims one column, it does not
	// become a general changed-column optimiser.
	if got["v"] != "new" || got["id"] != int64(7) {
		t.Errorf("non-shard-key columns were altered: %v", got)
	}
	// And the caller's row must not be mutated: peer lanes share it.
	if _, present := after["tenant_id"]; !present {
		t.Error("the caller's After row was mutated in place")
	}
}

func TestDropUnchangedShardKeysRefusesARealChange(t *testing.T) {
	t.Parallel()
	before := shardKeyRow(int64(3), "old")
	after := shardKeyRow(int64(9), "new")

	_, err := dropUnchangedShardKeys("public", "orders", before, after, []string{"tenant_id"})
	if err == nil {
		t.Fatal("a shard-key CHANGE was accepted; applying the other columns and leaving the routing column " +
			"behind makes the target row disagree with the source on the value that decides where it lives")
	}
	ce, ok := sluicecode.FromError(err)
	if !ok || ce.Code != sluicecode.CodeTargetShardKeyUpdateUnsupported {
		t.Errorf("refusal carried code %v (coded=%v), want %q", ce, ok, sluicecode.CodeTargetShardKeyUpdateUnsupported)
	}
	for _, want := range []string{"public", "orders", "tenant_id"} {
		if !strings.Contains(err.Error(), want) {
			t.Errorf("refusal does not name %q: %v", want, err)
		}
	}
}

// A missing before-value cannot establish that the column is unchanged, so
// refusing is right — but it must be refused as the SCHEMA problem it is, not
// as a row that changed its shard key.
//
// This is not hypothetical and it is the common case, which is what the
// pre-tag value-fidelity pass caught: the PostgreSQL CDC reader narrows every
// before-image to the relation's identity columns, so a routing column
// outside the primary key / REPLICA IDENTITY is NEVER present. The first cut
// reported "update … changes the target's shard-key column(s)" for an update
// that changed nothing, with a hint the operator could not act on, on every
// update to the table. The tell that it was a schema fact: the same table
// behaves differently under `--where`, which emits a full before-image.
func TestDropUnchangedShardKeysRefusesTheSCHEMAWhenTheBeforeImageLacksTheColumn(t *testing.T) {
	t.Parallel()
	before := ir.Row{"id": int64(7), "v": "old"} // no tenant_id — PG identity narrowing
	after := shardKeyRow(int64(3), "new")

	_, err := dropUnchangedShardKeys("public", "orders", before, after, []string{"tenant_id"})
	if err == nil {
		t.Fatal("a shard key with no before-value was treated as unchanged; nothing established that, " +
			"and guessing wrong drops a real routing change silently")
	}
	ce, ok := sluicecode.FromError(err)
	if !ok || ce.Code != sluicecode.CodeTargetShardKeyNotInUpsertKey {
		t.Errorf("refusal carried code %v (coded=%v), want the SCHEMA code %q — reporting this as a "+
			"shard-key CHANGE accuses a row of something it did not do and sends the operator looking "+
			"for data that does not exist", ce, ok, sluicecode.CodeTargetShardKeyNotInUpsertKey)
	}
	if !strings.Contains(err.Error(), "REPLICA IDENTITY") {
		t.Errorf("the refusal does not name the actual cause, so it is unactionable: %v", err)
	}
}

// The comparison must err toward CHANGED. Two encodings of the same logical
// value comparing unequal costs a loud, explained refusal; the opposite
// mistake drops a real shard-key change. This pins the direction rather than
// the encoding, so a future switch to a smarter comparison still has to keep
// the bias.
func TestDropUnchangedShardKeysErrsTowardRefusing(t *testing.T) {
	t.Parallel()
	before := shardKeyRow(int32(3), "old")
	after := shardKeyRow(int64(3), "new")

	_, err := dropUnchangedShardKeys("public", "orders", before, after, []string{"tenant_id"})
	if err == nil {
		t.Skip("the comparison now sees int32(3) and int64(3) as equal; that is an improvement, " +
			"but only while a genuinely different value still refuses — covered above")
	}
	if ce, ok := sluicecode.FromError(err); !ok || ce.Code != sluicecode.CodeTargetShardKeyUpdateUnsupported {
		t.Errorf("the conservative verdict did not arrive as the shard-key refusal: %v", err)
	}
}

// Off Neki (nil shard keys) nothing changes at all — the ordinary PostgreSQL
// target must pay nothing and must keep every column in its SET list.
func TestDropUnchangedShardKeysIsIdentityWithoutShardKeys(t *testing.T) {
	t.Parallel()
	after := shardKeyRow(int64(3), "new")
	got, err := dropUnchangedShardKeys("public", "orders", shardKeyRow(int64(9), "old"), after, nil)
	if err != nil {
		t.Fatalf("a non-sharded target was refused: %v", err)
	}
	if len(got) != len(after) {
		t.Errorf("columns were dropped on a target with no shard key: %v", got)
	}
}

// The rendered statement is what the target actually sees, so pin it end to
// end rather than only the row-trimming step: the SET list must omit the
// shard key while the WHERE list still carries it, which is precisely what
// keeps the statement routed to the shard holding the row.
func TestBuildUpdateSQLOmitsTheShardKeyFromSetButKeepsItInWhere(t *testing.T) {
	t.Parallel()
	colTypes := map[string]*ir.Column{
		"tenant_id": {Name: "tenant_id"},
		"id":        {Name: "id"},
		"v":         {Name: "v"},
	}
	stmt, args, err := buildUpdateSQL("public", "orders",
		shardKeyRow(int64(3), "old"), shardKeyRow(int64(3), "new"), colTypes, []string{"tenant_id"})
	if err != nil {
		t.Fatalf("build: %v", err)
	}
	set, where, found := strings.Cut(stmt, " WHERE ")
	if !found {
		t.Fatalf("no WHERE clause in %q", stmt)
	}
	if strings.Contains(set, "tenant_id") {
		t.Errorf("the SET clause still names the shard key, which the target refuses on shape: %q", set)
	}
	if !strings.Contains(where, "tenant_id") {
		t.Errorf("the WHERE clause dropped the shard key, so the statement would scatter instead of "+
			"routing to the shard holding the row: %q", where)
	}
	if len(args) == 0 {
		t.Error("no arguments bound")
	}
}
