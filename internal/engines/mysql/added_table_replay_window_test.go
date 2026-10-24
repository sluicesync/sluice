// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package mysql

import (
	"reflect"
	"testing"
	"time"

	"sluicesync.dev/sluice/internal/appliershared"
	"sluicesync.dev/sluice/internal/ir"
)

// TestAddedTableReplayWindow_OrdererBoundsTheCopy pins the window predicate
// (Bug 333) over every position family this engine emits: a row change is in
// the added table's window exactly when PositionAtOrAfter(row, copy) is
// false. Rows carry the PRE-transaction position (item 132 on binlog,
// ADR-0190 phase 4 on VStream), so:
//
//   - the copy's last transaction T (in the copy) has row position pre-T,
//     which the copy strictly contains → in the window;
//   - the first transaction after the copy has row position = the copy's
//     own → out of it, applied normally;
//   - anything the orderer cannot place (a shard still behind the copy)
//     stays in it, which is safe: the idempotent form is right whatever the
//     target holds.
//
// If a reader ever stamped the POST-transaction position instead, the copy's
// last transaction would read "at the copy" and leave the window one
// transaction early; the real-server pins named at
// [Engine.AddedTableCopyPosition] hold that premise.
func TestAddedTableReplayWindow_OrdererBoundsTheCopy(t *testing.T) {
	inWindow := func(t *testing.T, e Engine, row, copyPos ir.Position) bool {
		t.Helper()
		past, err := e.PositionAtOrAfter(row, copyPos)
		if err != nil {
			t.Fatalf("PositionAtOrAfter: %v", err)
		}
		return !past
	}
	vanilla, maria := Engine{Flavor: FlavorVanilla}, Engine{Flavor: FlavorMariaDB}
	for _, c := range []struct {
		name     string
		e        Engine
		row, cp  ir.Position
		inWindow bool
	}{
		{"gtid: the copy's last transaction", vanilla, gtidPos(t, uuidA+":1-9"), gtidPos(t, uuidA+":1-10"), true},
		{"gtid: an early transaction", vanilla, gtidPos(t, uuidA+":1-4"), gtidPos(t, uuidA+":1-10"), true},
		{"gtid: the first transaction after the copy", vanilla, gtidPos(t, uuidA+":1-10"), gtidPos(t, uuidA+":1-10"), false},
		{"gtid: well after", vanilla, gtidPos(t, uuidA+":1-20,"+uuidB+":1-3"), gtidPos(t, uuidA+":1-10"), false},
		{"gtid: two servers, one behind", vanilla, gtidPos(t, uuidA+":1-10,"+uuidB+":1-2"), gtidPos(t, uuidA+":1-10,"+uuidB+":1-3"), true},
		{"mariadb: the copy's last transaction", maria, gtidPos(t, "0-1-9"), gtidPos(t, "0-1-10"), true},
		{"mariadb: after", maria, gtidPos(t, "0-1-10"), gtidPos(t, "0-1-10"), false},
		{"mariadb: another domain behind", maria, gtidPos(t, "0-1-12,1-2-3"), gtidPos(t, "0-1-10,1-2-4"), true},
		{"file/pos: a row event before the copy point", vanilla, filePos(t, "binlog.000003", 900, uuidA), filePos(t, "binlog.000003", 1000, uuidA), true},
		{"file/pos: after", vanilla, filePos(t, "binlog.000003", 1100, uuidA), filePos(t, "binlog.000003", 1000, uuidA), false},
		{"file/pos: the next file", vanilla, filePos(t, "binlog.000004", 4, uuidA), filePos(t, "binlog.000003", 1000, uuidA), false},
		{
			"vstream: one shard, the copy's last transaction", vanilla,
			vstreamPos(t, false, sg("ks", "0", "MySQL56/"+uuidA+":1-9")), vstreamPos(t, false, sg("ks", "0", "MySQL56/"+uuidA+":1-10")), true,
		},
		{
			"vstream: one shard, after", vanilla,
			vstreamPos(t, false, sg("ks", "0", "MySQL56/"+uuidA+":1-10")), vstreamPos(t, false, sg("ks", "0", "MySQL56/"+uuidA+":1-10")), false,
		},
		{
			"vstream: a copy cursor on the row position is ignored", vanilla,
			vstreamPos(t, true, sg("ks", "0", "MySQL56/"+uuidA+":1-10")), vstreamPos(t, false, sg("ks", "0", "MySQL56/"+uuidA+":1-10")), false,
		},
		{
			"vstream: two shards, this one past, the other behind", vanilla,
			vstreamPos(t, false, sg("ks", "-80", "MySQL56/"+uuidA+":1-12"), sg("ks", "80-", "MySQL56/"+uuidB+":1-3")),
			vstreamPos(t, false, sg("ks", "-80", "MySQL56/"+uuidA+":1-10"), sg("ks", "80-", "MySQL56/"+uuidB+":1-5")), true,
		},
		{
			"vstream: two shards, both past", vanilla,
			vstreamPos(t, false, sg("ks", "-80", "MySQL56/"+uuidA+":1-12"), sg("ks", "80-", "MySQL56/"+uuidB+":1-5")),
			vstreamPos(t, false, sg("ks", "-80", "MySQL56/"+uuidA+":1-10"), sg("ks", "80-", "MySQL56/"+uuidB+":1-5")), false,
		},
		{
			"vstream: a reshard (the copy's shards gone) never closes", vanilla,
			vstreamPos(t, false, sg("ks", "-40", "MySQL56/"+uuidA+":1-90")),
			vstreamPos(t, false, sg("ks", "-80", "MySQL56/"+uuidA+":1-10")), true,
		},
	} {
		t.Run(c.name, func(t *testing.T) {
			if got := inWindow(t, c.e, c.row, c.cp); got != c.inWindow {
				t.Fatalf("in window = %v, want %v", got, c.inWindow)
			}
		})
	}
	// A shape mismatch is an error (the pipeline refuses it as
	// ADDED-TABLE-COPY-POSITION-UNORDERED), never a guess.
	if _, err := vanilla.PositionAtOrAfter(gtidPos(t, uuidA+":1-9"), vstreamPos(t, false, sg("ks", "0", "MySQL56/"+uuidA+":1-10"))); err == nil {
		t.Fatal("a binlog row position ordered against a VStream copy position; want an error")
	}
}

// TestAddedTableReplayWindow_CodecRoundTripsEveryPositionFamily round-trips a
// copy position from each of this engine's real encoders through the
// added_table_copy_positions codec (the stored text) and back, requiring the
// token byte for byte, the decoded position equal, and the orderer to give
// the same verdicts on the stored copy as on the original (the codec's job,
// stated in what the replay actually asks of it).
func TestAddedTableReplayWindow_CodecRoundTripsEveryPositionFamily(t *testing.T) {
	vanilla, maria := Engine{Flavor: FlavorVanilla}, Engine{Flavor: FlavorMariaDB}
	families := []struct {
		name  string
		e     Engine
		copy  ir.Position
		probe ir.Position // a row position inside the window
	}{
		{"binlog gtid", vanilla, gtidPos(t, uuidA+":1-10,"+uuidB+":1-7"), gtidPos(t, uuidA+":1-9,"+uuidB+":1-7")},
		{"binlog file/pos", vanilla, filePos(t, "binlog.1000000", 4294967295, uuidA), filePos(t, "binlog.999999", 77, uuidA)},
		{"mariadb gtid", maria, gtidPos(t, "0-1-10,1-2-4"), gtidPos(t, "0-1-9,1-2-4")},
		{
			"vstream multi-shard with a copy cursor", vanilla,
			vstreamPos(t, true, sg("ks", "-80", "MySQL56/"+uuidA+":1-10"), sg("ks", "80-", "MySQL56/"+uuidB+":1-5")),
			vstreamPos(t, false, sg("ks", "-80", "MySQL56/"+uuidA+":1-9"), sg("ks", "80-", "MySQL56/"+uuidB+":1-5")),
		},
	}
	for _, f := range families {
		t.Run(f.name, func(t *testing.T) {
			w := ir.AddedTableReplayWindow{Table: "orders", CopyPosition: f.copy, Key: []string{"tenant", "id"}, Columns: []string{"tenant", "id", "body"}}
			edit, err := appliershared.ReplayWindowsReset([]ir.AddedTableReplayWindow{w})
			if err != nil {
				t.Fatal(err)
			}
			next, _ := edit.Apply(nil, false, time.Time{})
			stored := appliershared.EncodeTableSet(replayEntriesOf(next))
			got, err := appliershared.DecodeReplayWindows(stored)
			if err != nil {
				t.Fatal(err)
			}
			if len(got) != 1 || !reflect.DeepEqual(got[0], w) {
				t.Fatalf("round trip:\n got %+v\nwant %+v", got, w)
			}
			for _, row := range []ir.Position{f.probe, f.copy} {
				a, errA := f.e.PositionAtOrAfter(row, f.copy)
				b, errB := f.e.PositionAtOrAfter(row, got[0].CopyPosition)
				if errA != nil || errB != nil || a != b {
					t.Fatalf("orderer verdict changed through the codec: %v/%v vs %v/%v", a, errA, b, errB)
				}
			}
		})
	}
}

func replayEntriesOf(set map[string]struct{}) []string {
	out := make([]string, 0, len(set))
	for k := range set {
		out = append(out, k)
	}
	return out
}
