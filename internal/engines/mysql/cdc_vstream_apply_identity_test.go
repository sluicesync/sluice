// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package mysql

import (
	"context"
	"fmt"
	"testing"
	"time"

	"vitess.io/vitess/go/vt/proto/binlogdata"
	"vitess.io/vitess/go/vt/proto/query"

	"sluicesync.dev/sluice/internal/ir"
)

// vstreamTwoShardTxEvents is two shard transactions as vtgate delivers them
// (BEGIN … ROW … VGTID … COMMIT): two inserts on -80, then one on 80-.
func vstreamTwoShardTxEvents() []*binlogdata.VEvent {
	field := func(shard string) *binlogdata.VEvent {
		return &binlogdata.VEvent{Type: binlogdata.VEventType_FIELD, FieldEvent: &binlogdata.FieldEvent{
			TableName: "users", Keyspace: "main", Shard: shard,
			Fields: []*query.Field{{Name: "id", Type: query.Type_INT64}, {Name: "email", Type: query.Type_VARCHAR}},
		}}
	}
	row := func(shard, id string) *binlogdata.VEvent {
		return &binlogdata.VEvent{Type: binlogdata.VEventType_ROW, RowEvent: &binlogdata.RowEvent{
			TableName: "users", Keyspace: "main", Shard: shard,
			RowChanges: []*binlogdata.RowChange{{After: makeRow([]string{id, id + "@x"})}},
		}}
	}
	vgtid := func(a, b string) *binlogdata.VEvent {
		return &binlogdata.VEvent{Type: binlogdata.VEventType_VGTID, Vgtid: &binlogdata.VGtid{ShardGtids: []*binlogdata.ShardGtid{
			{Keyspace: "main", Shard: "-80", Gtid: a}, {Keyspace: "main", Shard: "80-", Gtid: b},
		}}}
	}
	boundary := func(typ binlogdata.VEventType, shard string) *binlogdata.VEvent {
		return &binlogdata.VEvent{Type: typ, Keyspace: "main", Shard: shard}
	}
	return []*binlogdata.VEvent{
		field("-80"), field("80-"),
		boundary(binlogdata.VEventType_BEGIN, "-80"), row("-80", "1"), row("-80", "2"),
		vgtid("MySQL56/a:1-6", "MySQL56/b:1-9"), boundary(binlogdata.VEventType_COMMIT, "-80"),
		boundary(binlogdata.VEventType_BEGIN, "80-"), row("80-", "3"),
		vgtid("MySQL56/a:1-6", "MySQL56/b:1-10"), boundary(binlogdata.VEventType_COMMIT, "80-"),
	}
}

func vstreamTwoShardStart() []shardGtid {
	return []shardGtid{{Keyspace: "main", Shard: "-80", Gtid: "MySQL56/a:1-5"}, {Keyspace: "main", Shard: "80-", Gtid: "MySQL56/b:1-9"}}
}

// vstreamEmitted renders the emitted stream: boundaries and each row's identity.
func vstreamEmitted(t *testing.T, got []ir.Change) []string {
	t.Helper()
	var out []string
	for _, c := range got {
		switch v := c.(type) {
		case ir.TxBegin:
			out = append(out, "BEGIN")
		case ir.TxCommit:
			out = append(out, "COMMIT")
		default:
			id := ir.ApplyIDOf(c)
			out = append(out, fmt.Sprintf("%T %s#%d", v, id.TxID, id.Seq))
		}
	}
	return out
}

// TestVStream_TransactionBoundariesAndIdentity pins phase 4's emission on
// BOTH VStream dispatchers — the tail reader and the snapshot stream's
// post-COPY pump — from the same events: BEGIN/COMMIT reach the applier, and
// each row carries its shard's identity (the shard component BEFORE the
// transaction, never the merged VGTID) with a per-table ordinal. The two
// dispatchers must agree, because an original delivery on the cold-start
// stream is re-delivered by the tail reader after a restart.
func TestVStream_TransactionBoundariesAndIdentity(t *testing.T) {
	want := []string{
		"BEGIN",
		"ir.Insert vstream:main/-80:MySQL56/a:1-5#1",
		"ir.Insert vstream:main/-80:MySQL56/a:1-5#2",
		"COMMIT",
		"BEGIN",
		"ir.Insert vstream:main/80-:MySQL56/b:1-9#1",
		"COMMIT",
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	tail := &vstreamCDCReader{keyspace: "main", fields: map[string][]*query.Field{}, currentVgtid: vstreamTwoShardStart()}
	out := make(chan ir.Change, 16)
	for _, ev := range vstreamTwoShardTxEvents() {
		if err := tail.dispatch(ctx, ev, out); err != nil {
			t.Fatalf("tail dispatch %v: %v", ev.GetType(), err)
		}
	}
	close(out)
	tailGot := drainChannel(out)
	assertShape(t, "tail reader", vstreamEmitted(t, tailGot), want)
	commit := tailGot[3].(ir.TxCommit)
	if wantPos, _ := encodeVStreamPos([]shardGtid{{Keyspace: "main", Shard: "-80", Gtid: "MySQL56/a:1-6"}, {Keyspace: "main", Shard: "80-", Gtid: "MySQL56/b:1-9"}}); commit.Position != wantPos {
		t.Errorf("the COMMIT carries %q; want the post-transaction VGTID %q", commit.Position.Token, wantPos.Token)
	}

	snap := &vstreamSnapshotStream{keyspace: "main", fields: map[string][]*query.Field{}, currentVgtid: vstreamTwoShardStart()}
	out = make(chan ir.Change, 16)
	for _, ev := range vstreamTwoShardTxEvents() {
		if err := snap.dispatchCDCEvent(ctx, ev, out); err != nil {
			t.Fatalf("snapshot dispatch %v: %v", ev.GetType(), err)
		}
	}
	close(out)
	assertShape(t, "snapshot stream", vstreamEmitted(t, drainChannel(out)), want)
}

// TestVStream_NoIdentityDuringCopyOrInterleaving pins the cases that must
// never be skippable: rows while a COPY runs (until the stream-wide
// COPY_COMPLETED), and a transaction whose BEGIN arrives inside another.
func TestVStream_NoIdentityDuringCopyOrInterleaving(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	r := &vstreamCDCReader{keyspace: "main", fields: map[string][]*query.Field{}, currentVgtid: vstreamTwoShardStart()}
	r.tx.copying = true
	out := make(chan ir.Change, 32)
	evs := vstreamTwoShardTxEvents()
	for _, ev := range evs[:7] { // the -80 transaction, during the COPY
		if err := r.dispatch(ctx, ev, out); err != nil {
			t.Fatal(err)
		}
	}
	if err := r.dispatch(ctx, &binlogdata.VEvent{Type: binlogdata.VEventType_COPY_COMPLETED, Keyspace: "main", Shard: "-80"}, out); err != nil {
		t.Fatal(err)
	}
	if !r.tx.copying {
		t.Fatal("a per-shard COPY_COMPLETED ended the COPY; only the stream-wide one may")
	}
	if err := r.dispatch(ctx, &binlogdata.VEvent{Type: binlogdata.VEventType_COPY_COMPLETED}, out); err != nil {
		t.Fatal(err)
	}
	for _, ev := range evs[7:] { // the 80- transaction, after it
		if err := r.dispatch(ctx, ev, out); err != nil {
			t.Fatal(err)
		}
	}
	close(out)
	assertShape(t, "copy", vstreamEmitted(t, drainChannel(out)), []string{
		"BEGIN", "ir.Insert #0", "ir.Insert #0", "COMMIT",
		"BEGIN", "ir.Insert vstream:main/80-:MySQL56/b:1-9#1", "COMMIT",
	})

	// Interleaved: a second BEGIN before the first COMMIT, then both groups
	// finish. From the nested BEGIN until nothing is open, no row carries an
	// identity, and the emission stays balanced — ONE bracket spanning both
	// groups, closed by the COMMIT that leaves nothing open — on both
	// dispatchers. The trailing un-interleaved transaction proves the state
	// recovers.
	interleaved := []*binlogdata.VEvent{
		evs[0], evs[1], evs[2], evs[3], evs[7], evs[8], evs[4], evs[5], evs[6], evs[9], evs[10],
		evs[7], evs[8], evs[9], evs[10],
	}
	wantInterleaved := []string{
		"BEGIN", "ir.Insert vstream:main/-80:MySQL56/a:1-5#1", "ir.Insert #0", "ir.Insert #0", "COMMIT",
		"BEGIN", "ir.Insert vstream:main/80-:MySQL56/b:1-10#1", "COMMIT",
	}
	r = &vstreamCDCReader{keyspace: "main", fields: map[string][]*query.Field{}, currentVgtid: vstreamTwoShardStart()}
	out = make(chan ir.Change, 32)
	for _, ev := range interleaved {
		if err := r.dispatch(ctx, ev, out); err != nil {
			t.Fatal(err)
		}
	}
	close(out)
	assertShape(t, "interleaved (tail reader)", vstreamEmitted(t, drainChannel(out)), wantInterleaved)

	snap := &vstreamSnapshotStream{keyspace: "main", fields: map[string][]*query.Field{}, currentVgtid: vstreamTwoShardStart()}
	out = make(chan ir.Change, 32)
	for _, ev := range interleaved {
		if err := snap.dispatchCDCEvent(ctx, ev, out); err != nil {
			t.Fatal(err)
		}
	}
	close(out)
	assertShape(t, "interleaved (snapshot stream)", vstreamEmitted(t, drainChannel(out)), wantInterleaved)
}

func TestVStreamTxIdentity(t *testing.T) {
	for _, tc := range []struct {
		name string
		sg   shardGtid
		want string
	}{
		{"a GTID set", shardGtid{Keyspace: "k", Shard: "-80", Gtid: "MySQL56/a:1-5"}, "vstream:k/-80:MySQL56/a:1-5"},
		{"empty (COPY from the start)", shardGtid{Keyspace: "k", Shard: "-80"}, ""},
		{"the current sentinel", shardGtid{Keyspace: "k", Shard: "-80", Gtid: "current"}, ""},
		{"a COPY cursor", shardGtid{Keyspace: "k", Shard: "-80", Gtid: "MySQL56/a:1-5", TablePKs: []encodedTablePK{{}}}, ""},
	} {
		if got := vstreamTxIdentity([]shardGtid{tc.sg}, "k", "-80"); got != tc.want {
			t.Errorf("%s: vstreamTxIdentity = %q, want %q", tc.name, got, tc.want)
		}
	}
	if got := vstreamTxIdentity([]shardGtid{{Keyspace: "k", Shard: "-80", Gtid: "MySQL56/a:1-5"}}, "k", "80-"); got != "" {
		t.Errorf("a shard not in the VGTID got identity %q", got)
	}
}

func assertShape(t *testing.T, what string, got, want []string) {
	t.Helper()
	if fmt.Sprint(got) != fmt.Sprint(want) {
		t.Errorf("%s emitted\n  %v\nwant\n  %v", what, got, want)
	}
}
