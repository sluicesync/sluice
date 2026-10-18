// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

// Package tablesetstest holds the engine-independent round-trip checks for
// the sluice_cdc_state table-set columns (pending_added_tables,
// established_tables, dropped_tables — INCLUDE-TABLE-SCOPE-DRIFT, Bug 321),
// run by each engine's integration tests against a real server so both
// appliers are held to one contract.
package tablesetstest

import (
	"context"
	"errors"
	"reflect"
	"sort"
	"strconv"
	"strings"
	"testing"
	"time"

	"sluicesync.dev/sluice/internal/ir"
)

// Store is an applier carrying both table-set surfaces, as both engines'
// appliers do.
type Store interface {
	ir.PendingAddedTablesStore
	ir.EstablishedTablesStore
}

// Corpus is every shape a recorded table name can take: each must come back
// byte-exact through the real column. It includes the longest names the
// engines allow (R6 pin 4): a 63-byte Postgres identifier, and a
// 64-character MySQL one in a 3-byte character (192 bytes).
var Corpus = []string{
	"orders", "Orders", "naïve_表", "a,b", "line\nbreak", `quo"te`, `back\slash`, " padded ", "😀", "\xff\xferaw",
	strings.Repeat("p", 63), strings.Repeat("表", 64),
}

// EstablishedCorpus adds what only the established and dropped sets carry:
// multi-database `<namespace>\x00<table>` keys, one with the longest
// namespace and table names MySQL allows.
var EstablishedCorpus = append(append([]string(nil), Corpus...),
	strings.Repeat("n", 64)+"\x00"+strings.Repeat("t", 64), "app\x00orders")

// PendingRoundTrip runs the pending-record contract on stream s1, whose row
// exists in a control table that predates the column.
func PendingRoundTrip(ctx context.Context, t *testing.T, store ir.PendingAddedTablesStore, errStreamNotFound error) {
	t.Helper()
	read := func() ([]ir.PendingAddedTable, time.Time) {
		t.Helper()
		got, now, err := store.ReadPendingAddedTables(ctx, "s1")
		if err != nil {
			t.Fatal(err)
		}
		// S-C / P1: "now" is the clock of the server holding the row, read in
		// the same statement; with no row or no column there is none. The
		// container shares the host's kernel clock, so it is near ours.
		if d := time.Since(now); len(got) > 0 && (d > time.Minute || d < -time.Minute) {
			t.Fatalf("Read's server now %v is not near the host's %v", now, time.Now())
		}
		return got, now
	}
	if got, _ := read(); len(got) != 0 {
		t.Fatalf("pre-column table: Read = %v; want empty", got)
	}
	if err := store.ClearPendingAddedTables(ctx, "s1", []string{"orders"}); err != nil {
		t.Fatalf("pre-column table: Clear = %v; want nothing to clear", err)
	}
	for _, name := range Corpus {
		if err := store.RecordPendingAddedTable(ctx, "s1", name, "tok-"+name); err != nil {
			t.Fatalf("%q: Record: %v", name, err)
		}
	}
	if err := store.RecordPendingAddedTable(ctx, "s1", "orders", "tok-new"); err != nil {
		t.Fatalf("repeated Record: %v", err)
	}
	got, now := read()
	names := make([]string, 0, len(got))
	for _, p := range got {
		names = append(names, p.Table)
		wantToken := "tok-" + p.Table
		if p.Table == "orders" {
			wantToken = "tok-new"
		}
		if p.Token != wantToken || !p.StartedAt.Equal(p.HeartbeatAt) || now.Sub(p.StartedAt) > time.Minute || !p.Fresh(now) {
			t.Fatalf("%q: %+v; want token %q, started = heartbeat = the server's recent now (%v)", p.Table, p, wantToken, now)
		}
	}
	sort.Strings(names)
	want := append([]string(nil), Corpus...)
	sort.Strings(want)
	if !reflect.DeepEqual(names, want) {
		t.Fatalf("Read = %q\nwant  %q (a repeated Record must replace, not duplicate)", names, want)
	}

	// S-B: a Touch compares-and-sets on the token.
	if err := store.TouchPendingAddedTable(ctx, "s1", "orders", "tok-orders", true); !errors.Is(err, ir.ErrPendingAddSuperseded) {
		t.Fatalf("Touch under a superseded token = %v; want ErrPendingAddSuperseded", err)
	}
	if err := store.TouchPendingAddedTable(ctx, "s1", "never-recorded", "tok", true); !errors.Is(err, ir.ErrPendingAddSuperseded) {
		t.Fatalf("Touch of an entry that is gone = %v; want ErrPendingAddSuperseded", err)
	}
	if err := store.TouchPendingAddedTable(ctx, "s1", "orders", "tok-new", true); err != nil {
		t.Fatal(err)
	}
	if err := store.TouchPendingAddedTable(ctx, "s1", "Orders", "tok-Orders", false); err != nil {
		t.Fatal(err)
	}
	got, now = read()
	for _, p := range got {
		switch p.Table {
		case "orders":
			if p.Token != "tok-new" || p.HeartbeatAt.Before(p.StartedAt) || !p.Fresh(now) {
				t.Fatalf("alive Touch: %+v", p)
			}
		case "Orders":
			if !p.HeartbeatAt.Equal(time.Unix(0, 0)) || p.Fresh(now) || p.StartedAt.Equal(p.HeartbeatAt) {
				t.Fatalf("stopped Touch: %+v; want the epoch heartbeat, start kept", p)
			}
		}
	}
	if err := store.RecordPendingAddedTable(ctx, "no-such-stream", "x", "tok"); !errors.Is(err, errStreamNotFound) {
		t.Fatalf("Record on a missing row = %v; want the stream-not-found error", err)
	}
	if err := store.ClearPendingAddedTables(ctx, "s1", []string{"line\nbreak", "\xff\xferaw", "never-recorded"}); err != nil {
		t.Fatal(err)
	}
	if got, _ := read(); len(got) != len(want)-2 {
		t.Fatalf("after Clear of two: %v", got)
	}
	if err := store.ClearPendingAddedTables(ctx, "s1", Corpus); err != nil {
		t.Fatal(err)
	}
	if got, _ := read(); len(got) != 0 {
		t.Fatalf("after Clear of all: %v", got)
	}
	if err := store.ClearPendingAddedTables(ctx, "no-such-stream", []string{"x"}); err != nil {
		t.Fatalf("Clear on a missing row = %v; want nil", err)
	}
}

// EstablishedRoundTrip runs the established/dropped record contract, and the
// completion that writes it together with the pending set, on stream s1,
// whose row exists in a control table that predates the columns.
func EstablishedRoundTrip(ctx context.Context, t *testing.T, store Store, errStreamNotFound error) {
	t.Helper()
	read := func() ir.EstablishedRecord {
		t.Helper()
		rec, err := store.ReadEstablishedRecord(ctx, "s1")
		if err != nil {
			t.Fatal(err)
		}
		return rec
	}
	pending := func() []string {
		t.Helper()
		got, _, err := store.ReadPendingAddedTables(ctx, "s1")
		if err != nil {
			t.Fatal(err)
		}
		names := make([]string, 0, len(got))
		for _, p := range got {
			names = append(names, p.Table)
		}
		sort.Strings(names)
		return names
	}
	complete := func(table, token string) error {
		t.Helper()
		return store.CompletePendingAddedTable(ctx, "s1", table, token, "")
	}
	recordPending := func(table, token string) {
		t.Helper()
		if err := store.RecordPendingAddedTable(ctx, "s1", table, token); err != nil {
			t.Fatal(err)
		}
	}
	if read().Recorded {
		t.Fatal("pre-column table: want not recorded")
	}
	if err := store.BeginColdStartRecord(ctx, "no-such-stream"); err != nil {
		t.Fatalf("BeginColdStartRecord on a missing row: %v; want nothing to mark", err)
	}

	// A completion on an unrecorded stream clears its entry and creates no
	// record (one table is not a record of what the stream established).
	recordPending("orders", "tok-a")
	if err := complete("orders", "tok-wrong"); !errors.Is(err, ir.ErrPendingAddSuperseded) {
		t.Fatalf("Complete under another token = %v; want ErrPendingAddSuperseded", err)
	}
	if got := pending(); !reflect.DeepEqual(got, []string{"orders"}) {
		t.Fatalf("a refused Complete changed the pending set: %q", got)
	}
	if err := complete("orders", "tok-a"); err != nil {
		t.Fatal(err)
	}
	if read().Recorded || len(pending()) != 0 {
		t.Fatalf("Complete on an unrecorded stream: %+v pending %q", read(), pending())
	}

	if err := store.RecordEstablishedTables(ctx, "s1", EstablishedCorpus, nil); err != nil {
		t.Fatal(err)
	}
	if err := store.RecordDroppedTables(ctx, "s1", []string{"orders", "line\nbreak"}); err != nil {
		t.Fatal(err)
	}
	want := append([]string(nil), EstablishedCorpus...)
	sort.Strings(want)
	if rec := read(); !rec.Recorded || rec.ColdStartInProgress || !reflect.DeepEqual(rec.Established, want) ||
		!reflect.DeepEqual(rec.Dropped, []string{"line\nbreak", "orders"}) {
		t.Fatalf("Read = %+v\nwant established %q", rec, want)
	}

	// A completed add-table re-establishes a dropped table.
	if err := store.RecordEstablishedTables(ctx, "s1", []string{"a"}, nil); err != nil {
		t.Fatal(err)
	}
	recordPending("orders", "tok-b")
	if err := complete("orders", "tok-b"); err != nil {
		t.Fatal(err)
	}
	if rec := read(); !reflect.DeepEqual(rec.Established, []string{"a", "orders"}) || !reflect.DeepEqual(rec.Dropped, []string{"line\nbreak"}) {
		t.Fatalf("after Complete: %+v; want orders established and not dropped", rec)
	}

	// S-B: an abandoned add (its entry cleared) and a superseded one (its
	// entry taken over) record nothing.
	recordPending("x", "tok-x")
	if err := store.ClearPendingAddedTables(ctx, "s1", []string{"x"}); err != nil {
		t.Fatal(err)
	}
	if err := complete("x", "tok-x"); !errors.Is(err, ir.ErrPendingAddSuperseded) {
		t.Fatalf("Complete of an abandoned add = %v; want ErrPendingAddSuperseded", err)
	}
	recordPending("y", "tok-y1")
	recordPending("y", "tok-y2")
	if err := complete("y", "tok-y1"); !errors.Is(err, ir.ErrPendingAddSuperseded) {
		t.Fatalf("Complete of a superseded add = %v; want ErrPendingAddSuperseded", err)
	}
	if rec := read(); !reflect.DeepEqual(rec.Established, []string{"a", "orders"}) {
		t.Fatalf("an abandoned or superseded add recorded established: %q", rec.Established)
	}
	if err := complete("y", "tok-y2"); err != nil {
		t.Fatal(err)
	}

	// S-A: a cold start in progress is a recorded state, never "unrecorded";
	// it keeps the dropped set, and a completion leaves it alone.
	if err := store.BeginColdStartRecord(ctx, "s1"); err != nil {
		t.Fatal(err)
	}
	if rec := read(); !rec.Recorded || !rec.ColdStartInProgress || len(rec.Established) != 0 || !reflect.DeepEqual(rec.Dropped, []string{"line\nbreak"}) {
		t.Fatalf("after BeginColdStartRecord: %+v", rec)
	}
	recordPending("z", "tok-z")
	if err := complete("z", "tok-z"); err != nil {
		t.Fatal(err)
	}
	if rec := read(); !rec.ColdStartInProgress || len(rec.Established) != 0 {
		t.Fatalf("Complete wrote into a cold start in progress: %+v", rec)
	}
	if err := store.RecordEstablishedTables(ctx, "s1", nil, nil); err != nil {
		t.Fatal(err)
	}
	if rec := read(); !rec.Recorded || rec.ColdStartInProgress || len(rec.Established) != 0 {
		t.Fatalf("the empty set: %+v; want recorded, empty, finished", rec)
	}

	narrowAndAbandon(ctx, t, store)

	// A multi-database record is left alone by add-table (F3).
	if err := store.RecordEstablishedTables(ctx, "s1", []string{"app\x00orders"}, nil); err != nil {
		t.Fatal(err)
	}
	recordPending("orders", "tok-c")
	if err := complete("orders", "tok-c"); err != nil {
		t.Fatal(err)
	}
	if rec := read(); len(rec.Established) != 1 || rec.Established[0] != "app\x00orders" {
		t.Fatalf("add-table wrote into a multi-database record: %q", rec.Established)
	}
	if err := store.RecordEstablishedTables(ctx, "no-such-stream", []string{"x"}, nil); !errors.Is(err, errStreamNotFound) {
		t.Fatalf("Record on a missing row = %v; want the stream-not-found error", err)
	}
}

// narrowAndAbandon is the round-5 half of [EstablishedRoundTrip] on stream
// s1: the row-locked narrowing (R4-4) and the abandoned pending entry (R4-1).
// It leaves s1 in a cold start in progress.
func narrowAndAbandon(ctx context.Context, t *testing.T, store Store) {
	t.Helper()
	read := func() ir.EstablishedRecord {
		t.Helper()
		rec, err := store.ReadEstablishedRecord(ctx, "s1")
		if err != nil {
			t.Fatal(err)
		}
		return rec
	}
	recordPending := func(table, token string) {
		t.Helper()
		if err := store.RecordPendingAddedTable(ctx, "s1", table, token); err != nil {
			t.Fatal(err)
		}
	}
	complete := func(table, token string) error {
		t.Helper()
		return store.CompletePendingAddedTable(ctx, "s1", table, token, "")
	}

	// R6 rename: an id beside each name round-trips, a completion records
	// its table's id, and a rename under the narrowing's lock keeps the id
	// under the new name.
	if err := store.RecordEstablishedTables(ctx, "s1", []string{"t8", "kd"}, map[string]string{"t8": "16391"}); err != nil {
		t.Fatal(err)
	}
	recordPending("users", "tok-u")
	if err := store.CompletePendingAddedTable(ctx, "s1", "users", "tok-u", "4242"); err != nil {
		t.Fatal(err)
	}
	if rec := read(); rec.IDs["t8"] != "16391" || rec.IDs["users"] != "4242" || rec.IDs["kd"] != "" {
		t.Fatalf("ids read back as %v", rec.IDs)
	}
	if _, err := store.NarrowEstablishedRecord(ctx, "s1", ir.RecordNarrowing{
		InScope: []string{"t8b", "kd", "users"}, Gone: []string{"t8"}, Renames: map[string]string{"t8": "t8b"},
		IDs: map[string]string{"users": "5151", "nope": "1"},
	}); err != nil {
		t.Fatal(err)
	}
	// R6-S3: a rebuilt table's new id replaces its recorded one; an id for
	// an entry not established is not recorded.
	if rec := read(); !reflect.DeepEqual(rec.Established, []string{"kd", "t8b", "users"}) || rec.IDs["t8b"] != "16391" || rec.IDs["t8"] != "" ||
		rec.IDs["users"] != "5151" || rec.IDs["nope"] != "" {
		t.Fatalf("after the rename and id update: %+v; want t8b carrying t8's id and users at 5151", rec)
	}

	// R4-4: the narrowing is judged on the values under its row lock. The
	// caller decided the scope from an older read of {a, b}; a completion of
	// c has landed since. c survives, b moves to dropped.
	if err := store.RecordEstablishedTables(ctx, "s1", []string{"a", "b"}, nil); err != nil {
		t.Fatal(err)
	}
	if err := store.RecordDroppedTables(ctx, "s1", []string{"c", "gone"}); err != nil {
		t.Fatal(err)
	}
	recordPending("c", "tok-n")
	if err := complete("c", "tok-n"); err != nil {
		t.Fatal(err)
	}
	leaving, err := store.NarrowEstablishedRecord(ctx, "s1", ir.RecordNarrowing{InScope: []string{"a", "c"}, Gone: []string{"gone"}})
	if err != nil {
		t.Fatal(err)
	}
	if rec := read(); !reflect.DeepEqual(rec.Established, []string{"a", "c"}) || !reflect.DeepEqual(rec.Dropped, []string{"b"}) ||
		!reflect.DeepEqual(leaving, []string{"b"}) {
		t.Fatalf("after the narrowing: %+v leaving %q; want established [a c], dropped [b]", rec, leaving)
	}
	if err := store.BeginColdStartRecord(ctx, "s1"); err != nil {
		t.Fatal(err)
	}
	if leaving, err := store.NarrowEstablishedRecord(ctx, "s1", ir.RecordNarrowing{}); err != nil || leaving != nil || !read().ColdStartInProgress {
		t.Fatalf("a narrowing during a cold start in progress: %q, %v, %+v; want nothing written", leaving, err, read())
	}
	if leaving, err := store.NarrowEstablishedRecord(ctx, "no-such-stream", ir.RecordNarrowing{}); err != nil || leaving != nil {
		t.Fatalf("a narrowing of a missing row: %q, %v; want nothing to do", leaving, err)
	}

	// R4-1: --abandon rewrites the entry, never removes it, and the add it
	// abandoned is superseded.
	recordPending("ab", "tok-ab")
	if err := store.AbandonPendingAddedTable(ctx, "s1", "ab"); err != nil {
		t.Fatal(err)
	}
	got, _, err := store.ReadPendingAddedTables(ctx, "s1")
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 1 || got[0].Table != "ab" || !got[0].Abandoned() || got[0].StartedAt.IsZero() || !got[0].HeartbeatAt.Equal(time.Unix(0, 0)) {
		t.Fatalf("after --abandon: %+v; want one abandoned entry, start kept", got)
	}
	if err := complete("ab", "tok-ab"); !errors.Is(err, ir.ErrPendingAddSuperseded) {
		t.Fatalf("Complete of an abandoned add = %v; want ErrPendingAddSuperseded", err)
	}
	if err := store.AbandonPendingAddedTable(ctx, "s1", "never-recorded"); err != nil {
		t.Fatalf("abandoning nothing: %v", err)
	}
	if err := store.ClearPendingAddedTables(ctx, "s1", []string{"ab"}); err != nil {
		t.Fatal(err)
	}
}

// RawColumn writes and reads stream s1's raw column value with the test's
// own SQL, bypassing the store — the independent reader and writer the
// codec checklist asks for. set with ok=false stores NULL.
type RawColumn struct {
	Set func(column, value string, ok bool)
	Get func(column string) (value string, ok bool)
}

// RawRoundTrip is R6 pin 6 and pin 3: the encoding checked from outside the
// store in both directions, and every corrupted shape refusing with its
// recovery, or recovered by the write a cold start makes, on stream s1.
func RawRoundTrip(ctx context.Context, t *testing.T, store Store, raw RawColumn) {
	t.Helper()
	read := func() (ir.EstablishedRecord, error) { return store.ReadEstablishedRecord(ctx, "s1") }

	// A hand-written value, read through the store.
	raw.Set("established_tables", `"orders"`+"\n"+`"a\nb"`+"\n"+`"\u00e9"`+"\n"+`"app\x00orders"`, true)
	raw.Set("dropped_tables", "", true)
	rec, err := read()
	if err != nil {
		t.Fatal(err)
	}
	got := append([]string(nil), rec.Established...)
	sort.Strings(got)
	if want := []string{"a\nb", "app\x00orders", "orders", "é"}; !reflect.DeepEqual(got, want) {
		t.Fatalf("a hand-written record read back as %q; want %q", got, want)
	}

	// A store write, inspected byte for byte.
	if err := store.RecordEstablishedTables(ctx, "s1", []string{"x\ny", "é", `q"\`}, nil); err != nil {
		t.Fatal(err)
	}
	if v, _ := raw.Get("established_tables"); v != `"q\"\\"`+"\n"+`"x\ny"`+"\n"+`"\u00e9"` {
		t.Fatalf("stored bytes %q", v)
	}
	if err := store.BeginColdStartRecord(ctx, "s1"); err != nil {
		t.Fatal(err)
	}
	if v, _ := raw.Get("established_tables"); v != strconv.QuoteToASCII("\x00cold-start-in-progress") {
		t.Fatalf("the in-progress sentinel is stored as %q", v)
	}

	// Corruption, then recovery by the cold start's writes.
	var bad *ir.UndecodableTableSetError
	raw.Set("established_tables", `"unterminated`, true)
	if _, err := read(); !errors.As(err, &bad) || bad.Column != "established_tables" || !strings.Contains(bad.Recovery, "--restart-from-scratch") {
		t.Fatalf("an undecodable established set: %v; want UndecodableTableSetError naming --restart-from-scratch", err)
	}
	if err := store.BeginColdStartRecord(ctx, "s1"); err != nil {
		t.Fatalf("a cold start could not overwrite an undecodable record: %v", err)
	}
	if rec, err := read(); err != nil || !rec.ColdStartInProgress {
		t.Fatalf("after the overwrite: %+v, %v", rec, err)
	}
	raw.Set("dropped_tables", "garbage", true)
	if _, err := read(); !errors.As(err, &bad) || bad.Column != "dropped_tables" {
		t.Fatalf("an undecodable dropped set: %v", err)
	}
	if err := store.RecordDroppedTables(ctx, "s1", []string{ir.DroppedAllUncopiedEntry}); err != nil {
		t.Fatalf("overwriting an undecodable dropped set: %v", err)
	}
	if err := store.RecordEstablishedTables(ctx, "s1", []string{"kd"}, nil); err != nil {
		t.Fatal(err)
	}
	if rec, err := read(); err != nil || !rec.DroppedAllUncopied || len(rec.Dropped) != 0 || !reflect.DeepEqual(rec.Established, []string{"kd"}) {
		t.Fatalf("after the cold start's writes: %+v, %v; want established [kd] and all-uncopied", rec, err)
	}

	// A malformed pending entry is reported, not an error, and --abandon
	// retires it; a line that is not even quoted is reported too, and the
	// write that would need to decode it names the SQL recovery.
	raw.Set("pending_added_tables", strconv.QuoteToASCII("kd\x00bad"), true)
	pending, _, err := store.ReadPendingAddedTables(ctx, "s1")
	if err != nil || len(pending) != 1 || pending[0].Table != "kd" || pending[0].Malformed == "" {
		t.Fatalf("a malformed entry: %+v, %v; want it reported for kd", pending, err)
	}
	if err := store.AbandonPendingAddedTable(ctx, "s1", "kd"); err != nil {
		t.Fatal(err)
	}
	if pending, _, err = store.ReadPendingAddedTables(ctx, "s1"); err != nil || len(pending) != 1 || !pending[0].Abandoned() || pending[0].Malformed != "" {
		t.Fatalf("after --abandon of a malformed entry: %+v, %v", pending, err)
	}
	raw.Set("pending_added_tables", "not quoted", true)
	if pending, _, err = store.ReadPendingAddedTables(ctx, "s1"); err != nil || len(pending) != 1 || pending[0].Malformed != "not quoted" || pending[0].Table != "" {
		t.Fatalf("an unquoted line: %+v, %v", pending, err)
	}
	if err := store.AbandonPendingAddedTable(ctx, "s1", "kd"); !errors.As(err, &bad) || !strings.Contains(bad.Recovery, "pending_added_tables = NULL") {
		t.Fatalf("a write over an unquoted pending line: %v; want the SQL recovery", err)
	}
	raw.Set("pending_added_tables", "", false)
}
