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
// byte-exact through the real column.
var Corpus = []string{
	"orders", "Orders", "naïve_表", "a,b", "line\nbreak", `quo"te`, `back\slash`, " padded ", "😀", "\xff\xferaw",
}

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
		// S-C: "now" is the server's clock, read with the entries. The
		// container shares the host's kernel clock, so it is near ours.
		if d := time.Since(now); d > time.Minute || d < -time.Minute {
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
		return store.CompletePendingAddedTable(ctx, "s1", table, token)
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

	if err := store.RecordEstablishedTables(ctx, "s1", Corpus); err != nil {
		t.Fatal(err)
	}
	if err := store.RecordDroppedTables(ctx, "s1", []string{"orders", "line\nbreak"}); err != nil {
		t.Fatal(err)
	}
	want := append([]string(nil), Corpus...)
	sort.Strings(want)
	if rec := read(); !rec.Recorded || rec.ColdStartInProgress || !reflect.DeepEqual(rec.Established, want) ||
		!reflect.DeepEqual(rec.Dropped, []string{"line\nbreak", "orders"}) {
		t.Fatalf("Read = %+v\nwant established %q", rec, want)
	}

	// A completed add-table re-establishes a dropped table.
	if err := store.RecordEstablishedTables(ctx, "s1", []string{"a"}); err != nil {
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
	if err := store.RecordEstablishedTables(ctx, "s1", nil); err != nil {
		t.Fatal(err)
	}
	if rec := read(); !rec.Recorded || rec.ColdStartInProgress || len(rec.Established) != 0 {
		t.Fatalf("the empty set: %+v; want recorded, empty, finished", rec)
	}

	narrowAndAbandon(ctx, t, store)

	// A multi-database record is left alone by add-table (F3).
	if err := store.RecordEstablishedTables(ctx, "s1", []string{"app\x00orders"}); err != nil {
		t.Fatal(err)
	}
	recordPending("orders", "tok-c")
	if err := complete("orders", "tok-c"); err != nil {
		t.Fatal(err)
	}
	if rec := read(); len(rec.Established) != 1 || rec.Established[0] != "app\x00orders" {
		t.Fatalf("add-table wrote into a multi-database record: %q", rec.Established)
	}
	if err := store.RecordEstablishedTables(ctx, "no-such-stream", []string{"x"}); !errors.Is(err, errStreamNotFound) {
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
		return store.CompletePendingAddedTable(ctx, "s1", table, token)
	}

	// R4-4: the narrowing is judged on the values under its row lock. The
	// caller decided the scope from an older read of {a, b}; a completion of
	// c has landed since. c survives, b moves to dropped.
	if err := store.RecordEstablishedTables(ctx, "s1", []string{"a", "b"}); err != nil {
		t.Fatal(err)
	}
	if err := store.RecordDroppedTables(ctx, "s1", []string{"c", "gone"}); err != nil {
		t.Fatal(err)
	}
	recordPending("c", "tok-n")
	if err := complete("c", "tok-n"); err != nil {
		t.Fatal(err)
	}
	leaving, err := store.NarrowEstablishedRecord(ctx, "s1", []string{"a", "c"}, []string{"gone"})
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
	if leaving, err := store.NarrowEstablishedRecord(ctx, "s1", nil, nil); err != nil || leaving != nil || !read().ColdStartInProgress {
		t.Fatalf("a narrowing during a cold start in progress: %q, %v, %+v; want nothing written", leaving, err, read())
	}
	if leaving, err := store.NarrowEstablishedRecord(ctx, "no-such-stream", nil, nil); err != nil || leaving != nil {
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
