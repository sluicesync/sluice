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

// Corpus is every shape a recorded table name can take: each must come back
// byte-exact through the real column.
var Corpus = []string{
	"orders", "Orders", "naïve_表", "a,b", "line\nbreak", `quo"te`, `back\slash`, " padded ", "😀", "\xff\xferaw",
}

// PendingRoundTrip runs the pending-record contract on stream s1, whose row
// exists in a control table that predates the column.
func PendingRoundTrip(ctx context.Context, t *testing.T, store ir.PendingAddedTablesStore, errStreamNotFound error) {
	t.Helper()
	if got, err := store.ReadPendingAddedTables(ctx, "s1"); err != nil || len(got) != 0 {
		t.Fatalf("pre-column table: Read = (%v, %v); want empty", got, err)
	}
	if err := store.ClearPendingAddedTables(ctx, "s1", []string{"orders"}); err != nil {
		t.Fatalf("pre-column table: Clear = %v; want nothing to clear", err)
	}
	started := time.UnixMilli(time.Now().UnixMilli()).UTC()
	for _, name := range Corpus {
		if err := store.RecordPendingAddedTable(ctx, "s1", name, started); err != nil {
			t.Fatalf("%q: Record: %v", name, err)
		}
	}
	if err := store.RecordPendingAddedTable(ctx, "s1", "orders", started); err != nil {
		t.Fatalf("repeated Record: %v", err)
	}
	beat := started.Add(time.Minute)
	if err := store.HeartbeatPendingAddedTable(ctx, "s1", "orders", beat); err != nil {
		t.Fatal(err)
	}
	got, err := store.ReadPendingAddedTables(ctx, "s1")
	if err != nil {
		t.Fatal(err)
	}
	names := make([]string, 0, len(got))
	for _, p := range got {
		names = append(names, p.Table)
		wantBeat := started
		if p.Table == "orders" {
			wantBeat = beat
		}
		if !p.StartedAt.Equal(started) || !p.HeartbeatAt.Equal(wantBeat) {
			t.Fatalf("%q: started %v heartbeat %v; want %v %v", p.Table, p.StartedAt, p.HeartbeatAt, started, wantBeat)
		}
	}
	sort.Strings(names)
	want := append([]string(nil), Corpus...)
	sort.Strings(want)
	if !reflect.DeepEqual(names, want) {
		t.Fatalf("Read = %q\nwant  %q (a repeated Record must replace, not duplicate)", names, want)
	}
	if err := store.RecordPendingAddedTable(ctx, "no-such-stream", "x", time.Now()); !errors.Is(err, errStreamNotFound) {
		t.Fatalf("Record on a missing row = %v; want the stream-not-found error", err)
	}
	if err := store.HeartbeatPendingAddedTable(ctx, "s1", "never-recorded", time.Now()); err != nil {
		t.Fatalf("Heartbeat of an entry that is gone: %v; want nothing to do", err)
	}
	if err := store.ClearPendingAddedTables(ctx, "s1", []string{"line\nbreak", "\xff\xferaw", "never-recorded"}); err != nil {
		t.Fatal(err)
	}
	if got, _ := store.ReadPendingAddedTables(ctx, "s1"); len(got) != len(want)-2 {
		t.Fatalf("after Clear of two: %v", got)
	}
	if err := store.ClearPendingAddedTables(ctx, "s1", Corpus); err != nil {
		t.Fatal(err)
	}
	if got, err := store.ReadPendingAddedTables(ctx, "s1"); err != nil || len(got) != 0 {
		t.Fatalf("after Clear of all: (%v, %v)", got, err)
	}
	if err := store.ClearPendingAddedTables(ctx, "no-such-stream", []string{"x"}); err != nil {
		t.Fatalf("Clear on a missing row = %v; want nil", err)
	}
}

// EstablishedRoundTrip runs the established/dropped record contract on
// stream s1, whose row exists in a control table that predates the columns.
func EstablishedRoundTrip(ctx context.Context, t *testing.T, store ir.EstablishedTablesStore, errStreamNotFound error) {
	t.Helper()
	read := func() ir.EstablishedRecord {
		t.Helper()
		rec, err := store.ReadEstablishedRecord(ctx, "s1")
		if err != nil {
			t.Fatal(err)
		}
		return rec
	}
	if read().Recorded {
		t.Fatal("pre-column table: want not recorded")
	}
	if err := store.ResetEstablishedRecord(ctx, "s1"); err != nil {
		t.Fatalf("Reset on a pre-column table (no DDL, nothing to reset): %v", err)
	}
	if err := store.AddEstablishedTable(ctx, "s1", "orders"); err != nil {
		t.Fatalf("Add with no record: %v", err)
	}
	if read().Recorded {
		t.Fatal("Add on an unrecorded stream created a record")
	}
	if err := store.RecordEstablishedTables(ctx, "s1", Corpus); err != nil {
		t.Fatal(err)
	}
	if err := store.RecordDroppedTables(ctx, "s1", []string{"orders", "line\nbreak"}); err != nil {
		t.Fatal(err)
	}
	want := append([]string(nil), Corpus...)
	sort.Strings(want)
	if rec := read(); !rec.Recorded || !reflect.DeepEqual(rec.Established, want) || !reflect.DeepEqual(rec.Dropped, []string{"line\nbreak", "orders"}) {
		t.Fatalf("Read = %+v\nwant established %q", rec, want)
	}
	// A completed add-table re-establishes a dropped table.
	if err := store.AddEstablishedTable(ctx, "s1", "orders"); err != nil {
		t.Fatal(err)
	}
	if rec := read(); !reflect.DeepEqual(rec.Dropped, []string{"line\nbreak"}) {
		t.Fatalf("Add left orders dropped: %q", rec.Dropped)
	}
	if err := store.RecordEstablishedTables(ctx, "s1", nil); err != nil {
		t.Fatal(err)
	}
	if rec := read(); !rec.Recorded || len(rec.Established) != 0 {
		t.Fatalf("the empty set: %+v; want recorded and empty", rec)
	}
	// A multi-database record is left alone by add-table (F3).
	if err := store.RecordEstablishedTables(ctx, "s1", []string{"app\x00orders"}); err != nil {
		t.Fatal(err)
	}
	if err := store.AddEstablishedTable(ctx, "s1", "orders"); err != nil {
		t.Fatal(err)
	}
	if rec := read(); len(rec.Established) != 1 || rec.Established[0] != "app\x00orders" {
		t.Fatalf("add-table wrote into a multi-database record: %q", rec.Established)
	}
	// N3: a cold start's reset leaves the stream unrecorded.
	if err := store.ResetEstablishedRecord(ctx, "s1"); err != nil {
		t.Fatal(err)
	}
	if rec := read(); rec.Recorded {
		t.Fatalf("after Reset: %+v; want not recorded", rec)
	}
	if err := store.RecordEstablishedTables(ctx, "no-such-stream", []string{"x"}); !errors.Is(err, errStreamNotFound) {
		t.Fatalf("Record on a missing row = %v; want the stream-not-found error", err)
	}
}
