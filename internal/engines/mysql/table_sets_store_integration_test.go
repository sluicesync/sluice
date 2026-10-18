//go:build integration

// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package mysql

import (
	"context"
	"database/sql"
	"errors"
	"reflect"
	"sort"
	"testing"
	"time"

	"sluicesync.dev/sluice/internal/ir"
)

// pendingAddedTablesCorpus is every shape a recorded table name can take; the
// record is a codec read back by a later process, so each must come back
// byte-exact through the real column. The 4-byte row is the one a utf8mb3
// column would mangle, were the encoding not ASCII.
var pendingAddedTablesCorpus = []string{
	"orders", "Orders", "naïve_表", "a,b", "line\nbreak", `quo"te`, `back\slash`, " padded ", "😀", "\xff\xferaw",
}

// TestPendingAddedTablesStore_RoundTrip pins the MySQL add-table pending
// record (INCLUDE-TABLE-SCOPE-DRIFT) against a real server, from a control
// table a pre-column binary created: no record reads as empty, Record adds
// the column itself, the corpus round-trips — a repeated identical Record
// included, which MySQL's changed-rows RowsAffected reports as zero rows —
// Record of a missing stream row is a loud error, and Clear removes exactly
// what it names and is idempotent.
func TestPendingAddedTablesStore_RoundTrip(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	dsn, applier := tableSetsStoreFixture(ctx, t)
	store := applier.(ir.PendingAddedTablesStore)

	if got, err := store.ReadPendingAddedTables(ctx, "s1"); err != nil || len(got) != 0 {
		t.Fatalf("pre-column table: Read = (%q, %v); want empty", got, err)
	}
	if err := store.ClearPendingAddedTables(ctx, "s1", []string{"orders"}); err != nil {
		t.Fatalf("pre-column table: Clear = %v; want nothing to clear", err)
	}
	for _, name := range pendingAddedTablesCorpus {
		if err := store.RecordPendingAddedTable(ctx, "s1", name); err != nil {
			t.Fatalf("%q: Record: %v", name, err)
		}
	}
	if err := store.RecordPendingAddedTable(ctx, "s1", "orders"); err != nil {
		t.Fatalf("repeated Record: %v", err)
	}
	got, err := store.ReadPendingAddedTables(ctx, "s1")
	if err != nil {
		t.Fatal(err)
	}
	want := append([]string(nil), pendingAddedTablesCorpus...)
	sort.Strings(want)
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("Read = %q\nwant  %q", got, want)
	}

	if err := store.RecordPendingAddedTable(ctx, "no-such-stream", "x"); !errors.Is(err, errStreamNotFound) {
		t.Fatalf("Record on a missing row = %v; want errStreamNotFound", err)
	}

	if err := store.ClearPendingAddedTables(ctx, "s1", []string{"line\nbreak", "\xff\xferaw", "never-recorded"}); err != nil {
		t.Fatal(err)
	}
	got, _ = store.ReadPendingAddedTables(ctx, "s1")
	if len(got) != len(want)-2 {
		t.Fatalf("after Clear of two: %q", got)
	}
	if err := store.ClearPendingAddedTables(ctx, "s1", pendingAddedTablesCorpus); err != nil {
		t.Fatal(err)
	}
	if got, err := store.ReadPendingAddedTables(ctx, "s1"); err != nil || len(got) != 0 {
		t.Fatalf("after Clear of all: (%q, %v)", got, err)
	}
	db, err := sql.Open("mysql", dsn)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = db.Close() }()
	var stored *string
	if err := db.QueryRowContext(ctx, "SELECT pending_added_tables FROM sluice_cdc_state WHERE stream_id = 's1'").Scan(&stored); err != nil || stored != nil {
		t.Fatalf("an emptied record is stored as NULL; got %v, %v", stored, err)
	}
	if err := store.ClearPendingAddedTables(ctx, "no-such-stream", []string{"x"}); err != nil {
		t.Fatalf("Clear on a missing row = %v; want nil", err)
	}
}

// tableSetsStoreFixture starts a server holding a control table a pre-column
// binary created, with stream s1's row, and opens an applier on it.
func tableSetsStoreFixture(ctx context.Context, t *testing.T) (string, ir.ChangeApplier) {
	t.Helper()
	dsn, cleanup := startMySQLForApplier(t)
	t.Cleanup(cleanup)
	applyMySQLApplier(t, dsn, "CREATE TABLE `sluice_cdc_state` ("+
		"stream_id VARCHAR(255) NOT NULL, "+
		"source_position LONGTEXT NOT NULL, "+
		"updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP, "+
		"PRIMARY KEY (stream_id)) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4")
	applyMySQLApplier(t, dsn, "INSERT INTO `sluice_cdc_state` (stream_id, source_position) VALUES ('s1', 'tok')")
	applier, err := Engine{Flavor: FlavorVanilla}.OpenChangeApplier(ctx, dsn)
	if err != nil {
		t.Fatalf("OpenChangeApplier: %v", err)
	}
	t.Cleanup(func() {
		if c, ok := applier.(interface{ Close() error }); ok {
			_ = c.Close()
		}
	})
	return dsn, applier
}

// TestEstablishedTablesStore_RoundTrip pins the established-table record:
// NULL (a pre-column stream) is "not recorded", which Add leaves alone; a
// recorded set round-trips the corpus; the empty set is a recorded fact, not
// NULL; Add extends a recorded set.
func TestEstablishedTablesStore_RoundTrip(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	_, applier := tableSetsStoreFixture(ctx, t)
	store := applier.(ir.EstablishedTablesStore)
	if _, recorded, err := store.ReadEstablishedTables(ctx, "s1"); err != nil || recorded {
		t.Fatalf("pre-column table: recorded=%v err=%v; want not recorded", recorded, err)
	}
	if err := store.AddEstablishedTable(ctx, "s1", "orders"); err != nil {
		t.Fatalf("Add with no record: %v", err)
	}
	if _, recorded, _ := store.ReadEstablishedTables(ctx, "s1"); recorded {
		t.Fatal("Add on an unrecorded stream created a record")
	}
	if err := store.RecordEstablishedTables(ctx, "s1", pendingAddedTablesCorpus); err != nil {
		t.Fatal(err)
	}
	got, recorded, err := store.ReadEstablishedTables(ctx, "s1")
	want := append([]string(nil), pendingAddedTablesCorpus...)
	sort.Strings(want)
	if err != nil || !recorded || !reflect.DeepEqual(got, want) {
		t.Fatalf("Read = (%q, %v, %v)\nwant  %q", got, recorded, err, want)
	}
	if err := store.RecordEstablishedTables(ctx, "s1", nil); err != nil {
		t.Fatal(err)
	}
	if got, recorded, err := store.ReadEstablishedTables(ctx, "s1"); err != nil || !recorded || len(got) != 0 {
		t.Fatalf("the empty set: (%q, %v, %v); want recorded and empty", got, recorded, err)
	}
	if err := store.AddEstablishedTable(ctx, "s1", "app\x00orders"); err != nil {
		t.Fatal(err)
	}
	if got, _, _ := store.ReadEstablishedTables(ctx, "s1"); len(got) != 1 || got[0] != "app\x00orders" {
		t.Fatalf("after Add: %q", got)
	}
	if err := store.RecordEstablishedTables(ctx, "no-such-stream", []string{"x"}); !errors.Is(err, errStreamNotFound) {
		t.Fatalf("Record on a missing row = %v; want errStreamNotFound", err)
	}
}
