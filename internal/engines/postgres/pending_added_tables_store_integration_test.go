//go:build integration

// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package postgres

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
// byte-exact through the real column.
var pendingAddedTablesCorpus = []string{
	"orders", "Orders", "naïve_表", "a,b", "line\nbreak", `quo"te`, `back\slash`, " padded ", "😀", "\xff\xferaw",
}

// TestPendingAddedTablesStore_RoundTrip pins the Postgres add-table pending
// record (INCLUDE-TABLE-SCOPE-DRIFT) against a real server, from a control
// table a pre-column binary created: no record reads as empty, Record adds
// the column itself, the corpus round-trips, Record of a missing stream row is
// a loud error, and Clear removes exactly what it names and is idempotent.
func TestPendingAddedTablesStore_RoundTrip(t *testing.T) {
	dsn, cleanup := startPostgresForApplier(t)
	defer cleanup()
	applyPGApplier(t, dsn, `
		CREATE TABLE "public"."sluice_cdc_state" (
			stream_id       VARCHAR(255) NOT NULL,
			source_position TEXT         NOT NULL,
			updated_at      TIMESTAMP    NOT NULL DEFAULT CURRENT_TIMESTAMP,
			PRIMARY KEY (stream_id)
		);
		INSERT INTO "public"."sluice_cdc_state" (stream_id, source_position) VALUES ('s1', 'tok');
	`)
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	applier, err := Engine{}.OpenChangeApplier(ctx, dsn)
	if err != nil {
		t.Fatalf("OpenChangeApplier: %v", err)
	}
	defer func() {
		if c, ok := applier.(interface{ Close() error }); ok {
			_ = c.Close()
		}
	}()
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
	var stored *string
	db, err := sql.Open("pgx", dsn)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = db.Close() }()
	if err := db.QueryRowContext(ctx, `SELECT pending_added_tables FROM sluice_cdc_state WHERE stream_id = 's1'`).Scan(&stored); err != nil || stored != nil {
		t.Fatalf("an emptied record is stored as NULL; got %v, %v", stored, err)
	}
	if err := store.ClearPendingAddedTables(ctx, "no-such-stream", []string{"x"}); err != nil {
		t.Fatalf("Clear on a missing row = %v; want nil", err)
	}
}
