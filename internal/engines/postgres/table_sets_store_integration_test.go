//go:build integration

// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package postgres

import (
	"context"
	"database/sql"
	"net/url"
	"strings"
	"testing"
	"time"

	"sluicesync.dev/sluice/internal/appliershared/tablesetstest"
	"sluicesync.dev/sluice/internal/ir"
)

// TestPendingAddedTablesStore_RoundTrip pins the Postgres add-table pending
// record (INCLUDE-TABLE-SCOPE-DRIFT) against a real server, from a control
// table a pre-column binary created: no record reads as empty, Record adds
// the column itself, every name and both timestamps round-trip, Record of a
// missing stream row is a loud error, and Clear is exact and idempotent —
// ending NULL, not an empty string.
func TestPendingAddedTablesStore_RoundTrip(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	dsn, applier := tableSetsStoreFixture(ctx, t)
	tablesetstest.PendingRoundTrip(ctx, t, applier.(ir.PendingAddedTablesStore), errStreamNotFound)

	db, err := sql.Open("pgx", dsn)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = db.Close() }()
	var stored *string
	if err := db.QueryRowContext(ctx, `SELECT pending_added_tables FROM sluice_cdc_state WHERE stream_id = 's1'`).Scan(&stored); err != nil || stored != nil {
		t.Fatalf("an emptied record is stored as NULL; got %v, %v", stored, err)
	}
}

// TestEstablishedTablesStore_RoundTrip pins the established/dropped record
// against a real server (see [tablesetstest.EstablishedRoundTrip]).
func TestEstablishedTablesStore_RoundTrip(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	_, applier := tableSetsStoreFixture(ctx, t)
	tablesetstest.EstablishedRoundTrip(ctx, t, applier.(tablesetstest.Store), errStreamNotFound)
}

// tableSetsStoreFixture starts a server holding a control table a pre-column
// binary created, with stream s1's row, and opens an applier on it.
func tableSetsStoreFixture(ctx context.Context, t *testing.T) (string, ir.ChangeApplier) {
	t.Helper()
	dsn, cleanup := startPostgresForApplier(t)
	t.Cleanup(cleanup)
	applyPGApplier(t, dsn, `
		CREATE TABLE "public"."sluice_cdc_state" (
			stream_id       VARCHAR(255) NOT NULL,
			source_position TEXT         NOT NULL,
			updated_at      TIMESTAMP    NOT NULL DEFAULT CURRENT_TIMESTAMP,
			PRIMARY KEY (stream_id)
		);
		INSERT INTO "public"."sluice_cdc_state" (stream_id, source_position) VALUES ('s1', 'tok');
	`)
	applier, err := Engine{}.OpenChangeApplier(ctx, dsn)
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

// TestTableSetsStore_RawRoundTrip runs [tablesetstest.RawRoundTrip] against
// a real server, writing and reading the raw column with this test's own SQL.
func TestTableSetsStore_RawRoundTrip(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	dsn, applier := tableSetsStoreFixture(ctx, t)
	store := applier.(tablesetstest.Store)
	if err := store.RecordEstablishedTables(ctx, "s1", nil, nil); err != nil { // adds the three columns
		t.Fatal(err)
	}
	db, err := sql.Open("pgx", dsn)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = db.Close() }()
	tablesetstest.RawRoundTrip(ctx, t, store, tablesetstest.RawColumn{
		Set: func(column, value string, ok bool) {
			v := sql.NullString{String: value, Valid: ok}
			if _, err := db.ExecContext(ctx, `UPDATE sluice_cdc_state SET `+column+` = $1 WHERE stream_id = 's1'`, v); err != nil {
				t.Fatal(err)
			}
		},
		Get: func(column string) (string, bool) {
			var v sql.NullString
			if err := db.QueryRowContext(ctx, `SELECT `+column+` FROM sluice_cdc_state WHERE stream_id = 's1'`).Scan(&v); err != nil {
				t.Fatal(err)
			}
			return v.String, v.Valid
		},
	})
}

// TestTableSetsStore_RefusesANarrowColumn is R6 L2: a pre-existing record
// column of another type than TEXT is refused, naming the ALTER, rather than
// written into.
func TestTableSetsStore_RefusesANarrowColumn(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	dsn, cleanup := startPostgresForApplier(t)
	t.Cleanup(cleanup)
	applyPGApplier(t, dsn, `
		CREATE TABLE "public"."sluice_cdc_state" (
			stream_id VARCHAR(255) NOT NULL, source_position TEXT NOT NULL,
			updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP, established_tables VARCHAR(40),
			PRIMARY KEY (stream_id)
		);
		INSERT INTO "public"."sluice_cdc_state" (stream_id, source_position) VALUES ('s1', 'tok');
	`)
	applier, err := Engine{}.OpenChangeApplier(ctx, dsn)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = applier.(interface{ Close() error }).Close() }()
	err = applier.(tablesetstest.Store).RecordEstablishedTables(ctx, "s1", []string{"orders"}, nil)
	if err == nil || !strings.Contains(err.Error(), "ALTER COLUMN established_tables TYPE TEXT") {
		t.Fatalf("a VARCHAR(40) established_tables: %v; want the refusal naming the ALTER", err)
	}
}

// TestTableSetsStore_NarrowingOnAControlTableThisRoleCannotAlter is R6 S1's
// likeliest cause, on a real server: a DBA shipped established_tables only,
// and the stream's role holds DML alone. The narrowing's write must fail —
// naming the statement that adds all three record columns — rather than
// pass (the door then refuses the start: TestScopeDriftDoor_NarrowingMustBeRecorded).
func TestTableSetsStore_NarrowingOnAControlTableThisRoleCannotAlter(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	dsn, cleanup := startPostgresForApplier(t)
	t.Cleanup(cleanup)
	applyPGApplier(t, dsn, `
		CREATE TABLE "public"."sluice_cdc_state" (
			stream_id VARCHAR(255) NOT NULL, source_position TEXT NOT NULL,
			updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP, established_tables TEXT,
			PRIMARY KEY (stream_id)
		);
		INSERT INTO "public"."sluice_cdc_state" (stream_id, source_position, established_tables) VALUES ('s1', 'tok', '"kd"' || chr(10) || '"nt"');
		DROP ROLE IF EXISTS sluice_dml_only;
		CREATE ROLE sluice_dml_only LOGIN PASSWORD 'dml';
		GRANT USAGE ON SCHEMA public TO sluice_dml_only;
		GRANT SELECT, INSERT, UPDATE, DELETE ON "public"."sluice_cdc_state" TO sluice_dml_only;
	`)
	u, err := url.Parse(dsn)
	if err != nil {
		t.Fatal(err)
	}
	u.User = url.UserPassword("sluice_dml_only", "dml")
	applier, err := Engine{}.OpenChangeApplier(ctx, u.String())
	if err != nil {
		t.Skipf("the DML-only role cannot open an applier here (%v); the refusal is pinned by the unit door test", err)
	}
	defer func() { _ = applier.(interface{ Close() error }).Close() }()
	_, err = applier.(tablesetstest.Store).NarrowEstablishedRecord(ctx, "s1", ir.RecordNarrowing{InScope: []string{"kd"}})
	if err == nil {
		t.Fatal("the narrowing wrote nothing and reported success on a control table lacking dropped_tables")
	}
	for _, want := range []string{"pending_added_tables", "established_tables", "dropped_tables"} {
		if !strings.Contains(err.Error(), want) {
			t.Fatalf("the refusal does not name %s in its statement: %v", want, err)
		}
	}
}

// TestTableIdentities_SurviveRename is Bug 321 R6's premise on a real
// server: pg_class.oid survives ALTER TABLE … RENAME, and a table dropped and
// re-created under the new name gets a different one.
func TestTableIdentities_SurviveRename(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	dsn, cleanup := startPostgresForApplier(t)
	t.Cleanup(cleanup)
	applyPGApplier(t, dsn, `CREATE TABLE t8 (id INT PRIMARY KEY)`)
	before, err := Engine{}.TableIdentities(ctx, dsn, []string{"t8"})
	if err != nil || before["t8"] == "" {
		t.Fatalf("no oid for t8: %v, %v", before, err)
	}
	applyPGApplier(t, dsn, `ALTER TABLE t8 RENAME TO t8b`)
	after, err := Engine{}.TableIdentities(ctx, dsn, []string{"t8", "t8b"})
	if err != nil || after["t8b"] != before["t8"] || after["t8"] != "" {
		t.Fatalf("after RENAME: %v (%v); want t8b to carry t8's oid %s", after, err, before["t8"])
	}
	applyPGApplier(t, dsn, `DROP TABLE t8b; CREATE TABLE t8b (id INT PRIMARY KEY)`)
	recreated, err := Engine{}.TableIdentities(ctx, dsn, []string{"t8b"})
	if err != nil || recreated["t8b"] == "" || recreated["t8b"] == before["t8"] {
		t.Fatalf("a re-created t8b: %v (%v); want a new oid, not %s", recreated, err, before["t8"])
	}

	// R6-S3's premise, bound to the capability the door keys on: a table
	// rewrite changes relfilenode, never the oid, so the engine says its id
	// survives a rebuild — and the door refuses an oid that changed under
	// the same name as a different relation.
	if !(Engine{}).TableIdentitySurvivesRebuild() {
		t.Fatal("TableIdentitySurvivesRebuild() = false on Postgres")
	}
	applyPGApplier(t, dsn, `INSERT INTO t8b SELECT g FROM generate_series(1, 100) g`)
	for _, rewrite := range []string{
		`VACUUM FULL t8b`, `CLUSTER t8b USING t8b_pkey`, `TRUNCATE t8b`, `ALTER TABLE t8b ALTER COLUMN id TYPE BIGINT`,
		`ALTER TABLE t8b SET UNLOGGED`, `ALTER TABLE t8b SET LOGGED`,
	} {
		applyPGApplier(t, dsn, rewrite)
		got, err := Engine{}.TableIdentities(ctx, dsn, []string{"t8b"})
		if err != nil || got["t8b"] != recreated["t8b"] {
			t.Fatalf("after %s: %v (%v); want the oid %s kept — the premise TableIdentitySurvivesRebuild rests on",
				rewrite, got, err, recreated["t8b"])
		}
	}
}
