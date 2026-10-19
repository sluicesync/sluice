//go:build integration

// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package mysql

import (
	"context"
	"database/sql"
	"fmt"
	"reflect"
	"sort"
	"strings"
	"testing"
	"time"

	"sluicesync.dev/sluice/internal/appliershared/tablesetstest"
	"sluicesync.dev/sluice/internal/ir"
)

// TestPendingAddedTablesStore_RoundTrip pins the MySQL add-table pending
// record (INCLUDE-TABLE-SCOPE-DRIFT) against a real server, from a control
// table a pre-column binary created — a repeated identical Record included,
// which MySQL's changed-rows RowsAffected reports as zero rows (see
// [tablesetstest.PendingRoundTrip]) — ending NULL, not an empty string.
func TestPendingAddedTablesStore_RoundTrip(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	dsn, applier := tableSetsStoreFixture(ctx, t)
	tablesetstest.PendingRoundTrip(ctx, t, applier.(ir.PendingAddedTablesStore), errStreamNotFound)

	db, err := sql.Open("mysql", dsn)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = db.Close() }()
	var stored *string
	if err := db.QueryRowContext(ctx, "SELECT pending_added_tables FROM sluice_cdc_state WHERE stream_id = 's1'").Scan(&stored); err != nil || stored != nil {
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

// TestMySQLServerNowMillis_DSTFallBack is R4-3: the pending-add clock reads
// the true instant under a DST session time_zone in the fall-back hour.
// `SET timestamp` pins the session's clock to 06:30:00.250 UTC on
// 2026-11-01 — 01:30:00.250 EST in America/New_York, the SECOND occurrence of
// that local time (05:30 UTC was 01:30 EDT). The expression must return that
// instant to the millisecond.
func TestMySQLServerNowMillis_DSTFallBack(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	dsn, _ := tableSetsStoreFixture(ctx, t)
	db, err := sql.Open("mysql", dsn)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = db.Close() }()
	conn, err := db.Conn(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = conn.Close() }()
	// The test image loads no named zones, so install one with New York's
	// 2026 rules (EDT from 2026-03-08 07:00 UTC, EST from 2026-11-01 06:00
	// UTC) straight into the mysql.time_zone tables, as root.
	for _, q := range []string{
		"DELETE FROM mysql.time_zone_name WHERE Name = 'Sluice/FallBack'", // a -count=N re-run on the shared server
		"INSERT INTO mysql.time_zone (Use_leap_seconds) VALUES ('N')",
		"SET @tz = LAST_INSERT_ID()",
		"INSERT INTO mysql.time_zone_name (Name, Time_zone_id) VALUES ('Sluice/FallBack', @tz)",
		"INSERT INTO mysql.time_zone_transition_type (Time_zone_id, Transition_type_id, Offset, Is_DST, Abbreviation) VALUES " +
			"(@tz, 0, -18000, 0, 'EST'), (@tz, 1, -14400, 1, 'EDT')",
		"INSERT INTO mysql.time_zone_transition (Time_zone_id, Transition_time, Transition_type_id) VALUES " +
			"(@tz, 1772953200, 1), (@tz, 1793512800, 0)",
		"SET time_zone = 'Sluice/FallBack'",
	} {
		if _, err := conn.ExecContext(ctx, q); err != nil {
			t.Fatalf("%s: %v", q, err)
		}
	}
	instant := time.Date(2026, 11, 1, 6, 30, 0, 250_000_000, time.UTC)
	if _, err := conn.ExecContext(ctx, "SET timestamp = ?", float64(instant.UnixMicro())/1e6); err != nil {
		t.Fatal(err)
	}
	var local string
	if err := conn.QueryRowContext(ctx, "SELECT CAST(NOW(3) AS CHAR)").Scan(&local); err != nil || local != "2026-11-01 01:30:00.250" {
		t.Fatalf("session local time %q (%v); want the ambiguous 2026-11-01 01:30:00.250", local, err)
	}
	var got int64
	if err := conn.QueryRowContext(ctx, "SELECT "+mysqlNowMillisExpr).Scan(&got); err != nil {
		t.Fatal(err)
	}
	if got != instant.UnixMilli() {
		t.Fatalf("server now = %d (%v); want %d (%v)", got, time.UnixMilli(got).UTC(), instant.UnixMilli(), instant)
	}
	// Anti-vacuity: the cell must reach the ambiguity. The round-4
	// expression reads this instant an hour early (measured: 05:30:00.250).
	var old float64
	if err := conn.QueryRowContext(ctx, "SELECT UNIX_TIMESTAMP(NOW(6)) * 1000").Scan(&old); err != nil {
		t.Fatal(err)
	}
	if int64(old) == instant.UnixMilli() {
		t.Fatal("UNIX_TIMESTAMP(NOW(6)) read the instant correctly, so this cell no longer reaches the fall-back ambiguity")
	}
	t.Logf("the round-4 expression read %v", time.UnixMilli(int64(old)).UTC())
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

// TestTableSetsStore_RawRoundTrip runs [tablesetstest.RawRoundTrip] on a real
// server through an applier whose sessions run NO_BACKSLASH_ESCAPES with
// client-side interpolation (interpolateParams=true) — the shape where a
// driver that escaped backslashes the default way would store every
// QuoteToASCII escape doubled. The raw column is read and written with this
// test's own SQL (R6 pin 6).
func TestTableSetsStore_RawRoundTrip(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()
	dsn, _ := tableSetsStoreFixture(ctx, t)
	sep := "?"
	if strings.Contains(dsn, "?") {
		sep = "&"
	}
	eng := Engine{Flavor: FlavorVanilla}.WithSQLMode(defaultStrictSQLMode + ",NO_BACKSLASH_ESCAPES")
	applier, err := eng.OpenChangeApplier(ctx, dsn+sep+"interpolateParams=true")
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = applier.(interface{ Close() error }).Close() }()
	store := applier.(tablesetstest.Store)
	if err := store.RecordEstablishedTables(ctx, "s1", nil, nil); err != nil {
		t.Fatal(err)
	}
	db, err := sql.Open("mysql", dsn)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = db.Close() }()
	tablesetstest.RawRoundTrip(ctx, t, store, tablesetstest.RawColumn{
		Set: func(column, value string, ok bool) {
			v := sql.NullString{String: value, Valid: ok}
			if _, err := db.ExecContext(ctx, "UPDATE sluice_cdc_state SET "+column+" = ? WHERE stream_id = 's1'", v); err != nil {
				t.Fatal(err)
			}
		},
		Get: func(column string) (string, bool) {
			var v sql.NullString
			if err := db.QueryRowContext(ctx, "SELECT "+column+" FROM sluice_cdc_state WHERE stream_id = 's1'").Scan(&v); err != nil {
				t.Fatal(err)
			}
			return v.String, v.Valid
		},
	})
	// The full corpus, through the same NO_BACKSLASH_ESCAPES applier.
	if err := store.RecordEstablishedTables(ctx, "s1", tablesetstest.EstablishedCorpus, nil); err != nil {
		t.Fatal(err)
	}
	rec, err := store.ReadEstablishedRecord(ctx, "s1")
	if err != nil {
		t.Fatal(err)
	}
	got := map[string]bool{}
	for _, e := range rec.Established {
		got[e] = true
	}
	for _, want := range tablesetstest.EstablishedCorpus {
		if !got[want] {
			t.Fatalf("%q did not round-trip through the NO_BACKSLASH_ESCAPES applier; got %q", want, rec.Established)
		}
	}
}

// TestTableSetsStore_LargeSetAndNarrowColumn is R6 pin 5: a record past
// TEXT's 64 KB round-trips through the LONGTEXT column, and a pre-existing
// TEXT column is refused with the MODIFY that fixes it (L2).
func TestTableSetsStore_LargeSetAndNarrowColumn(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()
	_, applier := tableSetsStoreFixture(ctx, t)
	store := applier.(tablesetstest.Store)
	big := make([]string, 2000)
	for i := range big {
		big[i] = fmt.Sprintf("table_%04d_%s", i, strings.Repeat("x", 40))
	}
	if err := store.RecordEstablishedTables(ctx, "s1", big, nil); err != nil {
		t.Fatal(err)
	}
	rec, err := store.ReadEstablishedRecord(ctx, "s1")
	if err != nil || len(rec.Established) != len(big) {
		t.Fatalf("a %d-entry record (~100 KB) came back as %d entries, %v", len(big), len(rec.Established), err)
	}

	dsn, cleanup := startMySQLForApplier(t)
	t.Cleanup(cleanup)
	applyMySQLApplier(t, dsn, "DROP TABLE IF EXISTS `sluice_cdc_state`")
	applyMySQLApplier(t, dsn, "CREATE TABLE `sluice_cdc_state` (stream_id VARCHAR(255) NOT NULL, source_position LONGTEXT NOT NULL, "+
		"updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP, established_tables TEXT NULL, PRIMARY KEY (stream_id)) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4")
	applyMySQLApplier(t, dsn, "INSERT INTO `sluice_cdc_state` (stream_id, source_position) VALUES ('s1', 'tok')")
	narrow, err := Engine{Flavor: FlavorVanilla}.OpenChangeApplier(ctx, dsn)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = narrow.(interface{ Close() error }).Close() }()
	err = narrow.(tablesetstest.Store).RecordEstablishedTables(ctx, "s1", []string{"orders"}, nil)
	if err == nil || !strings.Contains(err.Error(), "MODIFY COLUMN `established_tables` LONGTEXT") {
		t.Fatalf("a TEXT established_tables: %v; want the refusal naming the MODIFY", err)
	}
}

// TestLiveAddedTables_LegacyValueAndColumnShape is Bug 321 R9 on a real
// server: a live_added_tables value an older binary or a hand edit wrote
// with padding and duplicates ("x, y ,x") is read and retired by this one;
// the TEXT latin1 column v0.27.0–v0.162.0 created is widened to LONGTEXT
// utf8mb4 on the first table-set write (S-4), after which a non-latin1 name
// round-trips; and a name the comma list cannot hold is refused (S-1).
func TestLiveAddedTables_LegacyValueAndColumnShape(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()
	dsn, applier := tableSetsStoreFixture(ctx, t)
	applyMySQLApplier(t, dsn, "ALTER TABLE `sluice_cdc_state` ADD COLUMN live_added_tables TEXT CHARACTER SET latin1 NULL")
	applyMySQLApplier(t, dsn, "UPDATE `sluice_cdc_state` SET live_added_tables = 'x, y ,x' WHERE stream_id = 's1'")
	ca := applier.(*ChangeApplier)
	got, err := ca.ReadLiveAddedTables(ctx, "s1")
	if err != nil || !reflect.DeepEqual(got, []string{"x", "y"}) {
		t.Fatalf("legacy value read as %v, %v; want [x y]", got, err)
	}
	if unrecorded, err := ca.RetireLiveAddedTables(ctx, "s1", []string{"x"}); err != nil {
		t.Fatalf("retire on a legacy value: %q, %v", unrecorded, err)
	}
	if got, err := ca.ReadLiveAddedTables(ctx, "s1"); err != nil || !reflect.DeepEqual(got, []string{"y"}) {
		t.Fatalf("after the retire: %v, %v; want [y]", got, err)
	}
	var typ, charset string
	if err := withMySQL(t, dsn).QueryRowContext(ctx, "SELECT DATA_TYPE, CHARACTER_SET_NAME FROM information_schema.COLUMNS "+
		"WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = 'sluice_cdc_state' AND COLUMN_NAME = 'live_added_tables'").Scan(&typ, &charset); err != nil {
		t.Fatal(err)
	}
	if typ != "longtext" || charset != "utf8mb4" {
		t.Fatalf("live_added_tables is %s %s after a table-set write; want longtext utf8mb4 (widened)", typ, charset)
	}
	if err := ca.RecordLiveAddedTable(ctx, "s1", "naïve_表"); err != nil {
		t.Fatal(err)
	}
	if got, err := ca.ReadLiveAddedTables(ctx, "s1"); err != nil || !reflect.DeepEqual(got, []string{"naïve_表", "y"}) {
		t.Fatalf("a non-latin1 name came back as %v, %v", got, err)
	}
	for _, bad := range []string{" t", "t ", "a,b"} {
		if err := ca.RecordLiveAddedTable(ctx, "s1", bad); err == nil {
			t.Errorf("RecordLiveAddedTable(%q) accepted a name the comma list cannot hold", bad)
		}
		if err := ca.CheckLiveAddedTableName(bad); err == nil || !strings.Contains(err.Error(), "glob") {
			t.Errorf("CheckLiveAddedTableName(%q) = %v; want the refusal naming the glob route", bad, err)
		}
	}
}

// TestTableSetColumnsDDL_RunsOnEveryReleasedShape is R9e on a real server:
// the statement a table-set refusal names is EXECUTED against the two
// shapes an upgrade meets — (a) the v0.27.0–v0.162.0 table (live_added_tables
// TEXT latin1, no record columns), and (b) an older one without
// live_added_tables — and afterwards nothing is left to change and the
// ensure check passes. The fixed ADD-all form failed (a) with 1060. On (a) a
// non-ASCII latin1 value ("café") is carried through the MODIFY. On both, a
// 4-byte name round-trips, read back with this test's own SQL as well as
// the applier's.
func TestTableSetColumnsDDL_RunsOnEveryReleasedShape(t *testing.T) {
	for _, tc := range []struct {
		name       string
		setup      []string
		liveClause string
		legacy     []string
	}{
		{
			name: "v0.27.0-v0.162.0 shape",
			setup: []string{
				"ALTER TABLE `sluice_cdc_state` ADD COLUMN live_added_tables TEXT CHARACTER SET latin1 NULL",
				"UPDATE `sluice_cdc_state` SET live_added_tables = 'café' WHERE stream_id = 's1'",
			},
			liveClause: "MODIFY COLUMN `live_added_tables`",
			legacy:     []string{"café"},
		},
		{name: "no live_added_tables", liveClause: "ADD COLUMN `live_added_tables`"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
			defer cancel()
			dsn, applier := tableSetsStoreFixture(ctx, t)
			for _, q := range tc.setup {
				applyMySQLApplier(t, dsn, q)
			}
			ca := applier.(*ChangeApplier)
			stmt := ca.tableSetColumnsDDL(ctx)
			if !strings.Contains(stmt, tc.liveClause) {
				t.Fatalf("the statement for this shape is %q; want it to carry %s", stmt, tc.liveClause)
			}
			db := withMySQL(t, dsn)
			if _, err := db.ExecContext(ctx, stmt); err != nil {
				t.Fatalf("the statement a refusal names does not run on this shape: %v\n%s", err, stmt)
			}
			if left := ca.tableSetColumnsDDL(ctx); left != "" {
				t.Fatalf("after the statement ran, the table still needs: %s", left)
			}
			if err := ca.ensureTableSetColumns(ctx, "s1"); err != nil {
				t.Fatalf("the ensure check after the statement: %v", err)
			}
			raw := func() string {
				var v sql.NullString
				if err := db.QueryRowContext(ctx, "SELECT live_added_tables FROM sluice_cdc_state WHERE stream_id = 's1'").Scan(&v); err != nil {
					t.Fatal(err)
				}
				return v.String
			}
			if got := raw(); got != strings.Join(tc.legacy, ",") {
				t.Fatalf("live_added_tables reads %q after the statement; want %q carried through", got, strings.Join(tc.legacy, ","))
			}
			const emoji = "t_\U0001F600"
			if err := ca.RecordLiveAddedTable(ctx, "s1", emoji); err != nil {
				t.Fatal(err)
			}
			if got := raw(); !strings.Contains(got, emoji) {
				t.Fatalf("live_added_tables reads %q; want the 4-byte name %q stored intact", got, emoji)
			}
			want := append(append([]string(nil), tc.legacy...), emoji)
			sort.Strings(want)
			if got, err := ca.ReadLiveAddedTables(ctx, "s1"); err != nil || !reflect.DeepEqual(got, want) {
				t.Fatalf("the applier reads %q, %v; want %q", got, err, want)
			}
		})
	}
}

// withMySQL opens dsn for the test's own queries.
func withMySQL(t *testing.T, dsn string) *sql.DB {
	t.Helper()
	db, err := sql.Open("mysql", dsn)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = db.Close() })
	return db
}

// TestTableIdentities_SurviveRename is Bug 321 R6's premise on a real
// server: InnoDB's TABLE_ID survives RENAME TABLE, and a table dropped and
// re-created under the new name gets a different one.
func TestTableIdentities_SurviveRename(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	dsn, cleanup := startMySQLForApplier(t)
	t.Cleanup(cleanup)
	applyMySQLApplier(t, dsn, "CREATE TABLE t8 (id INT PRIMARY KEY)")
	eng := Engine{Flavor: FlavorVanilla}
	before, err := eng.TableIdentities(ctx, dsn, []string{"t8"})
	if err != nil || before["t8"] == "" {
		t.Fatalf("no TABLE_ID for t8: %v, %v", before, err)
	}
	applyMySQLApplier(t, dsn, "RENAME TABLE t8 TO t8b")
	after, err := eng.TableIdentities(ctx, dsn, []string{"t8", "t8b"})
	if err != nil || after["t8b"] != before["t8"] || after["t8"] != "" {
		t.Fatalf("after RENAME TABLE: %v (%v); want t8b to carry t8's id %s", after, err, before["t8"])
	}
	applyMySQLApplier(t, dsn, "DROP TABLE t8b")
	applyMySQLApplier(t, dsn, "CREATE TABLE t8b (id INT PRIMARY KEY)")
	recreated, err := eng.TableIdentities(ctx, dsn, []string{"t8b"})
	if err != nil || recreated["t8b"] == "" || recreated["t8b"] == before["t8"] {
		t.Fatalf("a re-created t8b: %v (%v); want a new id, not %s", recreated, err, before["t8"])
	}
	if ids, err := (Engine{Flavor: FlavorVitess}).TableIdentities(ctx, dsn, []string{"t8b"}); err != nil || len(ids) != 0 {
		t.Fatalf("the Vitess flavor reported ids %v, %v; it has none", ids, err)
	}

	// R6-S3's premise, bound to the capability the door keys on: InnoDB
	// gives a table a new TABLE_ID on routine rebuilds, so the engine must
	// say its id does not survive a rebuild — and the door therefore must
	// not refuse a changed id on MySQL without proof of a swap.
	if eng.TableIdentitySurvivesRebuild() {
		t.Fatal("TableIdentitySurvivesRebuild() = true; InnoDB rebuilds change TABLE_ID")
	}
	applyMySQLApplier(t, dsn, "INSERT INTO t8b VALUES (1), (2)")
	prev := recreated["t8b"]
	for _, rebuild := range []string{
		"OPTIMIZE TABLE t8b", "ALTER TABLE t8b ENGINE=InnoDB", "ALTER TABLE t8b ADD COLUMN v INT, ALGORITHM=COPY", "TRUNCATE TABLE t8b",
	} {
		applyMySQLApplier(t, dsn, rebuild)
		got, err := eng.TableIdentities(ctx, dsn, []string{"t8b"})
		if err != nil || got["t8b"] == "" {
			t.Fatalf("after %s: %v, %v", rebuild, got, err)
		}
		if got["t8b"] == prev {
			t.Fatalf("%s kept TABLE_ID %s; the measured premise (a rebuild changes it) no longer holds — revisit "+
				"TableIdentitySurvivesRebuild and the door's MySQL branch", rebuild, prev)
		}
		prev = got["t8b"]
	}
	applyMySQLApplier(t, dsn, "ALTER TABLE t8b ADD COLUMN w INT, ALGORITHM=INSTANT")
	if got, _ := eng.TableIdentities(ctx, dsn, []string{"t8b"}); got["t8b"] != prev {
		t.Fatalf("an INSTANT ADD COLUMN changed TABLE_ID %s → %s", prev, got["t8b"])
	}
}

// TestColdStartAnchor_MarksAFirstColdStart runs [tablesetstest.ColdStartAnchor]
// (Bug 321 R9b) on a control table the applier creates itself.
func TestColdStartAnchor_MarksAFirstColdStart(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	dsn, cleanup := startMySQLForApplier(t)
	t.Cleanup(cleanup)
	applyMySQLApplier(t, dsn, "DROP TABLE IF EXISTS `sluice_cdc_state`")
	for _, st := range (Engine{Flavor: FlavorVanilla}).ControlTableDDL() {
		applyMySQLApplier(t, dsn, st.DDL)
	}
	applier, err := Engine{Flavor: FlavorVanilla}.OpenChangeApplier(ctx, dsn)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = applier.(interface{ Close() error }).Close() }()
	tablesetstest.ColdStartAnchor(ctx, t, applier.(tablesetstest.Store))
}
