//go:build integration

// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package pipeline

import (
	"context"
	"database/sql"
	"strconv"
	"strings"
	"testing"
	"time"

	"sluicesync.dev/sluice/internal/engines"

	_ "sluicesync.dev/sluice/internal/engines/mysql"
	_ "sluicesync.dev/sluice/internal/engines/postgres"
)

// forwardedDefaultShape is one Bug 286 ADD COLUMN shape: the MariaDB DDL
// fragment, the DEFAULT the Postgres target's own catalog must record for
// it, and what a target-side INSERT that OMITS the column must read back.
type forwardedDefaultShape struct {
	col string
	ddl string
	// wantCatalog is information_schema.columns.column_default on the
	// PG target (Valid=false: no default recorded).
	wantCatalog sql.NullString
	// wantReadBack is the column's ::text after a target-side INSERT
	// that omits it (Valid=false: SQL NULL).
	wantReadBack sql.NullString
}

// TestStreamer_AddColumnForward_MariaDBToPostgres_DefaultShapes is the
// end-to-end pin for Bug 286: on a MariaDB BINLOG source, a forwarded
// `ALTER TABLE … ADD COLUMN` must land on the Postgres target with the
// DEFAULT the source declared — not the surface form MariaDB's
// information_schema reports. MariaDB (≥ 10.2.7) reports COLUMN_DEFAULT
// as DEFAULT-expression text: the bare keyword NULL for a defaultless
// nullable column, string literals WITH their quotes (inner quotes
// doubled), and
// current_timestamp() with an EMPTY extra. Before the fix the CDC
// reader's per-table loader translated that text with the MySQL
// convention on every flavor, and the ADR-0091 intercept re-emitted the
// result verbatim: the target recorded DEFAULT 'NULL' (the four-character
// string) and, for the 'abc', empty-string and it's shapes, MariaDB's
// own quote characters stored INSIDE the literal, silently, at exit 0 —
// and DEFAULT CURRENT_TIMESTAMP, misread as a string literal, bypassed
// the ADR-0058 §2a volatility door and killed the stream with a raw
// SQLSTATE 22007. The cold-start migrate path (SchemaReader) was correct
// on every shape, which is why the seed and the boundary projection are
// bound together at the unit tier by
// TestLoadTableSchema_DefaultAgreesWithSeed_EveryBinlogFlavor.
//
// The independent expected values are the TARGET's own catalog
// (column_default) and a target-side INSERT that omits every forwarded
// column, read back — never the forwarding log line. The table carries a
// secondary index so this also exercises the GC-1 population (every
// indexed table now forwards) on the MariaDB flavor end to end.
//
// Shard: the TestStreamer_ prefix routes it to the pipeline-rest-streamer
// shard (ci.yml `-run ^TestStreamer_`), which already boots mariadb:11.4
// for TestStreamer_MariaDB*; the package is on
// scripts/check-shard-coverage.sh's COVERED_PACKAGES list.
func TestStreamer_AddColumnForward_MariaDBToPostgres_DefaultShapes(t *testing.T) {
	sourceDSN, srcCleanup := startMariaDBBinlog(t)
	defer srcCleanup()
	_, pgTargetDSN, pgCleanup := startPostgres(t)
	defer pgCleanup()

	applyMariaDBSQL(t, sourceDSN, `
		CREATE TABLE w (
			id   BIGINT      NOT NULL PRIMARY KEY,
			name VARCHAR(80) NOT NULL,
			INDEX ix_w_name (name)
		) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
		INSERT INTO w (id, name) VALUES (1, 'a'), (2, 'b');`)

	mariaEng, ok := engines.Get("mariadb")
	if !ok {
		t.Fatal("mariadb engine not registered")
	}
	pgEng, ok := engines.Get("postgres")
	if !ok {
		t.Fatal("postgres engine not registered")
	}
	streamer := &Streamer{
		Source:                 mariaEng,
		Target:                 pgEng,
		SourceDSN:              sourceDSN,
		TargetDSN:              pgTargetDSN,
		StreamID:               "test-addcol-fwd-mariadb-pg-defaults",
		ForwardSchemaAddColumn: true,
	}
	streamCtx, streamCancel := context.WithCancel(context.Background())
	defer streamCancel()
	runErr := make(chan error, 1)
	go func() { runErr <- streamer.Run(streamCtx) }()

	if !waitForRowCount(t, pgTargetDSN, "w", 2, 90*time.Second) {
		t.Fatalf("phase A: bulk-copy never landed the seed rows on PG")
	}

	valid := func(s string) sql.NullString { return sql.NullString{String: s, Valid: true} }
	none := sql.NullString{}
	shapes := []forwardedDefaultShape{
		{col: "c1", ddl: "c1 VARCHAR(20) NULL", wantCatalog: none, wantReadBack: none},
		{col: "c2", ddl: "c2 VARCHAR(20) DEFAULT 'abc'", wantCatalog: valid("'abc'::character varying"), wantReadBack: valid("abc")},
		{col: "c3", ddl: "c3 INT DEFAULT 5", wantCatalog: valid("5"), wantReadBack: valid("5")},
		{col: "c5", ddl: "c5 VARCHAR(20) NOT NULL DEFAULT ''", wantCatalog: valid("''::character varying"), wantReadBack: valid("")},
		{col: "c6", ddl: "c6 VARCHAR(20) DEFAULT 'it''s'", wantCatalog: valid("'it''s'::character varying"), wantReadBack: valid("it's")},
	}

	// Phase B: forward each shape, with a source INSERT after each ALTER so
	// the boundary is exercised and the row count is the liveness signal.
	// Run is watched alongside the count: the failure shapes here are a
	// stream that halts (the CURRENT_TIMESTAMP arm's pre-fix SQLSTATE
	// 22007) and a bare timeout would only report the latter as 60s.
	nextID := 3
	for _, sc := range shapes {
		applyMariaDBSQL(t, sourceDSN, "ALTER TABLE w ADD COLUMN "+sc.ddl+";")
		applyMariaDBSQL(t, sourceDSN, "INSERT INTO w (id, name) VALUES ("+strconv.Itoa(nextID)+", 'r"+strconv.Itoa(nextID)+"');")
		deadline := time.Now().Add(60 * time.Second)
		for pollRowCount(pgTargetDSN, "w") < nextID {
			select {
			case err := <-runErr:
				t.Fatalf("phase B (%s): streamer halted instead of forwarding the ADD COLUMN: %v", sc.col, err)
			default:
			}
			if time.Now().After(deadline) {
				t.Fatalf("phase B (%s): post-ALTER row never landed — forwarding broken", sc.col)
			}
			time.Sleep(200 * time.Millisecond)
		}
		nextID++
	}

	tgtDB, err := sql.Open("pgx", pgTargetDSN)
	if err != nil {
		t.Fatalf("open pg target: %v", err)
	}
	defer func() { _ = tgtDB.Close() }()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	// Phase C: the target's own catalog records the declared DEFAULT.
	for _, sc := range shapes {
		var got sql.NullString
		if err := tgtDB.QueryRowContext(ctx, `
			SELECT column_default FROM information_schema.columns
			WHERE table_schema = 'public' AND table_name = 'w' AND column_name = $1`, sc.col).Scan(&got); err != nil {
			t.Fatalf("phase C (%s): read column_default: %v (was the column forwarded at all?)", sc.col, err)
		}
		if got != sc.wantCatalog {
			t.Errorf("phase C (%s): target column_default = %s; want %s — MariaDB's catalog surface form re-emitted as the DEFAULT (Bug 286)",
				sc.col, renderNullString(got), renderNullString(sc.wantCatalog))
		}
	}

	// Phase D: the independent value — a target-side write that omits every
	// forwarded column reads back the DECLARED default, not the string
	// "NULL", not quote characters.
	if _, err := tgtDB.ExecContext(ctx, `INSERT INTO w (id, name) VALUES (100, 'target-side')`); err != nil {
		t.Fatalf("phase D: target-side INSERT omitting the forwarded columns: %v", err)
	}
	for _, sc := range shapes {
		var got sql.NullString
		if err := tgtDB.QueryRowContext(ctx, `SELECT "`+sc.col+`"::text FROM w WHERE id = 100`).Scan(&got); err != nil {
			t.Fatalf("phase D (%s): read back: %v", sc.col, err)
		}
		if got != sc.wantReadBack {
			t.Errorf("phase D (%s): target-side INSERT read back %s; want %s",
				sc.col, renderNullString(got), renderNullString(sc.wantReadBack))
		}
	}

	// Phase E: the volatile arm. MariaDB reports DEFAULT CURRENT_TIMESTAMP
	// as `current_timestamp()` with an EMPTY extra; classified as the
	// expression it is, it must hit the ADR-0058 §2a designed refusal
	// exactly as the identical MySQL 8 DDL does
	// (TestStreamer_AddColumnForward_MySQL_RefusesComputedDefault) — not
	// reach the target as a string literal and die on SQLSTATE 22007.
	applyMariaDBSQL(t, sourceDSN, "ALTER TABLE w ADD COLUMN c4 DATETIME DEFAULT CURRENT_TIMESTAMP;")
	applyMariaDBSQL(t, sourceDSN, "INSERT INTO w (id, name) VALUES (200, 'after-c4');")
	var refusal error
	select {
	case refusal = <-runErr:
	case <-time.After(60 * time.Second):
		t.Fatal("phase E: streamer did not surface the refuse-loudly error for DEFAULT CURRENT_TIMESTAMP within timeout")
	}
	if refusal == nil {
		t.Fatal("phase E: streamer returned nil; expected the ADR-0058 §2a refusal on the computed DEFAULT")
	}
	errStr := strings.ToLower(refusal.Error())
	for _, want := range []string{"computed default", "current_timestamp", "adr-0058 §2a"} {
		if !strings.Contains(errStr, want) {
			t.Errorf("phase E: refusal %q does not mention %q — the MariaDB spelling bypassed the volatility door", refusal, want)
		}
	}
	if strings.Contains(errStr, "sqlstate 22007") {
		t.Errorf("phase E: refusal %q is the raw target error, not the designed refusal", refusal)
	}
	var c4Count int
	if err := tgtDB.QueryRowContext(ctx, `
		SELECT COUNT(*) FROM information_schema.columns
		WHERE table_schema = 'public' AND table_name = 'w' AND column_name = 'c4'`).Scan(&c4Count); err != nil {
		t.Fatalf("phase E: check target c4: %v", err)
	}
	if c4Count != 0 {
		t.Errorf("phase E: target w.c4 exists — the intercept forwarded the volatile DEFAULT instead of refusing")
	}
}

func renderNullString(s sql.NullString) string {
	if !s.Valid {
		return "<SQL NULL>"
	}
	return "[" + s.String + "]"
}
