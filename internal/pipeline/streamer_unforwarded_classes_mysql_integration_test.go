//go:build integration

// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package pipeline

import (
	"context"
	"database/sql"
	"strings"
	"testing"
	"time"

	"sluicesync.dev/sluice/internal/engines"
	"sluicesync.dev/sluice/internal/ir"
	"sluicesync.dev/sluice/internal/pipeline/migcore"

	_ "sluicesync.dev/sluice/internal/engines/mysql"
	_ "sluicesync.dev/sluice/internal/engines/postgres"
)

// unforwardedCell is one source DDL the binlog lane's boundary projection
// cannot carry. create/ddl run on the source; the stream must END with
// UNFORWARDED-SCHEMA-CHANGE naming wantDelta, and targetHas — a query on
// the Postgres target's catalog — must read 0: the target does NOT have
// the change, which is what makes the refusal the only thing standing
// between the operator and a silently weaker target.
type unforwardedCell struct {
	table, create, ddl string
	wantDelta          string
	targetHas          string
}

// runUnforwardedCell cold-starts one table on its own stream, applies the
// DDL plus one INSERT (the row is what makes the reader rebuild the table
// — the binlog lane's analogue of pgoutput re-sending a RelationMessage),
// and grades the refusal against the target's own catalog.
func runUnforwardedCell(t *testing.T, src ir.Engine, sourceDSN, targetDSN string, apply func(*testing.T, string, string), tc unforwardedCell) {
	t.Helper()
	tgtDB, err := sql.Open("pgx", targetDSN)
	if err != nil {
		t.Fatalf("open target: %v", err)
	}
	defer func() { _ = tgtDB.Close() }()
	pgEng, ok := engines.Get("postgres")
	if !ok {
		t.Fatal("postgres engine not registered")
	}

	apply(t, sourceDSN, tc.create+"; INSERT INTO "+tc.table+" (id, name) VALUES (1, 'a');")
	streamer := &Streamer{
		Source: src, Target: pgEng,
		SourceDSN: sourceDSN, TargetDSN: targetDSN,
		StreamID: "test-gc2-" + src.Name() + "-" + strings.ReplaceAll(tc.table, "_", "-"),
		Filter:   migcore.TableFilter{Include: []string{tc.table}},
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	runErr := make(chan error, 1)
	go func() { runErr <- streamer.Run(ctx) }()
	if !waitForPGRowCount(t, targetDSN, tc.table, 1, 90*time.Second) {
		select {
		case err := <-runErr:
			t.Fatalf("cold copy never landed; stream ended: %v", err)
		default:
		}
		t.Fatalf("cold copy never landed")
	}
	// A row that arrives through CDC proves StreamChanges ran — and so
	// that the baseline was taken — before the DDL. The copied row alone
	// does not: the stream opens after the copy's index and constraint
	// phases, and a DDL landing in that window is IN the baseline (the
	// door's documented start-of-stream residue), not a refusal.
	apply(t, sourceDSN, "INSERT INTO "+tc.table+" (id, name) VALUES (100, 'live');")
	if !waitForPGRowCount(t, targetDSN, tc.table, 2, 60*time.Second) {
		t.Fatalf("the CDC row never landed; the stream is not live")
	}

	apply(t, sourceDSN, tc.ddl+"; INSERT INTO "+tc.table+" (id, name) VALUES (2, 'b');")

	var got error
	select {
	case got = <-runErr:
	case <-time.After(60 * time.Second):
		t.Fatalf("stream did not refuse within 60s — the change was ignored (did the DDL clear the schema cache?)")
	}
	if got == nil || !strings.Contains(got.Error(), "UNFORWARDED-SCHEMA-CHANGE") || !strings.Contains(got.Error(), tc.wantDelta) {
		t.Fatalf("stream ended with %v; want UNFORWARDED-SCHEMA-CHANGE naming %q", got, tc.wantDelta)
	}
	var n int
	if err := tgtDB.QueryRow(tc.targetHas).Scan(&n); err != nil {
		t.Fatalf("read target catalog: %v", err)
	}
	if n != 0 {
		t.Errorf("target already reflects the change (%s = %d); the test is not measuring an uncarried change", tc.targetHas, n)
	}
	if pollRowCount(targetDSN, tc.table) != 2 {
		t.Errorf("the post-DDL row reached the target; the refusal must come before the change it guards")
	}
}

// TestStreamer_MySQLSource_UnforwardedSchemaChange_Refuses is the
// end-to-end pin for GC-2's binlog sibling on a real MySQL → Postgres
// stream. The binlog lane's boundary projection carries columns and the
// primary key, and the classifier compares neither foreign keys, unique
// keys, CHECKs, the primary key nor a column DEFAULT, so before the door
// each of these was ignored at exit 0. The door rests on one premise the
// unit tier cannot check — that each of these DDLs reaches the reader as
// an in-scope QueryEvent that clears the schema cache, so the next row
// forces a rebuild — and this test is where that premise is asserted,
// per DDL.
//
// The control forwards an ADD COLUMN NOT NULL DEFAULT and then a DROP
// COLUMN whose UNIQUE key goes with it, and requires the stream to keep
// applying: the exemptions are what keep the door from ending streams
// the ADR-0091 forward handles correctly.
//
// Shard: TestStreamer_ prefix → pipeline-rest-streamer.
func TestStreamer_MySQLSource_UnforwardedSchemaChange_Refuses(t *testing.T) {
	sourceDSN, _, cleanup := startMySQLBinlog(t)
	defer cleanup()
	_, targetDSN, pgCleanup := startPostgres(t)
	defer pgCleanup()
	myEng, ok := engines.Get("mysql")
	if !ok {
		t.Fatal("mysql engine not registered")
	}
	applyDDLMySQL(t, sourceDSN, `CREATE TABLE parent (id INT PRIMARY KEY) ENGINE=InnoDB; INSERT INTO parent VALUES (1);`)

	cells := []unforwardedCell{
		{
			table:     "gc2_fk",
			create:    `CREATE TABLE gc2_fk (id INT PRIMARY KEY, name VARCHAR(40)) ENGINE=InnoDB`,
			ddl:       `ALTER TABLE gc2_fk ADD COLUMN owner INT, ADD CONSTRAINT gc2_fk_owner FOREIGN KEY (owner) REFERENCES parent(id)`,
			wantDelta: `ADD CONSTRAINT "gc2_fk_owner" FOREIGN KEY (owner) REFERENCES source_db.parent (id)`,
			targetHas: `SELECT count(*) FROM pg_constraint WHERE conname = 'gc2_fk_owner'`,
		},
		{
			table:     "gc2_uq",
			create:    `CREATE TABLE gc2_uq (id INT PRIMARY KEY, name VARCHAR(40)) ENGINE=InnoDB`,
			ddl:       `ALTER TABLE gc2_uq ADD CONSTRAINT gc2_uq_name UNIQUE (name)`,
			wantDelta: `ADD CONSTRAINT "gc2_uq_name" UNIQUE (name)`,
			targetHas: `SELECT count(*) FROM pg_index i JOIN pg_class c ON c.oid = i.indrelid WHERE c.relname = 'gc2_uq' AND i.indisunique AND NOT i.indisprimary`,
		},
		{
			table:     "gc2_chk",
			create:    `CREATE TABLE gc2_chk (id INT PRIMARY KEY, name VARCHAR(40), CONSTRAINT gc2_chk_len CHECK (CHAR_LENGTH(name) < 50)) ENGINE=InnoDB`,
			ddl:       `ALTER TABLE gc2_chk DROP CHECK gc2_chk_len`,
			wantDelta: `DROP CONSTRAINT "gc2_chk_len"`,
			// The target KEEPS the check the source dropped.
			targetHas: `SELECT count(*) - 1 FROM pg_constraint c JOIN pg_class r ON r.oid = c.conrelid WHERE r.relname = 'gc2_chk' AND c.contype = 'c'`,
		},
		{
			table:     "gc2_enf",
			create:    `CREATE TABLE gc2_enf (id INT PRIMARY KEY, name VARCHAR(40), CONSTRAINT gc2_enf_len CHECK (CHAR_LENGTH(name) < 50)) ENGINE=InnoDB`,
			ddl:       `ALTER TABLE gc2_enf ALTER CHECK gc2_enf_len NOT ENFORCED`,
			wantDelta: `CONSTRAINT "gc2_enf_len" changed`,
			// The target still ENFORCES the check the source relaxed.
			targetHas: `SELECT count(*) - 1 FROM pg_constraint c JOIN pg_class r ON r.oid = c.conrelid WHERE r.relname = 'gc2_enf' AND c.contype = 'c'`,
		},
		{
			table:     "gc2_pk",
			create:    `CREATE TABLE gc2_pk (id INT NOT NULL, name VARCHAR(40) NOT NULL, PRIMARY KEY (id)) ENGINE=InnoDB`,
			ddl:       `ALTER TABLE gc2_pk DROP PRIMARY KEY, ADD PRIMARY KEY (id, name)`,
			wantDelta: `PRIMARY KEY changed: PRIMARY KEY (id) -> PRIMARY KEY (id, name)`,
			targetHas: `SELECT count(*) - 1 FROM pg_index i JOIN pg_class c ON c.oid = i.indrelid, unnest(i.indkey) AS k WHERE c.relname = 'gc2_pk' AND i.indisprimary`,
		},
		{
			table:     "gc2_def",
			create:    `CREATE TABLE gc2_def (id INT PRIMARY KEY, name VARCHAR(40)) ENGINE=InnoDB`,
			ddl:       `ALTER TABLE gc2_def ALTER COLUMN name SET DEFAULT 'z'`,
			wantDelta: `ALTER COLUMN "name" SET DEFAULT z`,
			targetHas: `SELECT count(*) FROM information_schema.columns WHERE table_name = 'gc2_def' AND column_name = 'name' AND column_default IS NOT NULL`,
		},
	}
	for _, tc := range cells {
		t.Run(tc.table, func(t *testing.T) {
			runUnforwardedCell(t, myEng, sourceDSN, targetDSN, applyDDLMySQL, tc)
		})
	}

	t.Run("control_forwarded_column_changes_do_not_refuse", func(t *testing.T) {
		runUnforwardedControl(t, myEng, sourceDSN, targetDSN, applyDDLMySQL)
	})
}

// runUnforwardedControl forwards an ADD COLUMN NOT NULL DEFAULT and then a
// DROP COLUMN whose single-column UNIQUE key goes with it, and requires
// the stream to keep applying. Every rebuild here is diffed by the door —
// the first against the StreamChanges baseline, which also makes this the
// pin that a table's first rebuild after a start compares clean — and
// none may refuse.
func runUnforwardedControl(t *testing.T, src ir.Engine, sourceDSN, targetDSN string, apply func(*testing.T, string, string)) {
	t.Helper()
	tgtDB, err := sql.Open("pgx", targetDSN)
	if err != nil {
		t.Fatalf("open target: %v", err)
	}
	defer func() { _ = tgtDB.Close() }()
	pgEng, ok := engines.Get("postgres")
	if !ok {
		t.Fatal("postgres engine not registered")
	}
	apply(t, sourceDSN, `
		CREATE TABLE gc2_ctl (id INT PRIMARY KEY, name VARCHAR(40), doomed VARCHAR(40), UNIQUE KEY gc2_ctl_doomed_uq (doomed)) ENGINE=InnoDB;
		INSERT INTO gc2_ctl (id, name, doomed) VALUES (1, 'a', 'x');`)
	streamer := &Streamer{
		Source: src, Target: pgEng,
		SourceDSN: sourceDSN, TargetDSN: targetDSN,
		StreamID: "test-gc2-" + src.Name() + "-ctl",
		Filter:   migcore.TableFilter{Include: []string{"gc2_ctl"}},
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	runErr := make(chan error, 1)
	go func() { runErr <- streamer.Run(ctx) }()
	if !waitForPGRowCount(t, targetDSN, "gc2_ctl", 1, 90*time.Second) {
		t.Fatalf("cold copy never landed")
	}
	// Live before the first DDL, so both boundaries below are diffed
	// against a baseline that predates them (see runUnforwardedCell).
	apply(t, sourceDSN, "INSERT INTO gc2_ctl (id, name, doomed) VALUES (100, 'live', 'l');")
	if !waitForPGRowCount(t, targetDSN, "gc2_ctl", 2, 60*time.Second) {
		t.Fatalf("the CDC row never landed; the stream is not live")
	}
	// ADD COLUMN NOT NULL DEFAULT: the added column's own attributes ride
	// the forward. Also the prime that makes the DROP below a CDC→CDC
	// boundary (the seed guard skips a destructive shape against the
	// cold-start seed).
	apply(t, sourceDSN, `
		ALTER TABLE gc2_ctl ADD COLUMN n INT NOT NULL DEFAULT 0;
		INSERT INTO gc2_ctl (id, name, doomed) VALUES (2, 'b', 'y');`)
	if !waitForPGColumn(t, tgtDB, "gc2_ctl", "n", true, 60*time.Second) {
		select {
		case err := <-runErr:
			t.Fatalf("stream ended on the ADD COLUMN forward: %v", err)
		default:
		}
		t.Fatalf("ADD COLUMN never forwarded")
	}
	// DROP COLUMN takes its single-column UNIQUE key with it: exempt, the
	// target's DROP does too.
	apply(t, sourceDSN, `
		ALTER TABLE gc2_ctl DROP COLUMN doomed;
		INSERT INTO gc2_ctl (id, name) VALUES (3, 'c');`)
	if !waitForPGRowID(t, tgtDB, "gc2_ctl", 3, 60*time.Second) {
		select {
		case err := <-runErr:
			t.Fatalf("stream ended on the DROP COLUMN forward: %v", err)
		default:
		}
		t.Fatalf("post-DROP row never landed")
	}
	select {
	case err := <-runErr:
		t.Fatalf("stream ended: %v", err)
	default:
	}
	cancel()
	select {
	case <-runErr:
	case <-time.After(15 * time.Second):
		t.Fatal("Streamer.Run did not return after ctx cancel")
	}
}

// TestStreamer_MariaDBSource_UnforwardedSchemaChange_Refuses is the
// MariaDB flavor's leg of the same pin. MariaDB differs from MySQL in
// exactly the catalog surfaces the door reads — COLUMN_DEFAULT's reporting
// convention (quoted literals, the bare NULL keyword), CHECK names unique
// per table rather than per schema, no statistics.expression — so the
// FK, UNIQUE and DEFAULT cells are re-run here against a real MariaDB
// rather than assumed from the MySQL leg.
func TestStreamer_MariaDBSource_UnforwardedSchemaChange_Refuses(t *testing.T) {
	sourceDSN, cleanup := startMariaDBBinlog(t)
	defer cleanup()
	_, targetDSN, pgCleanup := startPostgres(t)
	defer pgCleanup()
	mariaEng, ok := engines.Get("mariadb")
	if !ok {
		t.Fatal("mariadb engine not registered")
	}
	applyMariaDBSQL(t, sourceDSN, `CREATE TABLE parent (id INT PRIMARY KEY) ENGINE=InnoDB; INSERT INTO parent VALUES (1);`)

	cells := []unforwardedCell{
		{
			table:     "gc2_fk",
			create:    `CREATE TABLE gc2_fk (id INT PRIMARY KEY, name VARCHAR(40)) ENGINE=InnoDB`,
			ddl:       `ALTER TABLE gc2_fk ADD COLUMN owner INT, ADD CONSTRAINT gc2_fk_owner FOREIGN KEY (owner) REFERENCES parent(id)`,
			wantDelta: `ADD CONSTRAINT "gc2_fk_owner" FOREIGN KEY (owner) REFERENCES source_db.parent (id)`,
			targetHas: `SELECT count(*) FROM pg_constraint WHERE conname = 'gc2_fk_owner'`,
		},
		{
			table:     "gc2_uq",
			create:    `CREATE TABLE gc2_uq (id INT PRIMARY KEY, name VARCHAR(40)) ENGINE=InnoDB`,
			ddl:       `ALTER TABLE gc2_uq ADD CONSTRAINT gc2_uq_name UNIQUE (name)`,
			wantDelta: `ADD CONSTRAINT "gc2_uq_name" UNIQUE (name)`,
			targetHas: `SELECT count(*) FROM pg_index i JOIN pg_class c ON c.oid = i.indrelid WHERE c.relname = 'gc2_uq' AND i.indisunique AND NOT i.indisprimary`,
		},
		{
			table:     "gc2_def",
			create:    `CREATE TABLE gc2_def (id INT PRIMARY KEY, name VARCHAR(40)) ENGINE=InnoDB`,
			ddl:       `ALTER TABLE gc2_def ALTER COLUMN name SET DEFAULT 'z'`,
			wantDelta: `ALTER COLUMN "name" SET DEFAULT 'z'`,
			targetHas: `SELECT count(*) FROM information_schema.columns WHERE table_name = 'gc2_def' AND column_name = 'name' AND column_default IS NOT NULL`,
		},
	}
	for _, tc := range cells {
		t.Run(tc.table, func(t *testing.T) {
			runUnforwardedCell(t, mariaEng, sourceDSN, targetDSN, applyMariaDBSQL, tc)
		})
	}
	t.Run("control_forwarded_column_changes_do_not_refuse", func(t *testing.T) {
		runUnforwardedControl(t, mariaEng, sourceDSN, targetDSN, applyMariaDBSQL)
	})
}
