//go:build integration

// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package pipeline

import (
	"context"
	"database/sql"
	"strconv"
	"testing"
	"time"

	"sluicesync.dev/sluice/internal/engines"

	_ "sluicesync.dev/sluice/internal/engines/mysql"
	_ "sluicesync.dev/sluice/internal/engines/postgres"
)

// TestStreamer_AddColumnForward_MySQLToPostgres_BinaryDefaultNUL is the
// end-to-end pin for GC-29: on a vanilla MySQL binlog source, a forwarded
// `ALTER TABLE … ADD COLUMN` whose BINARY/VARBINARY literal DEFAULT
// contains a NUL byte must land on the Postgres target with the declared
// bytes. MySQL's information_schema.COLUMN_DEFAULT C-string-truncates
// such a default at its first NUL (0x2700 reads as "0x27", 0x00 as "0x");
// the cold-start SchemaReader re-reads SHOW CREATE TABLE to recover the
// true bytes, and before the fix the CDC reader's per-table loader did
// not, so the ADR-0091 intercept re-emitted the truncated value — silently,
// at exit 0, for every later target-side write that omits the column.
// The seed and the boundary projection are bound at the unit tier by
// TestLoadTableSchema_DefaultAgreesWithSeed_EveryBinlogFlavor.
//
// The independent expected value is the DECLARED DDL's bytes, read back
// from a target-side INSERT that omits every forwarded column. b3 carries
// no NUL and is the control (faithful before and after the fix).
//
// b1 is NOT a differential on its own, and that is measured, not assumed:
// the emitters NUL-pad a fixed-width BINARY default to its declared width,
// which restores a truncation that removed only TRAILING NULs (0x27 →
// 0x2700). The shapes the padding cannot repair — and which read back
// wrong with the recovery removed — are a leading NUL (b2: the bare "0x"
// lands as the two ASCII bytes "0x"), any VARBINARY (b4: no padding), and
// a NUL mid-value on BINARY (b5: 0xFF00AA → "0xFF" → padded ff0000).
//
// Shard: TestStreamer_ prefix → pipeline-rest-streamer.
func TestStreamer_AddColumnForward_MySQLToPostgres_BinaryDefaultNUL(t *testing.T) {
	mysqlDSN, _, mysqlCleanup := startMySQLBinlog(t)
	defer mysqlCleanup()
	_, pgDSN, pgCleanup := startPostgres(t)
	defer pgCleanup()

	applyDDLMySQL(t, mysqlDSN, `
		CREATE TABLE w (
			id   BIGINT      NOT NULL PRIMARY KEY,
			name VARCHAR(80) NOT NULL,
			INDEX ix_w_name (name)
		) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;`)
	applyDDLMySQL(t, mysqlDSN, "INSERT INTO w (id, name) VALUES (1, 'a'), (2, 'b');")

	myEng, ok := engines.Get("mysql")
	if !ok {
		t.Fatal("mysql engine not registered")
	}
	pgEng, ok := engines.Get("postgres")
	if !ok {
		t.Fatal("postgres engine not registered")
	}
	streamer := &Streamer{
		Source:                 myEng,
		Target:                 pgEng,
		SourceDSN:              mysqlDSN,
		TargetDSN:              pgDSN,
		StreamID:               "test-addcol-fwd-mysql-pg-binary-nul",
		ForwardSchemaAddColumn: true,
	}
	streamCtx, streamCancel := context.WithCancel(context.Background())
	defer streamCancel()
	runErr := make(chan error, 1)
	go func() { runErr <- streamer.Run(streamCtx) }()

	if !waitForRowCount(t, pgDSN, "w", 2, 90*time.Second) {
		t.Fatalf("phase A: bulk-copy never landed the seed rows on PG")
	}

	shapes := []struct {
		col, ddl, wantHex string
	}{
		{"b1", "b1 BINARY(2) DEFAULT 0x2700", "2700"},
		{"b2", "b2 BINARY(1) DEFAULT 0x00", "00"},
		{"b3", "b3 BINARY(3) DEFAULT 0xFFEEDD", "ffeedd"},
		{"b4", "b4 VARBINARY(4) DEFAULT 0xFF00", "ff00"},
		{"b5", "b5 BINARY(3) DEFAULT 0xFF00AA", "ff00aa"},
	}

	nextID := 3
	for _, sc := range shapes {
		applyDDLMySQL(t, mysqlDSN, "ALTER TABLE w ADD COLUMN "+sc.ddl+";")
		applyDDLMySQL(t, mysqlDSN, "INSERT INTO w (id, name) VALUES ("+strconv.Itoa(nextID)+", 'r"+strconv.Itoa(nextID)+"');")
		deadline := time.Now().Add(60 * time.Second)
		for pollRowCount(pgDSN, "w") < nextID {
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

	tgtDB, err := sql.Open("pgx", pgDSN)
	if err != nil {
		t.Fatalf("open pg target: %v", err)
	}
	defer func() { _ = tgtDB.Close() }()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	if _, err := tgtDB.ExecContext(ctx, `INSERT INTO w (id, name) VALUES (100, 'target-side')`); err != nil {
		t.Fatalf("phase C: target-side INSERT omitting the forwarded columns: %v", err)
	}
	for _, sc := range shapes {
		var got sql.NullString
		if err := tgtDB.QueryRowContext(ctx, `SELECT encode("`+sc.col+`", 'hex') FROM w WHERE id = 100`).Scan(&got); err != nil {
			t.Fatalf("phase C (%s): read back: %v", sc.col, err)
		}
		if !got.Valid || got.String != sc.wantHex {
			t.Errorf("phase C (%s): target-side INSERT read back %s; want [%s] — the NUL-truncated information_schema default was re-emitted (GC-29)",
				sc.col, renderNullString(got), sc.wantHex)
		}
	}

	streamCancel()
	select {
	case <-runErr:
	case <-time.After(15 * time.Second):
		t.Fatal("Streamer.Run did not return after ctx cancel")
	}
}
