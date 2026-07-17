//go:build integration

// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

// MariaDB Phase-3 CDC integration tests (roadmap item 73 Phase 3,
// ADR-0170). The binlog reader now streams MariaDB with domain GTIDs; the
// two silent-loss surfaces the ADR calls out are pinned here on BOTH
// supported LTS lines (11.4 + 10.11):
//
//   - reachability: a resume from a PURGED domain-GTID position must
//     refuse LOUDLY (ir.ErrPositionInvalid), never a silent
//     start-from-wrong-position — MariaDB has no SQL reachability
//     pre-check, so the authoritative signal is the stream's error 1236,
//     classified by isMariaDBPurgedGTIDError.
//   - schema-cache churn: MariaDB emits NO per-transaction dummy/BEGIN
//     QueryEvent, so a stream of N plain-DML transactions must invalidate
//     the schema cache ZERO times; a real ALTER mid-stream DOES invalidate
//     and its new column is decoded.
//
// The basic cold-start → INSERT/UPDATE/DELETE convergence pin lives in
// flavor_mariadb_integration_test.go (TestMariaDB_CDCReader_BasicChangeStream).

package mysql

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"log"
	"testing"
	"time"

	"github.com/moby/moby/api/types/network"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"

	"sluicesync.dev/sluice/internal/ir"
)

// TestMariaDB_CDCReader_ResumeAfterKill pins exactly-once warm resume on
// both LTS lines: stream a change, capture its domain-GTID position, close
// the reader, apply a while-down change, then reopen from the captured
// position and assert the while-down change arrives exactly once (and the
// already-consumed change does NOT replay).
func TestMariaDB_CDCReader_ResumeAfterKill(t *testing.T) {
	for _, image := range []string{mariadb114Image, mariadb1011Image} {
		image := image
		t.Run(image, func(t *testing.T) {
			dsn := newMariaDB(t, image, "mdb_cdc_resume")
			execSQLScript(t, dsn, `
				CREATE TABLE t (
					id BIGINT NOT NULL AUTO_INCREMENT,
					v  INT    NOT NULL,
					PRIMARY KEY (id)
				) ENGINE=InnoDB;`)

			eng := Engine{Flavor: FlavorMariaDB}
			ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
			defer cancel()

			rdr, err := eng.OpenCDCReader(ctx, dsn)
			if err != nil {
				t.Fatalf("OpenCDCReader: %v", err)
			}
			changes, err := rdr.StreamChanges(ctx, ir.Position{})
			if err != nil {
				t.Fatalf("StreamChanges (initial): %v", err)
			}
			time.Sleep(300 * time.Millisecond)

			applyMySQL(t, dsn, "INSERT INTO t (v) VALUES (100)")
			got := drainChanges(t, ctx, changes, 1, 30*time.Second)
			if len(got) != 1 {
				t.Fatalf("initial: got %d changes; want 1", len(got))
			}
			capturedPos := got[0].Pos()
			decoded, ok, derr := decodeBinlogPos(capturedPos)
			if derr != nil || !ok {
				t.Fatalf("decode captured position: ok=%v err=%v", ok, derr)
			}
			if decoded.Mode != positionModeGTID {
				t.Fatalf("captured position mode = %q; want %q (MariaDB is always GTID mode)", decoded.Mode, positionModeGTID)
			}
			if decoded.GTIDSet == "" {
				t.Fatal("captured MariaDB GTID position has empty gtid_set")
			}
			t.Logf("captured MariaDB resume set = %q", decoded.GTIDSet)

			// "Kill" the reader, then apply a while-down change.
			if c, ok := rdr.(interface{ Close() error }); ok {
				_ = c.Close()
			}
			applyMySQL(t, dsn, "INSERT INTO t (v) VALUES (200)")

			// Reopen from the captured position: the while-down INSERT must
			// arrive exactly once, and the pre-capture INSERT (v=100) must
			// NOT replay.
			rdr2, err := eng.OpenCDCReader(ctx, dsn)
			if err != nil {
				t.Fatalf("OpenCDCReader (resume): %v", err)
			}
			defer func() {
				if c, ok := rdr2.(interface{ Close() error }); ok {
					_ = c.Close()
				}
			}()
			changes2, err := rdr2.StreamChanges(ctx, capturedPos)
			if err != nil {
				t.Fatalf("StreamChanges (resume): %v", err)
			}
			got2 := drainChanges(t, ctx, changes2, 1, 30*time.Second)
			if len(got2) != 1 {
				if streamErr := rdr2.(*CDCReader).Err(); streamErr != nil {
					t.Fatalf("resume: got %d changes; want 1 (stream error: %v)", len(got2), streamErr)
				}
				t.Fatalf("resume: got %d changes; want 1 (the single while-down INSERT)", len(got2))
			}
			ins, ok := got2[0].(ir.Insert)
			if !ok {
				t.Fatalf("resume change = %T; want ir.Insert", got2[0])
			}
			if v, _ := ins.Row["v"].(int64); v != 200 {
				t.Errorf("resume INSERT v = %#v; want 200 (the while-down row) — a value of 100 means the "+
					"pre-capture change REPLAYED (resume started too early); anything else is a wrong-position gap", ins.Row["v"])
			}
		})
	}
}

// TestMariaDB_CDCReader_SchemaCacheChurn is the ADR-0170 no-per-transaction-
// churn pin (highest-risk silent-DDL surface). MariaDB emits no BEGIN/dummy
// QueryEvent for plain DML, so N plain-DML transactions must clear the
// schema cache ZERO times; a real ALTER mid-stream DOES clear it exactly
// once, and its new column is decoded on the next row.
func TestMariaDB_CDCReader_SchemaCacheChurn(t *testing.T) {
	for _, image := range []string{mariadb114Image, mariadb1011Image} {
		image := image
		t.Run(image, func(t *testing.T) {
			dsn := newMariaDB(t, image, "mdb_cdc_churn")
			execSQLScript(t, dsn, `
				CREATE TABLE t (
					id BIGINT NOT NULL AUTO_INCREMENT,
					v  INT    NOT NULL,
					PRIMARY KEY (id)
				) ENGINE=InnoDB;`)

			eng := Engine{Flavor: FlavorMariaDB}
			ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
			defer cancel()

			rdr, err := eng.OpenCDCReader(ctx, dsn)
			if err != nil {
				t.Fatalf("OpenCDCReader: %v", err)
			}
			defer func() {
				if c, ok := rdr.(interface{ Close() error }); ok {
					_ = c.Close()
				}
			}()
			cdcRdr := rdr.(*CDCReader)

			changes, err := rdr.StreamChanges(ctx, ir.Position{})
			if err != nil {
				t.Fatalf("StreamChanges: %v", err)
			}
			time.Sleep(300 * time.Millisecond)

			// N separate plain-DML transactions (autocommit → one tx each).
			const n = 8
			for i := 0; i < n; i++ {
				applyMySQL(t, dsn, fmt.Sprintf("INSERT INTO t (v) VALUES (%d)", i))
			}
			got := drainChanges(t, ctx, changes, n, 30*time.Second)
			if len(got) != n {
				if streamErr := cdcRdr.Err(); streamErr != nil {
					t.Fatalf("plain DML: got %d/%d changes (stream error: %v)", len(got), n, streamErr)
				}
				t.Fatalf("plain DML: got %d/%d changes", len(got), n)
			}
			// The crux: ZERO schema-cache invalidations across N plain-DML
			// transactions. A regression that started tripping the blanket
			// clear() per MariaDB transaction (a dummy-event filter that was
			// too broad, or the absence of one where MySQL has a BEGIN
			// short-circuit) shows up as clears == n here.
			if clears := cdcRdr.schemaCacheClears.Load(); clears != 0 {
				t.Fatalf("schemaCacheClears = %d after %d plain-DML transactions; want 0 — MariaDB emits no "+
					"per-transaction dummy/BEGIN QueryEvent, so plain DML must NOT invalidate the schema cache "+
					"(per-tx churn = perf trap + ADR-0049 snapshot churn)", clears, n)
			}

			// A real ALTER mid-stream MUST invalidate (exactly once) and its
			// new column must be decoded on the next row.
			applyMySQL(t, dsn, "ALTER TABLE t ADD COLUMN w INT NOT NULL DEFAULT 7")
			applyMySQL(t, dsn, "INSERT INTO t (v, w) VALUES (999, 42)")
			gotAfter := drainChanges(t, ctx, changes, 1, 30*time.Second)
			if len(gotAfter) != 1 {
				t.Fatalf("post-ALTER: got %d changes; want 1", len(gotAfter))
			}
			ins, ok := gotAfter[0].(ir.Insert)
			if !ok {
				t.Fatalf("post-ALTER change = %T; want ir.Insert", gotAfter[0])
			}
			if w, present := ins.Row["w"]; !present {
				t.Errorf("post-ALTER INSERT missing new column w — the ALTER's schema change was NOT picked up "+
					"(schema cache not invalidated): row = %#v", ins.Row)
			} else if wv, _ := w.(int64); wv != 42 {
				t.Errorf("post-ALTER INSERT w = %#v; want 42", ins.Row["w"])
			}
			if clears := cdcRdr.schemaCacheClears.Load(); clears != 1 {
				t.Errorf("schemaCacheClears = %d after one real ALTER (plus %d plain-DML txns); want exactly 1 — "+
					"the ALTER must invalidate once and nothing else should", clears, n)
			}
		})
	}
}

// TestMariaDB_CDCReader_PurgedPosition_LoudRefusal is the ADR-0170
// reachability pin (highest-risk silent-gap surface). MariaDB has no SQL
// reachability pre-check, so a resume from a purged domain-GTID position
// must be refused LOUDLY: the stream's error 1236 is classified as
// ir.ErrPositionInvalid (→ streamer ADR-0022 cold-start), never a silent
// start-from-wrong-position. Uses a DEDICATED container because PURGE
// BINARY LOGS mutates global binlog state.
func TestMariaDB_CDCReader_PurgedPosition_LoudRefusal(t *testing.T) {
	for _, image := range []string{mariadb114Image, mariadb1011Image} {
		image := image
		t.Run(image, func(t *testing.T) {
			dsn, cleanup := newMariaDBDedicatedForCDC(t, image)
			defer cleanup()
			execSQLScript(t, dsn, `
				CREATE TABLE t (
					id BIGINT NOT NULL AUTO_INCREMENT,
					v  INT    NOT NULL,
					PRIMARY KEY (id)
				) ENGINE=InnoDB;`)

			eng := Engine{Flavor: FlavorMariaDB}
			ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
			defer cancel()

			rdr, err := eng.OpenCDCReader(ctx, dsn)
			if err != nil {
				t.Fatalf("OpenCDCReader: %v", err)
			}
			changes, err := rdr.StreamChanges(ctx, ir.Position{})
			if err != nil {
				t.Fatalf("StreamChanges (initial): %v", err)
			}
			time.Sleep(300 * time.Millisecond)
			applyMySQL(t, dsn, "INSERT INTO t (v) VALUES (1)")
			got := drainChanges(t, ctx, changes, 1, 30*time.Second)
			if len(got) != 1 {
				t.Fatalf("initial: got %d changes; want 1", len(got))
			}
			capturedPos := got[0].Pos()
			if c, ok := rdr.(interface{ Close() error }); ok {
				_ = c.Close()
			}

			// Advance and purge so the captured position falls below the
			// oldest retained binlog's floor.
			applyMySQL(t, dsn, "INSERT INTO t (v) VALUES (2)")
			applyMySQL(t, dsn, "FLUSH BINARY LOGS")
			applyMySQL(t, dsn, "INSERT INTO t (v) VALUES (3)")
			applyMySQL(t, dsn, "FLUSH BINARY LOGS")
			purgeAllButLatestBinlogMariaDB(t, dsn)

			// Resume from the now-unreachable position.
			rdr2, err := eng.OpenCDCReader(ctx, dsn)
			if err != nil {
				t.Fatalf("OpenCDCReader (resume): %v", err)
			}
			defer func() {
				if c, ok := rdr2.(interface{ Close() error }); ok {
					_ = c.Close()
				}
			}()
			changes2, streamErr := rdr2.StreamChanges(ctx, capturedPos)
			// MariaDB surfaces the purge REACTIVELY (error 1236 on the first
			// GetEvent), so StreamChanges returns a channel; the loud coded
			// refusal arrives via Err() after the channel drains empty.
			if streamErr != nil {
				assertMariaDBPurgedRefusal(t, streamErr)
				return
			}
			drained := drainChanges(t, ctx, changes2, 1, 20*time.Second)
			readerErr := rdr2.(*CDCReader).Err()
			if readerErr == nil {
				t.Fatalf("PHASE-3 VERDICT (MariaDB GTID position-loss): resume from a purged domain-GTID "+
					"position produced no error (drained %d changes) — SILENT wrong-position risk; the "+
					"reachability floor was not enforced", len(drained))
			}
			assertMariaDBPurgedRefusal(t, readerErr)
		})
	}
}

// assertMariaDBPurgedRefusal fails unless err is the loud coded
// ir.ErrPositionInvalid refusal (the streamer's ADR-0022 cold-start
// trigger).
func assertMariaDBPurgedRefusal(t *testing.T, err error) {
	t.Helper()
	if !errors.Is(err, ir.ErrPositionInvalid) {
		t.Fatalf("PHASE-3 VERDICT (MariaDB GTID position-loss): resume errored but NOT with "+
			"ir.ErrPositionInvalid (got %v). The streamer's ADR-0022 cold-start fall-through keys on "+
			"errors.Is(err, ir.ErrPositionInvalid); without the wrap the recovery would not fire.", err)
	}
	t.Logf("PHASE-3 VERDICT (MariaDB GTID position-loss): LOUD — refused with %v (wraps "+
		"ir.ErrPositionInvalid → streamer cold-start). Oracle satisfied.", err)
}

// purgeAllButLatestBinlogMariaDB purges every binlog but the newest so the
// captured GTID position drops below the retained floor. MariaDB accepts
// the same PURGE BINARY LOGS TO syntax as MySQL.
func purgeAllButLatestBinlogMariaDB(t *testing.T, dsn string) {
	t.Helper()
	db, err := sql.Open("mysql", dsn)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer func() { _ = db.Close() }()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	rows, err := db.QueryContext(ctx, "SHOW BINARY LOGS")
	if err != nil {
		t.Fatalf("SHOW BINARY LOGS: %v", err)
	}
	var latest string
	for rows.Next() {
		var name string
		var size int64
		// MariaDB SHOW BINARY LOGS returns (Log_name, File_size).
		if err := rows.Scan(&name, &size); err != nil {
			_ = rows.Close()
			t.Fatalf("scan: %v", err)
		}
		latest = name
	}
	if err := rows.Err(); err != nil {
		_ = rows.Close()
		t.Fatalf("rows.Err: %v", err)
	}
	_ = rows.Close()
	if latest == "" {
		t.Fatal("SHOW BINARY LOGS returned no rows")
	}
	if _, err := db.ExecContext(ctx, "PURGE BINARY LOGS TO '"+latest+"'"); err != nil {
		t.Fatalf("PURGE BINARY LOGS TO %q: %v", latest, err)
	}
}

// newMariaDBDedicatedForCDC boots a MariaDB container of its OWN (not the
// shared one) with binlog enabled, for tests that mutate global binlog
// state (PURGE BINARY LOGS). Returns a DSN + terminate cleanup.
func newMariaDBDedicatedForCDC(t *testing.T, image string) (dsn string, cleanup func()) {
	t.Helper()
	testcontainers.SkipIfProviderIsNotHealthy(t)

	ctx, cancel := context.WithTimeout(context.Background(), 4*time.Minute)
	defer cancel()
	req := testcontainers.ContainerRequest{
		Image: image,
		Env: map[string]string{
			"MARIADB_ROOT_PASSWORD": "rootpw",
			"MARIADB_DATABASE":      "cdc_src",
		},
		Cmd: []string{
			"--server-id=1",
			"--log-bin=mysqld-bin",
			"--binlog-format=ROW",
			"--binlog-row-image=FULL",
		},
		ExposedPorts: []string{"3306/tcp"},
		WaitingFor: wait.ForSQL("3306/tcp", "mysql", func(host string, port network.Port) string {
			return fmt.Sprintf("root:rootpw@tcp(%s:%s)/cdc_src", host, port.Port())
		}).WithStartupTimeout(4 * time.Minute),
	}
	container, err := testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{
		ContainerRequest: req,
		Started:          true,
	})
	if err != nil {
		t.Fatalf("boot dedicated %s: %v", image, err)
	}
	host, err := container.Host(ctx)
	if err != nil {
		t.Fatalf("host: %v", err)
	}
	port, err := container.MappedPort(ctx, "3306/tcp")
	if err != nil {
		t.Fatalf("port: %v", err)
	}
	log.Printf("dedicated mariadb CDC container booted: %s at %s:%s", image, host, port.Port())
	cleanup = func() {
		shutdown, c := context.WithTimeout(context.Background(), 30*time.Second)
		defer c()
		_ = container.Terminate(shutdown)
	}
	return fmt.Sprintf("root:rootpw@tcp(%s:%s)/cdc_src?parseTime=true", host, port.Port()), cleanup
}
