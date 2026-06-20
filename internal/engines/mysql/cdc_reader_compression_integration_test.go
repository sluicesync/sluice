//go:build integration

// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

// Roadmap item 28: native-MySQL CDC support for binlog_transaction_compression
// (MySQL 8.0.20+). A compressed transaction is delivered as a single
// TRANSACTION_PAYLOAD_EVENT (event type 0x1f) wrapping the whole transaction's
// TABLE_MAP + ROWS + XID. Before the fix, dispatch had no case for it, so the
// container fell through to default (ignored): compressed transactions were
// SILENTLY not applied, the position never advanced, no error — only the
// Bug-12 "no row events" WARN. This pins the fix at the reader level:
//
//   - STEADY STATE: a reader streaming a compression=ON source EMITS the
//     inner Insert/Update/Delete with correct values (without the fix,
//     drainChanges times out at 0 — the silent-skip).
//   - WARM RESUME (the original item-28 symptom): the position captured from a
//     compressed change is the OUTER payload event's file/pos (the
//     transaction boundary), so a NEW reader resuming from it picks up the
//     subsequent compressed transactions — never the "no corresponding table
//     map event" mid-payload misalignment.
//
// Reader-level (no target), mirroring cdc_reader_gtid_position_loss; reuses
// applyMySQL / drainChanges. Boots its own container because
// binlog_transaction_compression is a server flag; gtid OFF so the reader is
// in file/pos mode (the mode item 28 is about).

package mysql

import (
	"context"
	"database/sql"
	"log"
	"testing"
	"time"

	"sluicesync.dev/sluice/internal/ir"

	"github.com/testcontainers/testcontainers-go"
	mysqltc "github.com/testcontainers/testcontainers-go/modules/mysql"
	"github.com/testcontainers/testcontainers-go/wait"
)

// startMySQLCompressionForCDC boots a MySQL container with binlog transaction
// compression ON and GTID OFF (file/pos mode). Sibling of
// startMySQLGTIDForCDC; boots its own container (compression is a server flag)
// and uses the same retry schedule as ensureSharedMySQL.
func startMySQLCompressionForCDC(t *testing.T) (dsn string, cleanup func()) {
	t.Helper()
	testcontainers.SkipIfProviderIsNotHealthy(t)

	var (
		container *mysqltc.MySQLContainer
		lastErr   error
	)
	for attempt := 1; attempt <= sharedMySQLBootAttempts; attempt++ {
		ctx, cancel := context.WithTimeout(context.Background(), sharedMySQLBootTimeout)
		c, err := mysqltc.Run(
			ctx,
			sharedMySQLImage,
			mysqltc.WithDatabase("source_db"),
			mysqltc.WithUsername("root"),
			mysqltc.WithPassword("rootpw"),
			testcontainers.CustomizeRequest(testcontainers.GenericContainerRequest{
				ContainerRequest: testcontainers.ContainerRequest{
					Cmd: []string{
						"mysqld",
						"--server-id=1",
						"--log-bin=mysql-bin",
						"--binlog-format=ROW",
						"--binlog-row-image=FULL",
						"--gtid-mode=OFF",
						"--enforce-gtid-consistency=OFF",
						// The item-28 variable: compress every transaction
						// into a TRANSACTION_PAYLOAD_EVENT.
						"--binlog-transaction-compression=ON",
					},
				},
			}),
			testcontainers.WithWaitStrategyAndDeadline(
				sharedMySQLBootTimeout,
				wait.ForLog("port: 3306  MySQL Community Server").
					WithStartupTimeout(sharedMySQLBootTimeout),
			),
		)
		cancel()
		if err == nil {
			container = c
			break
		}
		if c != nil {
			_ = c.Terminate(context.Background())
		}
		lastErr = err
		if attempt < sharedMySQLBootAttempts {
			backoff := sharedMySQLBootBackoff(attempt)
			log.Printf("startMySQLCompressionForCDC boot attempt %d/%d failed: %v; retrying in %s",
				attempt, sharedMySQLBootAttempts, err, backoff)
			time.Sleep(backoff)
		}
	}
	if container == nil {
		t.Fatalf("start compression container: %d attempts exhausted: %v", sharedMySQLBootAttempts, lastErr)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	terminate := func() {
		shutdown, c := context.WithTimeout(context.Background(), 30*time.Second)
		defer c()
		_ = container.Terminate(shutdown)
	}
	conn, err := container.ConnectionString(ctx, "parseTime=true")
	if err != nil {
		terminate()
		t.Fatalf("connection string: %v", err)
	}
	return conn, terminate
}

// rowID extracts the integer id column from an ir.Row regardless of the
// concrete numeric type the decoder produced.
func rowID(t *testing.T, row ir.Row) int64 {
	t.Helper()
	v, ok := row["id"]
	if !ok {
		t.Fatalf("row has no id column: %#v", row)
	}
	switch n := v.(type) {
	case int64:
		return n
	case uint64:
		return int64(n)
	case int:
		return int64(n)
	default:
		t.Fatalf("id column is %T, want integer: %#v", v, v)
		return 0
	}
}

// TestCDCReader_BinlogTransactionCompression_AppliesAndResumes is the item-28
// reader-level pin: a compression=ON source's compressed transactions are
// unpacked + emitted (steady state), and a resume from a compressed change's
// position picks up subsequent compressed transactions (the warm-resume
// symptom). Without the TransactionPayloadEvent handler both halves fail
// (drainChanges times out at zero).
func TestCDCReader_BinlogTransactionCompression_AppliesAndResumes(t *testing.T) {
	dsn, cleanup := startMySQLCompressionForCDC(t)
	defer cleanup()

	// Honest-test guard: the flag must actually be ON, else the test would
	// pass against an uncompressed binlog (testing nothing).
	assertCompressionOn(t, dsn)

	applyMySQL(t, dsn, `
		CREATE TABLE c (
			id   BIGINT      NOT NULL,
			name VARCHAR(64),
			n    INT,
			PRIMARY KEY (id)
		) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;`)
	applyMySQL(t, dsn, "INSERT INTO c (id,name,n) VALUES (1,'seed-1',1),(2,'seed-2',2)")

	eng := Engine{Flavor: FlavorVanilla}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()

	rdr, err := eng.OpenCDCReader(ctx, dsn)
	if err != nil {
		t.Fatalf("OpenCDCReader: %v", err)
	}
	changes, err := rdr.StreamChanges(ctx, ir.Position{})
	if err != nil {
		t.Fatalf("StreamChanges (initial): %v", err)
	}
	time.Sleep(500 * time.Millisecond)

	// ---- STEADY STATE: three compressed transactions (each its own
	// TRANSACTION_PAYLOAD_EVENT): INSERT, UPDATE, DELETE. ----
	applyMySQL(t, dsn, "INSERT INTO c (id,name,n) VALUES (201,'ins',201)")
	applyMySQL(t, dsn, "UPDATE c SET name='upd' WHERE id=1")
	applyMySQL(t, dsn, "DELETE FROM c WHERE id=2")

	got := drainChanges(t, ctx, changes, 3, 60*time.Second)
	if len(got) != 3 {
		t.Fatalf("STEADY STATE: got %d row changes from compressed transactions; want 3 "+
			"(0 = the silent-skip: dispatch is not unpacking TRANSACTION_PAYLOAD_EVENT)", len(got))
	}
	ins, ok := got[0].(ir.Insert)
	if !ok || rowID(t, ins.Row) != 201 {
		t.Fatalf("change[0] = %#v; want Insert id=201", got[0])
	}
	upd, ok := got[1].(ir.Update)
	if !ok || rowID(t, upd.After) != 1 {
		t.Fatalf("change[1] = %#v; want Update id=1", got[1])
	}
	del, ok := got[2].(ir.Delete)
	if !ok || rowID(t, del.Before) != 2 {
		t.Fatalf("change[2] = %#v; want Delete id=2", got[2])
	}

	// The captured position must be file/pos (gtid OFF) and non-empty — the
	// OUTER payload event's binlog position (item-28 payload-aligned resume).
	capturedPos := got[2].Pos()
	decoded, okPos, derr := decodeBinlogPos(capturedPos)
	if derr != nil || !okPos {
		t.Fatalf("decode captured position: ok=%v err=%v", okPos, derr)
	}
	if decoded.Mode != positionModeFilePos {
		t.Fatalf("captured position mode = %q; want %q (compression test runs file/pos)", decoded.Mode, positionModeFilePos)
	}
	if decoded.File == "" || decoded.Pos == 0 {
		t.Fatalf("captured file/pos is empty: %+v", decoded)
	}

	if c, ok := rdr.(interface{ Close() error }); ok {
		_ = c.Close()
	}

	// ---- WARM RESUME: more compressed transactions while the reader is
	// down, then resume from the captured payload-aligned position. ----
	applyMySQL(t, dsn, "INSERT INTO c (id,name,n) VALUES (202,'ins2',202)")
	applyMySQL(t, dsn, "UPDATE c SET n=999 WHERE id=201")

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
		// A "no corresponding table map event" / position-invalid here is the
		// item-28 resume failure.
		t.Fatalf("StreamChanges (resume from compressed position): %v", err)
	}
	got2 := drainChanges(t, ctx, changes2, 2, 60*time.Second)
	if len(got2) != 2 {
		t.Fatalf("WARM RESUME: got %d row changes after resuming from a compressed-transaction "+
			"position; want 2 (the item-28 resume symptom: a mid-payload-misaligned position emits 0)", len(got2))
	}
	if ins2, ok := got2[0].(ir.Insert); !ok || rowID(t, ins2.Row) != 202 {
		t.Fatalf("resume change[0] = %#v; want Insert id=202", got2[0])
	}
	if upd2, ok := got2[1].(ir.Update); !ok || rowID(t, upd2.After) != 201 {
		t.Fatalf("resume change[1] = %#v; want Update id=201", got2[1])
	}
}

// assertCompressionOn fails the test unless the source actually has
// binlog_transaction_compression enabled (keeps the pin honest).
func assertCompressionOn(t *testing.T, dsn string) {
	t.Helper()
	db, err := sql.Open("mysql", dsn)
	if err != nil {
		t.Fatalf("open for compression check: %v", err)
	}
	defer func() { _ = db.Close() }()
	var v string
	if err := db.QueryRow("SELECT @@global.binlog_transaction_compression").Scan(&v); err != nil {
		t.Fatalf("read binlog_transaction_compression: %v", err)
	}
	if v != "1" && v != "ON" {
		t.Fatalf("binlog_transaction_compression = %q; want ON (the test would otherwise validate an uncompressed binlog)", v)
	}
}
