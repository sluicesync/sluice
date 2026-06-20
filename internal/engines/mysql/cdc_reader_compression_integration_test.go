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
	// capturedPos is the DELETE row change's position, which is the payload's
	// START (the position fix: a row event anchors at the payload start so a
	// mid-payload interrupt re-reads the WHOLE payload — never one past it,
	// which would silently drop un-applied rows). So resuming here legitimately
	// RE-DELIVERS the delete payload (idempotent at-least-once) before the new
	// changes. Drain generously and assert the NEW changes both appear and
	// nothing is lost — tolerating the idempotent re-delivery.
	got2 := drainChanges(t, ctx, changes2, 3, 60*time.Second)
	var sawIns202, sawUpd201 bool
	for _, c := range got2 {
		if ins, ok := c.(ir.Insert); ok && rowID(t, ins.Row) == 202 {
			sawIns202 = true
		}
		if upd, ok := c.(ir.Update); ok && rowID(t, upd.After) == 201 {
			sawUpd201 = true
		}
	}
	if !sawIns202 || !sawUpd201 {
		t.Fatalf("WARM RESUME: after resuming from a compressed-transaction position the new "+
			"changes were not both delivered (Insert202=%v Update201=%v); got %d changes %#v "+
			"(item-28 resume symptom: a mid-payload-misaligned position emits 0/loses changes)",
			sawIns202, sawUpd201, len(got2), got2)
	}
}

// TestCDCReader_BinlogTransactionCompression_LargePayloadPositionAnchoring pins
// the position fix that prevents the v0.99.87 large-payload silent loss: in a
// compressed transaction big enough to span MULTIPLE inner ROWS events, every
// emitted ROW change must anchor at the payload's START position (so a partial
// mid-payload apply re-reads the WHOLE payload on resume), and ONLY the commit
// (TxCommit) advances to the payload's END. The original fix stamped every inner
// event with the END, so a partial batch commit + resume silently skipped the
// un-applied rows. With that bug, the row changes would carry the END position
// (== the commit's) and this test fails; with the fix, rows carry START < END.
func TestCDCReader_BinlogTransactionCompression_LargePayloadPositionAnchoring(t *testing.T) {
	dsn, cleanup := startMySQLCompressionForCDC(t)
	defer cleanup()
	assertCompressionOn(t, dsn)

	applyMySQL(t, dsn, `
		CREATE TABLE big (
			id   BIGINT       NOT NULL,
			pad  VARCHAR(500),
			PRIMARY KEY (id)
		) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;`)

	eng := Engine{Flavor: FlavorVanilla}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
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
	changes, err := rdr.StreamChanges(ctx, ir.Position{})
	if err != nil {
		t.Fatalf("StreamChanges: %v", err)
	}
	time.Sleep(500 * time.Millisecond)

	// ONE transaction inserting 400 rows × ~500 bytes ≈ 200 KB — far over the
	// ~8 KB binlog_row_event_max_size, so MySQL emits MANY WRITE_ROWS events
	// inside the single compressed payload (the multi-inner-event shape the
	// silent-loss bug needs). cte_max_recursion_depth raised so the generator
	// CTE isn't capped at the 1000 default.
	applyMySQL(t, dsn, "SET SESSION cte_max_recursion_depth=100000")
	applyMySQL(t, dsn, "INSERT INTO big (id,pad) SELECT seq, REPEAT('x',500) "+
		"FROM (WITH RECURSIVE s(seq) AS (SELECT 1 UNION ALL SELECT seq+1 FROM s WHERE seq<400) SELECT seq FROM s) q")

	// Collect the full transaction stream INCLUDING the TxCommit boundary
	// (drainChanges filters it, so read the channel directly). Stop at TxCommit.
	var (
		rowPositions []string
		commitPos    string
		gotRows      int
	)
	deadline := time.NewTimer(60 * time.Second)
	defer deadline.Stop()
collect:
	for {
		select {
		case c, ok := <-changes:
			if !ok {
				break collect
			}
			switch v := c.(type) {
			case ir.Insert:
				gotRows++
				rowPositions = append(rowPositions, v.Position.Token)
			case ir.TxCommit:
				commitPos = v.Position.Token
				break collect
			}
		case <-deadline.C:
			t.Fatalf("timed out collecting the large compressed transaction (got %d rows, commit=%q)", gotRows, commitPos)
		case <-ctx.Done():
			t.Fatal(ctx.Err())
		}
	}

	if gotRows != 400 {
		t.Fatalf("got %d row changes from the large compressed transaction; want 400 "+
			"(all inner ROWS events of the payload must be unpacked)", gotRows)
	}
	if commitPos == "" {
		t.Fatal("no TxCommit position captured")
	}
	// Every row anchors at the SAME payload-START position...
	for i, p := range rowPositions {
		if p != rowPositions[0] {
			t.Fatalf("row[%d] position %q != row[0] %q — inner rows must all anchor at the payload START", i, p, rowPositions[0])
		}
	}
	// ...and the commit advances PAST it (payload END > payload START). With the
	// silent-loss bug, rows carried the END too, so this would be equal.
	startPos := decodePosOrFatal(t, rowPositions[0])
	endPos := decodePosOrFatal(t, commitPos)
	if !(endPos.Mode == positionModeFilePos && startPos.Mode == positionModeFilePos) {
		t.Fatalf("expected file/pos mode; start=%q end=%q", startPos.Mode, endPos.Mode)
	}
	if endPos.Pos <= startPos.Pos {
		t.Fatalf("commit position (%d) must be GREATER than the row-anchor START position (%d) — "+
			"equal means rows were stamped with the payload END (the v0.99.87 silent-loss bug)",
			endPos.Pos, startPos.Pos)
	}
}

func decodePosOrFatal(t *testing.T, token string) binlogPos {
	t.Helper()
	p, ok, err := decodeBinlogPos(ir.Position{Engine: "mysql", Token: token})
	if err != nil || !ok {
		t.Fatalf("decode position %q: ok=%v err=%v", token, ok, err)
	}
	return p
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
