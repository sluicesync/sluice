//go:build integration

// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

// Bug 301 end to end: rows a MySQL source wrote BEFORE a schema change,
// captured by a stream that was behind the binlog head when the change
// landed, reach the target under the columns they were written under — or
// the run refuses before writing any of them.
//
// Paths: `backup incremental`, `backup stream` started behind the head,
// and a warm-resumed `sync start`. A live sync that is lagging reaches the
// reader through the same cold-cache load as all three
// (internal/engines/mysql's TestCDCShapeAtPosition_Matrix pins that, with
// the cache cleared mid-window). Each path runs every shape under
// binlog_row_metadata MINIMAL (no column names: refuse, except an in-place
// change) and FULL (names: decode by them).
//
// The independent expected value is the SOURCE table itself, read after
// the run: every column by name, rows in id order, md5 over the rendering.
// It shares nothing with the capture, the chain or the apply path.
//
// Outcomes that are not EXACT are each a pre-existing, documented loud
// posture of a layer downstream of the reader, named in the table:
//   - chain restore and the broker refuse a dropped column and a
//     column-order change (SLUICE-E-BACKUP-SCHEMA-DELTA-UNSUPPORTED; the
//     latter is RENAME-SWAP-FORWARD: a reorder and a rename swap are the
//     same delta);
//   - a warm resume refuses a column-order change (COLUMN-ORDER-CHANGED,
//     the same reason);
//   - a warm resume's first schema boundary is judged against the target
//     (GC-44): a dropped column is kept on the target with a WARN, and a
//     cross-family retype refuses RESUME-SCHEMA-DIVERGENCE.

package pipeline

import (
	"context"
	"crypto/md5"
	"database/sql"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"

	mysqltc "github.com/testcontainers/testcontainers-go/modules/mysql"

	"github.com/testcontainers/testcontainers-go"

	"sluicesync.dev/sluice/internal/engines"
	"sluicesync.dev/sluice/internal/ir"
	irbackup "sluicesync.dev/sluice/internal/ir/backup"
	"sluicesync.dev/sluice/internal/pipeline/backup"
	"sluicesync.dev/sluice/internal/pipeline/blobcodec"
	"sluicesync.dev/sluice/internal/pipeline/lineage"
	"sluicesync.dev/sluice/internal/sluicecode"
)

// shapeOutcome is what a path must do with a shape.
type shapeOutcome int

const (
	// shapeExact: the target equals the source, every column, every row.
	shapeExact shapeOutcome = iota
	// shapeExactKeepsDropped: equal on the source's columns; the target
	// also keeps the column the source dropped (the GC-44 WARN).
	shapeExactKeepsDropped
	// shapeRefusesCapture: the capture refuses with the replay-mismatch
	// code and writes nothing.
	shapeRefusesCapture
	// shapeRefusesRestore: the capture succeeds, restore refuses the
	// window's schema delta (a dropped column, or a column-order change).
	shapeRefusesRestore
	// shapeRefusesResume: the warm resume refuses its first boundary.
	shapeRefusesResume
	// shapeRefusesResumeOrder: the warm resume refuses its first boundary
	// as a column-order change (RENAME-SWAP-FORWARD): the rows decode
	// faithfully by their recorded names, but a reorder and a rename swap
	// are the same boundary, and only a swap would leave the target's
	// earlier rows under the wrong names.
	shapeRefusesResumeOrder
)

// e2eShape is one source history, run after the chain's full (or the
// sync's cold start). Every shape starts from
// kd(id INT PRIMARY KEY, b VARCHAR(40), c VARCHAR(40)) holding (1,'101','201').
type e2eShape struct {
	name    string
	history string
	// minimalDecodes marks the in-place change MINIMAL can still decode.
	minimalDecodes bool
	// full is the FULL-metadata outcome per path.
	full map[string]shapeOutcome
}

const e2ePre = "INSERT INTO kd VALUES (2,'102','202'),(5,'105','205'); UPDATE kd SET b='111' WHERE id=1; DELETE FROM kd WHERE id=5;"

func e2eShapes() []e2eShape {
	exact := map[string]shapeOutcome{"incremental": shapeExact, "stream": shapeExact, "resume": shapeExact}
	// A column-order change decodes exactly under FULL and is then refused
	// downstream, by chain replay and by the warm resume's boundary check
	// alike (RENAME-SWAP-FORWARD).
	order := map[string]shapeOutcome{"incremental": shapeRefusesRestore, "stream": shapeRefusesRestore, "resume": shapeRefusesResumeOrder}
	return []e2eShape{
		{name: "reorder", history: e2ePre + " ALTER TABLE kd MODIFY c VARCHAR(40) AFTER id; INSERT INTO kd(id,b,c) VALUES (3,'103','203');", full: order},
		{name: "add", history: e2ePre + " ALTER TABLE kd ADD COLUMN d INT; INSERT INTO kd VALUES (3,'103','203',3);", full: exact},
		{
			name: "drop", history: e2ePre + " ALTER TABLE kd DROP COLUMN c; INSERT INTO kd VALUES (3,'103');",
			full: map[string]shapeOutcome{"incremental": shapeRefusesRestore, "stream": shapeRefusesRestore, "resume": shapeExactKeepsDropped},
		},
		{
			name: "type-change", history: e2ePre + " ALTER TABLE kd MODIFY b INT; INSERT INTO kd VALUES (3,103,'203');",
			full: map[string]shapeOutcome{"incremental": shapeExact, "stream": shapeExact, "resume": shapeRefusesResume},
		},
		{
			name: "widen", history: e2ePre + " ALTER TABLE kd MODIFY b VARCHAR(80); INSERT INTO kd VALUES (3,'103','203');",
			minimalDecodes: true, full: exact,
		},
		{
			name: "two-close-together",
			history: e2ePre + " ALTER TABLE kd ADD COLUMN d INT; INSERT INTO kd VALUES (4,'104','204',4);" +
				" ALTER TABLE kd MODIFY c VARCHAR(40) FIRST; INSERT INTO kd(id,b,c,d) VALUES (3,'103','203',3);",
			full: order,
		},
	}
}

// TestShapeAtPosition_MySQLPaths is the path × shape × metadata matrix.
func TestShapeAtPosition_MySQLPaths(t *testing.T) {
	base, cleanup := startMySQLGTIDForShapes(t)
	defer cleanup()
	eng, ok := engines.Get("mysql")
	if !ok {
		t.Fatal("mysql engine not registered")
	}
	n := 0
	for _, metadata := range []string{"MINIMAL", "FULL"} {
		shapeExec(t, base, "SET GLOBAL binlog_row_metadata = "+metadata)
		for _, path := range []string{"incremental", "stream", "resume"} {
			for _, s := range e2eShapes() {
				n++
				want := shapeRefusesCapture
				switch {
				case metadata == "FULL":
					want = s.full[path]
				case s.minimalDecodes:
					want = shapeExact
				}
				t.Run(fmt.Sprintf("%s/%s/%s", metadata, path, s.name), func(t *testing.T) {
					src := mysqlFreshDB(t, base, fmt.Sprintf("s301_%d", n))
					tgt := mysqlFreshDB(t, base, fmt.Sprintf("t301_%d", n))
					shapeExec(t, src, "CREATE TABLE kd (id INT NOT NULL PRIMARY KEY, b VARCHAR(40), c VARCHAR(40)) DEFAULT CHARSET=utf8mb4; INSERT INTO kd VALUES (1,'101','201');")
					switch path {
					case "incremental", "stream":
						runBackupShape(t, eng, src, tgt, path, s.history, want)
					case "resume":
						runResumeShape(t, eng, src, tgt, fmt.Sprintf("s301-%d", n), s.history, want)
					}
				})
			}
		}
	}
}

// runBackupShape takes a full, records history, captures it with a one-shot
// incremental or a `backup stream` that starts behind the head, and
// restores the chain.
func runBackupShape(t *testing.T, eng ir.Engine, src, tgt, path, history string, want shapeOutcome) {
	ctx := context.Background()
	store, err := blobcodec.NewLocalStore(t.TempDir())
	if err != nil {
		t.Fatalf("NewLocalStore: %v", err)
	}
	if err := (&backup.Backup{Source: eng, SourceDSN: src, Store: store, SluiceVersion: "test"}).Run(ctx); err != nil {
		t.Fatalf("Backup.Run: %v", err)
	}
	full, err := lineage.ReadManifest(ctx, store)
	if err != nil {
		t.Fatalf("ReadManifest: %v", err)
	}
	shapeExec(t, src, history)

	var captureErr error
	if path == "incremental" {
		runCtx, cancel := context.WithTimeout(ctx, 60*time.Second)
		captureErr = (&IncrementalBackup{
			Source: eng, SourceDSN: src, Store: store, ParentRef: full.BackupID,
			Window: 5 * time.Second, SluiceVersion: "test",
		}).Run(runCtx)
		cancel()
	} else {
		captureErr = runStreamUntilSettled(t, eng, src, store, full.BackupID)
	}
	if want == shapeRefusesCapture {
		wantCode(t, captureErr, sluicecode.CodeCDCSchemaReplayMismatch, "capture")
		if n := committedChangeRecords(t, store); n != 0 {
			t.Errorf("the refused capture committed %d change records; want 0", n)
		}
		return
	}
	if captureErr != nil {
		t.Fatalf("capture: %v", captureErr)
	}
	restoreErr := (&backup.ChainRestore{Target: eng, TargetDSN: tgt, Store: store}).Run(ctx)
	if want == shapeRefusesRestore {
		wantCode(t, restoreErr, sluicecode.CodeBackupSchemaDeltaUnsupported, "restore")
		return
	}
	if restoreErr != nil {
		t.Fatalf("ChainRestore.Run: %v", restoreErr)
	}
	assertShapeOutcome(t, src, tgt, want)
}

// runStreamUntilSettled runs `backup stream` from the full — every event of
// the history is already behind the head — until it refuses or its
// rollovers stop growing, and returns its error.
func runStreamUntilSettled(t *testing.T, eng ir.Engine, src string, store irbackup.Store, parent string) error {
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	done := make(chan error, 1)
	go func() {
		done <- (&BackupStream{
			Source: eng, SourceDSN: src, Store: store, ParentRef: parent,
			RolloverWindow: 2 * time.Second, RolloverMaxChanges: 1000, RolloverMaxBytes: 1 << 30,
			ChunkChanges: 100, SluiceVersion: "test",
		}).Run(ctx)
	}()
	var last int64 = -1
	stable := 0
	for stable < 8 {
		select {
		case err := <-done:
			return err
		case <-time.After(500 * time.Millisecond):
		}
		n := committedChangeRecords(t, store)
		if n == last && n > 0 {
			stable++
		} else {
			stable = 0
		}
		last = n
	}
	cancel()
	select {
	case err := <-done:
		return err
	case <-time.After(15 * time.Second):
		t.Fatal("backup stream did not stop")
		return nil
	}
}

// runResumeShape cold-starts a sync, stops it, records history while it is
// down, and warm-resumes it.
func runResumeShape(t *testing.T, eng ir.Engine, src, tgt, streamID, history string, want shapeOutcome) {
	run := func() (context.CancelFunc, chan error) {
		ctx, cancel := context.WithCancel(context.Background())
		done := make(chan error, 1)
		go func() {
			done <- (&Streamer{Source: eng, Target: eng, SourceDSN: src, TargetDSN: tgt, StreamID: streamID}).Run(ctx)
		}()
		return cancel, done
	}
	stop := func(cancel context.CancelFunc, done chan error) {
		cancel()
		select {
		case <-done:
		case <-time.After(20 * time.Second):
			t.Fatal("streamer did not stop")
		}
	}
	cancel, done := run()
	if !waitForRowCountMySQL(t, tgt, "kd", 1, 60*time.Second) {
		t.Fatal("cold start never copied the seed row")
	}
	shapeExec(t, src, "INSERT INTO kd VALUES (9,'109','209');")
	if !waitForRowCountMySQL(t, tgt, "kd", 2, 30*time.Second) {
		t.Fatal("CDC never delivered the pre-stop row")
	}
	stop(cancel, done)
	before := tableSignature(t, tgt, "kd", nil)

	shapeExec(t, src, history)
	cancel, done = run()
	defer cancel()
	srcCols := sourceColumns(t, src)
	srcSig := tableSignature(t, src, "kd", srcCols)
	deadline := time.Now().Add(40 * time.Second)
	for time.Now().Before(deadline) {
		select {
		case err := <-done:
			switch want {
			case shapeRefusesCapture:
				wantCode(t, err, sluicecode.CodeCDCSchemaReplayMismatch, "warm resume")
				// Refused before decoding any row: the target is as the stop left it.
				if after := tableSignature(t, tgt, "kd", nil); after != before {
					t.Errorf("the refused resume changed the target: %s → %s", before, after)
				}
			case shapeRefusesResume:
				if !errors.Is(err, ir.ErrResumeSchemaDivergence) {
					t.Fatalf("warm resume: err = %v; want %v", err, ir.ErrResumeSchemaDivergence)
				}
				// The rows applied before the refusal are the pre-change rows,
				// decoded by the shape they were written under: each must equal
				// the source's row with the same id.
				assertNoWrongRow(t, src, tgt)
			case shapeRefusesResumeOrder:
				if !errors.Is(err, ir.ErrColumnOrderChanged) {
					t.Fatalf("warm resume: err = %v; want %v", err, ir.ErrColumnOrderChanged)
				}
				assertNoWrongRow(t, src, tgt)
			default:
				t.Fatalf("warm resume stopped: %v", err)
			}
			return
		case <-time.After(500 * time.Millisecond):
		}
		if sig, ok := tryTableSignature(tgt, srcCols); ok && sig == srcSig {
			break
		}
	}
	stop(cancel, done)
	if want == shapeRefusesCapture || want == shapeRefusesResume || want == shapeRefusesResumeOrder {
		t.Fatalf("warm resume did not refuse; target %s, source %s", tableSignature(t, tgt, "kd", nil), srcSig)
	}
	assertShapeOutcome(t, src, tgt, want)
}

// assertNoWrongRow requires every target row to equal the source's row with
// the same id, on the columns both have (values rendered as text).
func assertNoWrongRow(t *testing.T, src, tgt string) {
	t.Helper()
	var shared []string
	tgtCols := map[string]bool{}
	for _, c := range sourceColumns(t, tgt) {
		tgtCols[c] = true
	}
	for _, c := range sourceColumns(t, src) {
		if tgtCols[c] {
			shared = append(shared, c)
		}
	}
	srcRows := map[string]bool{}
	for _, r := range tableRows(t, src, shared) {
		srcRows[r] = true
	}
	for _, r := range tableRows(t, tgt, shared) {
		if !srcRows[r] {
			t.Errorf("target row %q (columns %v) matches no source row", r, shared)
		}
	}
}

// tryTableSignature is tableSignature for polling: false while the target
// cannot answer for cols yet (a column not forwarded yet).
func tryTableSignature(dsn string, cols []string) (string, bool) {
	db, err := sql.Open("mysql", dsn)
	if err != nil {
		return "", false
	}
	defer func() { _ = db.Close() }()
	rows, err := db.Query(tableRowsQuery(cols))
	if err != nil {
		return "", false
	}
	defer func() { _ = rows.Close() }()
	var out []string
	for rows.Next() {
		var s string
		if rows.Scan(&s) != nil {
			return "", false
		}
		out = append(out, s)
	}
	if rows.Err() != nil {
		return "", false
	}
	return fmt.Sprintf("%s:%x", strings.Join(cols, ","), md5.Sum([]byte(strings.Join(out, "\n")))), true
}

// tableRowsQuery renders kd's rows over cols, in id order.
func tableRowsQuery(cols []string) string {
	quoted := make([]string, len(cols))
	for i, c := range cols {
		quoted[i] = "COALESCE(CAST(`" + c + "` AS CHAR), '<NULL>')"
	}
	return "SELECT CONCAT_WS('|', " + strings.Join(quoted, ", ") + ") FROM kd ORDER BY id"
}

// assertShapeOutcome compares target to source.
func assertShapeOutcome(t *testing.T, src, tgt string, want shapeOutcome) {
	t.Helper()
	cols := sourceColumns(t, src)
	srcSig, tgtSig := tableSignature(t, src, "kd", cols), tableSignature(t, tgt, "kd", cols)
	if srcSig != tgtSig {
		t.Errorf("target differs from source on the source's columns:\n source %s\n target %s\n source rows %v\n target rows %v",
			srcSig, tgtSig, tableRows(t, src, cols), tableRows(t, tgt, cols))
	}
	tgtCols := sourceColumns(t, tgt)
	switch want {
	case shapeExact:
		if strings.Join(tgtCols, ",") != strings.Join(cols, ",") {
			t.Errorf("target columns %v; source columns %v", tgtCols, cols)
		}
	case shapeExactKeepsDropped:
		if len(tgtCols) != len(cols)+1 {
			t.Errorf("target columns %v; want the source's %v plus the dropped one", tgtCols, cols)
		}
	}
}

// sourceColumns lists kd's columns by name.
func sourceColumns(t *testing.T, dsn string) []string {
	t.Helper()
	db := mysqlOpen(t, dsn)
	defer func() { _ = db.Close() }()
	rows, err := db.Query("SELECT column_name FROM information_schema.columns WHERE table_schema = DATABASE() AND table_name = 'kd' ORDER BY column_name")
	if err != nil {
		t.Fatalf("columns: %v", err)
	}
	defer func() { _ = rows.Close() }()
	var out []string
	for rows.Next() {
		var c string
		if err := rows.Scan(&c); err != nil {
			t.Fatalf("scan: %v", err)
		}
		out = append(out, c)
	}
	return out
}

// tableRows renders kd's rows over cols (all columns by name when nil), in
// id order.
func tableRows(t *testing.T, dsn string, cols []string) []string {
	t.Helper()
	if cols == nil {
		cols = sourceColumns(t, dsn)
	}
	db := mysqlOpen(t, dsn)
	defer func() { _ = db.Close() }()
	rows, err := db.Query(tableRowsQuery(cols))
	if err != nil {
		t.Fatalf("rows: %v", err)
	}
	defer func() { _ = rows.Close() }()
	var out []string
	for rows.Next() {
		var s string
		if err := rows.Scan(&s); err != nil {
			t.Fatalf("scan: %v", err)
		}
		out = append(out, s)
	}
	return out
}

// tableSignature is the column list and an md5 over tableRows.
func tableSignature(t *testing.T, dsn, _ string, cols []string) string {
	t.Helper()
	if cols == nil {
		cols = sourceColumns(t, dsn)
	}
	return fmt.Sprintf("%s:%x", strings.Join(cols, ","), md5.Sum([]byte(strings.Join(tableRows(t, dsn, cols), "\n"))))
}

func mysqlOpen(t *testing.T, dsn string) *sql.DB {
	t.Helper()
	db, err := sql.Open("mysql", dsn)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	return db
}

// shapeExec runs a multi-statement script.
func shapeExec(t *testing.T, dsn, script string) {
	t.Helper()
	applyDDLMySQL(t, dsn, script)
}

// mysqlFreshDB creates an empty database on base's server and returns its DSN.
func mysqlFreshDB(t *testing.T, base, db string) string {
	t.Helper()
	shapeExec(t, base, "DROP DATABASE IF EXISTS "+db+"; CREATE DATABASE "+db+" DEFAULT CHARSET=utf8mb4;")
	dsn, err := buildMySQLDSN(base, db)
	if err != nil {
		t.Fatalf("build DSN: %v", err)
	}
	return dsn
}

// wantCode requires err to carry code.
func wantCode(t *testing.T, err error, code sluicecode.Code, site string) {
	t.Helper()
	ce, ok := sluicecode.FromError(err)
	if !ok || ce.Code != code {
		t.Fatalf("%s: err = %v; want %s", site, err, code)
	}
}

// startMySQLGTIDForShapes boots a GTID-mode MySQL whose binlog_row_metadata
// the test sets per cell.
func startMySQLGTIDForShapes(t *testing.T) (dsn string, cleanup func()) {
	t.Helper()
	container := runMySQLWithRetry(
		t,
		mysqltc.WithDatabase("source_db"),
		mysqltc.WithUsername("root"),
		mysqltc.WithPassword("rootpw"),
		testcontainers.CustomizeRequest(testcontainers.GenericContainerRequest{
			ContainerRequest: testcontainers.ContainerRequest{
				Cmd: []string{
					"mysqld", "--server-id=1", "--log-bin=mysql-bin", "--binlog-format=ROW",
					"--binlog-row-image=FULL", "--gtid-mode=ON", "--enforce-gtid-consistency=ON",
					"--net-write-timeout=600", "--net-read-timeout=600",
				},
			},
		}),
	)
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	cleanup = func() {
		shutdown, c := context.WithTimeout(context.Background(), 30*time.Second)
		defer c()
		_ = container.Terminate(shutdown)
	}
	conn, err := container.ConnectionString(ctx, "parseTime=true")
	if err != nil {
		cleanup()
		t.Fatalf("connection string: %v", err)
	}
	return conn, cleanup
}
