//go:build integration

// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

// GC-37 (j) end to end: `sync` from MySQL of tables whose character columns
// — keys included — are declared in a non-UTF-8 charset, to a Postgres and
// to a MySQL target. The cold copy reads through the server's own UTF-8
// conversion; the binlog tail used to hand on the column's STORED bytes,
// so a row inserted, updated or deleted after cutover landed differently
// from the same row copied at cold start (the engine pins measure how).
//
// # The independent expected value
//
// The source server's own conversion of every row, `HEX(CONVERT(col USING
// utf8mb4))`, compared with the TARGET's UTF-8 bytes of the same row
// (MySQL: HEX(CONVERT(col USING utf8mb4)); Postgres:
// encode(convert_to(col, 'UTF8'), 'hex')) — never a value sluice decoded.
// The update and delete are keyed on a non-ASCII text key, so a key carried
// in the wrong encoding shows up as a row the target kept or never changed.

package pipeline

import (
	"context"
	"database/sql"
	"fmt"
	"sort"
	"strings"
	"testing"
	"time"

	"sluicesync.dev/sluice/internal/engines"
	"sluicesync.dev/sluice/internal/ir"
	irbackup "sluicesync.dev/sluice/internal/ir/backup"
	"sluicesync.dev/sluice/internal/pipeline/backup"
	"sluicesync.dev/sluice/internal/pipeline/blobcodec"
	"sluicesync.dev/sluice/internal/pipeline/lineage"

	_ "sluicesync.dev/sluice/internal/engines/mysql"
	_ "sluicesync.dev/sluice/internal/engines/postgres"
)

// legacyCharsetCells are the charsets under test, each with one sample in
// its stored bytes. The latin1/cp1251/gbk C3A9 and swe7 cells are the ones
// whose raw bytes happen to be valid UTF-8 — the shape that was silent.
var legacyCharsetCells = []struct{ cs, sample string }{
	{"latin1", "E9"},
	{"latin1", "C3A9"},
	{"cp1250", "8A"},
	{"cp1251", "C3A9"},
	{"gbk", "C3A9"},
	{"swe7", "7B"},
	{"sjis", "82A0"},
	{"utf16", "00E9"},
}

func legacyCharsetTable(i int) string { return fmt.Sprintf("lc%d", i) }

// legacyCharsetRows returns table's rows as sorted "k|v|x" UTF-8 hex
// triples, read with query (which selects the three hex columns).
func legacyCharsetRows(t *testing.T, driver, dsn, query string) []string {
	t.Helper()
	db, err := sql.Open(driver, dsn)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer func() { _ = db.Close() }()
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	rows, err := db.QueryContext(ctx, query)
	if err != nil {
		return nil // table not there yet
	}
	defer func() { _ = rows.Close() }()
	var out []string
	for rows.Next() {
		var k, v, x sql.NullString
		if err := rows.Scan(&k, &v, &x); err != nil {
			t.Fatalf("scan: %v", err)
		}
		out = append(out, strings.ToUpper(k.String+"|"+v.String+"|"+x.String))
	}
	sort.Strings(out)
	return out
}

func legacyCharsetLane(t *testing.T, sourceDSN, targetEngine, targetDriver, targetDSN, targetHexQuery string) {
	t.Helper()
	lit := func(cs, h string) string { return fmt.Sprintf("_%s X'%s'", cs, h) }
	var ddl strings.Builder
	for i, c := range legacyCharsetCells {
		fmt.Fprintf(&ddl, `CREATE TABLE %[1]s (
			k VARCHAR(16) CHARACTER SET %[2]s NOT NULL PRIMARY KEY,
			v VARCHAR(16) CHARACTER SET %[2]s NULL,
			x TEXT CHARACTER SET %[2]s NULL);
			INSERT INTO %[1]s VALUES (CONCAT(%[3]s, 's'), %[3]s, %[3]s);`, legacyCharsetTable(i), c.cs, lit(c.cs, c.sample))
	}
	applyDDLMySQL(t, sourceDSN, ddl.String())

	src, _ := engines.Get("mysql")
	dst, ok := engines.Get(targetEngine)
	if !ok {
		t.Fatalf("%s engine not registered", targetEngine)
	}
	streamer := &Streamer{
		Source: src, Target: dst,
		SourceDSN: sourceDSN, TargetDSN: targetDSN,
		StreamID: "test-legacy-charset-" + targetEngine,
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	runErr := make(chan error, 1)
	go func() { runErr <- streamer.Run(ctx) }()

	srcQuery := func(table string) string {
		return fmt.Sprintf("SELECT HEX(CONVERT(k USING utf8mb4)), HEX(CONVERT(v USING utf8mb4)), HEX(CONVERT(x USING utf8mb4)) FROM %s", table)
	}
	converged := func(table string, timeout time.Duration) (want, got []string, ok bool) {
		deadline := time.Now().Add(timeout)
		for {
			want = legacyCharsetRows(t, "mysql", sourceDSN, srcQuery(table))
			got = legacyCharsetRows(t, targetDriver, targetDSN, strings.ReplaceAll(targetHexQuery, "{t}", table))
			if strings.Join(want, ",") == strings.Join(got, ",") {
				return want, got, true
			}
			if time.Now().After(deadline) {
				return want, got, false
			}
			select {
			case err := <-runErr:
				t.Fatalf("sync ended during the lane: %v", err)
			case <-time.After(300 * time.Millisecond):
			}
		}
	}

	// Cold start: the seed row, read by the server-converting copy.
	for i, c := range legacyCharsetCells {
		if want, got, ok := converged(legacyCharsetTable(i), 90*time.Second); !ok {
			t.Fatalf("%s %s cold copy: target %v, source %v", c.cs, c.sample, got, want)
		}
	}

	// CDC: insert, keyed update, keyed delete — every key non-ASCII.
	for i, c := range legacyCharsetCells {
		tb, s := legacyCharsetTable(i), lit(c.cs, c.sample)
		applyDDLMySQL(t, sourceDSN, fmt.Sprintf(`
			INSERT INTO %[1]s VALUES (%[2]s, %[2]s, CONCAT(%[2]s, %[2]s));
			INSERT INTO %[1]s VALUES (CONCAT(%[2]s, 'd'), %[2]s, NULL);
			UPDATE %[1]s SET v = CONCAT(%[2]s, 'u'), x = NULL WHERE k = %[2]s;
			DELETE FROM %[1]s WHERE k = CONCAT(%[2]s, 'd');`, tb, s))
	}
	for i, c := range legacyCharsetCells {
		if want, got, ok := converged(legacyCharsetTable(i), 60*time.Second); !ok {
			t.Errorf("%s %s after CDC: target %v, source %v (a row the target kept, lost or never updated is a key or value carried in the column's stored bytes)",
				c.cs, c.sample, got, want)
		}
	}
	cancel()
	if err := <-runErr; err != nil && !strings.Contains(err.Error(), "context canceled") {
		t.Errorf("sync ended with %v", err)
	}
}

// legacyCharsetChainRestoreLane is the backup lane of the same class — the
// one where the old behaviour was SILENT for every non-ASCII value, not only
// the ones whose bytes happened to be valid UTF-8: the change chunk
// JSON-encodes each value, and the encoder writes an invalid-UTF-8 byte as
// U+FFFD. MEASURED with the fix reverted: a chain restored into Postgres
// exited 0 holding U+FFFD for latin1 'é', cp1250 'Š' and sjis 'あ', and 'é'
// for latin1 'Ã©', cp1251 'Г©' and gbk '茅'; into a latin1 MySQL target the
// same chain failed loudly (U+FFFD has no latin1 spelling). The window's
// insert, keyed update and keyed delete are captured by `backup
// incremental`, chain-restored into a fresh target, and the target graded
// against the source server's own conversion.
func legacyCharsetChainRestoreLane(t *testing.T, src string, target ir.Engine, tgtDriver, tgtDSN, tgtHexQuery string) {
	t.Helper()
	lit := func(cs, h string) string { return fmt.Sprintf("_%s X'%s'", cs, h) }
	var seed, window strings.Builder
	for i, c := range legacyCharsetCells {
		tb, s := legacyCharsetTable(i), lit(c.cs, c.sample)
		fmt.Fprintf(&seed, `CREATE TABLE %[1]s (
			k VARCHAR(16) CHARACTER SET %[2]s NOT NULL PRIMARY KEY,
			v VARCHAR(16) CHARACTER SET %[2]s NULL,
			x TEXT CHARACTER SET %[2]s NULL);
			INSERT INTO %[1]s VALUES (CONCAT(%[3]s, 's'), %[3]s, %[3]s), (CONCAT(%[3]s, 'd'), %[3]s, NULL);`, tb, c.cs, s)
		fmt.Fprintf(&window, `
			INSERT INTO %[1]s VALUES (%[2]s, %[2]s, CONCAT(%[2]s, %[2]s));
			UPDATE %[1]s SET v = CONCAT(%[2]s, 'u') WHERE k = CONCAT(%[2]s, 's');
			DELETE FROM %[1]s WHERE k = CONCAT(%[2]s, 'd');`, tb, s)
	}
	applyDDLMySQL(t, src, seed.String())

	eng, _ := engines.Get("mysql")
	store, err := blobcodec.NewLocalStore(t.TempDir())
	if err != nil {
		t.Fatalf("NewLocalStore: %v", err)
	}
	file, pos := readMySQLBinlogPos(t, src)
	ctx := context.Background()
	if err := (&backup.Backup{Source: eng, SourceDSN: src, Store: store, SluiceVersion: "test"}).Run(ctx); err != nil {
		t.Fatalf("Backup.Run: %v", err)
	}
	full, err := lineage.ReadManifest(ctx, store)
	if err != nil {
		t.Fatalf("ReadManifest: %v", err)
	}
	full.Kind = irbackup.BackupKindFull
	full.EndPosition = ir.Position{Engine: "mysql", Token: fmt.Sprintf(`{"mode":"file_pos","file":%q,"pos":%d}`, file, pos)}
	full.BackupID = irbackup.ComputeBackupID(full)
	if err := lineage.WriteManifestAt(ctx, store, lineage.ManifestFileName, full); err != nil {
		t.Fatalf("rewrite full: %v", err)
	}

	applyDDLMySQL(t, src, window.String())
	incrCtx, cancel := context.WithTimeout(ctx, 60*time.Second)
	err = (&IncrementalBackup{
		Source: eng, SourceDSN: src, Store: store, ParentRef: full.BackupID,
		Window: 8 * time.Second, ChunkChanges: 3, SluiceVersion: "test",
	}).Run(incrCtx)
	cancel()
	if err != nil {
		t.Fatalf("IncrementalBackup.Run: %v", err)
	}
	if err := (&backup.Restore{Target: target, TargetDSN: tgtDSN, Store: store}).Run(ctx); err != nil {
		t.Fatalf("Restore.Run: %v", err)
	}

	q := "SELECT HEX(CONVERT(k USING utf8mb4)), HEX(CONVERT(v USING utf8mb4)), HEX(CONVERT(x USING utf8mb4)) FROM %s"
	for i, c := range legacyCharsetCells {
		tb := legacyCharsetTable(i)
		want := legacyCharsetRows(t, "mysql", src, fmt.Sprintf(q, tb))
		got := legacyCharsetRows(t, tgtDriver, tgtDSN, strings.ReplaceAll(tgtHexQuery, "{t}", tb))
		if len(want) != 2 {
			t.Fatalf("%s: source holds %d rows after the window; want 2 — the window did not run as written", tb, len(want))
		}
		if strings.Join(want, ",") != strings.Join(got, ",") {
			t.Errorf("%s %s: chain restore holds %v, source %v", c.cs, c.sample, got, want)
		}
	}
}

func TestIncrementalBackup_LegacyCharsetColumns_ChainRestore_ToMySQL(t *testing.T) {
	src, tgt, cleanup := startMySQLBinlog(t)
	defer cleanup()
	my, _ := engines.Get("mysql")
	legacyCharsetChainRestoreLane(t, src, my, "mysql", tgt,
		`SELECT HEX(CONVERT(k USING utf8mb4)), HEX(CONVERT(v USING utf8mb4)), HEX(CONVERT(x USING utf8mb4)) FROM {t}`)
}

func TestIncrementalBackup_LegacyCharsetColumns_ChainRestore_ToPostgres(t *testing.T) {
	src, _, cleanup := startMySQLBinlog(t)
	defer cleanup()
	_, pgDSN, pgCleanup := startPostgresLogical(t)
	defer pgCleanup()
	pg, _ := engines.Get("postgres")
	legacyCharsetChainRestoreLane(t, src, pg, "pgx", pgDSN,
		`SELECT encode(convert_to(k, 'UTF8'), 'hex'), encode(convert_to(v, 'UTF8'), 'hex'), encode(convert_to(x, 'UTF8'), 'hex') FROM {t}`)
}

func TestSync_LegacyCharsetColumns_MySQLToPostgres(t *testing.T) {
	mysqlDSN, _, mysqlCleanup := startMySQLBinlog(t)
	defer mysqlCleanup()
	_, pgDSN, pgCleanup := startPostgresLogical(t)
	defer pgCleanup()
	legacyCharsetLane(t, mysqlDSN, "postgres", "pgx", pgDSN,
		`SELECT encode(convert_to(k, 'UTF8'), 'hex'), encode(convert_to(v, 'UTF8'), 'hex'), encode(convert_to(x, 'UTF8'), 'hex') FROM {t}`)
}

func TestSync_LegacyCharsetColumns_MySQLToMySQL(t *testing.T) {
	srcDSN, tgtDSN, cleanup := startMySQLBinlog(t)
	defer cleanup()
	legacyCharsetLane(t, srcDSN, "mysql", "mysql", tgtDSN,
		`SELECT HEX(CONVERT(k USING utf8mb4)), HEX(CONVERT(v USING utf8mb4)), HEX(CONVERT(x USING utf8mb4)) FROM {t}`)
}
