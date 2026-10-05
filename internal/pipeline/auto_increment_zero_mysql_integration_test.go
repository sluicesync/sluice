//go:build integration

// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

// A carried AUTO_INCREMENT value of 0 lands as 0 on a MySQL target, on
// every write path that reaches one.
//
// MySQL treats an explicit 0 written into an AUTO_INCREMENT column as
// "generate the next value" unless the session's sql_mode carries
// NO_AUTO_VALUE_ON_ZERO. sluice's forced session mode did not, and its DDL
// emitter declares AUTO_INCREMENT at CREATE, so a source row keyed 0 landed
// under a fresh key, silently, at exit 0 (measured on 8.4: {0,'zero'},
// {100,'hundred'} became {1,'zero'},{100,'hundred'}).
//
// # The class, and why every path is pinned rather than one
//
// The fix lives at the one connection chokepoint (mysql.openDB: the
// injected mode plus the post-connect repair), and
// TestMySQLConnectionRoster_EveryPoolGoesThroughOpenDB holds every MySQL
// pool to it. That roster proves the PLUMBING; these pins prove the
// OUTCOME on each write path a value of 0 can travel — because each path
// reaches MySQL through a different statement (LOAD DATA, a batched
// INSERT, ON DUPLICATE KEY UPDATE, the CDC applier's serial and lane
// pools), and a path that opened its own session or ran a statement MySQL
// treats differently would be invisible to the roster:
//
//   - migrate, MySQL → MySQL, through BOTH bulk cores (which one ran is
//     read off the server's own Com_load counter);
//   - migrate, PG serial and PG identity → MySQL AUTO_INCREMENT;
//   - sync, MySQL → MySQL: the cold-start copy, then CDC apply through the
//     serial applier (ApplyConcurrency 1) and the lane pools (4);
//   - restore, MySQL backup → MySQL;
//   - the `sync from-backup` broker applying an incremental.
//
// Each table is the AUTO_INCREMENT family's two shapes: the key itself
// (BIGINT AUTO_INCREMENT PRIMARY KEY) and a non-key AUTO_INCREMENT column
// backed by a UNIQUE (GitHub #25's shape, INT UNSIGNED).
//
// # The independent expected value, named
//
// The SOURCE rows, read back from the source server itself and compared
// to the target's, key for key and value for value — not a row count,
// which the defect preserves (it renumbers, it does not drop).
//
// The idempotent (ON DUPLICATE KEY UPDATE) bulk core is pinned at the
// engine level, in TestWriteCores_AutoIncrementZero_EveryCore.
package pipeline

import (
	"context"
	"database/sql"
	"fmt"
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

// aiZeroDDL creates the two AUTO_INCREMENT shapes under a name prefix.
func aiZeroDDL(prefix string) string {
	return fmt.Sprintf(`
		CREATE TABLE %[1]s_pk (
			id BIGINT NOT NULL AUTO_INCREMENT,
			v  VARCHAR(32) NOT NULL,
			PRIMARY KEY (id)
		) ENGINE=InnoDB;
		CREATE TABLE %[1]s_uk (
			k VARCHAR(16) NOT NULL,
			n INT UNSIGNED NOT NULL AUTO_INCREMENT,
			PRIMARY KEY (k),
			UNIQUE KEY uq_%[1]s_n (n)
		) ENGINE=InnoDB;
	`, prefix)
}

// aiZeroRows inserts a 0 into each shape (the session must carry
// NO_AUTO_VALUE_ON_ZERO for the source to hold it at all) plus non-zero
// neighbours, so a renumbered 0 would land between or after them.
func aiZeroRows(prefix string) string {
	return fmt.Sprintf(`
		SET SESSION sql_mode = CONCAT_WS(',', NULLIF(@@SESSION.sql_mode, ''), 'NO_AUTO_VALUE_ON_ZERO');
		INSERT INTO %[1]s_pk (id, v) VALUES (0, 'zero'), (5, 'five'), (100, 'hundred');
		INSERT INTO %[1]s_uk (k, n) VALUES ('a', 0), ('b', 7);
	`, prefix)
}

// mysqlRowsAsText renders `SELECT <cols> FROM table ORDER BY <first col>`
// as one string per row.
func mysqlRowsAsText(t *testing.T, dsn, query string) []string {
	t.Helper()
	db, err := sql.Open("mysql", dsn)
	if err != nil {
		t.Fatalf("open %s: %v", dsn, err)
	}
	defer func() { _ = db.Close() }()
	return rowsAsText(t, db, query)
}

func rowsAsText(t *testing.T, db *sql.DB, query string) []string {
	t.Helper()
	rows, err := db.Query(query)
	if err != nil {
		t.Fatalf("query %q: %v", query, err)
	}
	defer func() { _ = rows.Close() }()
	cols, _ := rows.Columns()
	var out []string
	for rows.Next() {
		vals := make([]sql.NullString, len(cols))
		ptrs := make([]any, len(cols))
		for i := range vals {
			ptrs[i] = &vals[i]
		}
		if err := rows.Scan(ptrs...); err != nil {
			t.Fatalf("scan %q: %v", query, err)
		}
		parts := make([]string, len(vals))
		for i, v := range vals {
			parts[i] = v.String
			if !v.Valid {
				parts[i] = "<NULL>"
			}
		}
		out = append(out, strings.Join(parts, "|"))
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("rows %q: %v", query, err)
	}
	return out
}

// assertAIZeroMatches compares both shapes source-vs-target, row for row.
func assertAIZeroMatches(t *testing.T, sourceDSN, targetDSN, prefix string) {
	t.Helper()
	for _, q := range []string{
		"SELECT CAST(id AS CHAR), v FROM " + prefix + "_pk ORDER BY id",
		"SELECT k, CAST(n AS CHAR) FROM " + prefix + "_uk ORDER BY k",
	} {
		src := mysqlRowsAsText(t, sourceDSN, q)
		dst := mysqlRowsAsText(t, targetDSN, q)
		if strings.Join(src, ";") != strings.Join(dst, ";") {
			t.Errorf("%s:\n source %v\n target %v\nan AUTO_INCREMENT value of 0 was rewritten on the target "+
				"(the session lacks NO_AUTO_VALUE_ON_ZERO)", q, src, dst)
		}
	}
}

// waitForAIZeroMatch polls until the target's two shapes equal the source's.
func waitForAIZeroMatch(t *testing.T, sourceDSN, targetDSN, prefix string, timeout time.Duration) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if pollRowCountMySQL(targetDSN, prefix+"_pk") >= 3 && pollRowCountMySQL(targetDSN, prefix+"_uk") >= 2 {
			break
		}
		time.Sleep(250 * time.Millisecond)
	}
	assertAIZeroMatches(t, sourceDSN, targetDSN, prefix)
}

func TestMigrate_MySQLToMySQL_AutoIncrementZero_BothCores(t *testing.T) {
	sourceDSN, targetDSN, cleanup := startMySQL(t)
	defer cleanup()
	applyDDLMySQL(t, sourceDSN, aiZeroDDL("aiz")+aiZeroRows("aiz"))

	eng, ok := engines.Get("mysql")
	if !ok {
		t.Fatal("mysql engine not registered")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
	defer cancel()
	db, err := sql.Open("mysql", targetDSN)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = db.Close() }()

	for _, core := range []struct {
		name, infile string
		loadData     bool
	}{{"load-data core", "ON", true}, {"batched-insert core", "OFF", false}} {
		t.Run(core.name, func(t *testing.T) {
			for _, tbl := range []string{"aiz_pk", "aiz_uk"} {
				if _, err := db.ExecContext(ctx, "DROP TABLE IF EXISTS "+tbl); err != nil {
					t.Fatal(err)
				}
			}
			setMySQLLocalInfile(t, ctx, db, core.infile)
			before := mysqlComLoad(t, ctx, db)
			mig := &Migrator{
				Source: eng, Target: eng, SourceDSN: sourceDSN, TargetDSN: targetDSN,
				MigrationID: "aiz-" + core.infile,
			}
			if err := mig.Run(ctx); err != nil {
				t.Fatalf("Migrator.Run: %v", err)
			}
			if moved := mysqlComLoad(t, ctx, db) != before; moved != core.loadData {
				t.Fatalf("Com_load moved=%v, want %v: the copy did not run through the %s", moved, core.loadData, core.name)
			}
			assertAIZeroMatches(t, sourceDSN, targetDSN, "aiz")
		})
	}
}

func TestMigrate_PGToMySQL_SerialAndIdentityZero(t *testing.T) {
	pgSource, _, pgCleanup := startPostgres(t)
	defer pgCleanup()
	_, mysqlTarget, myCleanup := startMySQL(t)
	defer myCleanup()

	applyPGDDL(t, pgSource, `
		CREATE TABLE pgz_serial (id BIGSERIAL PRIMARY KEY, v TEXT NOT NULL);
		CREATE TABLE pgz_ident (id INT GENERATED BY DEFAULT AS IDENTITY PRIMARY KEY, v TEXT NOT NULL);
		INSERT INTO pgz_serial (id, v) VALUES (0, 'zero'), (5, 'five'), (100, 'hundred');
		INSERT INTO pgz_ident (id, v) VALUES (0, 'zero'), (5, 'five'), (100, 'hundred');
	`)
	pgEng, _ := engines.Get("postgres")
	myEng, _ := engines.Get("mysql")
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
	defer cancel()
	db, err := sql.Open("mysql", mysqlTarget)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = db.Close() }()
	pg, err := sql.Open("pgx", pgSource)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = pg.Close() }()

	for _, infile := range []string{"ON", "OFF"} {
		t.Run("local_infile="+infile, func(t *testing.T) {
			for _, tbl := range []string{"pgz_serial", "pgz_ident"} {
				if _, err := db.ExecContext(ctx, "DROP TABLE IF EXISTS "+tbl); err != nil {
					t.Fatal(err)
				}
			}
			setMySQLLocalInfile(t, ctx, db, infile)
			mig := &Migrator{
				Source: pgEng, Target: myEng, SourceDSN: pgSource, TargetDSN: mysqlTarget,
				MigrationID: "pgz-" + infile,
			}
			if err := mig.Run(ctx); err != nil {
				t.Fatalf("Migrator.Run: %v", err)
			}
			for _, tbl := range []string{"pgz_serial", "pgz_ident"} {
				extra := mysqlString(t, ctx, db, "SELECT EXTRA FROM information_schema.COLUMNS "+
					"WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = '"+tbl+"' AND COLUMN_NAME = 'id'")
				if !strings.Contains(strings.ToLower(extra), "auto_increment") {
					t.Fatalf("%s.id landed without AUTO_INCREMENT (EXTRA=%q): the pin is not exercising the class", tbl, extra)
				}
				src := rowsAsText(t, pg, "SELECT id::text AS idt, v FROM "+tbl+" ORDER BY id")
				dst := rowsAsText(t, db, "SELECT CAST(id AS CHAR), v FROM "+tbl+" ORDER BY id")
				if strings.Join(src, ";") != strings.Join(dst, ";") {
					t.Errorf("%s: source %v, target %v: a PG key of 0 was renumbered by MySQL AUTO_INCREMENT", tbl, src, dst)
				}
			}
		})
	}
}

func TestSync_MySQLToMySQL_AutoIncrementZero_ColdStartAndCDC(t *testing.T) {
	for _, w := range []int{1, 4} {
		t.Run(fmt.Sprintf("apply-concurrency=%d", w), func(t *testing.T) {
			sourceDSN, targetDSN, cleanup := startMySQLBinlog(t)
			defer cleanup()
			// cs_* rows exist before the stream starts (cold-start copy);
			// cdc_* tables exist empty and get their rows over CDC.
			applyDDLMySQL(t, sourceDSN, aiZeroDDL("cs")+aiZeroRows("cs")+aiZeroDDL("cdc"))

			eng, _ := engines.Get("mysql")
			streamer := &Streamer{
				Source: eng, Target: eng, SourceDSN: sourceDSN, TargetDSN: targetDSN,
				StreamID: fmt.Sprintf("aiz-sync-%d", w), ApplyConcurrency: w,
			}
			ctx, cancel := context.WithCancel(context.Background())
			runErr := make(chan error, 1)
			go func() { runErr <- streamer.Run(ctx) }()
			defer func() {
				cancel()
				select {
				case <-runErr:
				case <-time.After(20 * time.Second):
					t.Errorf("streamer did not exit within 20s of cancel")
				}
			}()

			waitForAIZeroMatch(t, sourceDSN, targetDSN, "cs", 90*time.Second)
			applyDDLMySQL(t, sourceDSN, aiZeroRows("cdc"))
			waitForAIZeroMatch(t, sourceDSN, targetDSN, "cdc", 60*time.Second)
		})
	}
}

func TestRestore_MySQLToMySQL_AutoIncrementZero(t *testing.T) {
	sourceDSN, targetDSN, cleanup := startMySQL(t)
	defer cleanup()
	applyDDLMySQL(t, sourceDSN, aiZeroDDL("rz")+aiZeroRows("rz"))

	eng, _ := engines.Get("mysql")
	store, err := blobcodec.NewLocalStore(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
	defer cancel()
	if err := (&backup.Backup{Source: eng, SourceDSN: sourceDSN, Store: store, SluiceVersion: "test"}).Run(ctx); err != nil {
		t.Fatalf("Backup.Run: %v", err)
	}
	if err := (&backup.Restore{Target: eng, TargetDSN: targetDSN, Store: store}).Run(ctx); err != nil {
		t.Fatalf("Restore.Run: %v", err)
	}
	assertAIZeroMatches(t, sourceDSN, targetDSN, "rz")
}

func TestSyncFromBackup_MySQL_AutoIncrementZero(t *testing.T) {
	sourceDSN, brokerTargetDSN, cleanup := startMySQLBinlog(t)
	defer cleanup()
	// The tables exist (empty) at the full; their rows, 0 included, arrive
	// in the incremental the broker applies.
	applyDDLMySQL(t, sourceDSN, aiZeroDDL("bz"))

	eng, _ := engines.Get("mysql")
	store, err := blobcodec.NewLocalStore(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	if err := (&backup.Backup{Source: eng, SourceDSN: sourceDSN, Store: store, SluiceVersion: "test"}).Run(ctx); err != nil {
		t.Fatalf("Backup.Run: %v", err)
	}
	binlogFile, binlogPos := readMySQLBinlogPos(t, sourceDSN)
	full, _ := lineage.ReadManifest(ctx, store)
	full.Kind = irbackup.BackupKindFull
	full.EndPosition = ir.Position{
		Engine: "mysql",
		Token:  fmt.Sprintf(`{"mode":"file_pos","file":%q,"pos":%d}`, binlogFile, binlogPos),
	}
	full.BackupID = irbackup.ComputeBackupID(full)
	if err := lineage.WriteManifestAt(ctx, store, lineage.ManifestFileName, full); err != nil {
		t.Fatalf("rewrite full manifest: %v", err)
	}
	if err := (&backup.Restore{Target: eng, TargetDSN: brokerTargetDSN, Store: store}).Run(ctx); err != nil {
		t.Fatalf("seed restore: %v", err)
	}

	applyDDLMySQL(t, sourceDSN, aiZeroRows("bz"))
	incrCtx, incrCancel := context.WithTimeout(ctx, 30*time.Second)
	defer incrCancel()
	if err := (&IncrementalBackup{
		Source: eng, SourceDSN: sourceDSN, Store: store,
		ParentRef: full.BackupID, Window: 10 * time.Second, MaxChanges: 50,
		ChunkChanges: 50, SluiceVersion: "test",
	}).Run(incrCtx); err != nil {
		t.Fatalf("IncrementalBackup.Run: %v", err)
	}

	broker := &SyncFromBackup{
		Target: eng, TargetDSN: brokerTargetDSN, Store: store,
		ChainURL: "test://aiz-broker", StreamID: "aiz-broker",
		PollInterval: 2 * time.Second, AtChainID: full.BackupID, SluiceVersion: "test",
	}
	bctx, bcancel := context.WithCancel(ctx)
	brokerErr := make(chan error, 1)
	go func() { brokerErr <- broker.Run(bctx) }()
	waitForAIZeroMatch(t, sourceDSN, brokerTargetDSN, "bz", 60*time.Second)
	bcancel()
	select {
	case err := <-brokerErr:
		if err != nil {
			t.Errorf("broker.Run = %v; want nil", err)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("broker did not exit within 10s of cancel")
	}
}
