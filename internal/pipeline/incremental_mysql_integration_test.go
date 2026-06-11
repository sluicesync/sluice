//go:build integration

// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

// MySQL → MySQL counterpart of incremental_pg_integration_test.go.
// Same Phase 3 acceptance criteria: full + ALTER/INSERT + incremental
// + chain restore round-trip.

package pipeline

import (
	"context"
	"database/sql"
	"fmt"
	"testing"
	"time"

	"sluicesync.dev/sluice/internal/engines"
	"sluicesync.dev/sluice/internal/ir"
	irbackup "sluicesync.dev/sluice/internal/ir/backup"

	_ "sluicesync.dev/sluice/internal/engines/mysql"
)

// TestIncrementalBackup_MySQLChainRestore drives the full Phase 3
// happy path on MySQL.
func TestIncrementalBackup_MySQLChainRestore(t *testing.T) {
	sourceDSN, targetDSN, cleanup := startMySQLBinlog(t)
	defer cleanup()

	const seedDDL = `
		CREATE TABLE users (
			id    BIGINT       NOT NULL AUTO_INCREMENT,
			email VARCHAR(255) NOT NULL,
			PRIMARY KEY (id)
		) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
		INSERT INTO users (email) VALUES ('alice@example.com'), ('bob@example.com');
	`
	applyDDLMySQL(t, sourceDSN, seedDDL)

	mysqlEng, ok := engines.Get("mysql")
	if !ok {
		t.Fatal("mysql engine not registered")
	}

	dir := t.TempDir()
	store, err := NewLocalStore(dir)
	if err != nil {
		t.Fatalf("NewLocalStore: %v", err)
	}

	// 1. Full backup.
	if err := (&Backup{
		Source: mysqlEng, SourceDSN: sourceDSN, Store: store,
		SluiceVersion: "test",
	}).Run(context.Background()); err != nil {
		t.Fatalf("Backup.Run: %v", err)
	}
	// 2. Patch in EndPosition (Phase 3.3 will record this on full).
	//    Use file+pos since the test container doesn't enable
	//    gtid_mode; the MySQL CDC reader supports both modes.
	binlogFile, binlogPos := readMySQLBinlogPos(t, sourceDSN)
	full, err := readManifest(context.Background(), store)
	if err != nil {
		t.Fatalf("readManifest: %v", err)
	}
	full.Kind = irbackup.BackupKindFull
	full.EndPosition = ir.Position{
		Engine: "mysql",
		Token:  fmt.Sprintf(`{"mode":"file_pos","file":%q,"pos":%d}`, binlogFile, binlogPos),
	}
	full.BackupID = irbackup.ComputeBackupID(full)
	if err := writeManifestAt(context.Background(), store, ManifestFileName, full); err != nil {
		t.Fatalf("rewrite full manifest: %v", err)
	}

	// 3. Insert on source after the snapshot.
	const evolveDDL = `
		INSERT INTO users (email) VALUES ('carol@example.com');
		INSERT INTO users (email) VALUES ('dave@example.com');
	`
	applyDDLMySQL(t, sourceDSN, evolveDDL)

	// 4. Incremental.
	incrCtx, incrCancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer incrCancel()
	incr := &IncrementalBackup{
		Source:        mysqlEng,
		SourceDSN:     sourceDSN,
		Store:         store,
		ParentRef:     full.BackupID,
		Window:        15 * time.Second,
		MaxChanges:    20,
		ChunkChanges:  10,
		SluiceVersion: "test",
	}
	if err := incr.Run(incrCtx); err != nil {
		t.Fatalf("IncrementalBackup.Run: %v", err)
	}

	// 5. Verify chunks intact.
	total, mismatches, err := VerifyBackup(context.Background(), store)
	if err != nil {
		t.Fatalf("VerifyBackup: %v", err)
	}
	if mismatches != 0 {
		t.Errorf("VerifyBackup: %d of %d chunks failed", mismatches, total)
	}

	// 6. Chain restore.
	if err := (&Restore{
		Target:    mysqlEng,
		TargetDSN: targetDSN,
		Store:     store,
	}).Run(context.Background()); err != nil {
		t.Fatalf("Restore.Run: %v", err)
	}

	// 7. Verify target reflects every row including carol/dave.
	got := mysqlQueryEmails(t, targetDSN)
	wantEmails := map[string]bool{
		"alice@example.com": true,
		"bob@example.com":   true,
		"carol@example.com": true,
		"dave@example.com":  true,
	}
	gotMap := map[string]bool{}
	for _, e := range got {
		gotMap[e] = true
	}
	for w := range wantEmails {
		if !gotMap[w] {
			t.Errorf("target missing email %q (got: %v)", w, got)
		}
	}
}

// readMySQLBinlogPos returns (file, pos) from SHOW BINARY LOG STATUS
// (or SHOW MASTER STATUS on older versions). Used to patch the full
// manifest's EndPosition until Phase 3.3 records it automatically.
func readMySQLBinlogPos(t *testing.T, dsn string) (file string, pos uint32) {
	t.Helper()
	db, err := sql.Open("mysql", dsn)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer func() { _ = db.Close() }()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	for _, q := range []string{"SHOW BINARY LOG STATUS", "SHOW MASTER STATUS"} {
		f, p, ok := tryReadMySQLBinlogPos(ctx, db, q)
		if ok {
			return f, p
		}
	}
	t.Fatal("could not read MySQL binlog position")
	return "", 0
}

// tryReadMySQLBinlogPos runs one of the master-status queries; ok=false
// when the query errors or returns no rows, in which case the caller
// falls through to the next variant.
func tryReadMySQLBinlogPos(ctx context.Context, db *sql.DB, query string) (file string, pos uint32, ok bool) {
	rows, err := db.QueryContext(ctx, query)
	if err != nil {
		return "", 0, false
	}
	defer func() { _ = rows.Close() }()
	cols, err := rows.Columns()
	if err != nil {
		return "", 0, false
	}
	if !rows.Next() {
		return "", 0, false
	}
	dest := make([]any, len(cols))
	holders := make([]any, len(cols))
	for i := range dest {
		holders[i] = &dest[i]
	}
	if err := rows.Scan(holders...); err != nil {
		return "", 0, false
	}
	if err := rows.Err(); err != nil {
		return "", 0, false
	}
	switch v := dest[0].(type) {
	case string:
		file = v
	case []byte:
		file = string(v)
	}
	var u uint64
	switch v := dest[1].(type) {
	case int64:
		u = uint64(v)
	case uint64:
		u = v
	case []byte:
		_, _ = fmt.Sscan(string(v), &u)
	case string:
		_, _ = fmt.Sscan(v, &u)
	}
	return file, uint32(u), true
}

// mysqlQueryEmails reads every email from the target users table.
func mysqlQueryEmails(t *testing.T, dsn string) []string {
	t.Helper()
	db, err := sql.Open("mysql", dsn)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer func() { _ = db.Close() }()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	rows, err := db.QueryContext(ctx, "SELECT email FROM users ORDER BY id")
	if err != nil {
		t.Fatalf("query: %v", err)
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
	if err := rows.Err(); err != nil {
		t.Fatalf("rows: %v", err)
	}
	return out
}
