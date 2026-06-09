//go:build integration

// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

// MySQL fallback for the ADR-0077 index-build overlap. MySQL's SchemaWriter
// does NOT implement ir.IncrementalIndexBuilder, so the orchestrator must
// take the pre-ADR-0077 path: serial cross-table copy → identity-sync →
// whole-schema CreateIndexes AFTER the copy completes. This pins that the
// overlap path is NOT engaged on a MySQL target (the copy-complete
// observability seam never fires — it only fires inside
// runOverlappedCopyAndIndexPhase) AND that indexes still land correctly.

package pipeline

import (
	"context"
	"database/sql"
	"fmt"
	"sync/atomic"
	"testing"
	"time"

	"sluicesync.dev/sluice/internal/engines"

	_ "sluicesync.dev/sluice/internal/engines/mysql"
)

func TestMigrate_MySQL_IndexOverlap_FallbackToWholeSchema(t *testing.T) {
	mysqlEng, ok := engines.Get("mysql")
	if !ok {
		t.Fatal("mysql engine not registered")
	}

	src, tgt, cleanup := startMySQL(t)
	defer cleanup()

	// Seed a few indexed tables on the MySQL source.
	const (
		tableCount = 6
		rowsEach   = 5_000
	)
	seedManyIndexedTablesMySQL(t, src, tableCount, rowsEach)

	// The overlap path's copy-complete seam must NEVER fire on a MySQL
	// target — runOverlappedCopyAndIndexPhase is only entered for an
	// IncrementalIndexBuilder target (PG). A non-zero count means the
	// fallback branch was wrongly bypassed.
	var overlapFired atomic.Int64
	restore := setOnTableCopiedObserverForTest(func(_ string) {
		overlapFired.Add(1)
	})
	defer restore()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()

	mig := &Migrator{
		Source:           mysqlEng,
		Target:           mysqlEng,
		SourceDSN:        src,
		TargetDSN:        tgt,
		TableParallelism: 3,
		MigrationID:      "mysql-idx-fallback",
	}
	if err := mig.Run(ctx); err != nil {
		t.Fatalf("Run: %v", err)
	}

	if n := overlapFired.Load(); n != 0 {
		t.Errorf("overlap path engaged on a MySQL target (copy-complete seam fired %d times); want 0 (fallback expected)", n)
	}

	// Zero-loss + every secondary index present on the MySQL target.
	tgtDB, err := sql.Open("mysql", tgt)
	if err != nil {
		t.Fatalf("open target: %v", err)
	}
	defer func() { _ = tgtDB.Close() }()
	srcDB, err := sql.Open("mysql", src)
	if err != nil {
		t.Fatalf("open source: %v", err)
	}
	defer func() { _ = srcDB.Close() }()

	for i := 0; i < tableCount; i++ {
		name := fmt.Sprintf("tbl_%02d", i)
		var sc, tc int64
		if err := srcDB.QueryRowContext(ctx, fmt.Sprintf("SELECT COUNT(*) FROM %s", name)).Scan(&sc); err != nil {
			t.Fatalf("src count %s: %v", name, err)
		}
		if err := tgtDB.QueryRowContext(ctx, fmt.Sprintf("SELECT COUNT(*) FROM %s", name)).Scan(&tc); err != nil {
			t.Fatalf("tgt count %s: %v", name, err)
		}
		if sc != tc {
			t.Errorf("table %s count mismatch: src=%d tgt=%d", name, sc, tc)
		}
		for _, idx := range []string{name + "_v_idx", name + "_b_idx"} {
			var got int
			if err := tgtDB.QueryRowContext(
				ctx,
				`SELECT COUNT(*) FROM information_schema.statistics WHERE table_schema=DATABASE() AND table_name=? AND index_name=?`,
				name, idx,
			).Scan(&got); err != nil {
				t.Fatalf("query statistics %s: %v", idx, err)
			}
			if got == 0 {
				t.Errorf("index %s missing on MySQL target", idx)
			}
		}
	}
}

func seedManyIndexedTablesMySQL(t *testing.T, dsn string, tableCount, rowsEach int) {
	t.Helper()
	db, err := sql.Open("mysql", dsn+"&multiStatements=true")
	if err != nil {
		t.Fatalf("open source: %v", err)
	}
	defer func() { _ = db.Close() }()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()
	for i := 0; i < tableCount; i++ {
		name := fmt.Sprintf("tbl_%02d", i)
		ddl := fmt.Sprintf(`
			CREATE TABLE %s (
				id BIGINT PRIMARY KEY,
				v  BIGINT NOT NULL,
				b  BIGINT NOT NULL,
				INDEX %s_v_idx (v),
				INDEX %s_b_idx (b)
			);`, name, name, name)
		if _, err := db.ExecContext(ctx, ddl); err != nil {
			t.Fatalf("create %s: %v", name, err)
		}
		// Batch-insert rowsEach rows.
		const batch = 1000
		for off := 0; off < rowsEach; off += batch {
			vals := ""
			for j := off; j < off+batch && j < rowsEach; j++ {
				if vals != "" {
					vals += ","
				}
				vals += fmt.Sprintf("(%d,%d,%d)", j+1, (j+1)*7, (j+1)%100)
			}
			if _, err := db.ExecContext(ctx, fmt.Sprintf("INSERT INTO %s (id,v,b) VALUES %s", name, vals)); err != nil {
				t.Fatalf("seed %s: %v", name, err)
			}
		}
	}
}
