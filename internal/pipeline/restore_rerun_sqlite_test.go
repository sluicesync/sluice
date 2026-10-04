// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package pipeline

import (
	"context"
	"database/sql"
	"fmt"
	"path/filepath"
	"strings"
	"testing"

	_ "modernc.org/sqlite"

	"sluicesync.dev/sluice/internal/engines"
	_ "sluicesync.dev/sluice/internal/engines/sqlite"
	"sluicesync.dev/sluice/internal/pipeline/backup"
	"sluicesync.dev/sluice/internal/pipeline/blobcodec"
	"sluicesync.dev/sluice/internal/sluicecode"
)

// TestMigrate_SQLiteTargetHoldingRows_RefusedByColdStartPreflight pins the
// side effect of giving the SQLite row writer ir.TableEmptyChecker: the
// migrate cold-start pre-flight, which skips a writer without it, now runs
// for a SQLite target. A migrate into a SQLite file whose table already
// holds rows refuses (SLUICE-E-COLD-START-TARGET-NOT-EMPTY) instead of
// appending, as on Postgres and MySQL; --force-cold-start still overrides.
func TestMigrate_SQLiteTargetHoldingRows_RefusedByColdStartPreflight(t *testing.T) {
	ctx := context.Background()
	src := filepath.Join(t.TempDir(), "src.db")
	dst := filepath.Join(t.TempDir(), "dst.db")
	for _, p := range []struct{ path, ddl string }{
		{src, `CREATE TABLE logs (id INTEGER NOT NULL, v TEXT); INSERT INTO logs VALUES (1, 'a'), (2, 'b');`},
		{dst, `CREATE TABLE logs (id INTEGER NOT NULL, v TEXT); INSERT INTO logs VALUES (9, 'already here');`},
	} {
		db, err := sql.Open("sqlite", p.path)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := db.ExecContext(ctx, p.ddl); err != nil {
			t.Fatal(err)
		}
		_ = db.Close()
	}
	eng, _ := engines.Get("sqlite")
	err := (&Migrator{Source: eng, Target: eng, SourceDSN: src, TargetDSN: dst}).Run(ctx)
	ce, ok := sluicecode.FromError(err)
	if !ok || ce.Code != sluicecode.CodeColdStartTargetNotEmpty {
		t.Fatalf("migrate into a populated SQLite table = %v; want %s", err, sluicecode.CodeColdStartTargetNotEmpty)
	}
	db, err := sql.Open("sqlite", dst)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = db.Close() }()
	var n int
	if err := db.QueryRowContext(ctx, `SELECT count(*) FROM logs`).Scan(&n); err != nil {
		t.Fatal(err)
	}
	if n != 1 {
		t.Errorf("the refused migrate changed the target: %d rows, want the 1 already there", n)
	}
}

// TestRestoreRerun_SQLiteTarget pins the F-E1 restore re-run door on a REAL
// SQLite target, the engine the review found the door open on: its writer
// implemented neither probe, the judge treated "cannot tell" as "empty", and
// a keyless table restored twice held 200 rows for 100 at exit 0.
//
// The independent expected value is the target's own row count, read
// straight from the file — not anything the restore reports about itself.
// The matrix is the shapes whose re-run outcome differs:
//
//   - keyless: refused before writing, still 100 rows;
//   - keyed (INTEGER PRIMARY KEY): not this door's case — the re-run's plain
//     INSERT collides and fails loudly, still 100 rows;
//   - recorded keyed, but the TARGET table was pre-created keyed on a
//     surrogate the backup's rows do not carry: refused on the target
//     judgment, still 100 rows (the review's HIGH 1 on this engine).
func TestRestoreRerun_SQLiteTarget(t *testing.T) {
	cases := []struct {
		name       string
		srcDDL     string
		preTarget  string // created on the target before the first restore
		wantRefuse bool
		wantReason string
	}{
		{"keyless", `CREATE TABLE logs (id INTEGER NOT NULL, v TEXT)`, "", true, "recorded schema"},
		{"keyed", `CREATE TABLE logs (id INTEGER NOT NULL PRIMARY KEY, v TEXT)`, "", false, ""},
		// Conservative, and stated: SQLite reports a rowid-alias INTEGER
		// PRIMARY KEY as nullable (table_xinfo notnull=0) on both the
		// source read and the target, and nothing in the recorded table
		// distinguishes it from a non-alias PRIMARY KEY that does admit
		// NULLs. The re-run is refused up front instead of failing on the
		// key — loud either way, same remedy.
		{"rowid alias declared without NOT NULL", `CREATE TABLE logs (id INTEGER PRIMARY KEY, v TEXT)`, "", true, "target table"},
		{
			"target keyed on a surrogate", `CREATE TABLE logs (id INTEGER PRIMARY KEY, v TEXT)`,
			// NOT NULL so only the SUPPLY check can refuse it (an INTEGER
			// PRIMARY KEY otherwise reads as nullable and is refused anyway).
			`CREATE TABLE logs (sid INTEGER NOT NULL PRIMARY KEY AUTOINCREMENT, id INTEGER NOT NULL, v TEXT)`,
			true, "target table",
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			ctx := context.Background()
			src := filepath.Join(t.TempDir(), "src.db")
			srcDB, err := sql.Open("sqlite", src)
			if err != nil {
				t.Fatal(err)
			}
			defer func() { _ = srcDB.Close() }()
			if _, err := srcDB.ExecContext(ctx, tc.srcDDL); err != nil {
				t.Fatal(err)
			}
			for i := 0; i < 100; i++ {
				if _, err := srcDB.ExecContext(ctx, `INSERT INTO logs (id, v) VALUES (?, ?)`, i, fmt.Sprint("v", i)); err != nil {
					t.Fatal(err)
				}
			}
			eng, ok := engines.Get("sqlite")
			if !ok {
				t.Fatal("sqlite engine not registered")
			}
			store, err := blobcodec.NewLocalStore(t.TempDir())
			if err != nil {
				t.Fatal(err)
			}
			if err := (&backup.Backup{Source: eng, SourceDSN: src, Store: store}).Run(ctx); err != nil {
				t.Fatalf("backup: %v", err)
			}

			dst := filepath.Join(t.TempDir(), "dst.db")
			dstDB, err := sql.Open("sqlite", dst)
			if err != nil {
				t.Fatal(err)
			}
			defer func() { _ = dstDB.Close() }()
			if tc.preTarget != "" {
				if _, err := dstDB.ExecContext(ctx, tc.preTarget); err != nil {
					t.Fatal(err)
				}
			}
			count := func() int {
				var n int
				if err := dstDB.QueryRowContext(ctx, `SELECT count(*) FROM logs`).Scan(&n); err != nil {
					t.Fatal(err)
				}
				return n
			}

			if err := (&backup.Restore{Target: eng, TargetDSN: dst, Store: store}).Run(ctx); err != nil {
				t.Fatalf("first restore: %v", err)
			}
			if n := count(); n != 100 {
				t.Fatalf("first restore left %d rows; want 100", n)
			}

			err = (&backup.Restore{Target: eng, TargetDSN: dst, Store: store}).Run(ctx)
			if n := count(); n != 100 {
				t.Fatalf("re-run left %d rows for 100 (err=%v): the re-run was not stopped", n, err)
			}
			ce, coded := sluicecode.FromError(err)
			refused := coded && ce.Code == sluicecode.CodeRestoreKeylessTableNotEmpty
			if refused != tc.wantRefuse {
				t.Fatalf("re-run = %v; refused by %s: %v, want %v", err, sluicecode.CodeRestoreKeylessTableNotEmpty, refused, tc.wantRefuse)
			}
			if tc.wantRefuse && !strings.Contains(err.Error(), tc.wantReason) {
				t.Errorf("the refusal does not say the %s judgment failed: %v", tc.wantReason, err)
			}
			if !tc.wantRefuse && err == nil {
				t.Fatal("a keyed re-run exited nil; the plain INSERT must collide on the key and fail loudly")
			}
		})
	}
}
