// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package sqlitetrigger

import (
	"context"
	"database/sql"
	"path/filepath"
	"testing"

	_ "modernc.org/sqlite" // pure-Go driver — no cgo, no container needed
)

// These are real-SQLite-file UNIT tests: modernc.org/sqlite is pure Go, so a
// temp file gives the actual DELETE/VACUUM/stats path without Docker. The
// id <= cut boundary (the off-by-one that would either leak the frontier row or
// — worse — over-delete) is pinned here so it can't regress silently.

// seedChangeLog writes a temp SQLite file with a change-log table holding rows
// id=1..n and returns the file path.
func seedChangeLog(t *testing.T, n int64) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "src.db")
	db, err := sql.Open("sqlite", path)
	if err != nil {
		t.Fatalf("open sqlite: %v", err)
	}
	defer func() { _ = db.Close() }()
	ctx := context.Background()
	if _, err := db.ExecContext(ctx,
		`CREATE TABLE "`+ChangeLogTable+`" (id INTEGER PRIMARY KEY AUTOINCREMENT, op TEXT, tbl TEXT)`); err != nil {
		t.Fatalf("create change-log: %v", err)
	}
	for i := int64(1); i <= n; i++ {
		if _, err := db.ExecContext(ctx,
			`INSERT INTO "`+ChangeLogTable+`" (id, op, tbl) VALUES (?, 'I', 't')`, i); err != nil {
			t.Fatalf("seed row %d: %v", i, err)
		}
	}
	return path
}

// remainingIDs returns the sorted id set still in the change-log.
func remainingIDs(t *testing.T, path string) []int64 {
	t.Helper()
	db, err := sql.Open("sqlite", path)
	if err != nil {
		t.Fatalf("open sqlite: %v", err)
	}
	defer func() { _ = db.Close() }()
	rows, err := db.QueryContext(context.Background(), `SELECT id FROM "`+ChangeLogTable+`" ORDER BY id`)
	if err != nil {
		t.Fatalf("query ids: %v", err)
	}
	defer func() { _ = rows.Close() }()
	var ids []int64
	for rows.Next() {
		var id int64
		if err := rows.Scan(&id); err != nil {
			t.Fatalf("scan id: %v", err)
		}
		ids = append(ids, id)
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("rows err: %v", err)
	}
	return ids
}

func equalIDs(a, b []int64) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

// TestPrune_DeletesAtMostCut_Inclusive pins the load-bearing boundary: cut=5
// removes ids 1..5 and KEEPS 6..10 — `id <= cut`, not `id < cut` (which would
// leak id=5) and not `id < cut+1`-style over-deletes.
func TestPrune_DeletesAtMostCut_Inclusive(t *testing.T) {
	path := seedChangeLog(t, 10)
	res, err := Prune(context.Background(), path, PruneOptions{Cut: 5})
	if err != nil {
		t.Fatalf("Prune: %v", err)
	}
	if res.Deleted != 5 {
		t.Errorf("Deleted = %d; want 5", res.Deleted)
	}
	if res.RemainingMin != 6 {
		t.Errorf("RemainingMin = %d; want 6", res.RemainingMin)
	}
	if res.Remaining != 5 {
		t.Errorf("Remaining = %d; want 5", res.Remaining)
	}
	if got := remainingIDs(t, path); !equalIDs(got, []int64{6, 7, 8, 9, 10}) {
		t.Errorf("remaining ids = %v; want [6 7 8 9 10] (id <= cut deletes through 5 inclusive)", got)
	}
}

// TestPrune_Idempotent re-runs the same cut and asserts nothing new is deleted.
func TestPrune_Idempotent(t *testing.T) {
	path := seedChangeLog(t, 10)
	if _, err := Prune(context.Background(), path, PruneOptions{Cut: 5}); err != nil {
		t.Fatalf("Prune 1: %v", err)
	}
	res, err := Prune(context.Background(), path, PruneOptions{Cut: 5})
	if err != nil {
		t.Fatalf("Prune 2: %v", err)
	}
	if res.Deleted != 0 {
		t.Errorf("second Prune Deleted = %d; want 0 (idempotent)", res.Deleted)
	}
	if got := remainingIDs(t, path); !equalIDs(got, []int64{6, 7, 8, 9, 10}) {
		t.Errorf("remaining ids after re-prune = %v; want [6 7 8 9 10]", got)
	}
}

// TestPrune_Vacuum exercises the --vacuum path (it must not error and must not
// change which rows remain).
func TestPrune_Vacuum(t *testing.T) {
	path := seedChangeLog(t, 10)
	res, err := Prune(context.Background(), path, PruneOptions{Cut: 5, Vacuum: true})
	if err != nil {
		t.Fatalf("Prune with vacuum: %v", err)
	}
	if !res.Vacuumed {
		t.Error("Vacuumed = false; want true")
	}
	if got := remainingIDs(t, path); !equalIDs(got, []int64{6, 7, 8, 9, 10}) {
		t.Errorf("remaining ids after vacuum = %v; want [6 7 8 9 10]", got)
	}
}

// TestPrune_DryRun asserts a dry-run deletes nothing and reports current stats.
func TestPrune_DryRun(t *testing.T) {
	path := seedChangeLog(t, 10)
	res, err := Prune(context.Background(), path, PruneOptions{Cut: 5, DryRun: true})
	if err != nil {
		t.Fatalf("Prune dry-run: %v", err)
	}
	if res.Deleted != 0 {
		t.Errorf("dry-run Deleted = %d; want 0", res.Deleted)
	}
	if res.Remaining != 10 || res.RemainingMin != 1 {
		t.Errorf("dry-run stats = (min %d, count %d); want (1, 10)", res.RemainingMin, res.Remaining)
	}
	if got := remainingIDs(t, path); len(got) != 10 {
		t.Errorf("dry-run mutated the change-log: %v", got)
	}
}

// TestPrune_RefusesMissingChangeLog asserts a prune against a source without the
// change-log table refuses loudly (not a silent no-op).
func TestPrune_RefusesMissingChangeLog(t *testing.T) {
	path := filepath.Join(t.TempDir(), "empty.db")
	db, err := sql.Open("sqlite", path)
	if err != nil {
		t.Fatalf("open sqlite: %v", err)
	}
	// Create some unrelated table so the file is a valid DB.
	if _, err := db.ExecContext(context.Background(), `CREATE TABLE t (id INTEGER PRIMARY KEY)`); err != nil {
		t.Fatalf("create t: %v", err)
	}
	_ = db.Close()

	if _, err := Prune(context.Background(), path, PruneOptions{Cut: 5}); err == nil {
		t.Fatal("Prune against a source with no change-log returned nil; want a loud error")
	}
}

// TestAppliedLastID covers the token decode used to derive the prune bound.
func TestAppliedLastID(t *testing.T) {
	got, err := AppliedLastID(`{"last_id":42}`)
	if err != nil {
		t.Fatalf("AppliedLastID valid token: %v", err)
	}
	if got != 42 {
		t.Errorf("AppliedLastID = %d; want 42", got)
	}

	if _, err := AppliedLastID(""); err == nil {
		t.Error("AppliedLastID(empty) returned nil; want a loud error")
	}
	if _, err := AppliedLastID("not-json"); err == nil {
		t.Error("AppliedLastID(malformed) returned nil; want a loud error")
	}
	// A negative last_id is rejected by decodePos (the persisted watermark must be >= 0).
	if _, err := AppliedLastID(`{"last_id":-1}`); err == nil {
		t.Error("AppliedLastID(negative) returned nil; want a loud error")
	}
	// A FOREIGN token that happens to unmarshal cleanly (a pgoutput {slot,lsn},
	// a broker envelope) must REFUSE — not silently decode to last_id=0 and look
	// like "nothing to prune" against the wrong stream.
	for _, foreign := range []string{
		`{"slot":"sluice_slot","lsn":"0/16B3748"}`,
		`{"gtid":"3E11FA47-71CA-11E1-9E33-C80AA9429562:1-5"}`,
		`{"chain_id":"c1","segment":3}`,
	} {
		if _, err := AppliedLastID(foreign); err == nil {
			t.Errorf("AppliedLastID(%q) returned nil; want a loud refuse (no last_id key)", foreign)
		}
	}
}
