// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package sqlitetrigger

import (
	"bytes"
	"database/sql"
	"log/slog"
	"strings"
	"testing"
)

// These pin the SQT-2 pair (audit 2026-08-11): the REPLACE implicit-delete
// capture blind spot is now a NAMED, warned, documented limitation instead of
// an undocumented silent divergence. Two halves:
//
//   - the setup-time WARN for any table carrying a non-PK UNIQUE constraint
//     (the exact class whose REPLACE conflicts delete a row no trigger sees);
//   - the ENVIRONMENTAL PREMISE the WARN's safety argument cites — that with
//     recursive_triggers OFF the implicit delete fires no AFTER DELETE
//     trigger, and with it ON the D is captured — pinned against the real
//     SQLite engine per the premise-naming rule, so a future SQLite/driver
//     behaviour change fails a build instead of silently orphaning the docs.

// captureSlogWarn installs a WARN-level JSON slog handler into a buffer for
// the test's duration (the house pattern; see mysql/rls_warn_test.go).
func captureSlogWarn(t *testing.T) *bytes.Buffer {
	t.Helper()
	buf := &bytes.Buffer{}
	prev := slog.Default()
	slog.SetDefault(slog.New(slog.NewJSONHandler(buf, &slog.HandlerOptions{
		Level: slog.LevelWarn,
	})))
	t.Cleanup(func() { slog.SetDefault(prev) })
	return buf
}

func TestSetup_WarnsOnNonPKUniqueReplaceBlindSpot(t *testing.T) {
	t.Run("table-level UNIQUE index warns", func(t *testing.T) {
		path := newSourceFile(
			t,
			`CREATE TABLE t (id INTEGER PRIMARY KEY, u TEXT, v TEXT)`,
			`CREATE UNIQUE INDEX t_u ON t(u)`,
		)
		buf := captureSlogWarn(t)
		if _, err := Setup(bg(), path, SetupOptions{Tables: []string{"t"}}); err != nil {
			t.Fatalf("Setup: %v", err)
		}
		out := buf.String()
		for _, want := range []string{"REPLACE capture blind spot", `"t"`, "t_u", "recursive_triggers"} {
			if !strings.Contains(out, want) {
				t.Errorf("setup WARN should carry %q; got:\n%s", want, out)
			}
		}
	})
	t.Run("inline column UNIQUE (auto-index) warns too", func(t *testing.T) {
		path := newSourceFile(t, `CREATE TABLE t (id INTEGER PRIMARY KEY, u TEXT UNIQUE)`)
		buf := captureSlogWarn(t)
		if _, err := Setup(bg(), path, SetupOptions{Tables: []string{"t"}}); err != nil {
			t.Fatalf("Setup: %v", err)
		}
		if !strings.Contains(buf.String(), "REPLACE capture blind spot") {
			t.Errorf("inline UNIQUE column must warn (its auto-index is the same conflict source); got:\n%s", buf.String())
		}
	})
	t.Run("PK-only table stays silent", func(t *testing.T) {
		path := newSourceFile(t, `CREATE TABLE t (id INTEGER PRIMARY KEY, v TEXT)`)
		buf := captureSlogWarn(t)
		if _, err := Setup(bg(), path, SetupOptions{Tables: []string{"t"}}); err != nil {
			t.Fatalf("Setup: %v", err)
		}
		if strings.Contains(buf.String(), "REPLACE capture blind spot") {
			t.Errorf("a table with no non-PK UNIQUE must not warn (a PK-conflict REPLACE converges via the "+
				"target upsert); got:\n%s", buf.String())
		}
	})
}

// changeLogOps reads the raw captured op codes in id order — the ground truth
// the premise pin grades, deliberately below buildChange so it sees exactly
// what the triggers wrote.
func changeLogOps(t *testing.T, path string) []string {
	t.Helper()
	db, err := sql.Open("sqlite", path)
	if err != nil {
		t.Fatalf("open for ops read: %v", err)
	}
	defer func() { _ = db.Close() }()
	rows, err := db.QueryContext(bg(), "SELECT op FROM "+ChangeLogTable+" ORDER BY id")
	if err != nil {
		t.Fatalf("read change log: %v", err)
	}
	defer func() { _ = rows.Close() }()
	var ops []string
	for rows.Next() {
		var op string
		if err := rows.Scan(&op); err != nil {
			t.Fatalf("scan op: %v", err)
		}
		ops = append(ops, op)
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("iterate ops: %v", err)
	}
	return ops
}

// TestCapture_ReplaceImplicitDeleteBlindSpotPremise pins the environmental
// fact the SQT-2 WARN cites, in BOTH directions, against the real engine:
//
//	OFF (the default): INSERT OR REPLACE across a non-PK UNIQUE conflict
//	  captures ONLY the I — the implicitly deleted row's D never fires. This
//	  is the documented limitation; if this arm ever fails, SQLite/modernc
//	  changed behaviour and the WARN + ADR-0135 must be revisited.
//	ON  (the remedy):  the same statement captures the D too — proving the
//	  trigger set is capable and the remedy the WARN names actually works.
func TestCapture_ReplaceImplicitDeleteBlindSpotPremise(t *testing.T) {
	path := newSourceFile(t, `CREATE TABLE t (id INTEGER PRIMARY KEY, u TEXT UNIQUE, v TEXT)`)
	if _, err := Setup(bg(), path, SetupOptions{Tables: []string{"t"}}); err != nil {
		t.Fatalf("Setup: %v", err)
	}

	exec(t, path, `INSERT INTO t (id, u, v) VALUES (1, 'a', 'x')`)
	// The blind-spot arm: driver-default connection (recursive_triggers OFF).
	// Row id=1 is implicitly deleted to satisfy the u='a' conflict.
	exec(t, path, `INSERT OR REPLACE INTO t (id, u, v) VALUES (2, 'a', 'y')`)

	ops := changeLogOps(t, path)
	if want := []string{"I", "I"}; strings.Join(ops, ",") != strings.Join(want, ",") {
		t.Fatalf("ops after OR-REPLACE with recursive_triggers OFF = %v; want %v — if a D appeared, SQLite's "+
			"implicit-delete trigger behaviour CHANGED and the SQT-2 WARN + ADR-0135 limitation must be revisited", ops, want)
	}

	// The remedy arm: same statement shape on a connection with the pragma ON.
	db, err := sql.Open("sqlite", path)
	if err != nil {
		t.Fatalf("open remedy conn: %v", err)
	}
	defer func() { _ = db.Close() }()
	conn, err := db.Conn(bg())
	if err != nil {
		t.Fatalf("pin remedy conn: %v", err)
	}
	defer func() { _ = conn.Close() }()
	if _, err := conn.ExecContext(bg(), "PRAGMA recursive_triggers = ON"); err != nil {
		t.Fatalf("set recursive_triggers: %v", err)
	}
	if _, err := conn.ExecContext(bg(), `INSERT OR REPLACE INTO t (id, u, v) VALUES (3, 'a', 'z')`); err != nil {
		t.Fatalf("remedy OR-REPLACE: %v", err)
	}

	ops = changeLogOps(t, path)
	if want := []string{"I", "I", "D", "I"}; strings.Join(ops, ",") != strings.Join(want, ",") {
		t.Fatalf("ops after OR-REPLACE with recursive_triggers ON = %v; want %v — the D must fire, or the "+
			"remedy the WARN names does not work", ops, want)
	}
}
