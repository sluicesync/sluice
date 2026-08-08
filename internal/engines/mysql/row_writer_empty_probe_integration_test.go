//go:build integration

// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

// Audit backlog C-1 against a real mysqld: [RowWriter.IsTableEmpty] gates the
// populated-target refusal, so the "that table isn't there" answer must come
// from the server's error CODE and not from the words in its message.
//
// The unit table in missing_table_test.go pins the classifier against
// synthesised errors. This pins it against the error a real server actually
// sends — including the one a real server sends when its `lc_messages` is not
// English, which is where the substring check silently stopped working.

package mysql

import (
	"context"
	"strings"
	"testing"
	"time"

	"sluicesync.dev/sluice/internal/ir"
)

// TestIsTableEmpty_RealServerErrors covers the three answers the probe can give
// against a real server, plus the localized variant of the missing-table case.
//
// The lc_messages subtest is the one that fails on the pre-fix code: the errno
// is still 1146, the message is French, and the substring `doesn't exist` is
// simply not in it — so the probe returned an ERROR and the cold-start preflight
// refused a migration whose target table merely did not exist yet.
func TestIsTableEmpty_RealServerErrors(t *testing.T) {
	dsn, cleanup := startMySQL(t)
	defer cleanup()

	applyDDL(t, dsn, `
		CREATE TABLE present_empty (id BIGINT NOT NULL PRIMARY KEY);
		CREATE TABLE present_full  (id BIGINT NOT NULL PRIMARY KEY);
		INSERT INTO present_full (id) VALUES (1);
	`)

	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	run := func(t *testing.T, dsn, table string) (bool, error) {
		t.Helper()
		rw, err := Engine{}.OpenRowWriter(ctx, dsn)
		if err != nil {
			t.Fatalf("open row writer: %v", err)
		}
		defer func() { _ = rw.(*RowWriter).Close() }()
		return rw.(*RowWriter).IsTableEmpty(ctx, &ir.Table{Name: table})
	}

	t.Run("absent table reads as empty", func(t *testing.T) {
		empty, err := run(t, dsn, "no_such_table_here")
		if err != nil {
			t.Fatalf("IsTableEmpty on an absent table: %v", err)
		}
		if !empty {
			t.Error("an absent table must read as empty (the CREATE step makes it)")
		}
	})

	t.Run("present but empty", func(t *testing.T) {
		empty, err := run(t, dsn, "present_empty")
		if err != nil || !empty {
			t.Fatalf("IsTableEmpty(present_empty) = (%v, %v); want (true, nil)", empty, err)
		}
	})

	t.Run("present with rows — the refusal's true case", func(t *testing.T) {
		empty, err := run(t, dsn, "present_full")
		if err != nil || empty {
			t.Fatalf("IsTableEmpty(present_full) = (%v, %v); want (false, nil)", empty, err)
		}
	})

	t.Run("absent table on a non-English session", func(t *testing.T) {
		// go-sql-driver sends any unrecognised DSN parameter as a session
		// system-variable SET, so this makes the SERVER render its errors in
		// French. Errno 1146 is unchanged; only the text moves.
		frDSN := dsn + "&lc_messages=%27fr_FR%27"
		empty, err := run(t, frDSN, "no_such_table_here")
		if err != nil {
			t.Fatalf("IsTableEmpty under lc_messages=fr_FR: %v "+
				"(this is the C-1 defect: the code is still 1146, only the words changed)", err)
		}
		if !empty {
			t.Error("an absent table must read as empty regardless of the server's message language")
		}
		// Anti-vacuity: prove the session really was French, so a green result
		// above cannot come from the SET having been ignored.
		if msg := missingTableMessage(t, frDSN); !strings.Contains(msg, "n'existe pas") {
			t.Fatalf("the fr_FR session was not in effect — server said %q; "+
				"the localization half of this test proved nothing", msg)
		}
	})
}

// missingTableMessage returns the raw server message for a missing-table query
// on dsn, so a localization-dependent test can prove its premise held.
func missingTableMessage(t *testing.T, dsn string) string {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	rw, err := Engine{}.OpenRowWriter(ctx, dsn)
	if err != nil {
		t.Fatalf("open row writer: %v", err)
	}
	defer func() { _ = rw.(*RowWriter).Close() }()
	var dummy int
	err = rw.(*RowWriter).db.QueryRowContext(ctx, "SELECT 1 FROM no_such_table_here LIMIT 1").Scan(&dummy)
	if err == nil {
		t.Fatal("expected an error probing a missing table")
	}
	return err.Error()
}
