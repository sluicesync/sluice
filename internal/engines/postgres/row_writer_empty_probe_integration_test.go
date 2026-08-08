//go:build integration

// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

// Audit backlog C-1 against a real PG: [RowWriter.IsTableEmpty] gates the
// populated-target refusal, so the "that relation isn't there" answer must come
// from the server's SQLSTATE and not from the words in its message.
//
// The unit table in missing_table_test.go pins the classifier against
// synthesised errors. This pins it against the errors a real server sends —
// including the control that matters: a query whose failure ALSO reads
// "does not exist" but means something else entirely.

package postgres

import (
	"context"
	"strings"
	"testing"
	"time"

	"sluicesync.dev/sluice/internal/ir"
)

func TestIsTableEmpty_RealServerErrors(t *testing.T) {
	dsn, cleanup := startPostgres(t)
	defer cleanup()

	applyDDL(t, dsn, `
		CREATE TABLE present_empty (id BIGINT PRIMARY KEY);
		CREATE TABLE present_full  (id BIGINT PRIMARY KEY);
		INSERT INTO present_full (id) VALUES (1);
	`)

	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	rw, err := Engine{}.OpenRowWriter(ctx, dsn)
	if err != nil {
		t.Fatalf("open row writer: %v", err)
	}
	defer func() { _ = rw.(*RowWriter).Close() }()
	w := rw.(*RowWriter)

	t.Run("absent relation reads as empty", func(t *testing.T) {
		empty, err := w.IsTableEmpty(ctx, &ir.Table{Name: "no_such_table_here"})
		if err != nil || !empty {
			t.Fatalf("IsTableEmpty(absent) = (%v, %v); want (true, nil)", empty, err)
		}
	})

	t.Run("present but empty", func(t *testing.T) {
		empty, err := w.IsTableEmpty(ctx, &ir.Table{Name: "present_empty"})
		if err != nil || !empty {
			t.Fatalf("IsTableEmpty(present_empty) = (%v, %v); want (true, nil)", empty, err)
		}
	})

	t.Run("present with rows — the refusal's true case", func(t *testing.T) {
		empty, err := w.IsTableEmpty(ctx, &ir.Table{Name: "present_full"})
		if err != nil || empty {
			t.Fatalf("IsTableEmpty(present_full) = (%v, %v); want (false, nil)", empty, err)
		}
	})

	// The control the fix exists for, driven THROUGH IsTableEmpty rather than
	// through the classifier — a mutation run showed that pinning only the
	// classifier leaves the probe's own branch ungated.
	//
	// A pooled *sql.DB re-dials, so a target database that goes away surfaces as
	// `FATAL: database "…" does not exist` (SQLSTATE 3D000) from an ordinary
	// query. The substring check read that as "the table is absent" and reported
	// the target EMPTY, which is the answer that lets a run past the
	// populated-target refusal. It must be an ERROR.
	t.Run("a vanished database must not read as an empty table", func(t *testing.T) {
		host, port, user, password := sharedPrimitives()
		const scratch = "c1_vanishing_db"
		adminDSN := sharedPGDSN(host, port, user, password, "postgres")
		applyDDL(t, adminDSN, `DROP DATABASE IF EXISTS `+scratch+` WITH (FORCE)`)
		applyDDL(t, adminDSN, `CREATE DATABASE `+scratch)
		t.Cleanup(func() { applyDDL(t, adminDSN, `DROP DATABASE IF EXISTS `+scratch+` WITH (FORCE)`) })

		scratchDSN := sharedPGDSN(host, port, user, password, scratch)
		applyDDL(t, scratchDSN, `CREATE TABLE doomed (id BIGINT PRIMARY KEY)`)

		vw, err := Engine{}.OpenRowWriter(ctx, scratchDSN)
		if err != nil {
			t.Fatalf("open row writer on the scratch database: %v", err)
		}
		defer func() { _ = vw.(*RowWriter).Close() }()
		// Keep no idle connection, so the post-drop probe has to DIAL rather
		// than reuse a socket the DROP would simply reset. Without this the
		// error is a TCP reset and the test stops exercising the text/code
		// disagreement it exists for — which is exactly what the first
		// mutation run reported.
		vw.(*RowWriter).db.SetMaxIdleConns(0)

		// Prove the writer works before the database goes away, so a later error
		// cannot be blamed on the writer never having been usable.
		if _, err := vw.(*RowWriter).IsTableEmpty(ctx, &ir.Table{Name: "doomed"}); err != nil {
			t.Fatalf("baseline probe on a live database: %v", err)
		}

		applyDDL(t, adminDSN, `DROP DATABASE `+scratch+` WITH (FORCE)`)

		empty, err := vw.(*RowWriter).IsTableEmpty(ctx, &ir.Table{Name: "doomed"})
		if err == nil {
			t.Fatalf("IsTableEmpty returned (empty=%v, nil) after the database vanished; "+
				"a target sluice cannot even reach must never report as EMPTY", empty)
		}
		// Anti-vacuity: the whole point is an error whose TEXT says "does not
		// exist" while its code says something other than 42P01. If the server
		// stops phrasing it that way, this subtest still passes for a weaker
		// reason, and that must be visible rather than assumed.
		if !strings.Contains(err.Error(), "does not exist") {
			t.Fatalf("premise failed — the vanished-database error no longer carries the shared "+
				"phrase, so this subtest is no longer exercising the text/code disagreement: %v", err)
		}
	})

	// PG says "does not exist" about a whole
	// family of unrelated things; only 42P01 means "that relation is absent".
	// Ground-truthed here against the same live server so the unit table's
	// SQLSTATE/text pairs are not this test's own invention.
	t.Run("the same English phrase, three different SQLSTATEs", func(t *testing.T) {
		for _, tc := range []struct {
			name, stmt, wantCode string
		}{
			{"missing relation", `SELECT 1 FROM public.no_such_table_here LIMIT 1`, "42P01"},
			{"missing schema in a qualified ref", `SELECT 1 FROM no_such_schema.t LIMIT 1`, "42P01"},
			{"missing function", `SELECT 1 FROM generate_serie(1,2) LIMIT 1`, "42883"},
			{"missing role", `SET ROLE no_such_role_here`, "22023"},
		} {
			t.Run(tc.name, func(t *testing.T) {
				_, err := w.db.ExecContext(ctx, tc.stmt)
				if err == nil {
					t.Fatal("expected an error")
				}
				if !strings.Contains(err.Error(), "does not exist") {
					t.Fatalf("premise failed — %q did not render the shared phrase: %v", tc.name, err)
				}
				got := isUndefinedTableErr(err)
				if want := tc.wantCode == "42P01"; got != want {
					t.Errorf("isUndefinedTableErr for %s (SQLSTATE %s) = %v; want %v — "+
						"the substring check answered true for all four",
						tc.name, tc.wantCode, got, want)
				}
			})
		}
	})
}
