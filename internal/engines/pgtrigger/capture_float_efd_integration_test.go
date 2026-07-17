//go:build integration

// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

// Bug 194's trigger-capture face. The capture format is to_jsonb(),
// and PG converts float4/float8 into jsonb numerics THROUGH the type's
// text output function — which honors the FIRING session's
// extra_float_digits. The firing session is the APPLICATION's, which
// sluice can never pin; a server/database/role default < 1 (Supabase
// ships 0 server-wide — and the managed no-slot tier is exactly this
// engine's audience) silently rounded every captured float
// (ground-truthed on PG 17: to_jsonb(pi()) at efd=0 → 3.14159265358979).
// The fix is a per-function `SET extra_float_digits = 3` clause on the
// capture function, pinning the GUC for exactly the trigger's
// execution. This test reproduces the Supabase shape as a DATABASE
// default (which the DML session inherits), fires the trigger, and
// asserts the captured jsonb carries the full-precision values —
// jsonb→text output is numeric_out, which is efd-independent, so the
// assertion cannot be fooled by the reading session.

package pgtrigger

import (
	"context"
	"database/sql"
	"testing"
	"time"
)

func TestCaptureTrigger_FloatExactUnderSessionEFDDefault(t *testing.T) {
	src, _, cleanup := startPGTrigSrcTgtPair(t)
	defer cleanup()

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()

	db, err := sql.Open("pgx", src)
	if err != nil {
		t.Fatalf("open src: %v", err)
	}
	defer func() { _ = db.Close() }()

	exec := func(stmt string) {
		t.Helper()
		if _, err := db.ExecContext(ctx, stmt); err != nil {
			t.Fatalf("exec %q: %v", stmt, err)
		}
	}
	exec(`CREATE TABLE fefd (id BIGINT PRIMARY KEY, f8 float8, f4 float4)`)
	// The Supabase shape: every NEW session on this database — including
	// the "application" session performing the DML below — defaults to
	// the lossy rendering.
	exec(`ALTER DATABASE src_db SET extra_float_digits = 0`)

	if _, err := Setup(ctx, src, SetupOptions{
		Tables: []string{"fefd"},
		Schema: "public",
	}); err != nil {
		t.Fatalf("Setup: %v", err)
	}

	// A FRESH connection (new pool) inherits the efd=0 database default —
	// the application-session shape. π needs 17 significant digits;
	// 16777215 = 2^24-1 needs 8 (the legacy float4 %.6g rendering rounds
	// it to 16777200).
	appDB, err := sql.Open("pgx", src)
	if err != nil {
		t.Fatalf("open app session: %v", err)
	}
	defer func() { _ = appDB.Close() }()
	var sessionEFD string
	if err := appDB.QueryRowContext(ctx, "SHOW extra_float_digits").Scan(&sessionEFD); err != nil {
		t.Fatalf("show efd: %v", err)
	}
	if sessionEFD != "0" {
		t.Fatalf("test-shape precondition broken: application session extra_float_digits = %s; want 0", sessionEFD)
	}
	if _, err := appDB.ExecContext(ctx, `INSERT INTO fefd VALUES (1, pi(), 16777215.0)`); err != nil {
		t.Fatalf("insert: %v", err)
	}

	var f8, f4 string
	if err := appDB.QueryRowContext(ctx, `
		SELECT after_jsonb->>'f8', after_jsonb->>'f4'
		FROM public.sluice_change_log
		WHERE table_name = 'fefd' AND op = 'I'`).Scan(&f8, &f4); err != nil {
		t.Fatalf("read capture row: %v", err)
	}
	if f8 != "3.141592653589793" {
		t.Errorf("captured f8 = %q; want %q — the capture function rendered under the application session's lossy extra_float_digits (per-function SET missing?)", f8, "3.141592653589793")
	}
	if f4 != "16777215" {
		t.Errorf("captured f4 = %q; want %q", f4, "16777215")
	}
}
