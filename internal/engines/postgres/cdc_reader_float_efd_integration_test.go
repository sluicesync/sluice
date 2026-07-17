//go:build integration

// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

// Bug 194's CDC face. pgoutput delivers tuple data in TEXT format,
// rendered by the WALSENDER session's float4out/float8out — which honor
// that session's extra_float_digits, inherited from the
// server/database/role default (NOT from sluice). A default < 1
// (Supabase ships 0 server-wide) silently rounds every streamed
// float4/float8: ground-truthed on PG 17, logical decoding emits π as
// 3.14159265358979 at efd=0 and 3.141592653589793 at ≥ 1. The fix pins
// `SET extra_float_digits = 3` on the replication connection right
// after connect (a logical walsender accepts plain SQL SET — verified
// live), so this test reproduces the Supabase shape via ALTER DATABASE
// and asserts the DECODED ir.Row floats are bit-exact.

package postgres

import (
	"context"
	"database/sql"
	"math"
	"testing"
	"time"

	"sluicesync.dev/sluice/internal/ir"
)

func TestCDCReader_FloatExactUnderSourceEFDDefault(t *testing.T) {
	dsn, cleanup := startPostgresForCDC(t)
	defer cleanup()

	// Reproduce the Supabase shape: the DATABASE default is below the
	// shortest-exact threshold BEFORE the reader's replication session
	// connects (per-database, so the shared container's other tests are
	// untouched). current_database() resolves the per-test db name.
	applyPGSQL(t, dsn, `
		CREATE TABLE fefd (id BIGINT PRIMARY KEY, f8 float8, f4 float4);
	`)
	dbName := pgQueryString(t, dsn, "SELECT current_database()")
	applyPGSQL(t, dsn, "ALTER DATABASE "+quoteIdent(dbName)+" SET extra_float_digits = 0")

	eng := Engine{}
	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()

	rdr, err := eng.OpenCDCReader(ctx, dsn)
	if err != nil {
		t.Fatalf("OpenCDCReader: %v", err)
	}
	defer func() {
		if c, ok := rdr.(interface{ Close() error }); ok {
			_ = c.Close()
		}
	}()
	changes, err := rdr.StreamChanges(ctx, ir.Position{})
	if err != nil {
		t.Fatalf("StreamChanges: %v", err)
	}
	time.Sleep(200 * time.Millisecond)

	// π needs 17 significant digits (the legacy %.15g rendering rounds
	// it); 16777215 = 2^24-1 needs 8 (the legacy float4 %.6g rendering
	// rounds it to 16777200).
	applyPGSQL(t, dsn, `INSERT INTO fefd VALUES (1, pi(), 16777215.0);`)

	got := drainChanges(t, ctx, changes, 1, 30*time.Second)
	if len(got) != 1 {
		t.Fatalf("got %d changes; want 1", len(got))
	}
	ins, ok := got[0].(ir.Insert)
	if !ok {
		t.Fatalf("change = %T; want ir.Insert", got[0])
	}

	f8, ok := ins.Row["f8"].(float64)
	if !ok {
		t.Fatalf("f8 decoded as %T (%#v); want float64", ins.Row["f8"], ins.Row["f8"])
	}
	if f8 != math.Pi {
		t.Errorf("f8 = %.17g (bits %016x); want π %.17g (bits %016x) — the walsender session rendered under a lossy extra_float_digits default",
			f8, math.Float64bits(f8), math.Pi, math.Float64bits(math.Pi))
	}
	f4, ok := ins.Row["f4"].(float64)
	if !ok {
		t.Fatalf("f4 decoded as %T (%#v); want float64", ins.Row["f4"], ins.Row["f4"])
	}
	if want := float64(float32(16777215)); f4 != want {
		t.Errorf("f4 = %.9g; want %.9g (float4 2^24-1 needs 8 significant digits)", f4, want)
	}
}

// pgQueryString runs a single-value text query against dsn.
func pgQueryString(t *testing.T, dsn, query string) string {
	t.Helper()
	db, err := sql.Open("pgx", dsn)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer func() { _ = db.Close() }()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	var s string
	if err := db.QueryRowContext(ctx, query).Scan(&s); err != nil {
		t.Fatalf("query %q: %v", query, err)
	}
	return s
}
