//go:build integration

// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

// The Postgres twin of the MySQL mid-stream-drop probe, and the ground truth
// behind two claims this package now makes (audit 2026-07-26).
//
// Claim 1 — the FIX: a mid-table source drop on a PG cold copy must leave
// RowReader.Err() RETRIABLE, so ADR-0109's engine-neutral per-table
// reconnect-and-resume can engage. It could not before: MySQL classified its
// rows-iteration error and Postgres parked it raw, so the identical transient
// aborted a PG cold copy and resumed a MySQL one. A retry technique that
// landed in one engine and silently missed its sibling — found by the shared
// setErr gate the moment it could see this package.
//
// Claim 2 — the EXCEPTION: the `postgres: scan` exit is allowlisted in that
// gate on the grounds that a drop surfaces at rows.Err() instead. That
// reasoning was first established on MySQL against a real server; carrying it
// to Postgres by analogy would be exactly the derived-not-verified move this
// project keeps getting burned by (different driver, different protocol). So
// it is observed here too, on real Postgres.
package postgres

import (
	"context"
	"database/sql"
	"errors"
	"strings"
	"testing"
	"time"

	"sluicesync.dev/sluice/internal/ir"
)

func TestRowReader_MidStreamConnectionDrop_IsClassifiedRetriable(t *testing.T) {
	dsn, cleanup := newSharedPGDB(t, "midstream_drop")
	defer cleanup()
	ctx, cancel := context.WithTimeout(context.Background(), 300*time.Second)
	defer cancel()

	// Enough rows that the read is provably still streaming when the kill
	// lands, with a wide-ish payload so the socket cannot buffer it all.
	const wantRows = 200000
	applyPGApplier(t, dsn, `
		CREATE TABLE "public"."dropprobe" (
			id      BIGINT PRIMARY KEY,
			payload TEXT NOT NULL
		);
		INSERT INTO "public"."dropprobe" (id, payload)
		SELECT g, repeat('x', 200) FROM generate_series(1, 200000) g;
	`)

	admin, err := sql.Open("pgx", dsn)
	if err != nil {
		t.Fatalf("admin open: %v", err)
	}
	defer func() { _ = admin.Close() }()

	rr, err := Engine{}.OpenRowReader(ctx, dsn)
	if err != nil {
		t.Fatalf("OpenRowReader: %v", err)
	}
	defer func() {
		if c, ok := rr.(interface{ Close() error }); ok {
			_ = c.Close()
		}
	}()

	sr, err := Engine{}.OpenSchemaReader(ctx, dsn)
	if err != nil {
		t.Fatalf("OpenSchemaReader: %v", err)
	}
	defer closeIf(sr)
	schema, err := sr.ReadSchema(ctx)
	if err != nil {
		t.Fatalf("ReadSchema: %v", err)
	}
	tbl := findTable(schema, "dropprobe")
	if tbl == nil {
		t.Fatalf("dropprobe table not found")
	}
	ch, err := rr.ReadRows(ctx, tbl)
	if err != nil {
		t.Fatalf("ReadRows: %v", err)
	}

	read := 0
	killed := false
	for range ch {
		read++
		if read == 100 && !killed {
			killStreamingBackend(t, ctx, admin)
			killed = true
		}
	}

	if !killed {
		t.Fatalf("drained all %d rows before the kill could land — the probe never exercised a mid-stream drop", read)
	}
	if read >= wantRows {
		t.Fatalf("read %d of %d rows: the kill did not interrupt the stream, so this run proves nothing", read, wantRows)
	}

	rd, ok := rr.(*RowReader)
	if !ok {
		t.Fatalf("OpenRowReader returned %T, not *RowReader", rr)
	}
	err = rd.Err()
	if err == nil {
		t.Fatalf("the backend was terminated after %d of %d rows but RowReader.Err() is nil — a truncated copy reported as SUCCESS is silent data loss", read, wantRows)
	}

	// Claim 2: which exit fired.
	switch {
	case strings.Contains(err.Error(), "rows iteration"):
		t.Logf("drop surfaced at the rows.Err() exit (classified) after %d rows: %v", read, err)
	case strings.Contains(err.Error(), "postgres: scan"):
		t.Fatalf("drop surfaced at the postgres: scan exit after %d rows, which the setErr gate allowlists on the "+
			"grounds that this cannot happen: %v\nThat exit now needs classifyApplierError too.", read, err)
	default:
		t.Logf("drop surfaced at an unexpected exit after %d rows: %v", read, err)
	}

	// Claim 1: it must be retriable.
	var re ir.RetriableError
	if !errors.As(err, &re) || !re.Retriable() {
		t.Fatalf("a mid-stream source connection drop left RowReader.Err() NON-retriable, so ADR-0109's per-table "+
			"reconnect-and-resume cannot engage and a routine blip aborts the whole PG cold copy — while the MySQL "+
			"twin recovers from the identical transient.\n  error: %v", err)
	}
}

// killStreamingBackend terminates the backend running the reader's scan,
// identified by its query text so it can never match the admin connection.
func killStreamingBackend(t *testing.T, ctx context.Context, admin *sql.DB) {
	t.Helper()
	deadline := time.Now().Add(30 * time.Second)
	for time.Now().Before(deadline) {
		var pid int64
		err := admin.QueryRowContext(ctx,
			`SELECT pid FROM pg_stat_activity
			  WHERE query LIKE '%dropprobe%'
			    AND pid <> pg_backend_pid()
			    AND state <> 'idle'
			  LIMIT 1`).Scan(&pid)
		if errors.Is(err, sql.ErrNoRows) {
			time.Sleep(100 * time.Millisecond)
			continue
		}
		if err != nil {
			t.Fatalf("find streaming backend: %v", err)
		}
		if _, err := admin.ExecContext(ctx, "SELECT pg_terminate_backend($1)", pid); err != nil {
			time.Sleep(100 * time.Millisecond)
			continue
		}
		return
	}
	t.Fatalf("never found the reader's streaming backend to terminate")
}
