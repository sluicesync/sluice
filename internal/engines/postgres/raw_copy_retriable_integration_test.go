//go:build integration

// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

// Roadmap item 86 — a raw-copy export interrupted by a terminated backend must
// return a RETRIABLE error, so the cold-copy source-read retry engages instead
// of aborting the table.
//
// # Why this needed a real backend
//
// The failure was never a missing retry. ADR-0109's retry is wired around this
// path (migrate_bulk.go), and the raw-copy block runs inside the very attempt
// the wrapper invokes. The wrapper decides with
// errors.As(err, &ir.RetriableError), and ExportRawCopy returned a bare error —
// so the predicate was false, the wrapper declined, and the copy died on a
// transient whose resume strategy (truncate the target, restart the table from
// a fresh reader) was already correct and already available.
//
// A unit test calling classifyApplierError on a synthetic pgconn error would
// prove the classifier works and say nothing about whether this site reaches
// it. That is the exact mistake this class is made of — four separate fixes in
// this repo have shipped inert that way — so the pin terminates a real backend
// mid-export and asserts on what ExportRawCopy actually returns.
package postgres

import (
	"context"
	"database/sql"
	"errors"
	"io"
	"testing"
	"time"

	"sluicesync.dev/sluice/internal/ir"
)

func TestExportRawCopy_TerminatedBackendIsRetriable(t *testing.T) {
	dsn, cleanup := newSharedPGDB(t, "rawcopy_retriable")
	defer cleanup()
	ctx, cancel := context.WithTimeout(context.Background(), 300*time.Second)
	defer cancel()

	// Wide enough that the export is still streaming when the kill lands.
	applyPGApplier(t, dsn, `
		CREATE TABLE "public"."rawprobe" (
			id      BIGINT PRIMARY KEY,
			payload TEXT NOT NULL
		);
		INSERT INTO "public"."rawprobe" (id, payload)
		SELECT g, repeat('x', 400) FROM generate_series(1, 300000) g;
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
	defer closeIf(rr)

	exp, ok := rr.(ir.RawCopyExporter)
	if !ok {
		t.Fatalf("postgres RowReader does not implement ir.RawCopyExporter (%T)", rr)
	}

	sr, err := Engine{}.OpenSchemaReader(ctx, dsn)
	if err != nil {
		t.Fatalf("OpenSchemaReader: %v", err)
	}
	defer closeIf(sr)
	schema, err := sr.ReadSchema(ctx)
	if err != nil {
		t.Fatalf("ReadSchema: %v", err)
	}
	tbl := findTable(schema, "rawprobe")
	if tbl == nil {
		t.Fatal("rawprobe table not found")
	}

	// Kill the exporting backend once the COPY is underway. slowWriter both
	// paces the export (so the kill lands mid-stream) and triggers the kill
	// after enough bytes have moved to prove it had started.
	killed := make(chan struct{})
	w := &slowWriter{
		t:         t,
		admin:     admin,
		threshold: 64 << 10,
		killed:    killed,
	}

	err = exp.ExportRawCopy(ctx, tbl, nil, ir.RawCopyText, w)

	select {
	case <-killed:
	default:
		t.Fatal("the backend was never terminated — the export finished first, so this run proves nothing")
	}
	if err == nil {
		t.Fatal("the exporting backend was terminated mid-COPY and ExportRawCopy returned nil. A truncated " +
			"export reported as success is silent data loss.")
	}

	var re ir.RetriableError
	if !errors.As(err, &re) || !re.Retriable() {
		t.Fatalf("ExportRawCopy returned a NON-retriable error for a terminated backend, so the cold-copy "+
			"source-read retry declines it and the whole table copy aborts — on a path whose truncate-and-"+
			"restart resume strategy is already correct and already wired (roadmap item 86).\n  error: %v", err)
	}
}

// slowWriter paces the COPY stream and terminates the exporting backend once
// enough bytes have flowed to prove the export was genuinely underway.
type slowWriter struct {
	t         *testing.T
	admin     *sql.DB
	threshold int
	n         int
	killed    chan struct{}
	done      bool
}

func (w *slowWriter) Write(p []byte) (int, error) {
	w.n += len(p)
	if !w.done && w.n >= w.threshold {
		w.done = true
		w.terminate()
		close(w.killed)
	}
	// Pace slightly so the terminate has time to land mid-stream.
	time.Sleep(time.Millisecond)
	return len(p), nil
}

func (w *slowWriter) terminate() {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	_, err := w.admin.ExecContext(ctx,
		`SELECT pg_terminate_backend(pid) FROM pg_stat_activity
		  WHERE query LIKE '%rawprobe%' AND pid <> pg_backend_pid()`)
	if err != nil {
		w.t.Errorf("terminate exporting backend: %v", err)
	}
}

var _ io.Writer = (*slowWriter)(nil)
