//go:build integration

// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

// Ground truth for roadmap item 83, and the empirical pin for the claim
// ADR-0109's whole cold-copy read-retry rests on.
//
// # The question
//
// row_reader.go's streaming loop has TWO error exits, and only one of them
// classifies:
//
//	for rows.Next() {
//	    if err := rows.Scan(...); err != nil {
//	        r.setErr(fmt.Errorf("mysql: scan: %w", err))   // UNCLASSIFIED
//	...
//	if err := rows.Err(); err != nil {
//	    r.setErr(classifyApplierError(...))               // classified
//
// An unclassified error is TERMINAL to the pipeline. So if a mid-copy source
// connection drop surfaces at the Scan exit rather than the rows.Err() exit,
// ADR-0109's per-table reconnect-and-resume can never engage and a routine
// blip aborts a multi-hour cold copy — the bulk-path sibling of the Bug 207
// class (a retriable carve-out that is unreachable from the path that
// actually raises the error).
//
// ADR-0109's header asserts the drop "surfaces here as the rows-iteration
// error". That assertion is load-bearing and was never tested against a real
// server: the existing source-timeout test only pins that the session
// timeouts get RAISED, not what a real drop does. Reading database/sql
// supports the assertion (a driver failure in Next sets lasterr, Next returns
// false, the loop exits, rows.Err() reports it) — but the Bug 207 lesson is
// precisely that a fix which is correct by reading can be inert in practice,
// so this kills a real connection mid-stream and looks.
//
// # What it pins
//
// A source connection killed while a full-scan read is streaming must leave
// RowReader.Err() carrying an ir.RetriableError. If a future driver or
// database/sql change reroutes the drop to the Scan exit, this fails — which
// is the signal to classify that exit too, rather than discovering it when a
// long migration dies.
package mysql

import (
	"context"
	"database/sql"
	"errors"
	"strconv"
	"strings"
	"testing"
	"time"

	"sluicesync.dev/sluice/internal/ir"
)

func TestRowReader_MidStreamConnectionDrop_IsClassifiedRetriable(t *testing.T) {
	dsn, cleanup := newSharedDB(t, "midstream_drop")
	defer cleanup()
	ctx, cancel := context.WithTimeout(context.Background(), 300*time.Second)
	defer cancel()

	applyDDL(t, dsn, `CREATE TABLE dropprobe (
		id      BIGINT NOT NULL AUTO_INCREMENT,
		payload VARCHAR(255) NOT NULL,
		PRIMARY KEY (id)
	) ENGINE=InnoDB;`)

	admin, err := sql.Open("mysql", dsn)
	if err != nil {
		t.Fatalf("admin open: %v", err)
	}
	defer func() { _ = admin.Close() }()

	// Enough rows that the read is provably still streaming when the kill
	// lands. Seeded by doubling so the seed is a handful of statements.
	const wantRows = 262144
	if _, err := admin.ExecContext(ctx, "INSERT INTO dropprobe (payload) VALUES (REPEAT('x', 200))"); err != nil {
		t.Fatalf("seed first row: %v", err)
	}
	for n := 1; n < wantRows; n *= 2 {
		if _, err := admin.ExecContext(ctx, "INSERT INTO dropprobe (payload) SELECT payload FROM dropprobe"); err != nil {
			t.Fatalf("seed doubling at %d: %v", n, err)
		}
	}

	rr, err := Engine{}.OpenRowReader(ctx, dsn)
	if err != nil {
		t.Fatalf("OpenRowReader: %v", err)
	}
	defer closeIf(rr)
	tbl := readTableIR(t, ctx, dsn, "dropprobe")

	ch, err := rr.ReadRows(ctx, tbl)
	if err != nil {
		t.Fatalf("ReadRows: %v", err)
	}

	// Drain slowly enough that the reader is mid-result-set — and note this
	// is the realistic shape ADR-0109 describes: a slow consumer backpressures
	// the reader, which is exactly when the source closes an idle read
	// connection.
	read := 0
	killed := false
	for range ch {
		read++
		if read == 100 && !killed {
			killStreamingReader(t, ctx, admin)
			killed = true
		}
	}

	if !killed {
		t.Fatalf("drained all %d rows before the kill could land — the probe never exercised a mid-stream drop", read)
	}
	if read >= wantRows {
		t.Fatalf("read %d of %d rows: the connection kill did not interrupt the stream, so this run proves nothing", read, wantRows)
	}

	rd, ok := rr.(*RowReader)
	if !ok {
		t.Fatalf("OpenRowReader returned %T, not *RowReader", rr)
	}
	err = rd.Err()
	if err == nil {
		t.Fatalf("connection was killed after %d of %d rows but RowReader.Err() is nil — a truncated copy reported as SUCCESS is silent data loss", read, wantRows)
	}

	// Which exit fired is the item-83 answer; log it either way so the run is
	// self-documenting.
	switch {
	case strings.Contains(err.Error(), "rows iteration"):
		t.Logf("item 83: drop surfaced at the rows.Err() exit (classified) after %d rows: %v", read, err)
	case strings.Contains(err.Error(), "mysql: scan"):
		t.Logf("item 83: drop surfaced at the rows.Scan exit (UNCLASSIFIED) after %d rows: %v", read, err)
	default:
		t.Logf("item 83: drop surfaced at an unexpected exit after %d rows: %v", read, err)
	}

	var re ir.RetriableError
	if !errors.As(err, &re) || !re.Retriable() {
		t.Fatalf("a mid-stream source connection drop left RowReader.Err() NON-retriable, so ADR-0109's per-table "+
			"reconnect-and-resume cannot engage and a routine blip aborts the whole cold copy.\n"+
			"  error: %v\n"+
			"If this error came from the `mysql: scan` exit, that exit needs classifyApplierError too "+
			"(roadmap item 83 — the bulk-path sibling of the Bug 207 class).", err)
	}
}

// killStreamingReader kills the connection running the reader's full scan,
// identified by the table in its query text so it can never match the admin
// connection or a sibling test's.
func killStreamingReader(t *testing.T, ctx context.Context, admin *sql.DB) {
	t.Helper()
	deadline := time.Now().Add(30 * time.Second)
	for time.Now().Before(deadline) {
		var id int64
		err := admin.QueryRowContext(ctx,
			`SELECT id FROM information_schema.processlist
			  WHERE info LIKE '%dropprobe%' AND id <> CONNECTION_ID()
			  LIMIT 1`).Scan(&id)
		if errors.Is(err, sql.ErrNoRows) {
			time.Sleep(100 * time.Millisecond)
			continue
		}
		if err != nil {
			t.Fatalf("find streaming connection: %v", err)
		}
		if _, err := admin.ExecContext(ctx, "KILL "+strconv.FormatInt(id, 10)); err != nil {
			// A connection that finished between the SELECT and the KILL is
			// not a probe failure; retry until the deadline.
			time.Sleep(100 * time.Millisecond)
			continue
		}
		return
	}
	t.Fatalf("never found the reader's streaming connection to kill")
}
