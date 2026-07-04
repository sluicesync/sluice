// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package sqlite

import (
	"context"
	"database/sql"
	"fmt"
	"log/slog"
	"strings"

	"sluicesync.dev/sluice/internal/ir"
)

// This file implements Strategy A: lossless local staging of a live Cloudflare
// D1 database into a local SQLite file. A live `--source-driver d1` source must
// run every read over D1's HTTP query API, which has two limits that block
// `--infer-types` on real data: a per-query CPU ceiling (HTTP 429 code 7429 on a
// multi-GB full-table scan) and a LIKE/GLOB pattern-complexity limit (HTTP 400
// code 7500 — the char-class conformance GLOBs are rejected outright). Staging
// the database to a local file once, then running the whole migrate against that
// file via the existing `sqlite` engine, sidesteps BOTH: the local SQLite has no
// CPU/pattern limits, so validation, ad-hoc counts, and the bulk read all run at
// full speed.
//
// The copy is BYTE-FAITHFUL, not a translating migrate: it recreates each table
// from D1's verbatim `sqlite_master` DDL and copies every cell at its EXACT
// storage class (integer/real/text/blob/null) via the same CAST/typeof lossless
// projection the D1 row reader uses (so integers > 2^53 survive exactly — unlike
// `wrangler d1 export`, which rounds them through a JS double). Because the
// staged file carries the ORIGINAL conservative SQLite types, `--infer-types`
// sees exactly what it would have seen on D1 and makes the identical decisions.

// StageD1ToLocalFile replicates the live D1 database named by d1DSN into a local
// SQLite file at destPath (which must not already exist — modernc creates it).
// The result is a drop-in `--source-driver sqlite` source. Foreign keys are off
// on the staging connection (writePragmas), so table-creation/insert order is
// irrelevant and a cyclic-FK schema stages cleanly.
func StageD1ToLocalFile(ctx context.Context, d1DSN, destPath string, log *slog.Logger) error {
	client, err := openD1Client(d1DSN)
	if err != nil {
		return err
	}
	if err := client.ping(ctx); err != nil {
		return err
	}
	return stageD1ClientToLocalFile(ctx, client, destPath, log)
}

// stageD1ClientToLocalFile is the staging core, taking an already-opened
// [d1Client] so tests can inject a mock-backed client (the httptest D1 server)
// without env credentials.
func stageD1ClientToLocalFile(ctx context.Context, client *d1Client, destPath string, log *slog.Logger) error {
	if log == nil {
		log = slog.Default()
	}
	sr := &D1SchemaReader{client: client}
	schema, err := sr.ReadSchema(ctx)
	if err != nil {
		return fmt.Errorf("d1 stage: read schema: %w", err)
	}

	db, _, err := openWritable(ctx, destPath)
	if err != nil {
		return fmt.Errorf("d1 stage: open local file %q: %w", destPath, err)
	}
	defer func() { _ = db.Close() }()

	// 1. Recreate every table from its VERBATIM CREATE TABLE DDL — preserving the
	// exact declared types, PK, UNIQUE/CHECK constraints, DEFAULTs, generated
	// columns, and WITHOUT ROWID-ness. This is what makes the staged file
	// indistinguishable (to the sqlite reader + --infer-types) from D1 itself.
	for _, t := range schema.Tables {
		ddl, err := sr.objectSQL(ctx, "table", t.Name)
		if err != nil {
			return fmt.Errorf("d1 stage: read DDL for table %q: %w", t.Name, err)
		}
		if strings.TrimSpace(ddl) == "" {
			return fmt.Errorf("d1 stage: no CREATE TABLE sql for %q (cannot stage faithfully)", t.Name)
		}
		if _, err := db.ExecContext(ctx, ddl); err != nil {
			return fmt.Errorf("d1 stage: create table %q: %w", t.Name, err)
		}
	}

	// 2. Copy each table's rows at exact storage class.
	var totalRows int64
	for _, t := range schema.Tables {
		n, err := stageD1Table(ctx, &D1RowReader{client: client}, db, t, log)
		if err != nil {
			return err
		}
		totalRows += n
	}

	// 3. Recreate explicit indexes AFTER the bulk load (faster, and the load can't
	// violate a deferred UNIQUE). Auto-indexes from inline UNIQUE/PK constraints
	// have NULL sqlite_master.sql and are already present from the table DDL.
	for _, t := range schema.Tables {
		for _, idx := range t.Indexes {
			ddl, err := sr.objectSQL(ctx, "index", idx.Name)
			if err != nil {
				return fmt.Errorf("d1 stage: read DDL for index %q: %w", idx.Name, err)
			}
			if strings.TrimSpace(ddl) == "" {
				continue // auto-index recreated by the table DDL
			}
			if _, err := db.ExecContext(ctx, ddl); err != nil {
				return fmt.Errorf("d1 stage: create index %q: %w", idx.Name, err)
			}
		}
	}

	log.InfoContext(ctx, "d1 stage: complete",
		slog.String("dest", destPath),
		slog.Int("tables", len(schema.Tables)),
		slog.Int64("rows", totalRows))
	return nil
}

// stageInsertBatch bounds how many rows are inserted per transaction during
// staging — large enough to amortise commit overhead, small enough to bound the
// transaction's memory/WAL footprint. Decoupled from the D1 read page size.
const stageInsertBatch = 1000

// stageD1Table copies one table's rows from D1 into the local db at exact storage
// class. It reuses the D1 row reader's pagination plan + lossless projection AND
// its single-page prefetch fetcher (ADR-0151): [fetchPages] issues page N+1's
// HTTP request while page N's rows are inserted locally, hiding one HTTP RTT per
// page exactly as the bulk-read stream loop does — same requests, same bounds,
// same explicit `final` marker so an aborted fetch can never read as a clean
// short result. Instead of decoding each cell to an IR value (which the bulk
// migrate would do later, from the staged file) it binds the RAW storage-class
// value, so the staged file holds the same integer/real/text/blob/null SQLite
// would have read from D1. Generated columns are skipped (recomputed locally
// from the DDL).
func stageD1Table(ctx context.Context, rr *D1RowReader, db *sql.DB, t *ir.Table, log *slog.Logger) (int64, error) {
	plan, err := rr.planPagination(ctx, t)
	if err != nil {
		return 0, fmt.Errorf("d1 stage: plan pagination for %q: %w", t.Name, err)
	}
	insertSQL, stored := buildStageInsert(t)
	if len(stored) == 0 {
		return 0, fmt.Errorf("d1 stage: table %q has no storable (non-generated) columns", t.Name)
	}

	// Cancelling fetchCtx on any early return (insert failure, key refusal)
	// aborts the fetcher's in-flight HTTP request and its blocked handoff, so
	// the goroutine is always reaped — the same shape as the reader's stream
	// loop.
	fetchCtx, cancelFetch := context.WithCancel(ctx)
	defer cancelFetch()
	pages := make(chan d1Page) // unbuffered: exactly one page of read-ahead
	go rr.fetchPages(fetchCtx, t, plan, pages)

	var (
		ordinal  int64
		total    int64
		sawFinal bool
	)
	for page := range pages {
		if page.err != nil {
			return total, fmt.Errorf("d1 stage: table %q: read page: %w", t.Name, page.err)
		}
		if len(page.rows) > 0 {
			if err := stageInsertPage(ctx, db, t, plan, rr, insertSQL, stored, page.rows, &ordinal); err != nil {
				return total, err
			}
			total += int64(len(page.rows))
		}
		sawFinal = page.final
	}
	if !sawFinal {
		// The channel closed without a terminal page: the fetcher was aborted
		// (ctx cancellation) mid-table. Refuse loudly — a clean return here
		// would leave a SILENTLY TRUNCATED staged file.
		if err := ctx.Err(); err != nil {
			return total, fmt.Errorf("d1 stage: table %q: %w", t.Name, err)
		}
		return total, fmt.Errorf("d1 stage: table %q: page fetch aborted before the final page", t.Name)
	}

	log.InfoContext(ctx, "d1 stage: table copied",
		slog.String("table", t.Name), slog.Int64("rows", total))
	return total, nil
}

// stageInsertPage inserts one page of D1 rows into the local db, committing in
// batches of [stageInsertBatch]. It binds each cell's exact storage-class value
// (via [d1StorageValue]) and advances the 1-based ordinal exactly as the row
// reader's stream loop does.
func stageInsertPage(
	ctx context.Context, db *sql.DB, t *ir.Table, plan pagePlan, rr *D1RowReader,
	insertSQL string, stored []int, rows []d1Row, ordinal *int64,
) error {
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("d1 stage: begin tx for %q: %w", t.Name, err)
	}
	committed := false
	defer func() {
		if !committed {
			_ = tx.Rollback()
		}
	}()
	stmt, err := tx.PrepareContext(ctx, insertSQL)
	if err != nil {
		return fmt.Errorf("d1 stage: prepare insert for %q: %w", t.Name, err)
	}
	defer func() { _ = stmt.Close() }()

	sinceCommit := 0
	for _, raw := range rows {
		*ordinal++
		vals := make([]any, 0, len(stored))
		for _, i := range stored {
			col := t.Columns[i]
			typeofText, ok, jerr := jsonString(raw[plan.typeofAliases[i]])
			if jerr != nil {
				return fmt.Errorf("d1 stage: table %q column %q row %d: decode typeof: %w",
					t.Name, col.Name, *ordinal, jerr)
			}
			if !ok {
				typeofText = "null"
			}
			sv, serr := d1StorageValue(typeofText, raw[col.Name])
			if serr != nil {
				return fmt.Errorf("d1 stage: table %q column %q row %d: %w",
					t.Name, col.Name, *ordinal, serr)
			}
			vals = append(vals, sv)
		}
		if _, err := stmt.ExecContext(ctx, vals...); err != nil {
			return fmt.Errorf("d1 stage: insert into %q row %d: %w", t.Name, *ordinal, err)
		}

		// The fetcher derives the next page's bound itself; this per-row
		// re-derivation exists to REPRODUCE its key-extraction refusal loudly
		// with full row context (the fetcher stops silently on that failure —
		// see [fetchPages]), mirroring the reader's decodeRow.
		if _, kerr := rr.extractKey(t, plan, raw, *ordinal); kerr != nil {
			return kerr
		}

		sinceCommit++
		if sinceCommit >= stageInsertBatch {
			if err := tx.Commit(); err != nil {
				return fmt.Errorf("d1 stage: commit %q: %w", t.Name, err)
			}
			committed = true
			// Start a fresh tx + stmt for the remainder of the page.
			if tx, err = db.BeginTx(ctx, nil); err != nil {
				return fmt.Errorf("d1 stage: begin tx for %q: %w", t.Name, err)
			}
			committed = false
			if stmt, err = tx.PrepareContext(ctx, insertSQL); err != nil {
				return fmt.Errorf("d1 stage: prepare insert for %q: %w", t.Name, err)
			}
			sinceCommit = 0
		}
	}

	if err := tx.Commit(); err != nil {
		return fmt.Errorf("d1 stage: commit %q: %w", t.Name, err)
	}
	committed = true
	return nil
}

// buildStageInsert builds the parameterised INSERT for a staged table and the
// indices (into t.Columns) of the columns it binds — every column EXCEPT
// generated ones, which SQLite computes from the recreated DDL and rejects an
// explicit value for.
func buildStageInsert(t *ir.Table) (query string, stored []int) {
	cols := make([]string, 0, len(t.Columns))
	for i, c := range t.Columns {
		if c.IsGenerated() {
			continue
		}
		stored = append(stored, i)
		cols = append(cols, quoteIdent(c.Name))
	}
	if len(cols) == 0 {
		return "", nil
	}
	ph := strings.TrimSuffix(strings.Repeat("?, ", len(cols)), ", ")
	return "INSERT INTO " + quoteIdent(t.Name) + " (" + strings.Join(cols, ", ") + ") VALUES (" + ph + ")", stored
}
