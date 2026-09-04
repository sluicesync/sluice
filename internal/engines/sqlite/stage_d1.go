// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package sqlite

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"log/slog"
	"strings"

	"sluicesync.dev/sluice/internal/ir"
	"sluicesync.dev/sluice/internal/sluicecode"
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
// inScope reports whether the migration will actually READ a table, and
// it scopes the LA-4 mangle refusal -- not what gets staged.
//
// Everything is still staged. The staged file is a faithful whole-database
// replica by design; that is what lets the sqlite reader and --infer-types
// treat it as indistinguishable from D1 itself, and filtering it here would
// change what a later phase sees.
//
// What is scoped is the REFUSAL. A mangled table the migration will never
// read cannot reach the target, so refusing over it stops a run that was
// going to be correct -- and neither --include-table nor --exclude-table
// could get past it, because staging ran before the filter was consulted at
// all. That was Bug 265, found by the v0.140.0 regression cycle against the
// refusal this release added. A nil predicate means everything is in scope.
func StageD1ToLocalFile(ctx context.Context, d1DSN, destPath string, inScope func(string) bool, log *slog.Logger) error {
	client, err := openD1Client(d1DSN)
	if err != nil {
		return err
	}
	if err := client.ping(ctx); err != nil {
		return err
	}
	return stageD1ClientToLocalFile(ctx, client, destPath, inScope, log)
}

// stageD1ClientToLocalFile is the staging core, taking an already-opened
// [d1Client] so tests can inject a mock-backed client (the httptest D1 server)
// without env credentials.
func stageD1ClientToLocalFile(ctx context.Context, client *d1Client, destPath string, inScope func(string) bool, log *slog.Logger) error {
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
		if err := execEmittedDDL(ctx, db, ddl); err != nil {
			return fmt.Errorf("d1 stage: create table %q: %w", t.Name, err)
		}
	}

	// 2. Copy each table's rows at exact storage class.
	var totalRows int64
	for _, t := range schema.Tables {
		n, err := stageD1Table(ctx, &D1RowReader{client: client}, db, t, inScope == nil || inScope(t.Name), log)
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
			if err := execEmittedDDL(ctx, db, ddl); err != nil {
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
func stageD1Table(ctx context.Context, rr *D1RowReader, db *sql.DB, t *ir.Table, inScope bool, log *slog.Logger) (int64, error) {
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
		// LA-4, the staging half. fetchPages has TWO consumers and the byte
		// bracket first reached only the reader's — while `--infer-types`
		// against D1 engages staging AUTOMATICALLY and then swaps the source
		// to the staged file, so a mangled cell would have been written here
		// as valid UTF-8 with nothing downstream able to tell.
		gotTextBytes int64
		srcTextBytes int64
		quiescent    bool
	)
	for page := range pages {
		if page.final {
			srcTextBytes, quiescent = page.srcTextBytes, page.quiescent
		}
		if page.err != nil {
			return total, fmt.Errorf("d1 stage: table %q: %w", t.Name, page.err)
		}
		if len(page.rows) > 0 {
			pageBytes, err := stageInsertPage(ctx, db, t, plan, rr, insertSQL, stored, page.rows, &ordinal)
			if err != nil {
				return total, err
			}
			gotTextBytes += pageBytes
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

	// LA-4: the same comparison the reader runs, on the same evidence. A
	// staged file is the artifact the rest of the migrate reads, so a
	// mangle that reaches it is undetectable from then on.
	//
	// Scoped to the tables the migration will actually read (Bug 265). The
	// first cut refused for ANY table, and staging copies the whole database
	// by design, so a mangled table in a schema the operator had excluded
	// failed the entire run -- with no flag that could get past it, because
	// staging happens before the filter is consulted. An out-of-scope mangle
	// cannot reach the target, so it WARNS: the operator still learns their
	// source holds bytes D1 will not return faithfully, and the run they
	// asked for still completes.
	mangled := quiescent && srcTextBytes >= 0 && gotTextBytes != srcTextBytes
	if mangled && !inScope {
		log.WarnContext(ctx, "d1 stage: table holds text D1 rewrote in transit, but it is OUT OF SCOPE for this run",
			slog.String("table", t.Name),
			slog.Int64("bytes_received", gotTextBytes),
			slog.Int64("bytes_stored", srcTextBytes),
			slog.String("note", "staged as delivered and NOT copied to the target, because your table filter excludes it; "+
				"the source is intact and hex(col) still returns the true bytes. Including this table without repairing it "+
				"would refuse the run"))
	}
	if mangled && inScope {
		return total, sluicecode.Wrap(
			sluicecode.CodeD1TextMangled,
			"read the affected columns as hex(col) and repair the values at the source",
			fmt.Errorf(
				"d1 stage: table %q: the text this read received is %d bytes where the source stores %d, on a table "+
					"whose row count and text size did not move -- D1 replaces every invalid UTF-8 byte with U+FFFD in "+
					"its query response (three bytes for one), so at least one cell was silently rewritten in transit "+
					"and staging it would bake the mangled value into the local file every later phase reads. The "+
					"source is intact: hex(col) still returns the true bytes",
				t.Name, gotTextBytes, srcTextBytes,
			),
		)
	}

	log.InfoContext(ctx, "d1 stage: table copied",
		slog.String("table", t.Name), slog.Int64("rows", total))
	return total, nil
}

// stageInsertPage inserts one page of D1 rows into the local db, committing in
// batches of [stageInsertBatch]. It binds each cell's exact storage-class value
// (via [d1StorageValue]) and advances the 1-based ordinal exactly as the row
// reader's stream loop does.
// The int64 return is the delivered byte length of every text-storage cell
// in the page (LA-4). typeof is already decoded per cell here, so this is
// the same exact-and-free accounting the reader's decodeRow does.
func stageInsertPage(
	ctx context.Context, db *sql.DB, t *ir.Table, plan pagePlan, rr *D1RowReader,
	insertSQL string, stored []int, rows []d1Row, ordinal *int64,
) (textBytes int64, retErr error) {
	// Normalise a cancellation-race error to carry context.Canceled.
	// A cancel mid-page can land on ANY of this loop's DB operations —
	// the ExecContext insert, a batch Commit, the BeginTx/Prepare that
	// starts the next batch — and database/sql surfaces the abort
	// differently per site (`sql: statement is closed`, `context
	// canceled`, a driver-specific message). Callers and the retry
	// classifier need a STABLE identity, so at the single return
	// boundary we wrap ctx.Err() whenever the context is done and the
	// error doesn't already carry it. Doing it here rather than at each
	// site means a new DB call added to the loop can't reintroduce the
	// nondeterminism (the source of the v0.99.287 tag-CI flake).
	defer func() {
		if retErr == nil {
			return
		}
		if ctxErr := ctx.Err(); ctxErr != nil && !errors.Is(retErr, ctxErr) {
			retErr = fmt.Errorf("d1 stage: table %q cancelled: %w (driver detail: %w)",
				t.Name, ctxErr, retErr)
		}
	}()

	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		return 0, fmt.Errorf("d1 stage: begin tx for %q: %w", t.Name, err)
	}
	committed := false
	defer func() {
		if !committed {
			_ = tx.Rollback()
		}
	}()
	stmt, err := tx.PrepareContext(ctx, insertSQL)
	if err != nil {
		return 0, fmt.Errorf("d1 stage: prepare insert for %q: %w", t.Name, err)
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
				return 0, fmt.Errorf("d1 stage: table %q column %q row %d: decode typeof: %w",
					t.Name, col.Name, *ordinal, jerr)
			}
			if !ok {
				typeofText = "null"
			}
			sv, serr := d1StorageValue(typeofText, raw[col.Name])
			if typeofText == "text" {
				if str, isStr := sv.(string); isStr {
					textBytes += int64(len(str))
				}
			}
			if serr != nil {
				return 0, fmt.Errorf("d1 stage: table %q column %q row %d: %w",
					t.Name, col.Name, *ordinal, serr)
			}
			vals = append(vals, sv)
		}
		if _, err := stmt.ExecContext(ctx, vals...); err != nil {
			return 0, fmt.Errorf("d1 stage: insert into %q row %d: %w", t.Name, *ordinal, err)
		}

		// The fetcher derives the next page's bound itself; this per-row
		// re-derivation exists to REPRODUCE its key-extraction refusal loudly
		// with full row context (the fetcher stops silently on that failure —
		// see [fetchPages]), mirroring the reader's decodeRow.
		if _, kerr := rr.extractKey(t, plan, raw, *ordinal); kerr != nil {
			return 0, kerr
		}

		sinceCommit++
		if sinceCommit >= stageInsertBatch {
			if err := tx.Commit(); err != nil {
				return 0, fmt.Errorf("d1 stage: commit %q: %w", t.Name, err)
			}
			committed = true
			// Start a fresh tx + stmt for the remainder of the page.
			if tx, err = db.BeginTx(ctx, nil); err != nil {
				return 0, fmt.Errorf("d1 stage: begin tx for %q: %w", t.Name, err)
			}
			committed = false
			if stmt, err = tx.PrepareContext(ctx, insertSQL); err != nil {
				return 0, fmt.Errorf("d1 stage: prepare insert for %q: %w", t.Name, err)
			}
			sinceCommit = 0
		}
	}

	if err := tx.Commit(); err != nil {
		return 0, fmt.Errorf("d1 stage: commit %q: %w", t.Name, err)
	}
	committed = true
	return textBytes, nil
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
