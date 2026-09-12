//go:build integration

// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

// A server-side statement_timeout is a wall-clock cap on TABLE SIZE for
// any bulk copy, because a copy is ONE statement over a whole table or
// chunk. Cross it and the run dies with 57014 having written nothing,
// and the re-run hits the same wall at the same place — the copy never
// converges.
//
// PostgreSQL's own default is 0, which is why this went unnoticed:
// against vanilla PG the pins these cells assert are a no-op. It stops
// being a no-op the moment a managed platform ships a non-zero default
// or an operator sets one for interactive workloads without meaning it
// to govern a migration. Ground truth, 2026-09-12, on a live PlanetScale
// Neki cluster (which ships statement_timeout=30s BY DEFAULT): a plain
// 40-second statement was killed at exactly 30s with
//
//	ERROR: canceling statement due to statement timeout (SQLSTATE 57014)
//	CONTEXT: neki: router cell=aws_useast1b_6 …
//
// and the identical statement under SET LOCAL statement_timeout = 0 ran
// the full 40s and succeeded. The same 30s wall had already failed a
// real cold copy of a multi-GB table into that cluster.
//
// WHICH PATHS THESE CELLS REACH, stated rather than implied — the whole
// point of the shared [withCopySessionPins] helper is that the pin is
// structural instead of remembered:
//
//   - ExportRawCopy   (source COPY TO STDOUT)   — covered, cell 1.
//   - ImportRawCopy   (target COPY FROM STDIN)  — covered, cell 2.
//   - WriteRows/typed (target pgx CopyFrom)     — covered, cell 3. This
//     is the lane EVERY cross-engine copy takes, so it is the common
//     case rather than the fast path.
//
// AND THE ONE IT DOES NOT REACH, which is a real gap and not a
// rounding error: RowReader.ReadRows — the typed lane's SOURCE read —
// is also a single long-running statement and is NOT pinned. Covering
// it means holding a transaction open across the streaming goroutine's
// lifetime, which is a change to the reader's lifetime model rather
// than a line in a helper. So a copy OUT OF a server with a low
// statement_timeout still fails on the typed lane. That is not the
// shape the Neki finding has (there the timeout is on the TARGET), but
// it is the sibling, and it is filed rather than left implied.
//
// Index builds and constraint adds are DELIBERATELY exempt — see
// [withCopySessionPins]'s doc for the argument: pgx cancels a statement
// by breaking the socket, which a COPY notices immediately and a
// CREATE INDEX does not, so for those phases the server's timeout is
// the only bound there is.

package postgres

import (
	"bytes"
	"context"
	"database/sql"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgconn"

	"sluicesync.dev/sluice/internal/ir"
)

// copyTimeoutBudget is the statement_timeout the rig imposes on the
// database under test. It has to clear the ordinary catalog reads the
// reader and writer issue (single-digit milliseconds on a fixture this
// size) while staying far below the copy it is meant to kill.
const copyTimeoutBudget = 150 * time.Millisecond

// copyTimeoutRows is sized so that a COPY of the fixture takes
// comfortably longer than copyTimeoutBudget on any machine that can run
// this suite — the cells assert the measured elapsed time as their
// anti-vacuity floor, so an under-sized fixture fails loudly as
// "the copy was too fast to prove anything" rather than passing.
const copyTimeoutRows = 300_000

// TestCopyLanesSurviveAServerStatementTimeout drives all three pinned
// copy surfaces against a database whose statement_timeout would kill
// an unpinned copy, and proves for each that (a) it completed and (b)
// it took longer than the timeout it completed under.
func TestCopyLanesSurviveAServerStatementTimeout(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()

	// Two databases: the timeout-bearing one under test, and a plain one
	// to read from in cell 3 (the typed lane's read is the uncovered
	// sibling documented in the file header, so cell 3 must not depend
	// on it).
	walledDSN, cleanupWalled := newSharedPGDB(t, "sluice_stmt_timeout")
	defer cleanupWalled()
	plainDSN, cleanupPlain := newSharedPGDB(t, "sluice_stmt_timeout_src")
	defer cleanupPlain()

	const fixture = `
		CREATE TABLE src (id BIGINT PRIMARY KEY, pad TEXT NOT NULL);
		INSERT INTO src SELECT g, repeat('x', 120) FROM generate_series(1, %d) g;
		CREATE TABLE dst_raw   (id BIGINT PRIMARY KEY, pad TEXT NOT NULL);
		CREATE TABLE dst_typed (id BIGINT PRIMARY KEY, pad TEXT NOT NULL);
	`
	// Populated BEFORE the timeout is imposed — the seeding INSERT is
	// itself a long statement and is not what is under test.
	applyPGSQL(t, walledDSN, fmt.Sprintf(fixture, copyTimeoutRows))
	applyPGSQL(t, plainDSN, fmt.Sprintf(fixture, copyTimeoutRows))

	applyPGSQL(t, walledDSN, fmt.Sprintf(
		"ALTER DATABASE sluice_stmt_timeout SET statement_timeout = '%dms'",
		copyTimeoutBudget.Milliseconds(),
	))

	// ---- ANTI-VACUITY: prove the wall is real on new connections. ----
	//
	// Without this, every cell below could pass because the ALTER
	// DATABASE silently did not apply, and the suite would report that
	// the pins work when nothing had been tested. This is the check the
	// cells' own greens are worthless without.
	assertStatementTimeoutIsEnforced(ctx, t, walledDSN)

	table := readTable(ctx, t, plainDSN, "src")

	// The three cells are deliberately INDEPENDENT — each sources its own
	// bytes rather than consuming the previous cell's output. A chain
	// would mean a mutation that breaks cell 1 also fails cells 2 and 3
	// for want of data, and a red "caught" by the wrong guard is not
	// evidence about the guard you are grading.

	t.Run("ExportRawCopy/source COPY TO STDOUT", func(t *testing.T) {
		rdr, err := Engine{}.OpenRowReader(ctx, walledDSN)
		if err != nil {
			t.Fatalf("OpenRowReader: %v", err)
		}
		defer closeIfCloser(rdr)
		exp, ok := rdr.(ir.RawCopyExporter)
		if !ok {
			t.Fatalf("RowReader %T does not implement ir.RawCopyExporter", rdr)
		}
		var raw bytes.Buffer
		elapsed := timed(func() {
			if err := exp.ExportRawCopy(ctx, table, nil, ir.RawCopyText, &raw); err != nil {
				t.Fatalf("ExportRawCopy under statement_timeout=%v: %v\n"+
					"the source-side pin did not reach the COPY TO STDOUT", copyTimeoutBudget, err)
			}
		})
		assertOutranTheWall(t, "ExportRawCopy", elapsed)
	})

	t.Run("ImportRawCopy/target COPY FROM STDIN", func(t *testing.T) {
		// Exported from the PLAIN database so this cell measures the
		// IMPORT pin alone; an export from the walled one would make a
		// missing export pin fail this cell too.
		raw := exportFrom(ctx, t, plainDSN, table)

		wtr, err := Engine{}.OpenRowWriter(ctx, walledDSN)
		if err != nil {
			t.Fatalf("OpenRowWriter: %v", err)
		}
		defer closeIfCloser(wtr)
		imp, ok := wtr.(ir.RawCopyImporter)
		if !ok {
			t.Fatalf("RowWriter %T does not implement ir.RawCopyImporter", wtr)
		}
		dstRaw := *table
		dstRaw.Name = "dst_raw"
		var copied int64
		elapsed := timed(func() {
			var ierr error
			copied, ierr = imp.ImportRawCopy(ctx, &dstRaw, ir.RawCopyText, bytes.NewReader(raw))
			if ierr != nil {
				t.Fatalf("ImportRawCopy under statement_timeout=%v: %v\n"+
					"the target-side pin did not reach the COPY FROM STDIN", copyTimeoutBudget, ierr)
			}
		})
		if copied != copyTimeoutRows {
			t.Fatalf("ImportRawCopy copied %d rows, want %d", copied, copyTimeoutRows)
		}
		assertOutranTheWall(t, "ImportRawCopy", elapsed)
		assertRowCount(ctx, t, walledDSN, "dst_raw", copyTimeoutRows)
	})

	t.Run("WriteRows/typed lane pgx CopyFrom", func(t *testing.T) {
		// Reads from the PLAIN database on purpose: the typed lane's
		// SOURCE read is the uncovered sibling documented in the file
		// header, so sourcing it from the walled database would test that
		// gap instead of this pin.
		plainRdr, err := Engine{}.OpenRowReader(ctx, plainDSN)
		if err != nil {
			t.Fatalf("OpenRowReader (plain): %v", err)
		}
		defer closeIfCloser(plainRdr)
		rows, err := plainRdr.ReadRows(ctx, table)
		if err != nil {
			t.Fatalf("ReadRows (plain): %v", err)
		}

		wtr, err := Engine{}.OpenRowWriter(ctx, walledDSN)
		if err != nil {
			t.Fatalf("OpenRowWriter: %v", err)
		}
		defer closeIfCloser(wtr)
		dstTyped := *table
		dstTyped.Name = "dst_typed"
		elapsed := timed(func() {
			if err := wtr.WriteRows(ctx, &dstTyped, rows); err != nil {
				t.Fatalf("WriteRows (typed lane) under statement_timeout=%v: %v\n"+
					"the typed lane's COPY is unpinned — this is the lane every "+
					"CROSS-ENGINE copy takes", copyTimeoutBudget, err)
			}
		})
		assertOutranTheWall(t, "WriteRows (typed lane)", elapsed)
		assertRowCount(ctx, t, walledDSN, "dst_typed", copyTimeoutRows)
	})
}

// exportFrom raw-copies table out of dsn and returns the bytes. Used to
// feed the import cell from a database with no statement_timeout, so
// that cell measures the import pin and nothing else.
func exportFrom(ctx context.Context, t *testing.T, dsn string, table *ir.Table) []byte {
	t.Helper()
	rdr, err := Engine{}.OpenRowReader(ctx, dsn)
	if err != nil {
		t.Fatalf("OpenRowReader for export: %v", err)
	}
	defer closeIfCloser(rdr)
	exp, ok := rdr.(ir.RawCopyExporter)
	if !ok {
		t.Fatalf("RowReader %T does not implement ir.RawCopyExporter", rdr)
	}
	var raw bytes.Buffer
	if err := exp.ExportRawCopy(ctx, table, nil, ir.RawCopyText, &raw); err != nil {
		t.Fatalf("ExportRawCopy from the plain database: %v", err)
	}
	return raw.Bytes()
}

// assertStatementTimeoutIsEnforced is the rig's own gate: it proves a
// fresh connection to dsn both REPORTS the timeout and is actually
// killed by it. A cell that passes while this is silently absent has
// measured nothing.
func assertStatementTimeoutIsEnforced(ctx context.Context, t *testing.T, dsn string) {
	t.Helper()
	db, err := sql.Open("pgx", dsn)
	if err != nil {
		t.Fatalf("open for anti-vacuity probe: %v", err)
	}
	defer func() { _ = db.Close() }()

	var reported string
	if err := db.QueryRowContext(ctx, "SHOW statement_timeout").Scan(&reported); err != nil {
		t.Fatalf("SHOW statement_timeout: %v", err)
	}
	if reported == "0" {
		t.Fatalf("the rig's ALTER DATABASE did not take: a fresh connection reports "+
			"statement_timeout=%q. Every cell below would pass vacuously.", reported)
	}

	// And it must actually FIRE, not merely be reported.
	sleep := (copyTimeoutBudget * 4).Seconds()
	_, err = db.ExecContext(ctx, fmt.Sprintf("SELECT pg_sleep(%f)", sleep))
	var pgErr *pgconn.PgError
	if !errors.As(err, &pgErr) || pgErr.Code != "57014" {
		t.Fatalf("an unpinned %.2fs statement under statement_timeout=%v was not killed with 57014 "+
			"(got %v) — the wall these cells claim to survive is not standing, so their greens prove nothing",
			sleep, copyTimeoutBudget, err)
	}
}

// assertOutranTheWall fails when a cell finished so fast that it would
// have passed with no pin at all. The elapsed time is the cell's
// evidence; without it a green says only that the copy was short.
func assertOutranTheWall(t *testing.T, surface string, elapsed time.Duration) {
	t.Helper()
	if elapsed <= copyTimeoutBudget {
		t.Fatalf("%s completed in %v, which is INSIDE the %v statement_timeout — "+
			"this cell proves nothing about the pin. Raise copyTimeoutRows until the "+
			"copy genuinely outlasts the wall.", surface, elapsed, copyTimeoutBudget)
	}
	t.Logf("%s: %v under a %v statement_timeout", surface, elapsed, copyTimeoutBudget)
}

func timed(fn func()) time.Duration {
	start := time.Now()
	fn()
	return time.Since(start)
}

func readTable(ctx context.Context, t *testing.T, dsn, name string) *ir.Table {
	t.Helper()
	sr, err := Engine{}.OpenSchemaReader(ctx, dsn)
	if err != nil {
		t.Fatalf("OpenSchemaReader: %v", err)
	}
	defer closeIfCloser(sr)
	schema, err := sr.ReadSchema(ctx)
	if err != nil {
		t.Fatalf("ReadSchema: %v", err)
	}
	for _, tb := range schema.Tables {
		if tb.Name == name {
			return tb
		}
	}
	t.Fatalf("table %q not found in schema", name)
	return nil
}

func assertRowCount(ctx context.Context, t *testing.T, dsn, table string, want int) {
	t.Helper()
	db, err := sql.Open("pgx", dsn)
	if err != nil {
		t.Fatalf("open for row count: %v", err)
	}
	defer func() { _ = db.Close() }()
	// The count runs under the same wall the copy did, and a seq scan of
	// the fixture outlasts it — so lift the timeout on this throwaway
	// connection first, as its OWN statement: database/sql prepares, and
	// a prepared statement carries exactly one command.
	conn, err := db.Conn(ctx)
	if err != nil {
		t.Fatalf("conn for row count: %v", err)
	}
	defer func() { _ = conn.Close() }()
	if _, err := conn.ExecContext(ctx, "SET statement_timeout = 0"); err != nil {
		t.Fatalf("lift statement_timeout for row count: %v", err)
	}
	var got int
	if err := conn.QueryRowContext(
		ctx,
		fmt.Sprintf("SELECT count(*) FROM %s", table),
	).Scan(&got); err != nil {
		t.Fatalf("count %s: %v", table, err)
	}
	if got != want {
		t.Fatalf("%s holds %d rows, want %d", table, got, want)
	}
}

func closeIfCloser(v any) {
	if c, ok := v.(interface{ Close() error }); ok {
		_ = c.Close()
	}
}
