// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package sqlitetrigger

import (
	"context"
	"database/sql"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"testing"

	_ "modernc.org/sqlite" // pure-Go driver; keeps this in the unit gate (no Docker)

	"sluicesync.dev/sluice/internal/engines/sqlite"
)

// The change-log poll's SQL, executed for real.
//
// WHY THIS EXISTS. The adaptive page added for LA-2b halves an over-cap batch
// and re-requests it against the SAME `id > sinceID` watermark. The entire
// safety argument for that retry is one clause: because the query is
// `WHERE id > ? ORDER BY id ASC LIMIT n`, a smaller LIMIT returns a strict
// PREFIX of the same rows, so asking for fewer can never skip one.
//
// Nothing graded that clause. Both D1 mocks in this package ignore ORDER BY
// and LIMIT entirely — `mockD1.pollResultsLocked` filters on `id > since` and
// returns rows in insertion order — and the only SQL-text assertion checks
// for `CAST(id AS TEXT)`, `WHERE id > ?` and the substring "LIMIT ", never the
// ordering. So `ORDER BY id ASC` could be changed to `DESC` and the whole
// suite stayed green.
//
// That mutant is silent CDC loss, not a cosmetic slip. The pump advances its
// watermark to the MAX id of whatever page it received (`newLast` in
// CDCReader.poll), so a DESC page hands it the highest id in the log and
// every change between the old watermark and that id is skipped forever, at
// exit 0, with a healthy-looking stream.
//
// Unlike the local-file executor, which runs against real SQLite through
// database/sql, the D1 executor has no real-SQL backing anywhere. This closes
// that: the mock D1 endpoint executes the poll's actual SQL against a real
// (pure-Go) SQLite database, so ordering, the bound watermark and the
// embedded LIMIT are all evaluated by an engine rather than by a fake.
func TestD1PollChangeLogSQL_IsAPrefixQuery(t *testing.T) {
	db := seedPollLog(t, 1, 2, 3, 4, 5)
	e := &d1Executor{conn: startRealSQLMockD1(t, db)}
	ctx := context.Background()

	full, err := e.pollChangeLog(ctx, 0, 5)
	if err != nil {
		t.Fatalf("poll(since=0, batch=5): %v", err)
	}
	if len(full) != 5 {
		t.Fatalf("full poll returned %d rows, want 5 — the fixture or the query is not what this grades", len(full))
	}

	// ORDER: the pump takes its next watermark from the MAX id it saw, so a
	// page that is not ascending hands it a watermark it has not actually
	// consumed up to.
	for i, r := range full {
		if r.id != int64(i+1) {
			t.Fatalf("row %d has id %d; the poll is not returning ids in ASCENDING order, so the pump's "+
				"watermark would jump past changes it never read: %v", i, r.id, ids(full))
		}
	}

	// PREFIX: the whole idempotency argument for halving the batch.
	half, err := e.pollChangeLog(ctx, 0, 2)
	if err != nil {
		t.Fatalf("poll(since=0, batch=2): %v", err)
	}
	if len(half) != 2 {
		t.Fatalf("LIMIT is not being honoured: asked for 2, got %d (%v) — an unbounded page defeats the "+
			"whole shrink-and-retry design", len(half), ids(half))
	}
	for i := range half {
		if half[i].id != full[i].id {
			t.Fatalf("the smaller page is not a PREFIX of the larger: batch=2 gave %v, batch=5 gave %v. "+
				"Re-requesting a shrunk page would then skip rows the first attempt would have returned",
				ids(half), ids(full))
		}
	}

	// WATERMARK: `id >` is strict, so a resume from the last id consumed
	// must not replay it.
	after, err := e.pollChangeLog(ctx, 3, 5)
	if err != nil {
		t.Fatalf("poll(since=3, batch=5): %v", err)
	}
	if len(after) != 2 || after[0].id != 4 || after[1].id != 5 {
		t.Fatalf("poll(since=3) returned %v, want [4 5] — the watermark bound is not strict `id >`, so the "+
			"stream would replay or skip at every resume", ids(after))
	}
}

func ids(rows []rawChangeRow) []int64 {
	out := make([]int64, 0, len(rows))
	for _, r := range rows {
		out = append(out, r.id)
	}
	return out
}

// seedPollLog builds a real change log holding the given ids, deliberately
// INSERTED OUT OF ORDER so a query that returns rows in physical order
// rather than by the ORDER BY is distinguishable from one that sorts.
func seedPollLog(t *testing.T, want ...int64) *sql.DB {
	t.Helper()
	path := filepath.Join(t.TempDir(), "changelog.db")
	db, err := sql.Open("sqlite", path)
	if err != nil {
		t.Fatalf("open sqlite: %v", err)
	}
	t.Cleanup(func() { _ = db.Close() })

	if _, err := db.ExecContext(context.Background(),
		`CREATE TABLE "`+ChangeLogTable+`" (
			id INTEGER PRIMARY KEY,
			op TEXT NOT NULL,
			tbl TEXT NOT NULL,
			before TEXT,
			after TEXT,
			captured_at TEXT
		)`); err != nil {
		t.Fatalf("create change log: %v", err)
	}
	// Insertion order is scrambled on purpose (5, 2, 4, 3, 1 for 1..5), so a
	// query returning rows in PHYSICAL order is distinguishable from one that
	// actually sorts. The exact permutation does not matter; that it is not
	// already ascending does.
	order := make([]int64, len(want))
	copy(order, want)
	for i, j := 0, len(order)-1; i < j; i, j = i+2, j-1 {
		order[i], order[j] = order[j], order[i]
	}
	for _, id := range order {
		if _, err := db.ExecContext(context.Background(),
			`INSERT INTO "`+ChangeLogTable+`" (id, op, tbl, after, captured_at) VALUES (?, 'INSERT', 't', ?, ?)`,
			id, `{"id":{"t":"integer","v":"1"}}`, "2026-09-04T00:00:00Z"); err != nil {
			t.Fatalf("seed id %d: %v", id, err)
		}
	}
	return db
}

// startRealSQLMockD1 serves the D1 query API by EXECUTING the received SQL
// against a real SQLite database, so the poll's own ORDER BY, LIMIT and bound
// watermark are evaluated by an engine instead of by a fake that ignores them.
func startRealSQLMockD1(t *testing.T, db *sql.DB) *sqlite.D1Conn {
	t.Helper()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		raw, _ := io.ReadAll(r.Body)
		var req struct {
			SQL    string   `json:"sql"`
			Params []string `json:"params"`
		}
		_ = json.Unmarshal(raw, &req)

		args := make([]any, len(req.Params))
		for i, p := range req.Params {
			args[i] = p
		}
		rows, err := db.QueryContext(r.Context(), req.SQL, args...)
		if err != nil {
			t.Errorf("the poll SQL did not execute: %v", err)
			w.WriteHeader(http.StatusInternalServerError)
			return
		}
		defer func() { _ = rows.Close() }()
		cols, err := rows.Columns()
		if err != nil {
			t.Errorf("the poll SQL did not execute: %v", err)
			w.WriteHeader(http.StatusInternalServerError)
			return
		}
		var out []map[string]any
		for rows.Next() {
			cells := make([]any, len(cols))
			ptrs := make([]any, len(cols))
			for i := range cells {
				ptrs[i] = &cells[i]
			}
			if err := rows.Scan(ptrs...); err != nil {
				t.Errorf("scan: %v", err)
				break
			}
			row := make(map[string]any, len(cols))
			for i, c := range cols {
				switch v := cells[i].(type) {
				case nil:
					row[c] = nil
				case []byte:
					row[c] = string(v)
				default:
					row[c] = v
				}
			}
			out = append(out, row)
		}
		// Not decoration: a truncated iteration would hand the poll a
		// SHORT page, and a short page is exactly what this test reads as
		// evidence about LIMIT and ordering. Silence here would let the
		// pin pass for the wrong reason.
		if err := rows.Err(); err != nil {
			t.Errorf("iterating the poll result truncated it: %v", err)
			w.WriteHeader(http.StatusInternalServerError)
			return
		}
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write(d1OK(out))
	}))
	t.Cleanup(srv.Close)
	return sqlite.D1ConnForTest(srv.URL, "acct", "db", "tok")
}
