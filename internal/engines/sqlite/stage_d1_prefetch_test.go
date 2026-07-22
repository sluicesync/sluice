// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

// Pins for the stage-local materializer's single-page prefetch (ADR-0151
// addendum): stageD1Table drives the SAME [fetchPages] fetcher the bulk-read
// stream loop uses, so page N+1's HTTP request goes out while page N's rows
// insert locally — and EVERYTHING ELSE is byte-identical to the sequential
// staging loop: request order and keyset bounds (the > 2^53 string-bound
// discipline included), the loud in-sequence surfacing of a failed page, and
// prompt fetcher reaping on cancellation (never a silently truncated staged
// file). Mirrors d1_prefetch_test.go at the staging seam.

package sqlite

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"
)

// openStageDest opens a writable staging destination file and applies ddl (the
// table the staging loop under test inserts into).
func openStageDest(t *testing.T, ddl string) *sql.DB {
	t.Helper()
	dest := filepath.Join(t.TempDir(), "stage.db")
	db, _, err := openWritable(context.Background(), dest)
	if err != nil {
		t.Fatalf("openWritable: %v", err)
	}
	t.Cleanup(func() { _ = db.Close() })
	if _, err := db.ExecContext(context.Background(), ddl); err != nil {
		t.Fatalf("create dest table: %v", err)
	}
	return db
}

// stagedIDs reads back the staged ids in PK order (as int64 — modernc returns
// the exact stored integer, so a > 2^53 id proves the exact-text carry).
func stagedIDs(t *testing.T, db *sql.DB, table string) []int64 {
	t.Helper()
	rows, err := db.QueryContext(context.Background(),
		"SELECT id FROM "+quoteIdent(table)+" ORDER BY id")
	if err != nil {
		t.Fatalf("read staged rows: %v", err)
	}
	defer func() { _ = rows.Close() }()
	var out []int64
	for rows.Next() {
		var id int64
		if err := rows.Scan(&id); err != nil {
			t.Fatalf("scan staged id: %v", err)
		}
		out = append(out, id)
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("staged rows: %v", err)
	}
	return out
}

// TestStageD1Table_PrefetchOverlapsInsert is the load-bearing perf pin at the
// staging seam: the page-2 request must be ISSUED while page 1's rows are
// still being inserted. The test holds SQLite's write lock on the staging
// destination (BEGIN IMMEDIATE on a second connection), so page 1's insert
// BLOCKS on busy-retry — a sequential fetch-then-insert loop could never reach
// the page-2 request while the lock is held. The mock observing the page-2
// request during the blocked insert proves the fetcher runs ahead; the drain
// after release proves stitching is intact.
func TestStageD1Table_PrefetchOverlapsInsert(t *testing.T) {
	table := intPKTable("items")
	page1 := idRows(table, 1, 2) // full page → a page 2 exists
	page2 := idRows(table, 3, 1)

	page2Requested := make(chan struct{})
	var once sync.Once
	client := startMockD1(t, func(sql string, _ []string) (int, []byte) {
		if !strings.Contains(sql, "WHERE") {
			return http.StatusOK, d1OK(page1)
		}
		once.Do(func() { close(page2Requested) })
		return http.StatusOK, d1OK(page2)
	})
	db := openStageDest(t, `CREATE TABLE items (id INTEGER PRIMARY KEY, label TEXT)`)

	// Hold the destination's write lock so the staging insert blocks (the
	// writePragmas busy_timeout retries it) while the fetcher — and only a
	// prefetching fetcher — issues the page-2 request.
	ctx := context.Background()
	lock, err := db.Conn(ctx)
	if err != nil {
		t.Fatalf("lock conn: %v", err)
	}
	defer func() { _ = lock.Close() }()
	if _, err := lock.ExecContext(ctx, "BEGIN IMMEDIATE"); err != nil {
		t.Fatalf("acquire write lock: %v", err)
	}

	type result struct {
		total int64
		err   error
	}
	done := make(chan result, 1)
	go func() {
		rr := &D1RowReader{client: client, pageSize: 2}
		total, err := stageD1Table(ctx, rr, db, table, slog.Default())
		done <- result{total, err}
	}()

	select {
	case <-page2Requested:
	case <-time.After(10 * time.Second):
		t.Fatal("page-2 request was not issued while page 1's insert was blocked — staging prefetch is not overlapping the HTTP round-trip")
	}
	if _, err := lock.ExecContext(ctx, "COMMIT"); err != nil {
		t.Fatalf("release write lock: %v", err)
	}

	res := <-done
	if res.err != nil {
		t.Fatalf("stageD1Table: %v", res.err)
	}
	if res.total != 3 {
		t.Fatalf("total = %d; want 3", res.total)
	}
	if ids := stagedIDs(t, db, "items"); len(ids) != 3 || ids[0] != 1 || ids[1] != 2 || ids[2] != 3 {
		t.Fatalf("staged ids = %v; want [1 2 3] (order/dup/gap under prefetch)", ids)
	}
}

// TestStageD1Table_PrefetchRequestBoundsExact pins that the staging leg's
// requests are byte-identical to the pre-prefetch sequential loop's (and to the
// bulk-read path's): same count, same order, page N+1 bounded by page N's last
// PK as an EXACT string — including a > 2^53 key that a JSON number would round
// (the ADR-0132 §6 discipline) — and that the staged rows carry those exact ids.
func TestStageD1Table_PrefetchRequestBoundsExact(t *testing.T) {
	table := intPKTable("snowflakes")
	const base = int64(9007199254740993) // 2^53+1 — the rounding tripwire
	mkPage := func(first, n int) []map[string]any {
		rows := make([]map[string]any, 0, n)
		for i := 0; i < n; i++ {
			rows = append(rows, dataRow(table, map[string]cell{
				"id":    ival(fmt.Sprintf("%d", base+int64(first+i))),
				"label": tval(fmt.Sprintf("v%d", first+i)),
			}))
		}
		return rows
	}
	pages := [][]map[string]any{mkPage(0, 2), mkPage(2, 2), mkPage(4, 1)} // 2+2+1, last short → final

	var (
		mu   sync.Mutex
		sqls []string
		prms [][]string
	)
	client := startMockD1(t, func(sql string, params []string) (int, []byte) {
		mu.Lock()
		n := len(sqls)
		sqls = append(sqls, sql)
		prms = append(prms, params)
		mu.Unlock()
		if n >= len(pages) {
			return http.StatusOK, d1OK(nil)
		}
		return http.StatusOK, d1OK(pages[n])
	})
	db := openStageDest(t, `CREATE TABLE snowflakes (id INTEGER PRIMARY KEY, label TEXT)`)

	rr := &D1RowReader{client: client, pageSize: 2}
	total, err := stageD1Table(context.Background(), rr, db, table, slog.Default())
	if err != nil {
		t.Fatalf("stageD1Table: %v", err)
	}
	if total != 5 {
		t.Fatalf("total = %d; want 5", total)
	}
	ids := stagedIDs(t, db, "snowflakes")
	if len(ids) != 5 {
		t.Fatalf("staged %d rows; want 5", len(ids))
	}
	for i, id := range ids {
		if want := base + int64(i); id != want {
			t.Fatalf("staged id[%d] = %d; want %d (order/dup/gap or a rounded > 2^53 id)", i, id, want)
		}
	}

	mu.Lock()
	defer mu.Unlock()
	if len(sqls) != 3 {
		t.Fatalf("saw %d page requests; want exactly 3 (prefetch must not add or drop requests)", len(sqls))
	}
	if strings.Contains(sqls[0], "WHERE") || len(prms[0]) != 0 {
		t.Errorf("page-1 request must carry no keyset bound; sql=%q params=%v", sqls[0], prms[0])
	}
	wantBounds := []string{"9007199254740994", "9007199254740996"}
	for i, want := range wantBounds {
		req := i + 1
		if !strings.Contains(sqls[req], "WHERE") || len(prms[req]) != 1 || prms[req][0] != want {
			t.Errorf("page-%d bound = %v; want [%q] (the previous page's last PK, exact text)", req+1, prms[req], want)
		}
	}
}

// TestStageD1Table_PrefetchErrorSurfacesInSequence pins the loud-failure
// contract under prefetch: a failed page 2 (fetched while page 1 inserts)
// surfaces exactly as it would have sequentially — AFTER page 1's rows are
// committed, as a loud error naming the table and the D1 message. The staging
// caller then discards the partial file; the error is what prevents a
// silently-truncated staged DB.
func TestStageD1Table_PrefetchErrorSurfacesInSequence(t *testing.T) {
	table := intPKTable("items")
	page1 := idRows(table, 1, 2) // full page → page 2 requested
	client := startMockD1(t, func(sql string, _ []string) (int, []byte) {
		if !strings.Contains(sql, "WHERE") {
			return http.StatusOK, d1OK(page1)
		}
		return http.StatusOK, d1Err(7500, "simulated D1 failure on page 2")
	})
	db := openStageDest(t, `CREATE TABLE items (id INTEGER PRIMARY KEY, label TEXT)`)

	rr := &D1RowReader{client: client, pageSize: 2}
	total, err := stageD1Table(context.Background(), rr, db, table, slog.Default())
	if err == nil {
		t.Fatal("page-2 failure must surface as a loud staging error — a nil error here is a silently truncated staged file")
	}
	if !strings.Contains(err.Error(), `table "items"`) || !strings.Contains(err.Error(), "read page") ||
		!strings.Contains(err.Error(), "simulated D1 failure on page 2") {
		t.Errorf("err = %q; must name the table, the page read, and the D1 error text", err)
	}
	if total != 2 {
		t.Errorf("total = %d; want page 1's 2 rows inserted before the page-2 failure (in-sequence surfacing)", total)
	}
	if ids := stagedIDs(t, db, "items"); len(ids) != 2 {
		t.Errorf("staged %d rows; want page 1's 2 rows committed before the failure", len(ids))
	}
}

// TestStageD1Table_PrefetchCancelReapsFetcherLoud pins prompt, loud
// cancellation at the staging seam: cancelling mid-stage (with the fetcher's
// page-2 request in flight) must abort that HTTP request (the fetcher-reap
// signal) and surface via a non-nil error — NEVER a clean return over a
// truncated staged file.
func TestStageD1Table_PrefetchCancelReapsFetcherLoud(t *testing.T) {
	table := intPKTable("items")
	page1 := idRows(table, 1, 2)

	// A custom mock (not startMockD1) so the page-2 handler can observe its
	// OWN request context: it blocks until the client tears the request down,
	// which happens only when the staging cancellation propagates into the
	// fetcher's in-flight HTTP request — the deterministic reap signal.
	page2InFlight := make(chan struct{})
	requestAborted := make(chan struct{})
	var inFlightOnce, abortOnce sync.Once
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		var body d1RequestBody
		if err := json.NewDecoder(req.Body).Decode(&body); err != nil {
			w.WriteHeader(http.StatusBadRequest)
			return
		}
		if !strings.Contains(body.SQL, "WHERE") {
			_, _ = w.Write(d1OK(page1))
			return
		}
		inFlightOnce.Do(func() { close(page2InFlight) })
		<-req.Context().Done() // page 2: never answered — only aborted
		abortOnce.Do(func() { close(requestAborted) })
	}))
	t.Cleanup(srv.Close)
	client := &d1Client{
		httpClient:   srv.Client(),
		endpointBase: srv.URL,
		accountID:    "acct",
		databaseID:   "db",
		token:        "tok",
	}
	db := openStageDest(t, `CREATE TABLE items (id INTEGER PRIMARY KEY, label TEXT)`)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	type result struct {
		total int64
		err   error
	}
	done := make(chan result, 1)
	go func() {
		rr := &D1RowReader{client: client, pageSize: 2}
		total, err := stageD1Table(ctx, rr, db, table, slog.Default())
		done <- result{total, err}
	}()

	select {
	case <-page2InFlight:
	case <-time.After(10 * time.Second):
		t.Fatal("prefetched page-2 request never reached the server")
	}
	cancel()

	select {
	case <-requestAborted:
	case <-time.After(10 * time.Second):
		t.Fatal("cancellation did not abort the fetcher's in-flight page-2 request — the fetcher goroutine leaked")
	}
	select {
	case res := <-done:
		if res.err == nil {
			t.Fatal("stageD1Table returned nil after cancellation — a silently truncated staged file")
		}
		// context.Canceled is now GUARANTEED in the chain: the insert
		// site wraps ctx.Err() whenever the context is done, so a
		// cancellation-race `sql: statement is closed` from the driver is
		// carried as detail rather than replacing the canonical error.
		// Before that fix this assertion flaked (v0.99.287 tag CI).
		if !errors.Is(res.err, context.Canceled) {
			t.Errorf("err = %v; want context.Canceled in the chain", res.err)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("stageD1Table did not return after cancellation — consumer wedged on the handoff")
	}
}
