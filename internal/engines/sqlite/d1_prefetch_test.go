// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

// Pins for the D1 reader's single-page prefetch (ADR-0151): the fetcher
// goroutine issues page N+1's HTTP request while page N's rows stream, and
// EVERYTHING ELSE is byte-identical to the sequential loop — row order, the
// keyset bounds (including the > 2^53 string-bound discipline), the loud
// in-sequence surfacing of a failed page, and prompt reaping on cancellation
// (no goroutine leak, no silently truncated read).

package sqlite

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"sluicesync.dev/sluice/internal/ir"
)

// intPKTable builds a keyset-safe single-int-PK table for the prefetch pins.
func intPKTable(name string) *ir.Table {
	return &ir.Table{
		Name: name,
		Columns: []*ir.Column{
			{Name: "id", Type: ir.Integer{Width: 64}},
			{Name: "label", Type: ir.Text{Size: ir.TextLong}},
		},
		PrimaryKey: &ir.Index{Columns: []ir.IndexColumn{{Column: "id"}}, Unique: true},
	}
}

// idRows builds n data rows with sequential integer ids starting at first.
func idRows(table *ir.Table, first, n int) []map[string]any {
	rows := make([]map[string]any, 0, n)
	for i := 0; i < n; i++ {
		rows = append(rows, dataRow(table, map[string]cell{
			"id":    ival(fmt.Sprintf("%d", first+i)),
			"label": tval(fmt.Sprintf("v%d", first+i)),
		}))
	}
	return rows
}

// TestD1RowReader_PrefetchOverlapsStream is the load-bearing perf pin: the
// page-2 request must be ISSUED while page 1's rows are still streaming.
// Page 1 is one row larger than the output-channel buffer, so a sequential
// fetch-then-stream loop could never reach the page-2 request without the
// consumer draining (the decode loop blocks on the full channel) — the mock
// observing the page-2 request while the test has consumed NOTHING proves the
// fetcher runs ahead. Then the drain proves stitching is intact.
func TestD1RowReader_PrefetchOverlapsStream(t *testing.T) {
	table := intPKTable("items")
	pageSize := d1RowChanBuffer + 1
	page1 := idRows(table, 1, pageSize) // full page → a page 2 exists
	page2 := idRows(table, pageSize+1, 1)

	page2Requested := make(chan struct{})
	var once sync.Once
	client := startMockD1(t, func(sql string, _ []string) (int, []byte) {
		if !strings.Contains(sql, "WHERE") {
			return http.StatusOK, d1OK(page1)
		}
		once.Do(func() { close(page2Requested) })
		return http.StatusOK, d1OK(page2)
	})
	r := &D1RowReader{client: client, pageSize: pageSize}

	ch, err := r.ReadRows(context.Background(), table)
	if err != nil {
		t.Fatalf("ReadRows: %v", err)
	}

	// Consume NOTHING yet: only a prefetching fetcher can issue the page-2
	// request now (the decode loop is blocked on the full output buffer).
	select {
	case <-page2Requested:
	case <-time.After(10 * time.Second):
		t.Fatal("page-2 request was not issued while page 1 streamed — prefetch is not overlapping the HTTP round-trip")
	}

	got := drain(ch)
	if err := r.Err(); err != nil {
		t.Fatalf("Err after drain: %v", err)
	}
	if len(got) != pageSize+1 {
		t.Fatalf("got %d rows; want %d", len(got), pageSize+1)
	}
	for i, row := range got {
		if want := fmt.Sprintf("v%d", i+1); row["label"] != want {
			t.Fatalf("row %d label = %#v; want %q (order/dup/gap under prefetch)", i, row["label"], want)
		}
	}
}

// TestD1RowReader_PrefetchRequestBoundsExact pins that prefetch changes
// NOTHING about the requests: same count, same order, page N+1 bounded by
// page N's last PK as an EXACT string — including a > 2^53 key that a JSON
// number would round (the ADR-0132 §6 discipline).
func TestD1RowReader_PrefetchRequestBoundsExact(t *testing.T) {
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
	r := &D1RowReader{client: client, pageSize: 2}

	ch, err := r.ReadRows(context.Background(), table)
	if err != nil {
		t.Fatalf("ReadRows: %v", err)
	}
	got := drain(ch)
	if err := r.Err(); err != nil {
		t.Fatalf("Err: %v", err)
	}
	if len(got) != 5 {
		t.Fatalf("got %d rows; want 5", len(got))
	}
	for i, row := range got {
		if want := base + int64(i); row["id"] != want {
			t.Fatalf("row %d id = %#v; want %d (order/dup/gap under prefetch)", i, row["id"], want)
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

// TestD1RowReader_PrefetchErrorSurfacesInSequence pins the loud-failure
// contract under prefetch: a failed page 2 (fetched while page 1 streams)
// surfaces exactly as it would have sequentially — AFTER page 1's rows are
// delivered, via Err(), naming the D1 error.
func TestD1RowReader_PrefetchErrorSurfacesInSequence(t *testing.T) {
	table := intPKTable("items")
	page1 := idRows(table, 1, 2) // full page → page 2 requested
	client := startMockD1(t, func(sql string, _ []string) (int, []byte) {
		if !strings.Contains(sql, "WHERE") {
			return http.StatusOK, d1OK(page1)
		}
		return http.StatusOK, d1Err(7500, "simulated D1 failure on page 2")
	})
	r := &D1RowReader{client: client, pageSize: 2}

	ch, err := r.ReadRows(context.Background(), table)
	if err != nil {
		t.Fatalf("ReadRows: %v", err)
	}
	got := drain(ch)
	if len(got) != 2 {
		t.Fatalf("got %d rows; want page 1's 2 rows delivered before the page-2 failure", len(got))
	}
	if err := r.Err(); err == nil {
		t.Fatal("page-2 failure must surface via Err() — a nil error here is a silently truncated read")
	} else if !strings.Contains(err.Error(), "read page") || !strings.Contains(err.Error(), "simulated D1 failure on page 2") {
		t.Errorf("Err = %q; must name the page read and the D1 error text", err)
	}
}

// TestD1RowReader_PrefetchCancelReapsFetcher pins prompt, loud cancellation:
// cancelling mid-stream (with the fetcher's page-2 request in flight) must
// abort that HTTP request, close the row channel, and report the
// cancellation via Err() — NEVER a clean (silently truncated) read. The
// blocked mock handler observes the request abort, which is the fetcher-reap
// signal (its in-flight request context is cancelled by the reader).
func TestD1RowReader_PrefetchCancelReapsFetcher(t *testing.T) {
	table := intPKTable("items")
	page1 := idRows(table, 1, 2)

	// A custom mock (not startMockD1) so the page-2 handler can observe its
	// OWN request context: it blocks until the client tears the request down,
	// which happens only when the reader's cancellation propagates into the
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
	ctx, cancel := context.WithCancel(context.Background())

	r := &D1RowReader{client: client, pageSize: 2}
	ch, err := r.ReadRows(ctx, table)
	if err != nil {
		t.Fatalf("ReadRows: %v", err)
	}

	// Take page 1's rows, wait for the prefetched page-2 request to be IN
	// FLIGHT (blocked in the handler), then cancel.
	for i := 0; i < 2; i++ {
		select {
		case <-ch:
		case <-time.After(10 * time.Second):
			t.Fatal("page-1 rows not delivered")
		}
	}
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

	// The channel must close promptly (no goroutine wedged on the handoff),
	// and the read must be reported CANCELLED, never as a clean short table.
	deadline := time.After(10 * time.Second)
	for {
		select {
		case _, ok := <-ch:
			if !ok {
				if err := r.Err(); !errors.Is(err, context.Canceled) {
					t.Fatalf("Err after cancel = %v; want context.Canceled (a nil error here is a silently truncated read)", err)
				}
				return
			}
		case <-deadline:
			t.Fatal("row channel did not close after cancellation — fetcher/decoder leaked")
		}
	}
}
