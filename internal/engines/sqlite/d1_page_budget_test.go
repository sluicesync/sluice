// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

// Pins for the D1 reader's byte-budgeted page controller (audit 2026-09-01
// LA-2): the page-size rule across every row-width family, the first page
// sized from the server-side width probe, later pages from the previous
// page's measured bytes, the halve-and-retry on an overflowing page, the
// named refusal of the one row the cap cannot hold (on BOTH consumers of
// fetchPages — the stream loop and the stage-local materializer), the
// unchanged 1,000-row page for narrow rows, and the client's overflow
// boundary.

package sqlite

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"testing"

	"sluicesync.dev/sluice/internal/sluicecode"
)

// TestPageRowsFor_FamilyMatrix pins the page controller's rule across the
// row-width families × two budgets (the production 4 MiB budget and a small
// one), with every expectation written out rather than re-derived from the
// formula. The families: an unknown/empty width (0), a tiny row, 1 KiB,
// 8 KiB (the width at which the old fixed page first overflowed the cap),
// 64 KiB, a row exactly the budget, and an oversize row (wider than the
// budget — requested as a page of one; the cap decides its fate).
func TestPageRowsFor_FamilyMatrix(t *testing.T) {
	const (
		prod  = int64(4 << 20) // half the production cap
		small = int64(64 << 10)
	)
	if got := pageByteBudget(d1MaxResponseBytes); got != prod {
		t.Fatalf("pageByteBudget(production cap) = %d; want %d — the matrix below assumes half the cap", got, prod)
	}
	cases := []struct {
		family string
		width  int64
		budget int64
		want   int
	}{
		{"unknown", 0, prod, 1000},
		{"tiny", 80, prod, 1000},
		{"1KiB", 1 << 10, prod, 1000},
		{"8KiB", 8 << 10, prod, 512},
		{"64KiB", 64 << 10, prod, 64},
		{"budget-exact", prod, prod, 1},
		{"oversize", prod + 1, prod, 1},
		{"unknown", 0, small, 1000},
		{"tiny", 80, small, 819},
		{"1KiB", 1 << 10, small, 64},
		{"8KiB", 8 << 10, small, 8},
		{"64KiB", 64 << 10, small, 1},
		{"budget-exact", small, small, 1},
		{"oversize", small + 1, small, 1},
	}
	for _, tc := range cases {
		t.Run(fmt.Sprintf("%s/budget=%d", tc.family, tc.budget), func(t *testing.T) {
			if got := pageRowsFor(tc.width, tc.budget, d1PageSize); got != tc.want {
				t.Fatalf("pageRowsFor(%d, %d, %d) = %d; want %d", tc.width, tc.budget, d1PageSize, got, tc.want)
			}
		})
	}
	// The maxRows ceiling is the caller's, not a constant: a test-sized
	// reader (pageSize=2) must never be handed more than 2.
	if got := pageRowsFor(80, prod, 2); got != 2 {
		t.Fatalf("pageRowsFor(80, prod, 2) = %d; want 2 (the ceiling is maxRows)", got)
	}
}

// limitOf extracts the LIMIT of a page query.
func limitOf(t *testing.T, sqlStr string) int {
	t.Helper()
	m := regexp.MustCompile(` LIMIT (\d+)$`).FindStringSubmatch(sqlStr)
	if m == nil {
		t.Fatalf("no LIMIT in %q", sqlStr)
	}
	n, _ := strconv.Atoi(m[1])
	return n
}

// isD1DataPage reports whether sql is a keyset data-page request (the
// projection's typeof alias is the tell; the probes project no user column).
func isD1DataPage(sqlStr string) bool {
	return strings.HasPrefix(sqlStr, "SELECT typeof(")
}

// TestD1RowReader_SmallRowsKeepTheFullPage pins the "nothing changed for
// ordinary tables" half of LA-2: at the production cap, a tiny row width
// from the probe yields the same 1,000-row first page as before.
func TestD1RowReader_SmallRowsKeepTheFullPage(t *testing.T) {
	table := intPKTable("items")
	var limits []int
	client := startMockD1(t, withWidth(80, withCount(0, func(sql string, _ []string) (int, []byte) {
		limits = append(limits, limitOf(t, sql))
		return http.StatusOK, d1OK(nil)
	})))
	r := &D1RowReader{client: client} // production page size AND cap
	ch, err := r.ReadRows(context.Background(), table)
	if err != nil {
		t.Fatalf("ReadRows: %v", err)
	}
	_ = drain(ch)
	if err := r.Err(); err != nil {
		t.Fatalf("Err: %v", err)
	}
	if len(limits) != 1 || limits[0] != d1PageSize {
		t.Fatalf("page LIMITs = %v; want [%d] (an 80-byte row must still get the full page)", limits, d1PageSize)
	}
}

// TestD1RowReader_FirstPageFromProbeThenMeasured pins the two sizing
// sources in order: the FIRST page's LIMIT comes from the width probe
// (budget / probed width), and the SECOND page's LIMIT from page 1's
// measured bytes per row — the number the mock itself wrote.
func TestD1RowReader_FirstPageFromProbeThenMeasured(t *testing.T) {
	table := intPKTable("items")
	const responseCap = 4096 // budget 2048
	const probedWidth = 512  // → first page = 2048/512 = 4 rows
	page1 := idRows(table, 1, 4)
	page1Body := d1OK(page1)
	wantSecond := pageRowsFor(int64(len(page1Body))/4, 2048, 1000)
	if wantSecond <= 4 || wantSecond >= 1000 {
		t.Fatalf("test shape: measured page-2 size %d must differ from the probed 4 and stay under the ceiling", wantSecond)
	}

	var (
		mu     sync.Mutex
		limits []int
	)
	client := startMockD1(t, withWidth(probedWidth, withCount(5, func(sql string, _ []string) (int, []byte) {
		mu.Lock()
		defer mu.Unlock()
		limits = append(limits, limitOf(t, sql))
		if len(limits) == 1 {
			return http.StatusOK, page1Body
		}
		return http.StatusOK, d1OK(idRows(table, 5, 1)) // short → final
	})))
	client.maxResponseBytes = responseCap
	r := &D1RowReader{client: client}
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
	mu.Lock()
	defer mu.Unlock()
	if len(limits) != 2 || limits[0] != 4 || limits[1] != wantSecond {
		t.Fatalf("page LIMITs = %v; want [4 %d] (probe-sized, then measured-sized)", limits, wantSecond)
	}
}

// TestD1RowReader_OverflowHalvesUntilThePageFits pins the safety net under
// the estimate: a page that overflows the cap is re-requested at the SAME
// bound with half the rows until it fits, and the read then completes with
// every row exactly once. The mock overflows any request wider than 2 rows.
func TestD1RowReader_OverflowHalvesUntilThePageFits(t *testing.T) {
	table := intPKTable("items")
	const responseCap = 8192
	all := idRows(table, 1, 5)
	var (
		mu       sync.Mutex
		requests []string // "<limit>@<bound>"
	)
	client := startMockD1(t, withCount(len(all), func(sql string, params []string) (int, []byte) {
		limit := limitOf(t, sql)
		bound := 0
		if len(params) == 1 {
			bound, _ = strconv.Atoi(params[0])
		}
		mu.Lock()
		requests = append(requests, fmt.Sprintf("%d@%d", limit, bound))
		mu.Unlock()
		if limit > 2 {
			return http.StatusOK, bytes.Repeat([]byte("x"), responseCap+1) // over the cap
		}
		end := bound + limit
		if end > len(all) {
			end = len(all)
		}
		return http.StatusOK, d1OK(all[bound:end])
	}))
	client.maxResponseBytes = responseCap
	r := &D1RowReader{client: client, pageSize: 8}
	ch, err := r.ReadRows(context.Background(), table)
	if err != nil {
		t.Fatalf("ReadRows: %v", err)
	}
	got := drain(ch)
	if err := r.Err(); err != nil {
		t.Fatalf("Err: %v (an overflowing page must be retried smaller, not surfaced)", err)
	}
	if len(got) != 5 {
		t.Fatalf("got %d rows; want 5", len(got))
	}
	for i, row := range got {
		if want := fmt.Sprintf("v%d", i+1); row["label"] != want {
			t.Fatalf("row %d label = %#v; want %q (order/dup/gap across the retried page)", i, row["label"], want)
		}
	}
	mu.Lock()
	defer mu.Unlock()
	// 8 → overflow → 4 → overflow → 2 fits (rows 1-2); the measured width
	// of a ~200-byte page against a 4096-byte budget then asks for the
	// ceiling again, which overflows and halves back to 2, and so on.
	if len(requests) < 5 || requests[0] != "8@0" || requests[1] != "4@0" || requests[2] != "2@0" {
		t.Fatalf("requests = %v; want 8@0, 4@0, 2@0 first (halving at the SAME bound)", requests)
	}
	for _, req := range requests {
		if strings.HasPrefix(req, "1@") {
			t.Fatalf("requests = %v; a page of 2 fits, so the controller must never fall to 1", requests)
		}
	}
}

// TestD1RowReader_RowTooLargeRefusedByName is the matrix for the door at the
// bottom of the halving: {stream loop, stage-local materializer} × {first
// row, a later row}. Once a page of ONE row still overflows, the read is
// refused with SLUICE-E-BULKCOPY-ROW-TOO-LARGE naming the table, the row's
// key and estimated size (from the one-row probe), and the cap — and rows
// before it were delivered (the stage cell checks the staged file).
func TestD1RowReader_RowTooLargeRefusedByName(t *testing.T) {
	table := intPKTable("t")
	const responseCap = 4096
	good := idRows(table, 1, 2)

	// overflowAfter is the bound (last delivered key) after which every data
	// page overflows; 0 means the very first page.
	newClient := func(t *testing.T, overflowAfter int, limits *[]int, mu *sync.Mutex) *d1Client {
		t.Helper()
		client := startMockD1(t, withCount(len(good)+1, func(sql string, params []string) (int, []byte) {
			switch {
			case isD1DataPage(sql):
				limit := limitOf(t, sql)
				mu.Lock()
				*limits = append(*limits, limit)
				mu.Unlock()
				bound := 0
				if len(params) == 1 {
					bound, _ = strconv.Atoi(params[0])
				}
				if bound >= overflowAfter {
					return http.StatusOK, bytes.Repeat([]byte("x"), responseCap+1)
				}
				return http.StatusOK, d1OK(good[bound : bound+limit])
			case strings.HasPrefix(sql, `SELECT CAST("t"."id" AS TEXT) AS "k0"`) && strings.HasSuffix(sql, " LIMIT 1"):
				// The one-row probe for the refusal: key + estimated width.
				return http.StatusOK, d1OK([]map[string]any{{"k0": strconv.Itoa(overflowAfter + 1), "w": "6000"}})
			}
			t.Errorf("unexpected query %q", sql)
			return http.StatusOK, d1Err(1, "unexpected")
		}))
		client.maxResponseBytes = responseCap
		return client
	}

	assertRefusal := func(t *testing.T, err error, wantKey string, limits []int) {
		t.Helper()
		if err == nil {
			t.Fatal("a row wider than the cap must be refused, not read as a truncated body")
		}
		coded, ok := sluicecode.FromError(err)
		if !ok || coded.Code != sluicecode.CodeBulkCopyRowTooLarge {
			t.Fatalf("refusal must carry %s; got %v", sluicecode.CodeBulkCopyRowTooLarge, err)
		}
		for _, want := range []string{`"t"`, "the row with " + wantKey, "about 6000 bytes", "4096-byte response cap", rowTooLargeHint} {
			if !strings.Contains(err.Error(), want) {
				t.Errorf("refusal must name %q (table, key, size, cap, remedy); got %v", want, err)
			}
		}
		if len(limits) == 0 || limits[len(limits)-1] != 1 {
			t.Errorf("page LIMITs = %v; the refusal must come only after a page of ONE row overflowed", limits)
		}
	}

	for _, overflowAfter := range []int{0, 2} {
		wantKey := "id=" + strconv.Itoa(overflowAfter+1)
		t.Run(fmt.Sprintf("direct/overflow_after=%d", overflowAfter), func(t *testing.T) {
			var (
				mu     sync.Mutex
				limits []int
			)
			r := &D1RowReader{client: newClient(t, overflowAfter, &limits, &mu), pageSize: 2}
			ch, err := r.ReadRows(context.Background(), table)
			if err != nil {
				t.Fatalf("ReadRows: %v", err)
			}
			got := drain(ch)
			if len(got) != overflowAfter {
				t.Errorf("delivered %d rows before the refusal; want %d (in-sequence surfacing)", len(got), overflowAfter)
			}
			mu.Lock()
			defer mu.Unlock()
			assertRefusal(t, r.Err(), wantKey, limits)
		})
		t.Run(fmt.Sprintf("stage/overflow_after=%d", overflowAfter), func(t *testing.T) {
			var (
				mu     sync.Mutex
				limits []int
			)
			rr := &D1RowReader{client: newClient(t, overflowAfter, &limits, &mu), pageSize: 2}
			db := openStageDest(t, `CREATE TABLE t (id INTEGER PRIMARY KEY, label TEXT)`)
			total, err := stageD1Table(context.Background(), rr, db, table, true, slog.Default())
			if int(total) != overflowAfter || len(stagedIDs(t, db, "t")) != overflowAfter {
				t.Errorf("staged %d rows (%d reported) before the refusal; want %d", len(stagedIDs(t, db, "t")), total, overflowAfter)
			}
			mu.Lock()
			defer mu.Unlock()
			assertRefusal(t, err, wantKey, limits)
		})
	}
}

// TestD1RowReader_RowTooLargeProbeFailureStillRefuses pins that the coded
// refusal is never downgraded: when the one-row probe that names the key
// fails, the read is still refused with the code, naming the position that
// IS known (the row after the last delivered key) and the probe's failure.
func TestD1RowReader_RowTooLargeProbeFailureStillRefuses(t *testing.T) {
	table := intPKTable("t")
	const responseCap = 2048
	client := startMockD1(t, withCount(1, func(sql string, _ []string) (int, []byte) {
		if isD1DataPage(sql) {
			return http.StatusOK, bytes.Repeat([]byte("x"), responseCap+1)
		}
		return http.StatusOK, d1Err(7500, "simulated probe failure")
	}))
	client.maxResponseBytes = responseCap
	r := &D1RowReader{client: client, pageSize: 1}
	ch, err := r.ReadRows(context.Background(), table)
	if err != nil {
		t.Fatalf("ReadRows: %v", err)
	}
	_ = drain(ch)
	err = r.Err()
	coded, ok := sluicecode.FromError(err)
	if !ok || coded.Code != sluicecode.CodeBulkCopyRowTooLarge {
		t.Fatalf("refusal must carry %s even when the probe fails; got %v", sluicecode.CodeBulkCopyRowTooLarge, err)
	}
	for _, want := range []string{"the first row", "could not be probed", "simulated probe failure", "2048-byte response cap"} {
		if !strings.Contains(err.Error(), want) {
			t.Errorf("refusal must name %q; got %v", want, err)
		}
	}
}

// TestD1Client_ResponseCapBoundary pins the client's overflow door at the
// byte: a body exactly the cap decodes normally; one byte more is the named
// d1ResponseTooLargeError (never a decode of a truncated body), and the
// error names the cap.
func TestD1Client_ResponseCapBoundary(t *testing.T) {
	body := d1OK([]map[string]any{{"pad": strings.Repeat("p", 500)}})
	client := startMockD1(t, func(string, []string) (int, []byte) { return http.StatusOK, body })

	client.maxResponseBytes = len(body)
	rows, n, err := client.queryRowsSized(context.Background(), "SELECT 1")
	if err != nil || len(rows) != 1 || n != len(body) {
		t.Fatalf("a body exactly the cap must decode: rows=%d bytes=%d err=%v", len(rows), n, err)
	}

	client.maxResponseBytes = len(body) - 1
	_, _, err = client.queryRowsSized(context.Background(), "SELECT 1")
	var tooLarge *d1ResponseTooLargeError
	if !errors.As(err, &tooLarge) {
		t.Fatalf("a body one byte over the cap must be a d1ResponseTooLargeError; got %v", err)
	}
	if tooLarge.limit != len(body)-1 || !strings.Contains(err.Error(), strconv.Itoa(len(body)-1)+"-byte response cap") {
		t.Errorf("the error must carry and name the cap %d; got %v", len(body)-1, err)
	}
	if strings.Contains(err.Error(), "unexpected end of JSON input") {
		t.Errorf("the overflow must not be reported as a JSON decode failure: %v", err)
	}
}

// TestRowWidthExpr_EstimateWithinHeadroom runs the width probe's SQL against
// real SQLite over every storage class the projection renders — integer
// (> 2^53), real, ASCII text, text needing JSON escaping, control-character
// text, multibyte text, text with an embedded NUL, blob, NULL — and checks
// each row's estimate against the actual bytes of the page the mock
// serializes. For every family but one the estimate sits inside the
// controller's 2× headroom in both directions, which is the safety argument
// for sizing a page from it; this is the test that binds the two. The one
// exception is deliberate and pinned as such: control characters serialize
// as `\u00XX` (6 bytes for 1), so a text made of them under-estimates past
// the headroom — that family is what the overflow halve-and-retry in
// fetchPages exists for, and the pin asserts the estimate really is UNDER
// (so the retry is load-bearing there, not dead code). The serializer here
// is Go's encoding/json; real D1's was measured on 2026-09-02 to escape the
// same set (`"`, `\`, control characters) and to emit multibyte text raw, so
// the ratios carry over.
func TestRowWidthExpr_EstimateWithinHeadroom(t *testing.T) {
	src := openStageDest(t, `CREATE TABLE fam (id INTEGER PRIMARY KEY, v)`)
	seed := []struct {
		family         string
		sql            string
		outsideByRetry bool // the estimate is under the headroom; the overflow retry covers it
	}{
		{"integer", `INSERT INTO fam VALUES (1, 9007199254740993)`, false},
		{"real", `INSERT INTO fam VALUES (2, 0.30000000000000004)`, false},
		{"text-ascii", `INSERT INTO fam VALUES (3, 'plain ascii text of ordinary shape')`, false},
		{"text-escaped", `INSERT INTO fam VALUES (4, replace(hex(zeroblob(40)), '0', '"'))`, false},
		{"text-control", `INSERT INTO fam VALUES (5, replace(hex(zeroblob(40)), '0', char(1)))`, true},
		{"text-multibyte", `INSERT INTO fam VALUES (6, '日本語のテキスト and héllo')`, false},
		{"text-nul", `INSERT INTO fam VALUES (7, 'ab' || char(0) || 'cd' || char(0) || 'efghijklmnop')`, false},
		{"blob", `INSERT INTO fam VALUES (8, randomblob(64))`, false},
		{"null", `INSERT INTO fam VALUES (9, NULL)`, false},
	}
	for _, s := range seed {
		if _, err := src.ExecContext(context.Background(), s.sql); err != nil {
			t.Fatalf("seed %s: %v", s.family, err)
		}
	}
	client := startMockD1(t, execD1Handler(src))
	table := d1TableFromMock(t, client, "fam")
	r := &D1RowReader{client: client}
	plan, err := r.planPagination(context.Background(), table)
	if err != nil {
		t.Fatalf("planPagination: %v", err)
	}
	projection := buildD1Projection(table, plan)
	for i, s := range seed {
		t.Run(s.family, func(t *testing.T) {
			id := strconv.Itoa(i + 1)
			// The estimate, from the probe's own expression over this one row.
			est, err := client.queryRows(context.Background(),
				"SELECT CAST("+rowWidthExpr(table, plan)+" AS TEXT) AS w FROM fam WHERE id = "+id)
			if err != nil || len(est) != 1 {
				t.Fatalf("width expr: %v (%d rows)", err, len(est))
			}
			wText, _, _ := jsonString(est[0]["w"])
			w, _ := strconv.ParseInt(wText, 10, 64)
			// The actual bytes: the serialized one-row page, minus the
			// envelope around an empty page.
			sql, _ := buildD1PageQuery(table, plan, projection, nil, 1)
			_, page, err := client.queryRowsSized(context.Background(), strings.Replace(sql, " ORDER BY", " WHERE id = "+id+" ORDER BY", 1))
			if err != nil {
				t.Fatalf("page: %v", err)
			}
			actual := int64(page - len(d1OK(nil)))
			if w <= 0 || actual <= 0 {
				t.Fatalf("estimate %d / actual %d must both be positive", w, actual)
			}
			if s.outsideByRetry {
				if w*2 >= actual {
					t.Fatalf("estimate %d vs actual %d bytes: this family is documented as under-estimating past the headroom (the overflow retry's reason to exist); it no longer is — update the pin", w, actual)
				}
				return
			}
			if w*2 < actual || w > actual*2 {
				t.Fatalf("estimate %d vs actual %d bytes: outside the controller's 2× headroom", w, actual)
			}
		})
	}
}

// TestProbeMaxRowWidth_RealSQLite pins the first-page probe end-to-end on
// real SQLite: it returns the MAX over the first maxRows rows in keyset
// order (not the table's max, not an average), 0 for an empty table, and
// the estimate for a BLOB row counts hex's doubling.
func TestProbeMaxRowWidth_RealSQLite(t *testing.T) {
	src := openStageDest(t, `CREATE TABLE w (id INTEGER PRIMARY KEY, b BLOB)`)
	// Rows 1-3 narrow, row 4 wide: a probe over the first 3 must not see row 4.
	for _, s := range []string{
		`INSERT INTO w VALUES (1, randomblob(10)), (2, randomblob(20)), (3, randomblob(30)), (4, randomblob(5000))`,
	} {
		if _, err := src.ExecContext(context.Background(), s); err != nil {
			t.Fatal(err)
		}
	}
	client := startMockD1(t, execD1Handler(src))
	table := d1TableFromMock(t, client, "w")
	r := &D1RowReader{client: client}
	plan, err := r.planPagination(context.Background(), table)
	if err != nil {
		t.Fatal(err)
	}
	first3, err := r.probeMaxRowWidth(context.Background(), table, plan, 3)
	if err != nil {
		t.Fatal(err)
	}
	all, err := r.probeMaxRowWidth(context.Background(), table, plan, d1PageSize)
	if err != nil {
		t.Fatal(err)
	}
	// Row 3: 2*30 hex bytes + the id digit + framing; row 4: 2*5000 + ….
	if first3 < 60 || first3 > 200 {
		t.Errorf("probe over the first 3 rows = %d; want the 30-byte blob's doubled width plus framing, never the 5000-byte row", first3)
	}
	if all < 10000 || all > 10200 {
		t.Errorf("probe over the whole first page = %d; want the 5000-byte blob doubled through hex plus framing", all)
	}
	empty := openStageDest(t, `CREATE TABLE e (id INTEGER PRIMARY KEY, b BLOB)`)
	ec := startMockD1(t, execD1Handler(empty))
	et := d1TableFromMock(t, ec, "e")
	er := &D1RowReader{client: ec}
	eplan, _ := er.planPagination(context.Background(), et)
	if w, err := er.probeMaxRowWidth(context.Background(), et, eplan, d1PageSize); err != nil || w != 0 {
		t.Errorf("empty table probe = %d, %v; want 0, nil", w, err)
	}
}
