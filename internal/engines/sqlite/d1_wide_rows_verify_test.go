//go:build d1verify

// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package sqlite

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"sluicesync.dev/sluice/internal/sluicecode"
)

// TestD1Verify_WideRowsPageByBytes replays audit 2026-09-01 LA-2 on REAL
// Cloudflare D1 at the PRODUCTION cap and page ceiling. The finding: the
// reader's premise that "D1 caps a response at ~1 MiB" was false, so its
// fixed 1,000-row page met sluice's own 8 MiB response cap on any table of
// rows wider than ~8 KB. Ground-truthed 2026-09-02 (Phase A, same account):
// a 1,000-row page of 16 KiB BLOBs came back whole — HTTP 200, chunked,
// uncompressed, 32,839,264 bytes in 2.18 s — and the parent commit's reader
// delivered 0 rows of that table with
//
//	d1: table "wide": read page: d1: decode response from database "…":
//	unexpected end of JSON input (body: {"result":[{"results":[{…
//
// which is the anti-vacuity record for the `wide` cell below: this test
// fails on the parent commit with exactly that error.
//
// Four cells, each with its own independent expected value (a server-side
// COUNT(*) or SUM(LENGTH()) read through raw queryRows, never the reader
// under test) and the page LIMITs observed on the wire through a recording
// transport:
//
//   - wide: 300 rows × 16 KiB BLOB (~33 KB each as hex+JSON) migrates
//     completely on both the direct reader and `--stage-local`, in pages
//     the controller shrank below 1,000 (≥ 2 pages, every LIMIT < 1,000).
//   - small: 1,000 narrow rows still read as ONE 1,000-row page — the old
//     size, unchanged for ordinary tables.
//   - blob1m: a single 1 MiB BLOB row (a 2 MiB hex response) is READ, not
//     refused — the cap is 8 MiB and a row under it is a working
//     configuration. (The task spec expected a 1 MB row to refuse; on real
//     D1 it cannot and must not: D1 stores values up to ~4 MB and refuses
//     to hex() a BLOB above ~2 MiB with SQLITE_TOOBIG, so no BLOB row can
//     reach the cap through the projection.)
//   - control: ONE row whose 1.6 MB TEXT is all control characters — D1
//     stores it (well under its value limit) and serializes each byte as
//     a six-character JSON escape (backslash, `u`, four hex digits), so the
//     single-row response is ~9.6 MB:
//     over the production cap as a page of one. Refused on both consumers
//     with SLUICE-E-BULKCOPY-ROW-TOO-LARGE naming the table, `id=1`, the
//     estimated size and the 8,388,608-byte cap — after the controller
//     halved the page down to 1 (its first request is sized from the
//     LENGTH() estimate, which JSON escaping then defeats: exactly the
//     shape the overflow retry exists for).
//
// Same lifecycle as the other d1verify tests — a throwaway database
// (`la2-<nanos>`) created and deleted via the REST API; skip-clean without
// credentials.
func TestD1Verify_WideRowsPageByBytes(t *testing.T) {
	account, token := d1AdvCreds(t)
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Minute)
	defer cancel()

	dbID := createThrowawayD1DatabaseNamed(ctx, t, account, token, fmt.Sprintf("la2-%d", time.Now().UnixNano()))
	client, err := openD1Client("d1://" + account + "/" + dbID)
	if err != nil {
		t.Fatalf("openD1Client: %v", err)
	}
	rec := &recordingTransport{next: http.DefaultTransport}
	client.httpClient = &http.Client{Transport: rec, Timeout: 5 * time.Minute}

	// Seed. randomblob/hex/replace run server-side, so no statement carries
	// a large literal (D1's statement limit is 100 KB).
	d1AdvExec(ctx, t, client, `CREATE TABLE wide (id INTEGER PRIMARY KEY, b BLOB)`)
	for i := 0; i < 2; i++ {
		d1AdvExec(ctx, t, client, fmt.Sprintf(`WITH RECURSIVE n(x) AS (SELECT 1 UNION ALL SELECT x + 1 FROM n WHERE x < 150)
			INSERT INTO wide (id, b) SELECT %d + x, randomblob(16384) FROM n`, i*150))
	}
	d1AdvExec(ctx, t, client, `CREATE TABLE small (id INTEGER PRIMARY KEY, v TEXT)`)
	d1AdvExec(ctx, t, client, `WITH RECURSIVE n(x) AS (SELECT 1 UNION ALL SELECT x + 1 FROM n WHERE x < 1000)
		INSERT INTO small (id, v) SELECT x, 'value-' || x FROM n`)
	d1AdvExec(ctx, t, client, `CREATE TABLE blob1m (id INTEGER PRIMARY KEY, b BLOB)`)
	d1AdvExec(ctx, t, client, `INSERT INTO blob1m (id, b) VALUES (1, randomblob(1048576))`)

	// Independent expected values, read raw.
	wantWide := d1AdvScalar(ctx, t, client, `SELECT CAST(COUNT(*) AS TEXT) AS v FROM wide`)
	wantWideBytes := d1AdvScalar(ctx, t, client, `SELECT CAST(SUM(LENGTH(b)) AS TEXT) AS v FROM wide`)
	if wantWide != "300" || wantWideBytes != strconv.Itoa(300*16384) {
		t.Fatalf("seed: wide holds %s rows / %s blob bytes; want 300 / %d", wantWide, wantWideBytes, 300*16384)
	}

	wide := d1AdvFindTable(ctx, t, client, "wide")
	small := d1AdvFindTable(ctx, t, client, "small")
	blob1m := d1AdvFindTable(ctx, t, client, "blob1m")

	t.Run("wide/direct", func(t *testing.T) {
		rec.reset()
		reader := &D1RowReader{client: client} // production ceiling (1,000) and cap (8 MiB)
		start := time.Now()
		rows := d1AdvReadAll(ctx, t, reader, wide)
		elapsed := time.Since(start)
		if len(rows) != 300 {
			t.Fatalf("direct reader delivered %d rows; want 300", len(rows))
		}
		var total int
		seen := map[int64]bool{}
		for i, row := range rows {
			id, _ := row["id"].(int64)
			b, ok := row["b"].([]byte)
			if !ok || len(b) != 16384 || seen[id] {
				t.Fatalf("row %d: id=%v blob=%d bytes (duplicate or short)", i, row["id"], len(b))
			}
			seen[id] = true
			total += len(b)
		}
		if strconv.Itoa(total) != wantWideBytes {
			t.Fatalf("delivered %d blob bytes; server holds %s", total, wantWideBytes)
		}
		limits := rec.dataPageLimits(t)
		t.Logf("wide/direct: %d rows in %d pages %v, %s", len(rows), len(limits), limits, elapsed)
		assertShrunkPages(t, limits)
	})

	t.Run("wide/stage-local", func(t *testing.T) {
		rec.reset()
		dest := filepath.Join(t.TempDir(), "stage.db")
		if err := stageD1ClientToLocalFile(ctx, client, dest, nil, nil); err != nil {
			t.Fatalf("stage: %v", err)
		}
		db, err := sql.Open("sqlite", dest)
		if err != nil {
			t.Fatalf("open staged: %v", err)
		}
		defer func() { _ = db.Close() }()
		var n, sum int64
		if err := db.QueryRowContext(ctx, `SELECT COUNT(*), SUM(LENGTH(b)) FROM wide`).Scan(&n, &sum); err != nil {
			t.Fatalf("count staged: %v", err)
		}
		if n != 300 || strconv.FormatInt(sum, 10) != wantWideBytes {
			t.Fatalf("staged wide holds %d rows / %d blob bytes; want 300 / %s", n, sum, wantWideBytes)
		}
		assertShrunkPages(t, rec.dataPageLimitsFor(t, "wide"))
	})

	t.Run("small/full-page", func(t *testing.T) {
		rec.reset()
		reader := &D1RowReader{client: client}
		rows := d1AdvReadAll(ctx, t, reader, small)
		if len(rows) != 1000 {
			t.Fatalf("small: %d rows; want 1000", len(rows))
		}
		limits := rec.dataPageLimits(t)
		// 1,000 rows at LIMIT 1000 is a full page, so a second (empty) page
		// closes the read: two requests, both at the old ceiling.
		if len(limits) != 2 || limits[0] != d1PageSize || limits[1] != d1PageSize {
			t.Fatalf("small: page LIMITs = %v; want [%d %d] — narrow rows must keep the old page size", limits, d1PageSize, d1PageSize)
		}
	})

	t.Run("blob1m/read-not-refused", func(t *testing.T) {
		reader := &D1RowReader{client: client}
		rows := d1AdvReadAll(ctx, t, reader, blob1m)
		if len(rows) != 1 {
			t.Fatalf("blob1m: %d rows; want 1", len(rows))
		}
		if b, ok := rows[0]["b"].([]byte); !ok || len(b) != 1048576 {
			t.Fatalf("blob1m: blob = %d bytes; want 1048576 (a row under the cap is a working configuration)", len(b))
		}
	})

	// The control row is seeded only now, AFTER the wide/stage-local cell:
	// staging replicates every table, and this one is meant to refuse.
	d1AdvExec(ctx, t, client, `CREATE TABLE control (id INTEGER PRIMARY KEY, v TEXT)`)
	d1AdvExec(ctx, t, client, `INSERT INTO control (id, v) VALUES (1, replace(hex(zeroblob(800000)), '0', char(1)))`)
	if got := d1AdvScalar(ctx, t, client, `SELECT CAST(LENGTH(CAST(v AS BLOB)) AS TEXT) AS v FROM control`); got != "1600000" {
		t.Fatalf("seed: control's text is %s bytes; want 1600000 (six JSON bytes each → ~9.6 MB, over the 8 MiB cap)", got)
	}
	control := d1AdvFindTable(ctx, t, client, "control")

	assertTooLarge := func(t *testing.T, err error) {
		t.Helper()
		if err == nil {
			t.Fatal("a row over the cap must be refused, not read")
		}
		coded, ok := sluicecode.FromError(err)
		if !ok || coded.Code != sluicecode.CodeBulkCopyRowTooLarge {
			t.Fatalf("refusal must carry %s; got %v", sluicecode.CodeBulkCopyRowTooLarge, err)
		}
		for _, want := range []string{`"control"`, "the row with id=1", "about 1600", "bytes of values before JSON encoding", strconv.Itoa(d1MaxResponseBytes) + "-byte response cap", rowTooLargeHint} {
			if !strings.Contains(err.Error(), want) {
				t.Errorf("refusal must name %q; got %v", want, err)
			}
		}
		if strings.Contains(err.Error(), "unexpected end of JSON input") {
			t.Errorf("the refusal must not be the parent commit's truncated-body decode error: %v", err)
		}
		limits := rec.dataPageLimitsFor(t, "control")
		if len(limits) < 2 || limits[len(limits)-1] != 1 {
			t.Errorf("control: page LIMITs = %v; the refusal must follow the halving down to a page of ONE row", limits)
		}
		t.Logf("control: page LIMITs %v; refusal: %v", limits, err)
	}

	t.Run("control/direct", func(t *testing.T) {
		rec.reset()
		reader := &D1RowReader{client: client}
		ch, err := reader.ReadRows(ctx, control)
		if err != nil {
			t.Fatalf("ReadRows: %v", err)
		}
		if got := drain(ch); len(got) != 0 {
			t.Fatalf("control: %d rows delivered; want 0", len(got))
		}
		assertTooLarge(t, reader.Err())
	})

	t.Run("control/stage-local", func(t *testing.T) {
		rec.reset()
		dest := filepath.Join(t.TempDir(), "stage.db")
		assertTooLarge(t, stageD1ClientToLocalFile(ctx, client, dest, nil, nil))
	})
}

// assertShrunkPages is the anti-vacuity check on the wide cells: the
// controller must have engaged — at least two data pages, none at the old
// 1,000-row ceiling (16 KiB rows at ~33 KB each fit ~127 to a 4 MiB budget).
func assertShrunkPages(t *testing.T, limits []int) {
	t.Helper()
	if len(limits) < 2 {
		t.Fatalf("page LIMITs = %v; want ≥ 2 pages (a single page means the controller did not size by bytes)", limits)
	}
	for _, l := range limits {
		if l >= d1PageSize {
			t.Fatalf("page LIMITs = %v; every page of 16 KiB rows must be smaller than %d", limits, d1PageSize)
		}
	}
}

// d1AdvScalar runs a one-row, one-column (`v`) query raw and returns the text.
func d1AdvScalar(ctx context.Context, t *testing.T, c *d1Client, sql string) string {
	t.Helper()
	rows, err := c.queryRows(ctx, sql)
	if err != nil || len(rows) != 1 {
		t.Fatalf("%s: %v (%d rows)", sql, err, len(rows))
	}
	v, ok, err := jsonString(rows[0]["v"])
	if err != nil || !ok {
		t.Fatalf("%s: not a text scalar (%v)", sql, err)
	}
	return v
}

// recordingTransport records the SQL of every D1 request it forwards, so a
// test against real D1 can see the page LIMITs the controller chose.
type recordingTransport struct {
	next http.RoundTripper
	mu   sync.Mutex
	sqls []string
}

func (r *recordingTransport) RoundTrip(req *http.Request) (*http.Response, error) {
	body, err := io.ReadAll(req.Body)
	if err != nil {
		return nil, err
	}
	req.Body = io.NopCloser(bytes.NewReader(body))
	var rb d1RequestBody
	if json.Unmarshal(body, &rb) == nil {
		r.mu.Lock()
		r.sqls = append(r.sqls, rb.SQL)
		r.mu.Unlock()
	}
	return r.next.RoundTrip(req)
}

func (r *recordingTransport) reset() {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.sqls = nil
}

var limitRe = regexp.MustCompile(` LIMIT (\d+)$`)

// dataPageLimits returns the LIMIT of every keyset data-page request
// recorded (the width/count/key probes are not data pages).
func (r *recordingTransport) dataPageLimits(t *testing.T) []int {
	t.Helper()
	return r.dataPageLimitsFor(t, "")
}

// dataPageLimitsFor is dataPageLimits restricted to one table (a stage-local
// run reads every table).
func (r *recordingTransport) dataPageLimitsFor(t *testing.T, table string) []int {
	t.Helper()
	r.mu.Lock()
	defer r.mu.Unlock()
	var out []int
	for _, s := range r.sqls {
		if !strings.HasPrefix(s, "SELECT typeof(") {
			continue
		}
		if table != "" && !strings.Contains(s, " FROM "+quoteIdent(table)+" ") {
			continue
		}
		m := limitRe.FindStringSubmatch(s)
		if m == nil {
			t.Fatalf("data page without LIMIT: %q", s)
		}
		n, _ := strconv.Atoi(m[1])
		out = append(out, n)
	}
	return out
}
