// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package sqlitetrigger

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"testing"

	"sluicesync.dev/sluice/internal/engines/sqlite"
	"sluicesync.dev/sluice/internal/sluicecode"
)

// LA-2b: the change-log poll shares the transport's response cap with the bulk
// row reader but did NOT share its adaptive page.
//
// Why that was worse than it sounds, and why these cells exist: a poll error
// kills the pump, the poll batch is not an operator flag, and the batch was
// fixed — so a change-log batch whose before/after images exceeded the cap
// wedged the d1-trigger stream permanently, with every restart meeting the
// identical batch. There was no remedy at all, not a slow one.
//
// These drive the executor directly rather than through the reader so the
// batch-of-one cell costs one round trip instead of halving down from 1000.

var limitRe = regexp.MustCompile(`LIMIT (\d+)`)

// shrinkMock answers the change-log poll, returning a deliberately over-cap
// body for any LIMIT above overCapAbove and a normal one-row body otherwise.
// It records every LIMIT it was asked for, which is how the sticky cell proves
// the second poll never re-asked for the big batch.
type shrinkMock struct {
	mu           sync.Mutex
	overCapAbove int // a LIMIT strictly above this returns an over-cap body
	limits       []int
}

func (m *shrinkMock) seen() []int {
	m.mu.Lock()
	defer m.mu.Unlock()
	return append([]int(nil), m.limits...)
}

func startShrinkMock(t *testing.T, m *shrinkMock) *sqlite.D1Conn {
	t.Helper()
	// One body comfortably over the client's 8 MiB cap. Built once: the client
	// refuses on Content-Length/body length BEFORE decoding, so its contents
	// only have to be bytes.
	big := []byte(`{"padding":"` + strings.Repeat("x", 9<<20) + `"}`)

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		raw, _ := io.ReadAll(r.Body)
		var req struct {
			SQL    string   `json:"sql"`
			Params []string `json:"params"`
		}
		_ = json.Unmarshal(raw, &req)

		match := limitRe.FindStringSubmatch(req.SQL)
		if match == nil {
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write(d1OK(nil))
			return
		}
		limit, _ := strconv.Atoi(match[1])
		m.mu.Lock()
		m.limits = append(m.limits, limit)
		tooBig := limit > m.overCapAbove
		m.mu.Unlock()

		w.WriteHeader(http.StatusOK)
		if tooBig {
			_, _ = w.Write(big)
			return
		}
		_, _ = w.Write(d1OK([]map[string]any{{
			"id":          "1",
			"op":          "INSERT",
			"tbl":         "t",
			"before":      nil,
			"after":       `{"id":{"t":"integer","v":"1"}}`,
			"captured_at": "2026-09-03T00:00:00Z",
		}}))
	}))
	t.Cleanup(srv.Close)
	return sqlite.D1ConnForTest(srv.URL, "acct", "db", "tok")
}

func TestD1PollChangeLog_ShrinksInsteadOfWedging(t *testing.T) {
	t.Run("an over-cap batch is halved and re-requested, not surfaced as a poll error", func(t *testing.T) {
		m := &shrinkMock{overCapAbove: 500}
		e := &d1Executor{conn: startShrinkMock(t, m)}

		rows, err := e.pollChangeLog(context.Background(), 0, 1000)
		if err != nil {
			t.Fatalf("poll should have shrunk and succeeded, got: %v", err)
		}
		if len(rows) != 1 {
			t.Fatalf("rows = %d, want 1", len(rows))
		}
		if got := m.seen(); len(got) != 2 || got[0] != 1000 || got[1] != 500 {
			t.Fatalf("limits asked for = %v, want [1000 500]", got)
		}
		if e.pollBatchCap != 500 {
			t.Fatalf("sticky cap = %d, want 500", e.pollBatchCap)
		}
	})

	t.Run("the shrink is sticky: the next poll does not re-ask for the big batch", func(t *testing.T) {
		m := &shrinkMock{overCapAbove: 500}
		e := &d1Executor{conn: startShrinkMock(t, m)}

		if _, err := e.pollChangeLog(context.Background(), 0, 1000); err != nil {
			t.Fatalf("first poll: %v", err)
		}
		if _, err := e.pollChangeLog(context.Background(), 0, 1000); err != nil {
			t.Fatalf("second poll: %v", err)
		}
		// Without stickiness the second poll pays the over-cap round trip
		// again, and the sequence would be [1000 500 1000 500].
		if got := m.seen(); len(got) != 3 || got[2] != 500 {
			t.Fatalf("limits asked for = %v, want the second poll to start at 500", got)
		}
	})

	t.Run("a single change row over the cap is refused by name, not shrunk forever", func(t *testing.T) {
		m := &shrinkMock{overCapAbove: 0} // every LIMIT overflows
		e := &d1Executor{conn: startShrinkMock(t, m)}

		_, err := e.pollChangeLog(context.Background(), 41, 1)
		var coded *sluicecode.CodedError
		if !errors.As(err, &coded) || coded.Code != sluicecode.CodeBulkCopyRowTooLarge {
			t.Fatalf("want %s, got: %v", sluicecode.CodeBulkCopyRowTooLarge, err)
		}
		// The message has to name the watermark: it is the only handle an
		// operator has on WHICH row is stuck.
		if !strings.Contains(err.Error(), "id=41") {
			t.Fatalf("refusal should name the watermark it is stuck after: %v", err)
		}
	})

	t.Run("an ordinary transport error still surfaces unchanged", func(t *testing.T) {
		srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
			w.WriteHeader(http.StatusInternalServerError)
			_, _ = w.Write([]byte(`{"success":false,"errors":[{"message":"boom"}]}`))
		}))
		t.Cleanup(srv.Close)
		e := &d1Executor{conn: sqlite.D1ConnForTest(srv.URL, "acct", "db", "tok")}

		_, err := e.pollChangeLog(context.Background(), 0, 1000)
		if err == nil {
			t.Fatal("a 500 from the transport must still fail the poll")
		}
		var coded *sluicecode.CodedError
		if errors.As(err, &coded) && coded.Code == sluicecode.CodeBulkCopyRowTooLarge {
			t.Fatalf("a plain transport error must not be reported as a too-large row: %v", err)
		}
		if e.pollBatchCap != 0 {
			t.Fatalf("a non-cap error must not shrink the batch; cap = %d", e.pollBatchCap)
		}
	})
}
