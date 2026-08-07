// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

// Pins for [Client.ListDeployRequests] — the discovery read the
// leftover-dev-branch adoption rides (roadmap item 108).
//
// The property under test is not "it lists things", it is that the walk
// cannot come back SHORT without saying so. A short answer here is read by
// the caller as "this dev branch has no deploy request", whose remedy is a
// branch delete — and that delete, aimed at a branch whose deployment is
// running, is exactly the destructive advice item 108 exists to remove. So
// each case below is a way the server could make the walk stop early.

package api

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strconv"
	"testing"
)

// serveDeployRequestPages answers the collection endpoint by slicing refs
// with the server's OWN page size — which is deliberately allowed to be
// smaller than the one the client asks for.
func serveDeployRequestPages(t *testing.T, total, serverPageSize int) (client *Client, requestCount *int) {
	t.Helper()
	requests := 0
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requests++
		page, _ := strconv.Atoi(r.URL.Query().Get("page"))
		if page < 1 {
			page = 1
		}
		start := (page - 1) * serverPageSize
		out := []map[string]any{}
		for i := start; i < start+serverPageSize && i < total; i++ {
			out = append(out, map[string]any{
				"number":      i + 1,
				"branch":      fmt.Sprintf("sluice-index-%04d", i),
				"into_branch": "main",
			})
		}
		_ = json.NewEncoder(w).Encode(map[string]any{"data": out})
	}))
	t.Cleanup(srv.Close)
	return newTestClient(srv, nil), &requests
}

// TestClient_ListDeployRequests_WalksEveryPage pins the ordinary case.
func TestClient_ListDeployRequests_WalksEveryPage(t *testing.T) {
	c, requests := serveDeployRequestPages(t, 250, 100)
	got, err := c.ListDeployRequests(context.Background(), "o", "d")
	if err != nil {
		t.Fatalf("ListDeployRequests: %v", err)
	}
	if len(got) != 250 {
		t.Fatalf("got %d deploy requests; want 250", len(got))
	}
	if got[0].Number != 1 || got[249].Number != 250 || got[0].IntoBranch != "main" {
		t.Errorf("decoded refs look wrong: first=%+v last=%+v", got[0], got[249])
	}
	// 3 full-ish pages + the empty page that ends the walk.
	if *requests != 4 {
		t.Errorf("made %d requests; want 4 (three pages plus the terminating empty one)", *requests)
	}
}

// TestClient_ListDeployRequests_ServerCapsPerPage is the one that would
// have been silently wrong under a short-page stop: the client asks for
// 100 and the server hands back 25. A walk that stopped on "fewer than I
// asked for" would return 25 of 60 and the caller would report the dev
// branch as having no deploy request.
func TestClient_ListDeployRequests_ServerCapsPerPage(t *testing.T) {
	c, _ := serveDeployRequestPages(t, 60, 25)
	got, err := c.ListDeployRequests(context.Background(), "o", "d")
	if err != nil {
		t.Fatalf("ListDeployRequests: %v", err)
	}
	if len(got) != 60 {
		t.Fatalf("got %d deploy requests; want 60 — the server's per_page cap must not truncate the walk", len(got))
	}
}

// TestClient_ListDeployRequests_TruncationIsAnError pins the bound: a
// database with more deploy requests than sluice enumerates (or a server
// that ignores `page` and keeps answering) must produce an ERROR, never a
// partial list the caller would read as an exhaustive one.
func TestClient_ListDeployRequests_TruncationIsAnError(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		out := make([]map[string]any, 0, 100)
		for i := range 100 {
			out = append(out, map[string]any{"number": i + 1, "branch": "b", "into_branch": "main"})
		}
		_ = json.NewEncoder(w).Encode(map[string]any{"data": out})
	}))
	t.Cleanup(srv.Close)

	got, err := newTestClient(srv, nil).ListDeployRequests(context.Background(), "o", "d")
	if !errors.Is(err, ErrDeployRequestListTruncated) {
		t.Fatalf("err = %v; want ErrDeployRequestListTruncated", err)
	}
	if got != nil {
		t.Errorf("got %d refs alongside the truncation error; the partial list must not be usable", len(got))
	}
}

// TestClient_ListDeployRequests_ResponseShape pins the three fields the
// adoption discovery reads against a body the client did not produce.
// Provenance: DERIVED from the public PlanetScale API reference (path,
// `page`/`per_page`, `data` envelope) — NOT a live capture. The next
// psverify dispatch should replace this fixture with a sanitized one; the
// blast radius of it being wrong is bounded by design (see
// [DeployRequestRef]).
func TestClient_ListDeployRequests_ResponseShape(t *testing.T) {
	const body = `{
	  "current_page": 1,
	  "next_page": null,
	  "data": [
	    {
	      "id": "dr-abc123",
	      "number": 42,
	      "branch": "sluice-index-09c009c7f2",
	      "into_branch": "main",
	      "state": "open",
	      "approved": false,
	      "deployment_state": "in_progress",
	      "html_url": "https://app.planetscale.com/acme/db/deploy-requests/42"
	    }
	  ]
	}`
	// Page 1 serves the fixture, page 2 the empty page that ends the walk.
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Query().Get("page") != "1" {
			_, _ = w.Write([]byte(`{"data":[]}`))
			return
		}
		_, _ = w.Write([]byte(body))
	}))
	t.Cleanup(srv.Close)

	got, err := newTestClient(srv, nil).ListDeployRequests(context.Background(), "o", "d")
	if err != nil {
		t.Fatalf("ListDeployRequests: %v", err)
	}
	if len(got) != 1 {
		t.Fatalf("got %d refs; want 1", len(got))
	}
	want := DeployRequestRef{Number: 42, Branch: "sluice-index-09c009c7f2", IntoBranch: "main"}
	if got[0] != want {
		t.Errorf("ref = %+v; want %+v", got[0], want)
	}
}
