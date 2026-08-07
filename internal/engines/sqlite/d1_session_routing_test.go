// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package sqlite

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

// TestD1Transport_NeverRoutesThroughTheSessionsAPI is the binding test
// for the consistency premise ADR-0136 §4 states and that [D1Conn] and
// the `d1-trigger` engine both repeat in prose: every D1 query goes to
// the DEFAULT primary path, never through D1's Sessions / read-replica
// routing, because the exactly-once `id > watermark` invariant rests on
// commit-order = id-order and that holds at the write-serialised primary
// but can wobble against a lagging replica.
//
// That premise was TRUE when the audit graded it UNVERIFIED and it is
// still true — sluice builds the URL itself and sets exactly two headers
// — but "true by construction" is the phrase this project has been burned
// by, so it is asserted here instead of asserted in a comment. What
// makes it worth a test rather than an UNVERIFIED PREMISE note is that
// the premise is enforced by absence: adding Sessions routing is a small,
// attractive change (it is the documented way to cut D1 read latency),
// and nothing else in the tree would notice it. This test is what turns
// that edit into a red build, next to the reason it must not be made.
//
// SCOPE, stated so the name is not read as broader than the truth: this
// grades what [d1Client.queryRows] puts on the wire — the one request
// builder every D1 read, write, setup DDL, and CDC poll funnels through
// (both the `d1` cold-start reader and the `d1-trigger` CDC engine reach
// the network only through it). It does not grade the Cloudflare API's
// own default behaviour for that path; that D1 REST `/query` is the
// strongly-consistent primary is Cloudflare's contract, not sluice's, and
// is the residual UNVERIFIED PREMISE here.
func TestD1Transport_NeverRoutesThroughTheSessionsAPI(t *testing.T) {
	// sessionMarkers are the wire surfaces Cloudflare uses to opt a D1
	// query into Sessions/read-replica routing. A request carrying any of
	// them is a request that may be served by a replica.
	sessionHeaders := []string{
		"x-cf-d1-session-commit-token",
		"x-cf-d1-session-constraint",
		"x-cf-d1-session",
	}

	var (
		gotPath    string
		gotHeaders http.Header
		gotBody    string
	)
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotPath = r.URL.Path
		gotHeaders = r.Header.Clone()
		buf := make([]byte, 4096)
		n, _ := r.Body.Read(buf)
		gotBody = string(buf[:n])
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"success":true,"errors":[],"messages":[],"result":[{"success":true,"results":[]}]}`))
	}))
	defer srv.Close()

	conn := D1ConnForTest(srv.URL, "acct", "dbid", "tok")
	if _, err := conn.Query(context.Background(), "SELECT 1"); err != nil {
		t.Fatalf("Query: %v", err)
	}

	// Anti-vacuity: a handler that was never reached would leave every
	// assertion below trivially satisfied.
	if gotPath == "" {
		t.Fatal("the transport never reached the test server; nothing below is measuring anything")
	}
	if want := "/accounts/acct/d1/database/dbid/query"; gotPath != want {
		t.Fatalf("request path = %q; want %q — the D1 REST query endpoint is the "+
			"strongly-consistent primary path, and any other path (notably a "+
			"sessions-scoped one) can be served by a read replica, where the "+
			"trigger-CDC `id > watermark` invariant no longer holds", gotPath, want)
	}
	for _, h := range sessionHeaders {
		if v := gotHeaders.Get(h); v != "" {
			t.Errorf("request carries Sessions-routing header %s=%q — a poll routed to a "+
				"read replica can observe a LOWER change-log id after the watermark has "+
				"passed it, and that change is then never emitted. If Sessions routing is "+
				"genuinely wanted, ADR-0136 §4's safety-lag question has to be answered "+
				"first", h, v)
		}
	}
	if strings.Contains(strings.ToLower(gotBody), "session") ||
		strings.Contains(strings.ToLower(gotBody), "bookmark") {
		t.Errorf("request body carries a session/bookmark field: %s", gotBody)
	}
}

// TestD1EndpointBase_IsNotOperatorSettable pins the other half of the
// argument above. The path assertion only means something if an operator
// cannot re-point the transport at a sessions endpoint from outside the
// code: `endpointBase` is injectable, and if a DSN parameter reached it,
// the premise would be a runtime input rather than a property.
//
// It does not, and this is what says so. [D1ConnForTest] is the only
// injector and its doc already says NOT for production; production
// callers go through [OpenD1Conn], which always takes the default.
func TestD1EndpointBase_IsNotOperatorSettable(t *testing.T) {
	dsns := []string{
		"d1://acct/dbid",
		"d1://acct/dbid?sqlite_date_encoding=iso",
		"d1://acct/dbid?endpoint_base=https://evil.example/sessions",
		"d1://acct/dbid?endpointBase=https://evil.example",
	}
	t.Setenv("CLOUDFLARE_API_TOKEN", "tok")
	for _, dsn := range dsns {
		c, err := openD1Client(dsn)
		if err != nil {
			// An unknown DSN parameter may be refused outright; that is a
			// stronger answer than ignoring it, and equally fine here.
			continue
		}
		if c.endpointBase != defaultD1EndpointBase {
			t.Fatalf("DSN %q set endpointBase = %q; want the compiled-in default %q — "+
				"an operator-settable endpoint makes the primary-path premise a runtime "+
				"input rather than a property of the code", dsn, c.endpointBase, defaultD1EndpointBase)
		}
	}
}
