// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

// Unit tests for the shared PlanetScale control-plane client: the
// auth-header shape, the 429 backoff, the error-envelope decoding,
// and the no-token/no-URL-in-errors guarantees the telemetry pins
// rely on.

package api

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

// newTestClient wires a Client at srv with an instant fake sleep that
// records each backoff wait.
func newTestClient(srv *httptest.Server, waits *[]time.Duration) *Client {
	return New(Config{
		TokenID: "tok-id",
		Token:   "tok-secret",
		BaseURL: srv.URL,
		Sleep: func(_ context.Context, d time.Duration) error {
			if waits != nil {
				*waits = append(*waits, d)
			}
			return nil
		},
	})
}

func TestClient_AuthHeaderShape(t *testing.T) {
	var gotAuth, gotAccept string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotAuth = r.Header.Get("Authorization")
		gotAccept = r.Header.Get("Accept")
		_, _ = w.Write([]byte(`{}`))
	}))
	defer srv.Close()

	var out map[string]any
	if err := newTestClient(srv, nil).Get(context.Background(), "/v1/x", &out); err != nil {
		t.Fatalf("Get: %v", err)
	}
	// The pscale service-token convention: `Authorization: {ID}:{TOKEN}`,
	// no Bearer prefix — the exact shape the telemetry client shipped with.
	if gotAuth != "tok-id:tok-secret" {
		t.Errorf("Authorization = %q; want tok-id:tok-secret", gotAuth)
	}
	if gotAccept != "application/json" {
		t.Errorf("Accept = %q; want application/json", gotAccept)
	}
}

func TestClient_429BackoffThenSuccess(t *testing.T) {
	var calls int
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		calls++
		if calls <= 2 {
			w.Header().Set("Retry-After", "3")
			w.WriteHeader(http.StatusTooManyRequests)
			return
		}
		_, _ = w.Write([]byte(`{"name":"main"}`))
	}))
	defer srv.Close()

	var waits []time.Duration
	br, err := newTestClient(srv, &waits).GetBranch(context.Background(), "o", "d", "main")
	if err != nil {
		t.Fatalf("GetBranch after 429s: %v", err)
	}
	if br.Name != "main" {
		t.Errorf("branch name = %q; want main", br.Name)
	}
	if calls != 3 {
		t.Errorf("HTTP calls = %d; want 3 (two 429s + success)", calls)
	}
	if len(waits) != 2 || waits[0] != 3*time.Second || waits[1] != 3*time.Second {
		t.Errorf("backoff waits = %v; want two 3s waits (Retry-After honoured)", waits)
	}
}

func TestClient_429GivesUpAfterMaxAttempts(t *testing.T) {
	var calls int
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		calls++
		w.WriteHeader(http.StatusTooManyRequests)
	}))
	defer srv.Close()

	var waits []time.Duration
	_, err := newTestClient(srv, &waits).GetBranch(context.Background(), "o", "d", "main")
	if err == nil {
		t.Fatal("want a 429 error after exhausting retries; got nil")
	}
	var se *StatusError
	if !errors.As(err, &se) || se.Status != http.StatusTooManyRequests {
		t.Fatalf("err = %v; want a 429 StatusError", err)
	}
	if calls != maxAttempts {
		t.Errorf("HTTP calls = %d; want %d", calls, maxAttempts)
	}
	// No Retry-After header ⇒ the base delay.
	if len(waits) != maxAttempts-1 || waits[0] != baseRetryDelay {
		t.Errorf("backoff waits = %v; want %d × %s", waits, maxAttempts-1, baseRetryDelay)
	}
}

func TestClient_ErrorEnvelopeDecoded(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusNotFound)
		_ = json.NewEncoder(w).Encode(map[string]string{"code": "not_found", "message": "Not Found"})
	}))
	defer srv.Close()

	_, err := newTestClient(srv, nil).GetBranch(context.Background(), "o", "d", "nope")
	if err == nil {
		t.Fatal("want a 404 error; got nil")
	}
	if !IsNotFound(err) {
		t.Errorf("IsNotFound = false for a 404: %v", err)
	}
	msg := err.Error()
	if !strings.Contains(msg, "404") || !strings.Contains(msg, "not_found") || !strings.Contains(msg, "Not Found") {
		t.Errorf("error %q should carry status + envelope code + message", msg)
	}
	if strings.Contains(msg, "tok-secret") {
		t.Fatalf("error leaks the service token: %q", msg)
	}
}

func TestClient_UnauthorizedNamesTheTokenRemedy(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusUnauthorized)
	}))
	defer srv.Close()

	_, err := newTestClient(srv, nil).GetBranch(context.Background(), "o", "d", "main")
	if err == nil || !strings.Contains(err.Error(), "service token") {
		t.Fatalf("401 error = %v; want it to name the service token", err)
	}
	if strings.Contains(err.Error(), "tok-secret") {
		t.Fatalf("error leaks the service token: %q", err.Error())
	}
}

// TestClient_TransportErrorNeverLeaksURLOrToken pins the audit N-12
// treatment on the shared client: client.Do wraps failures in
// *url.Error, which embeds the full request URL verbatim; the client
// must strip it (the telemetry leak pins depend on this surviving the
// shared-client refactor).
func TestClient_TransportErrorNeverLeaksURLOrToken(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) {}))
	base := srv.URL
	srv.Close() // immediate transport error from now on

	c := New(Config{TokenID: "tok-id", Token: "tok-secret", BaseURL: base})
	_, err := c.GetBranch(context.Background(), "secret-org-name", "d", "main")
	if err == nil {
		t.Fatal("want a transport error against a closed listener; got nil")
	}
	msg := err.Error()
	if strings.Contains(msg, "secret-org-name") || strings.Contains(msg, "organizations") {
		t.Errorf("error carries the request URL: %q", msg)
	}
	if strings.Contains(msg, "tok-secret") {
		t.Fatalf("error leaks the service token: %q", msg)
	}
}

// TestClient_DeployRequestVerbs pins the wire shape of the deploy-
// request calls: paths, methods, and body fields.
func TestClient_DeployRequestVerbs(t *testing.T) {
	type call struct{ method, path, body string }
	var calls []call
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var body strings.Builder
		if r.Body != nil {
			buf := make([]byte, 4096)
			for {
				n, err := r.Body.Read(buf)
				body.Write(buf[:n])
				if err != nil {
					break
				}
			}
		}
		calls = append(calls, call{r.Method, r.URL.Path, body.String()})
		_, _ = w.Write([]byte(`{"number":7,"state":"open","deployment_state":"pending","html_url":"https://x/7"}`))
	}))
	defer srv.Close()

	c := newTestClient(srv, nil)
	ctx := context.Background()
	dr, err := c.CreateDeployRequest(ctx, "o", "d", "dev", "main")
	if err != nil {
		t.Fatalf("CreateDeployRequest: %v", err)
	}
	if dr.Number != 7 || dr.HTMLURL != "https://x/7" {
		t.Errorf("dr = %+v; want number 7 + html_url decoded", dr)
	}
	if _, err := c.GetDeployRequest(ctx, "o", "d", 7); err != nil {
		t.Fatalf("GetDeployRequest: %v", err)
	}
	if _, err := c.Deploy(ctx, "o", "d", 7); err != nil {
		t.Fatalf("Deploy: %v", err)
	}
	if _, err := c.SkipRevert(ctx, "o", "d", 7); err != nil {
		t.Fatalf("SkipRevert: %v", err)
	}
	if err := c.DeleteBranch(ctx, "o", "d", "dev"); err != nil {
		t.Fatalf("DeleteBranch: %v", err)
	}

	want := []call{
		{"POST", "/v1/organizations/o/databases/d/deploy-requests", `{"branch":"dev","into_branch":"main"}`},
		{"GET", "/v1/organizations/o/databases/d/deploy-requests/7", ""},
		{"POST", "/v1/organizations/o/databases/d/deploy-requests/7/deploy", ""},
		{"POST", "/v1/organizations/o/databases/d/deploy-requests/7/skip-revert", ""},
		{"DELETE", "/v1/organizations/o/databases/d/branches/dev", ""},
	}
	if len(calls) != len(want) {
		t.Fatalf("calls = %d; want %d", len(calls), len(want))
	}
	for i := range want {
		if calls[i] != want[i] {
			t.Errorf("call[%d] = %+v; want %+v", i, calls[i], want[i])
		}
	}
}
