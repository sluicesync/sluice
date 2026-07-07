// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package fleettui

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

func TestNormalizeURL(t *testing.T) {
	cases := []struct {
		name    string
		in      string
		want    string
		wantErr bool
	}{
		{name: "host:port", in: "localhost:9300", want: "http://localhost:9300/api/fleet"},
		{name: "bare port", in: ":9300", want: "http://:9300/api/fleet"},
		{name: "http scheme", in: "http://host:9300", want: "http://host:9300/api/fleet"},
		{name: "https scheme", in: "https://host:9300", want: "https://host:9300/api/fleet"},
		{name: "full api/fleet url", in: "http://host:9300/api/fleet", want: "http://host:9300/api/fleet"},
		{name: "https full api/fleet url", in: "https://host/api/fleet", want: "https://host/api/fleet"},
		{name: "trailing slash", in: "http://host:9300/", want: "http://host:9300/api/fleet"},
		{name: "whitespace trimmed", in: "  localhost:9300  ", want: "http://localhost:9300/api/fleet"},
		{name: "empty", in: "", wantErr: true},
		{name: "whitespace only", in: "   ", wantErr: true},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			got, err := NormalizeURL(c.in)
			if c.wantErr {
				if err == nil {
					t.Fatalf("NormalizeURL(%q) = %q, want error", c.in, got)
				}
				return
			}
			if err != nil {
				t.Fatalf("NormalizeURL(%q) unexpected error: %v", c.in, err)
			}
			if got != c.want {
				t.Fatalf("NormalizeURL(%q) = %q, want %q", c.in, got, c.want)
			}
		})
	}
}

// TestNormalizeURL_InvalidConnectDoesNotLeakCredential pins the
// credential-in-logs fix at the fleettui site: a connect address that
// carries basic-auth and fails url.Parse must not echo the userinfo in
// its error. url.Parse embeds the raw input verbatim; NormalizeURL
// routes it through diagnose.SafeParseError to strip it.
func TestNormalizeURL_InvalidConnectDoesNotLeakCredential(t *testing.T) {
	const secret = "SUPERSECRET"
	// The \x7f control byte makes url.Parse fail after it has captured
	// the userinfo.
	_, err := NormalizeURL("http://admin:" + secret + "@host\x7f/api/fleet")
	if err == nil {
		t.Fatal("expected NormalizeURL to reject the control-char address")
	}
	if strings.Contains(err.Error(), secret) {
		t.Errorf("NormalizeURL leaked the credential in its error: %q", err.Error())
	}
}

func TestHTTPFetcherParsesReport(t *testing.T) {
	const body = `{"generated_at":"2026-06-26T15:04:05Z","syncs":[` +
		`{"id":"orders","state":"running","consecutive_failures":0,"restarts":1,"last_error":"","last_start":"2026-06-26T15:00:00Z","since":"2026-06-26T15:00:00Z","seconds_in_state":245.5},` +
		`{"id":"users","state":"failed","consecutive_failures":3,"restarts":3,"last_error":"slot in use","last_start":"","since":"2026-06-26T15:03:00Z","seconds_in_state":5}]}`

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/api/fleet" {
			w.WriteHeader(http.StatusNotFound)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(body))
	}))
	defer srv.Close()

	endpoint, err := NormalizeURL(srv.URL)
	if err != nil {
		t.Fatalf("normalize: %v", err)
	}
	rep, err := HTTPFetcher(endpoint, 5*time.Second)(context.Background())
	if err != nil {
		t.Fatalf("fetch: %v", err)
	}
	if rep.GeneratedAt != "2026-06-26T15:04:05Z" {
		t.Errorf("generated_at = %q", rep.GeneratedAt)
	}
	if len(rep.Syncs) != 2 {
		t.Fatalf("syncs = %d, want 2", len(rep.Syncs))
	}
	if rep.Syncs[0].ID != "orders" || rep.Syncs[0].State != "running" || rep.Syncs[0].Restarts != 1 {
		t.Errorf("row 0 = %+v", rep.Syncs[0])
	}
	if rep.Syncs[1].ConsecutiveFailures != 3 || rep.Syncs[1].LastError != "slot in use" {
		t.Errorf("row 1 = %+v", rep.Syncs[1])
	}
	if rep.Syncs[0].SecondsInState != 245.5 {
		t.Errorf("seconds_in_state = %v, want 245.5", rep.Syncs[0].SecondsInState)
	}
}

func TestHTTPFetcherNon200IsError(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
	}))
	defer srv.Close()

	endpoint, _ := NormalizeURL(srv.URL)
	if _, err := HTTPFetcher(endpoint, 5*time.Second)(context.Background()); err == nil {
		t.Fatalf("non-200 should be an error")
	}
}

func TestHTTPFetcherBadBodyIsError(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write([]byte("not json"))
	}))
	defer srv.Close()

	endpoint, _ := NormalizeURL(srv.URL)
	if _, err := HTTPFetcher(endpoint, 5*time.Second)(context.Background()); err == nil {
		t.Fatalf("malformed body should be an error")
	}
}

func TestHTTPFetcherUnreachableIsError(t *testing.T) {
	// A closed server's address: connection refused.
	srv := httptest.NewServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) {}))
	endpoint, _ := NormalizeURL(srv.URL)
	srv.Close()

	if _, err := HTTPFetcher(endpoint, 1*time.Second)(context.Background()); err == nil {
		t.Fatalf("unreachable server should be an error")
	}
}
