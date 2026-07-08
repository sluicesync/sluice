// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package telemetry

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

// closedServer returns the URL and host:port of a loopback listener
// that has already been closed, so any request fails with an immediate
// transport error — the *url.Error shape that pre-fix embedded the full
// request URL (signed sig param included) in err.Error().
func closedServer(t *testing.T) (baseURL, hostPort string) {
	t.Helper()
	srv := httptest.NewServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) {}))
	baseURL = srv.URL
	hostPort = strings.TrimPrefix(srv.URL, "http://")
	srv.Close()
	return baseURL, hostPort
}

// TestScrape_TransportErrorNeverLeaksSignedURL pins the audit N-12 fix
// on the scrape leg: the scrape URL is SIGNED (sig + exp query params
// — a bearer credential until exp), and client.Do wraps failures in a
// *url.Error that embeds it verbatim. The provider's poll loop logs
// err.Error(), so neither the signature nor the metrics path may appear
// in the returned error string.
func TestScrape_TransportErrorNeverLeaksSignedURL(t *testing.T) {
	_, hostPort := closedServer(t)
	c := &client{httpClient: &http.Client{Timeout: 2 * time.Second}}
	target := sdTarget{
		Targets: []string{hostPort},
		Labels: map[string]string{
			sdLabelScheme:      "http",
			sdLabelMetricsPath: "/secret-metrics-path",
			sdLabelParamSig:    "SECRET-SIG-VALUE",
			sdLabelParamExp:    "1750000000",
		},
	}
	_, err := c.scrape(context.Background(), target)
	if err == nil {
		t.Fatal("expected a transport error against a closed listener; got nil")
	}
	if strings.Contains(err.Error(), "SECRET-SIG-VALUE") {
		t.Fatalf("error leaks the signed sig param (the N-12 leak): %q", err.Error())
	}
	if strings.Contains(err.Error(), "secret-metrics-path") {
		t.Fatalf("error leaks the signed metrics path: %q", err.Error())
	}
}

// TestDiscover_TransportErrorStripsRequestURL is the sibling pin on the
// SD leg: the org-scoped SD URL is not a credential, but the same
// *url.Error wrapper carries it; keep both legs on the stripped shape.
func TestDiscover_TransportErrorStripsRequestURL(t *testing.T) {
	baseURL, _ := closedServer(t)
	c := &client{
		httpClient: &http.Client{Timeout: 2 * time.Second},
		baseURL:    baseURL,
		org:        "leaky-org-name",
		tokenID:    "tok-id",
		token:      "tok-secret",
	}
	_, err := c.discover(context.Background(), "db", "main")
	if err == nil {
		t.Fatal("expected a transport error against a closed listener; got nil")
	}
	if strings.Contains(err.Error(), "leaky-org-name") || strings.Contains(err.Error(), "organizations") {
		t.Errorf("error should not carry the SD request URL: %q", err.Error())
	}
	if strings.Contains(err.Error(), "tok-secret") {
		t.Fatalf("error leaks the service token: %q", err.Error())
	}
}
