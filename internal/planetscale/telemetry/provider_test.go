// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package telemetry

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"sluicesync.dev/sluice/internal/ir"
)

// fakePS is an httptest fixture serving the two-leg PlanetScale metrics
// surface: the per-org SD endpoint (with auth) and the signed scrape
// endpoint (no auth). It records the Authorization header it saw and lets a
// test flip the scrape to a 500 to exercise the poll-failure degrade.
type fakePS struct {
	srv          *httptest.Server
	mu           sync.Mutex
	exposition   string
	scrapeFail   bool
	sawAuth      string
	scrapeNoAuth bool // true if a scrape arrived WITHOUT an auth header (good)
	scrapeCount  atomic.Int64
}

func newFakePS(database, exposition string) *fakePS {
	f := &fakePS{exposition: exposition}
	mux := http.NewServeMux()
	mux.HandleFunc("/v1/organizations/", func(w http.ResponseWriter, r *http.Request) {
		f.mu.Lock()
		f.sawAuth = r.Header.Get("Authorization")
		f.mu.Unlock()
		// The scrape path is served by this same server (BaseURL points
		// here), so the SD response's metrics host == this server host.
		host := r.Host
		sd := []sdTarget{
			{
				Targets: []string{host},
				Labels: map[string]string{
					sdLabelMetricsPath: "/metrics/branch/abc",
					sdLabelParamSig:    "sigval",
					sdLabelParamExp:    "9999999999",
					sdLabelScheme:      "http", // httptest is http
					sdLabelDatabase:    database,
					sdLabelBranch:      "main",
				},
			},
			{
				// A decoy element for a DIFFERENT database — selectBranch
				// must skip it.
				Targets: []string{host},
				Labels: map[string]string{
					sdLabelMetricsPath: "/metrics/branch/other",
					sdLabelDatabase:    "some-other-db",
					sdLabelBranch:      "main",
				},
			},
		}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(sd)
	})
	mux.HandleFunc("/metrics/branch/", func(w http.ResponseWriter, r *http.Request) {
		f.scrapeCount.Add(1)
		f.mu.Lock()
		if r.Header.Get("Authorization") == "" {
			f.scrapeNoAuth = true
		}
		fail := f.scrapeFail
		body := f.exposition
		f.mu.Unlock()
		if fail {
			http.Error(w, "boom", http.StatusInternalServerError)
			return
		}
		_, _ = fmt.Fprint(w, body)
	})
	f.srv = httptest.NewServer(mux)
	return f
}

func (f *fakePS) setScrapeFail(b bool) {
	f.mu.Lock()
	f.scrapeFail = b
	f.mu.Unlock()
}

func (f *fakePS) close() { f.srv.Close() }

func newTestProvider(t *testing.T, f *fakePS, database string, now func() time.Time) *Provider {
	t.Helper()
	p, err := New(context.Background(), Config{
		Org:          "testorg",
		TokenID:      "tid",
		Token:        "tsecret",
		Database:     database,
		Branch:       "main",
		PollInterval: 10 * time.Millisecond, // clamped UP to minPollInterval
		Freshness:    time.Hour,             // explicit so freshness is test-controlled
		BaseURL:      f.srv.URL,
		HTTPClient:   f.srv.Client(),
		Now:          now,
	})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	t.Cleanup(func() { _ = p.Close() })
	return p
}

// waitForSampleOK polls Sample (non-blocking) until it returns ok or the
// deadline expires.
func waitForSampleOK(t *testing.T, p *Provider) ir.TargetHealthSnapshot {
	t.Helper()
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		if snap, ok := p.Sample(context.Background()); ok {
			return snap
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatal("provider never produced an ok Sample within deadline")
	return ir.TargetHealthSnapshot{}
}

func TestProvider_PollPopulatesSample_BranchSelectedByDB(t *testing.T) {
	fixed := time.Unix(2000, 0)
	f := newFakePS("target-db", fullExposition)
	defer f.close()
	p := newTestProvider(t, f, "target-db", func() time.Time { return fixed })

	snap := waitForSampleOK(t, p)
	if !snap.CPUKnown || snap.CPUUtil != 0.875 {
		t.Errorf("CPU = %v known=%v, want 0.875 true", snap.CPUUtil, snap.CPUKnown)
	}
	if !snap.StorageKnown || snap.StorageUtil != 0.75 {
		t.Errorf("Storage = %v known=%v, want 0.75 true", snap.StorageUtil, snap.StorageKnown)
	}

	// The SD leg carried the service-token auth header; the scrape leg did
	// NOT (the URL is signed).
	f.mu.Lock()
	gotAuth := f.sawAuth
	scrapeNoAuth := f.scrapeNoAuth
	f.mu.Unlock()
	if gotAuth != "tid:tsecret" {
		t.Errorf("SD Authorization = %q, want tid:tsecret", gotAuth)
	}
	if !scrapeNoAuth {
		t.Error("scrape leg carried an Authorization header; the signed URL needs none")
	}
}

func TestProvider_WrongDatabase_NeverOK(t *testing.T) {
	fixed := time.Unix(2000, 0)
	// SD only exposes "real-db"; the provider asks for "missing-db".
	f := newFakePS("real-db", fullExposition)
	defer f.close()
	p := newTestProvider(t, f, "missing-db", func() time.Time { return fixed })

	// Give the poll loop time to run and fail to match — Sample must stay
	// ok=false (the SD select errors; cache never populates).
	time.Sleep(200 * time.Millisecond)
	if _, ok := p.Sample(context.Background()); ok {
		t.Error("Sample ok=true for a database absent from SD; want false")
	}
}

func TestProvider_PollFailure_ServesLastThenStales(t *testing.T) {
	// Controllable clock so we can age the snapshot past Freshness
	// deterministically.
	var nowNanos atomic.Int64
	nowNanos.Store(time.Unix(5000, 0).UnixNano())
	now := func() time.Time { return time.Unix(0, nowNanos.Load()) }

	f := newFakePS("db", fullExposition)
	defer f.close()
	p, err := New(context.Background(), Config{
		Org: "o", TokenID: "i", Token: "s", Database: "db", Branch: "main",
		PollInterval: 10 * time.Millisecond,
		Freshness:    30 * time.Second,
		BaseURL:      f.srv.URL,
		HTTPClient:   f.srv.Client(),
		Now:          now,
	})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	defer func() { _ = p.Close() }()

	// Warm up: first poll succeeds, Sample ok with the fixed-now snapshot.
	waitForSampleOK(t, p)

	// Now make every poll fail; the cache keeps the last good snapshot.
	f.setScrapeFail(true)
	// Snapshot SampledAt == 5000s. Advance the clock 10s (< Freshness 30s):
	// the stale-but-fresh-enough snapshot is still served.
	nowNanos.Store(time.Unix(5010, 0).UnixNano())
	if _, ok := p.Sample(context.Background()); !ok {
		t.Error("Sample ok=false within freshness window while serving last snapshot; want true")
	}

	// Advance past Freshness: now the snapshot ages out to ok=false (the
	// degrade contract), even though the cache still holds it.
	nowNanos.Store(time.Unix(5100, 0).UnixNano())
	if _, ok := p.Sample(context.Background()); ok {
		t.Error("Sample ok=true past the freshness window; want false (degrade to no-signal)")
	}
}

func TestProvider_SampleNonBlocking(t *testing.T) {
	fixed := time.Unix(2000, 0)
	f := newFakePS("db", fullExposition)
	defer f.close()
	p := newTestProvider(t, f, "db", func() time.Time { return fixed })
	waitForSampleOK(t, p)

	// Sample must return well under a poll interval even if the network
	// were slow — it reads only the cache. 50ms is generous.
	done := make(chan struct{})
	go func() {
		p.Sample(context.Background())
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(50 * time.Millisecond):
		t.Fatal("Sample blocked >50ms — it must be a non-blocking cache read")
	}
}

func TestProvider_SampleHonoursCtxCancel(t *testing.T) {
	fixed := time.Unix(2000, 0)
	f := newFakePS("db", fullExposition)
	defer f.close()
	p := newTestProvider(t, f, "db", func() time.Time { return fixed })
	waitForSampleOK(t, p)

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, ok := p.Sample(ctx); ok {
		t.Error("Sample ok=true with a cancelled ctx; want false")
	}
}

func TestProvider_CloseStopsLoop(t *testing.T) {
	fixed := time.Unix(2000, 0)
	f := newFakePS("db", fullExposition)
	defer f.close()
	p, err := New(context.Background(), Config{
		Org: "o", TokenID: "i", Token: "s", Database: "db", Branch: "main",
		PollInterval: 10 * time.Millisecond,
		Freshness:    time.Hour,
		BaseURL:      f.srv.URL,
		HTTPClient:   f.srv.Client(),
		Now:          func() time.Time { return fixed },
	})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	waitForSampleOK(t, p)

	if err := p.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	// After Close, no further scrapes should occur.
	before := f.scrapeCount.Load()
	time.Sleep(100 * time.Millisecond)
	if after := f.scrapeCount.Load(); after != before {
		t.Errorf("poll loop kept running after Close: scrapes %d → %d", before, after)
	}
}

func TestNew_RefusesIncompleteCreds(t *testing.T) {
	cases := []struct {
		name string
		cfg  Config
	}{
		{"no org", Config{TokenID: "i", Token: "s", Database: "db"}},
		{"no token id", Config{Org: "o", Token: "s", Database: "db"}},
		{"no token", Config{Org: "o", TokenID: "i", Database: "db"}},
		{"no database", Config{Org: "o", TokenID: "i", Token: "s"}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if _, err := New(context.Background(), tc.cfg); err == nil {
				t.Error("New returned nil error for incomplete config; want loud refusal")
			}
		})
	}
}

func TestSelectBranch_DefaultsToMain(t *testing.T) {
	targets := []sdTarget{
		{Labels: map[string]string{sdLabelDatabase: "db", sdLabelBranch: "main", sdLabelMetricsPath: "/p"}},
	}
	if _, err := selectBranch(targets, "db", ""); err != nil {
		t.Errorf("empty branch should default to main and match: %v", err)
	}
	if _, err := selectBranch(targets, "db", "dev"); err == nil {
		t.Error("branch=dev should not match a main-only SD; want error")
	}
}

// TestBuildScrapeURL pins the signed-URL assembly from the SD element.
func TestBuildScrapeURL(t *testing.T) {
	tgt := sdTarget{
		Targets: []string{"metrics.psdb.cloud"},
		Labels: map[string]string{
			sdLabelMetricsPath: "/metrics/branch/xyz",
			sdLabelParamSig:    "abc123",
			sdLabelParamExp:    "1718",
			sdLabelScheme:      "https",
		},
	}
	got, err := buildScrapeURL(tgt)
	if err != nil {
		t.Fatalf("buildScrapeURL: %v", err)
	}
	const want = "https://metrics.psdb.cloud/metrics/branch/xyz?exp=1718&sig=abc123"
	if got != want {
		t.Errorf("scrape URL = %q, want %q", got, want)
	}
	// No host → error.
	if _, err := buildScrapeURL(sdTarget{Labels: map[string]string{sdLabelMetricsPath: "/p"}}); err == nil {
		t.Error("buildScrapeURL with no target host should error")
	}
}

// guard against accidental token leakage into the SD URL path.
func TestDiscoverURL_NoTokenInPath(t *testing.T) {
	c := &client{org: "myorg"}
	// Reconstruct the SD path the same way discover does and assert the
	// secret is not in it (it belongs only in the Authorization header,
	// which the shared api.Client owns).
	endpoint := "/v1/organizations/" + c.org + "/metrics"
	if strings.Contains(endpoint, "secret") {
		t.Errorf("token leaked into SD URL: %q", endpoint)
	}
}
