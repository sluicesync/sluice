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

// pgOnlyExposition is a Postgres branch's scrape WITHOUT the Vitess decoys
// pgExposition carries — i.e. the shape the live endpoint actually serves
// (probed 2026-07-24: a PG branch carries planetscale_volume_* and NO
// planetscale_vttablet_* series at all).
const pgOnlyExposition = `
planetscale_pods_cpu_util_percentages{planetscale_component="hzinstance",planetscale_container="postgres"} 62
planetscale_pods_mem_util_percentages{planetscale_component="hzinstance",planetscale_container="postgres"} 55
planetscale_volume_available_bytes 40000000000
planetscale_volume_capacity_bytes 160000000000
`

// --- (b) discovery-source + engine-detection unit matrix ---

// TestMetricNamesForExposition_EngineMarkerMatrix pins the per-target engine
// resolution across every shape the fan-out can meet. The org-wide watch has
// no per-database engine label to read (the SD document carries none —
// ground-truthed live), so this marker scan is what keeps a MIXED org honest.
func TestMetricNamesForExposition_EngineMarkerMatrix(t *testing.T) {
	for _, tc := range []struct {
		name     string
		text     string
		declared metricNames
		want     metricNames
	}{
		{"vitess exposition ⇒ mysql table", fullExposition, postgresMetricNames, mysqlMetricNames},
		{"postgres exposition ⇒ pg table", pgOnlyExposition, mysqlMetricNames, postgresMetricNames},
		{"neither marker ⇒ declared table", "some_other_metric 1\n", postgresMetricNames, postgresMetricNames},
		{"empty exposition ⇒ declared table", "", mysqlMetricNames, mysqlMetricNames},
		// Ambiguity must NOT be resolved by listing order: both markers
		// present falls back to the declared table rather than letting the
		// first-seen series decide.
		{"both markers ⇒ declared table", pgExposition, mysqlMetricNames, mysqlMetricNames},
		{"both markers, pg declared ⇒ pg", pgExposition, postgresMetricNames, postgresMetricNames},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got := metricNamesForExposition(parsePromText(strings.NewReader(tc.text)), tc.declared)
			if got != tc.want {
				t.Errorf("metricNamesForExposition = %+v, want %+v", got, tc.want)
			}
		})
	}
}

// TestFleetFilter_Matrix pins the include/exclude scoping across pattern
// shapes: bare name, glob, database/branch form, and the combined case whose
// precedence (exclude wins) diverges deliberately from --include-table.
func TestFleetFilter_Matrix(t *testing.T) {
	target := func(db, br string) ir.FleetTarget { return ir.FleetTarget{Database: db, Branch: br} }
	for _, tc := range []struct {
		name    string
		include []string
		exclude []string
		want    map[ir.FleetTarget]bool
	}{
		{
			name: "no filters ⇒ everything",
			want: map[ir.FleetTarget]bool{
				target("shop", "main"): true, target("blog", "dev"): true,
			},
		},
		{
			name:    "include exact database",
			include: []string{"shop"},
			want: map[ir.FleetTarget]bool{
				target("shop", "main"): true, target("shop", "dev"): true, target("blog", "main"): false,
			},
		},
		{
			name:    "include glob",
			include: []string{"shop-*"},
			want: map[ir.FleetTarget]bool{
				target("shop-eu", "main"): true, target("shop", "main"): false, target("blog", "main"): false,
			},
		},
		{
			name:    "include database/branch pins one branch",
			include: []string{"shop/main"},
			want: map[ir.FleetTarget]bool{
				target("shop", "main"): true, target("shop", "dev"): false,
			},
		},
		{
			name:    "exclude only",
			exclude: []string{"scratch-*"},
			want: map[ir.FleetTarget]bool{
				target("shop", "main"): true, target("scratch-1", "main"): false,
			},
		},
		{
			name:    "combined — exclude wins over include",
			include: []string{"shop-*"},
			exclude: []string{"shop-test"},
			want: map[ir.FleetTarget]bool{
				target("shop-eu", "main"): true, target("shop-test", "main"): false, target("blog", "main"): false,
			},
		},
		{
			name:    "exclude a single branch of an included database",
			include: []string{"shop"},
			exclude: []string{"shop/dev"},
			want: map[ir.FleetTarget]bool{
				target("shop", "main"): true, target("shop", "dev"): false,
			},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			f, err := newFleetFilter(tc.include, tc.exclude)
			if err != nil {
				t.Fatalf("newFleetFilter: %v", err)
			}
			for tgt, want := range tc.want {
				if got := f.allows(tgt); got != want {
					t.Errorf("allows(%s) = %v, want %v", tgt, got, want)
				}
			}
		})
	}
}

// TestNewFleetFilter_BadPatternIsALoudRefusal pins that a malformed glob
// fails at construction rather than silently matching nothing (which would
// present as an empty fleet with no explanation).
func TestNewFleetFilter_BadPatternIsALoudRefusal(t *testing.T) {
	if _, err := newFleetFilter([]string{"["}, nil); err == nil {
		t.Error("a malformed --include-database pattern must be refused")
	}
	if _, err := newFleetFilter(nil, []string{"["}); err == nil {
		t.Error("a malformed --exclude-database pattern must be refused")
	}
}

// --- (b) end-to-end fan-out against a fake PlanetScale ---

// fakeFleetPS serves the org-wide SD document plus a per-branch scrape whose
// body depends on the branch path, so one fixture can present a MIXED org
// (Vitess + Postgres databases) exactly as the live `sluicesync` org does.
type fakeFleetPS struct {
	srv *httptest.Server

	mu          sync.Mutex
	elements    []sdTarget
	bodies      map[string]string // metrics path → exposition
	failPaths   map[string]bool   // metrics path → serve a 500
	discoverErr bool

	scrapes     atomic.Int64
	inFlight    atomic.Int64
	maxInFlight atomic.Int64
	scrapeHold  time.Duration
}

func newFakeFleetPS(t *testing.T) *fakeFleetPS {
	t.Helper()
	f := &fakeFleetPS{bodies: map[string]string{}, failPaths: map[string]bool{}}
	mux := http.NewServeMux()
	mux.HandleFunc("/v1/organizations/", func(w http.ResponseWriter, r *http.Request) {
		f.mu.Lock()
		fail := f.discoverErr
		els := make([]sdTarget, len(f.elements))
		copy(els, f.elements)
		f.mu.Unlock()
		if fail {
			http.Error(w, "boom", http.StatusInternalServerError)
			return
		}
		for i := range els {
			els[i].Targets = []string{r.Host}
		}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(els)
	})
	mux.HandleFunc("/metrics/", func(w http.ResponseWriter, r *http.Request) {
		f.scrapes.Add(1)
		cur := f.inFlight.Add(1)
		for {
			maximum := f.maxInFlight.Load()
			if cur <= maximum || f.maxInFlight.CompareAndSwap(maximum, cur) {
				break
			}
		}
		defer f.inFlight.Add(-1)
		f.mu.Lock()
		body, ok := f.bodies[r.URL.Path]
		fail := f.failPaths[r.URL.Path]
		hold := f.scrapeHold
		f.mu.Unlock()
		if hold > 0 {
			time.Sleep(hold)
		}
		if fail || !ok {
			http.Error(w, "boom", http.StatusInternalServerError)
			return
		}
		_, _ = fmt.Fprint(w, body)
	})
	f.srv = httptest.NewServer(mux)
	t.Cleanup(f.srv.Close)
	return f
}

// add registers one database+branch with its exposition body.
func (f *fakeFleetPS) add(database, branch, exposition string) {
	path := "/metrics/" + database + "/" + branch
	f.mu.Lock()
	defer f.mu.Unlock()
	f.elements = append(f.elements, sdTarget{
		Labels: map[string]string{
			sdLabelMetricsPath: path,
			sdLabelParamSig:    "sigval",
			sdLabelParamExp:    "9999999999",
			sdLabelScheme:      "http",
			sdLabelDatabase:    database,
			sdLabelBranch:      branch,
		},
	})
	f.bodies[path] = exposition
}

func (f *fakeFleetPS) setScrapeFail(database, branch string, fail bool) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.failPaths["/metrics/"+database+"/"+branch] = fail
}

func (f *fakeFleetPS) setDiscoverErr(b bool) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.discoverErr = b
}

func newTestFleet(t *testing.T, f *fakeFleetPS, cfg FleetConfig) *Fleet {
	t.Helper()
	cfg.Org = "testorg"
	cfg.TokenID = "tid"
	cfg.Token = "tsecret"
	if cfg.PollInterval == 0 {
		cfg.PollInterval = 10 * time.Millisecond // clamped UP to minPollInterval
	}
	if cfg.Freshness == 0 {
		cfg.Freshness = time.Hour
	}
	cfg.BaseURL = f.srv.URL
	cfg.HTTPClient = f.srv.Client()
	fleet, err := NewFleet(context.Background(), cfg)
	if err != nil {
		t.Fatalf("NewFleet: %v", err)
	}
	t.Cleanup(func() { _ = fleet.Close() })
	return fleet
}

// waitForFleet polls SampleFleet until want targets report OK, or fails.
func waitForFleet(t *testing.T, fleet *Fleet, wantOK int) []ir.FleetHealthSample {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		samples := fleet.SampleFleet(context.Background())
		ok := 0
		for _, s := range samples {
			if s.OK {
				ok++
			}
		}
		if ok >= wantOK {
			return samples
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatalf("fleet never reported %d ok targets within the deadline", wantOK)
	return nil
}

// TestFleet_MixedEngineOrgDistillsEachTargetCorrectly is the crux pin for
// (b): ONE service-discovery call enumerates a MIXED org, and each branch is
// distilled with ITS OWN engine's metric-name table — the Vitess database off
// planetscale_vttablet_volume_*, the Postgres one off planetscale_volume_* —
// even though a single --engine was declared for the whole watch.
func TestFleet_MixedEngineOrgDistillsEachTargetCorrectly(t *testing.T) {
	f := newFakeFleetPS(t)
	f.add("shop-mysql", "main", fullExposition)
	f.add("shop-pg", "main", pgOnlyExposition)

	// Declare the MySQL table: the PG branch must still read correctly.
	fleet := newTestFleet(t, f, FleetConfig{Engine: "mysql"})
	samples := waitForFleet(t, fleet, 2)

	if len(samples) != 2 {
		t.Fatalf("want 2 discovered targets, got %d", len(samples))
	}
	// Stable order: sorted by database.
	if samples[0].Target.Database != "shop-mysql" || samples[1].Target.Database != "shop-pg" {
		t.Fatalf("targets must be in stable (database, branch) order, got %v %v", samples[0].Target, samples[1].Target)
	}
	mysql, pg := samples[0].Snapshot, samples[1].Snapshot
	if !mysql.StorageKnown || mysql.StorageUtil != 0.75 {
		t.Errorf("vitess target storage = %v known=%v, want 0.75 true (vttablet volume names)", mysql.StorageUtil, mysql.StorageKnown)
	}
	if !mysql.CPUKnown || mysql.CPUUtil != 0.875 {
		t.Errorf("vitess target cpu = %v known=%v, want 0.875 true", mysql.CPUUtil, mysql.CPUKnown)
	}
	if !pg.StorageKnown || pg.StorageUtil != 0.75 {
		t.Errorf("postgres target storage = %v known=%v, want 0.75 true (planetscale_volume_* names)", pg.StorageUtil, pg.StorageKnown)
	}
	if !pg.CPUKnown || pg.CPUUtil != 0.62 {
		t.Errorf("postgres target cpu = %v known=%v, want 0.62 true (postgres container selection)", pg.CPUUtil, pg.CPUKnown)
	}
}

// TestFleet_FiltersScopeTheFanOut pins that --include/--exclude actually
// reduce the polled set (not merely the reported one): an excluded database
// is never scraped.
func TestFleet_FiltersScopeTheFanOut(t *testing.T) {
	f := newFakeFleetPS(t)
	f.add("shop-eu", "main", fullExposition)
	f.add("shop-us", "main", fullExposition)
	f.add("scratch", "main", fullExposition)

	fleet := newTestFleet(t, f, FleetConfig{Include: []string{"shop-*"}, Exclude: []string{"shop-us"}})
	samples := waitForFleet(t, fleet, 1)
	if len(samples) != 1 || samples[0].Target.Database != "shop-eu" {
		t.Fatalf("want only shop-eu in the fan-out, got %v", samples)
	}
}

// TestFleet_BranchFilterDefaultsToEveryBranch pins the deliberate divergence
// from the single-database provider: an unset branch means ALL branches
// org-wide (a fleet watch that silently skipped non-main branches would
// under-report), while a set branch restricts the fan-out.
func TestFleet_BranchFilterDefaultsToEveryBranch(t *testing.T) {
	f := newFakeFleetPS(t)
	f.add("shop", "main", fullExposition)
	f.add("shop", "dev", fullExposition)

	all := newTestFleet(t, f, FleetConfig{})
	if got := waitForFleet(t, all, 2); len(got) != 2 {
		t.Fatalf("unset branch must watch every branch, got %v", got)
	}

	f2 := newFakeFleetPS(t)
	f2.add("shop", "main", fullExposition)
	f2.add("shop", "dev", fullExposition)
	pinned := newTestFleet(t, f2, FleetConfig{Branch: "dev"})
	samples := waitForFleet(t, pinned, 1)
	if len(samples) != 1 || samples[0].Target.Branch != "dev" {
		t.Fatalf("branch filter must restrict the fan-out, got %v", samples)
	}
}

// TestFleet_PerTargetScrapeFailureDegradesOnlyThatTarget pins the per-target
// degrade: one branch's scrape failing must not blank the rest of the fleet,
// and the failing target keeps serving its last snapshot until freshness ages
// it out.
func TestFleet_PerTargetScrapeFailureDegradesOnlyThatTarget(t *testing.T) {
	f := newFakeFleetPS(t)
	f.add("good", "main", fullExposition)
	f.add("flaky", "main", fullExposition)

	fleet := newTestFleet(t, f, FleetConfig{})
	waitForFleet(t, fleet, 2)

	f.setScrapeFail("flaky", "main", true)
	// Let several polls run with the failure in place.
	time.Sleep(300 * time.Millisecond)

	samples := fleet.SampleFleet(context.Background())
	if len(samples) != 2 {
		t.Fatalf("a failing target must stay DISCOVERED, got %d samples", len(samples))
	}
	for _, s := range samples {
		if !s.OK {
			t.Errorf("%s should still serve its carried-forward snapshot inside the freshness window", s.Target)
		}
	}
}

// TestFleet_StaleTargetReportsNotOK pins that the carried-forward snapshot
// does age out: past the freshness window the target is reported with
// OK=false rather than a silently stale reading.
func TestFleet_StaleTargetReportsNotOK(t *testing.T) {
	var nowNanos atomic.Int64
	nowNanos.Store(time.Unix(5000, 0).UnixNano())
	clock := func() time.Time { return time.Unix(0, nowNanos.Load()) }

	f := newFakeFleetPS(t)
	f.add("shop", "main", fullExposition)
	fleet := newTestFleet(t, f, FleetConfig{Freshness: time.Minute, Now: clock})
	waitForFleet(t, fleet, 1)

	nowNanos.Add(int64(2 * time.Minute))
	samples := fleet.SampleFleet(context.Background())
	if len(samples) != 1 {
		t.Fatalf("want the target still discovered, got %d", len(samples))
	}
	if samples[0].OK {
		t.Error("a snapshot past the freshness window must report OK=false, not a stale reading")
	}
}

// TestFleet_DiscoveryFailureKeepsTheLastKnownFleet pins that a control-plane
// blip does not empty the fleet (which would look like "the org has no
// databases").
func TestFleet_DiscoveryFailureKeepsTheLastKnownFleet(t *testing.T) {
	f := newFakeFleetPS(t)
	f.add("shop", "main", fullExposition)
	fleet := newTestFleet(t, f, FleetConfig{})
	waitForFleet(t, fleet, 1)

	f.setDiscoverErr(true)
	time.Sleep(200 * time.Millisecond)
	if got := fleet.SampleFleet(context.Background()); len(got) != 1 {
		t.Fatalf("a discovery failure must keep the last known fleet, got %d targets", len(got))
	}
}

// TestFleet_BoundedConcurrency pins the rate-limit guard: no more than
// Concurrency scrapes are ever in flight, however large the org.
func TestFleet_BoundedConcurrency(t *testing.T) {
	f := newFakeFleetPS(t)
	f.scrapeHold = 25 * time.Millisecond
	for i := range 12 {
		f.add(fmt.Sprintf("db%02d", i), "main", fullExposition)
	}
	fleet := newTestFleet(t, f, FleetConfig{Concurrency: 3})
	waitForFleet(t, fleet, 12)

	if got := f.maxInFlight.Load(); got > 3 {
		t.Fatalf("concurrency bound breached: %d scrapes in flight, want ≤ 3", got)
	}
	if got := f.maxInFlight.Load(); got < 2 {
		t.Fatalf("only %d concurrent scrape observed — the fan-out is serial, so this gate is vacuous", got)
	}
}

// TestFleet_SkipsElementsWithoutAMetricsPath pins that an SD element sluice
// cannot scrape is dropped rather than becoming a permanently-failing target.
func TestFleet_SkipsElementsWithoutAMetricsPath(t *testing.T) {
	f := newFakeFleetPS(t)
	f.add("good", "main", fullExposition)
	f.mu.Lock()
	f.elements = append(f.elements, sdTarget{Labels: map[string]string{
		sdLabelDatabase: "pathless", sdLabelBranch: "main",
	}})
	f.mu.Unlock()

	fleet := newTestFleet(t, f, FleetConfig{})
	samples := waitForFleet(t, fleet, 1)
	if len(samples) != 1 || samples[0].Target.Database != "good" {
		t.Fatalf("an SD element with no metrics path must be dropped, got %v", samples)
	}
}

// TestNewFleet_IncompleteCredentialsRefused pins the all-or-nothing opt-in.
func TestNewFleet_IncompleteCredentialsRefused(t *testing.T) {
	for _, tc := range []struct {
		name string
		cfg  FleetConfig
	}{
		{"no org", FleetConfig{TokenID: "a", Token: "b"}},
		{"no token id", FleetConfig{Org: "o", Token: "b"}},
		{"no token", FleetConfig{Org: "o", TokenID: "a"}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if _, err := NewFleet(context.Background(), tc.cfg); err == nil {
				t.Error("incomplete telemetry opt-in must be refused")
			}
		})
	}
}
