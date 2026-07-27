// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package telemetry

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"path"
	"sort"
	"strings"
	"sync"
	"time"

	"sluicesync.dev/sluice/internal/ir"
)

// Org-wide (FLEET) telemetry — roadmap item 75b.
//
// [Provider] watches ONE database+branch. [Fleet] watches every
// database+branch an org exposes, on the same cadence, from the same
// credential, and hands them out through the engine-neutral
// [ir.FleetTelemetry] seam.
//
// DISCOVERY SOURCE (the load-bearing choice). The fan-out enumerates the org
// from the SAME per-org service-discovery document the single-database
// provider already calls — `GET /v1/organizations/{org}/metrics` — NOT from
// `ListDatabases` + per-database `ListBranches` control-plane calls. Probed
// live 2026-07-24 against the `sluicesync` org: the SD document returns ONE
// element per database+branch for the whole org, each carrying
// `planetscale_database_name`, `planetscale_branch_name`,
// `planetscale_organization_name`, `__scrape_interval__=1m`, and its OWN
// signed scrape URL. That makes it strictly better than the API-enumeration
// route on four counts:
//
//   - ONE control-plane call per discovery refresh regardless of org size,
//     instead of 1 + N (rate limits are the stated gotcha for this chunk);
//   - it lists exactly the branches that HAVE a metrics endpoint, so the
//     fan-out never polls a target that cannot answer;
//   - the signed scrape URL comes back with the element, so a poll cycle is
//     one authenticated call plus N unauthenticated scrapes;
//   - it needs no permission beyond the `read_metrics_endpoints` the feature
//     already requires (database/branch listing is a different scope).
//
// The one thing the SD document does NOT carry is the ENGINE. See
// [metricNamesForExposition] for how each target's metric-name table is
// resolved from its own exposition instead.

// Fleet fan-out defaults. The cadence mirrors the single-database provider
// (the PlanetScale metrics granularity is 1m — the SD elements advertise
// `__scrape_interval__=1m`), so an org-wide watch polls no faster than a
// single-database one; concurrency bounds how many scrapes are in flight at
// once so a large org doesn't burst the metrics endpoint.
const (
	defaultFleetConcurrency = 4
	maxFleetConcurrency     = 16
)

// FleetConfig configures a [Fleet]. Org + the service-token halves are the
// same control-plane credential [Config] takes; the remaining fields scope
// and pace the fan-out.
type FleetConfig struct {
	Org     string // PlanetScale org slug (--planetscale-org)
	TokenID string // service-token id (read_metrics_endpoints permission)
	Token   string // service-token secret (NEVER logged)

	// Branch, when non-empty, restricts the fan-out to branches with that
	// name across every database. Empty ⇒ EVERY discovered branch. (Note the
	// deliberate difference from [Config.Branch], which defaults to "main":
	// a fleet watch that silently skipped non-main branches would under-report
	// the org, which is the opposite of what this mode is for.)
	Branch string

	// Include / Exclude are [path.Match] glob filters evaluated against both
	// `database` and `database/branch`. See [newFleetFilter] for precedence.
	Include []string
	Exclude []string

	// Engine is the DECLARED target-engine registry name, used only as the
	// fallback metric-name table for a target whose exposition carries
	// neither engine marker ([metricNamesForExposition]).
	Engine string

	// Concurrency bounds in-flight scrapes per poll cycle. 0 ⇒
	// defaultFleetConcurrency; clamped to [1, maxFleetConcurrency].
	Concurrency int

	// PollInterval is the background poll cadence; clamped exactly as
	// [Config.PollInterval] is. 0 ⇒ defaultPollInterval.
	PollInterval time.Duration

	// Freshness is the window SampleFleet uses to age a cached per-target
	// snapshot out to OK=false. 0 ⇒ 3*PollInterval.
	Freshness time.Duration

	// BaseURL overrides the SD endpoint host root (tests / self-host).
	BaseURL string

	// HTTPClient is injected in tests; nil ⇒ a default client with a
	// per-request timeout.
	HTTPClient *http.Client

	// Now is injected for deterministic freshness in tests; nil ⇒ time.Now.
	Now func() time.Time

	// Logger receives poll-error WARNs; nil ⇒ slog.Default(). Poll errors are
	// logged and SWALLOWED, exactly as in the single-database provider.
	Logger *slog.Logger
}

// fleetEntry is one target's cached state: the last snapshot that a poll
// distilled successfully, and whether any poll has ever succeeded for it. A
// scrape failure keeps the previous snapshot in place and lets
// [ir.TargetHealthSnapshot.Fresh] age it out — the per-target twin of the
// single-database degrade contract.
type fleetEntry struct {
	snap   ir.TargetHealthSnapshot
	haveOK bool
}

// Fleet implements [ir.FleetTelemetry] against the PlanetScale per-org
// metrics service-discovery document. It runs one background loop that
// re-discovers the org and scrapes every in-scope branch with bounded
// concurrency, caching each target's distilled snapshot. SampleFleet returns
// the cached values non-blocking, never doing a live round-trip.
type Fleet struct {
	client      *client
	declared    metricNames
	filter      fleetFilter
	branch      string
	concurrency int
	interval    time.Duration
	freshness   time.Duration
	now         func() time.Time
	logger      *slog.Logger

	mu      sync.RWMutex
	entries map[ir.FleetTarget]fleetEntry
	order   []ir.FleetTarget // stable (sorted) discovery order

	cancel context.CancelFunc
	done   chan struct{}
}

// Compile-time proof the fleet provider satisfies the engine-neutral seam.
var _ ir.FleetTelemetry = (*Fleet)(nil)

// NewFleet constructs a Fleet and STARTS its background poll loop, scoped to
// ctx (cancel ctx, or call Close, to stop it). Like [New] it errors only for
// missing credentials or a malformed filter pattern — the loud
// opt-in-must-be-complete refusal; a network failure on the first poll is NOT
// an error (SampleFleet serves an empty fleet until a discovery succeeds).
func NewFleet(ctx context.Context, cfg FleetConfig) (*Fleet, error) {
	if cfg.Org == "" {
		return nil, errors.New("telemetry: PlanetScale org is required")
	}
	if cfg.TokenID == "" || cfg.Token == "" {
		return nil, errors.New("telemetry: PlanetScale metrics service-token id and secret are required")
	}
	filter, err := newFleetFilter(cfg.Include, cfg.Exclude)
	if err != nil {
		return nil, err
	}

	interval := cfg.PollInterval
	if interval <= 0 {
		interval = defaultPollInterval
	}
	if interval < minPollInterval {
		interval = minPollInterval
	}
	if interval > maxPollInterval {
		interval = maxPollInterval
	}
	freshness := cfg.Freshness
	if freshness <= 0 {
		freshness = 3 * interval
	}
	concurrency := cfg.Concurrency
	if concurrency <= 0 {
		concurrency = defaultFleetConcurrency
	}
	if concurrency > maxFleetConcurrency {
		concurrency = maxFleetConcurrency
	}
	now := cfg.Now
	if now == nil {
		now = time.Now
	}
	logger := cfg.Logger
	if logger == nil {
		logger = slog.Default()
	}
	httpClient := cfg.HTTPClient
	if httpClient == nil {
		httpClient = &http.Client{Timeout: 10 * time.Second}
	}
	baseURL := cfg.BaseURL
	if baseURL == "" {
		baseURL = defaultBaseURL
	}

	loopCtx, cancel := context.WithCancel(ctx)
	f := &Fleet{
		client:      newClient(cfg.Org, cfg.TokenID, cfg.Token, baseURL, httpClient),
		declared:    metricNamesFor(cfg.Engine),
		filter:      filter,
		branch:      cfg.Branch,
		concurrency: concurrency,
		interval:    interval,
		freshness:   freshness,
		now:         now,
		logger:      logger,
		entries:     map[ir.FleetTarget]fleetEntry{},
		cancel:      cancel,
		done:        make(chan struct{}),
	}
	go f.run(loopCtx)
	return f, nil
}

// SampleFleet returns one entry per currently-discovered target in stable
// (database, branch) order. A target whose snapshot has aged past the
// freshness window — or that has never been scraped successfully — is
// reported with OK=false rather than omitted, so a consumer can tell
// "discovered but unobserved" from "not in the org".
func (f *Fleet) SampleFleet(ctx context.Context) []ir.FleetHealthSample {
	if err := ctx.Err(); err != nil {
		return nil
	}
	now := f.now()
	f.mu.RLock()
	defer f.mu.RUnlock()
	out := make([]ir.FleetHealthSample, 0, len(f.order))
	for _, t := range f.order {
		e := f.entries[t]
		out = append(out, ir.FleetHealthSample{
			Target:   t,
			Snapshot: e.snap,
			OK:       e.haveOK && e.snap.Fresh(now, f.freshness),
		})
	}
	return out
}

// Close stops the background poll loop and waits for it to exit.
func (f *Fleet) Close() error {
	f.cancel()
	<-f.done
	return nil
}

// run is the background poll loop: discover + fan-out scrape immediately (so
// SampleFleet warms without waiting a full interval), then on every tick.
func (f *Fleet) run(ctx context.Context) {
	defer close(f.done)
	ticker := time.NewTicker(f.interval)
	defer ticker.Stop()
	f.pollOnce(ctx)
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			f.pollOnce(ctx)
		}
	}
}

// pollOnce re-discovers the org, scrapes every in-scope target with bounded
// concurrency, and swaps in the new cache. A discovery failure is logged and
// swallowed, leaving the previous fleet in place to age out via freshness; a
// per-target scrape failure carries that target's previous snapshot forward
// (same degrade, one target at a time).
func (f *Fleet) pollOnce(ctx context.Context) {
	sdTargets, err := f.client.discoverAll(ctx)
	if err != nil {
		f.warnDiscover(ctx, err)
		return
	}
	scoped := f.scope(sdTargets)

	f.mu.RLock()
	prev := make(map[ir.FleetTarget]fleetEntry, len(f.entries))
	for k, v := range f.entries {
		prev[k] = v
	}
	f.mu.RUnlock()

	var (
		mu    sync.Mutex
		next  = make(map[ir.FleetTarget]fleetEntry, len(scoped))
		order = make([]ir.FleetTarget, 0, len(scoped))
		wg    sync.WaitGroup
		sem   = make(chan struct{}, f.concurrency)
	)
	for _, st := range scoped {
		order = append(order, st.target)
		wg.Add(1)
		go func(st scopedTarget) {
			defer wg.Done()
			select {
			case sem <- struct{}{}:
			case <-ctx.Done():
				return
			}
			defer func() { <-sem }()

			entry, ok := f.scrapeOne(ctx, st)
			mu.Lock()
			defer mu.Unlock()
			if ok {
				next[st.target] = entry
				return
			}
			// Scrape/distill failed: carry the previous snapshot forward so
			// freshness (not an abrupt gap) governs the degrade.
			if p, had := prev[st.target]; had {
				next[st.target] = p
			}
		}(st)
	}
	wg.Wait()
	if ctx.Err() != nil {
		return
	}

	f.mu.Lock()
	f.entries = next
	f.order = order
	f.mu.Unlock()
}

// scrapeOne scrapes and distills ONE target. ok=false on any failure (logged
// at WARN and swallowed by the caller's carry-forward).
func (f *Fleet) scrapeOne(ctx context.Context, st scopedTarget) (fleetEntry, bool) {
	text, err := f.client.scrape(ctx, st.sd)
	if err != nil {
		f.warnScrape(ctx, st.target, err)
		return fleetEntry{}, false
	}
	samples, err := parsePromTextChecked(strings.NewReader(text))
	if err != nil {
		// Same disposition as an HTTP failure: the caller carries the previous
		// entry forward rather than recording an all-unknown one as observed
		// (audit SL-14).
		f.warnScrape(ctx, st.target, err)
		return fleetEntry{}, false
	}
	names := metricNamesForExposition(samples, f.declared)
	return fleetEntry{snap: distill(samples, names, f.now()), haveOK: true}, true
}

// scopedTarget pairs a discovered SD element with its resolved identity.
type scopedTarget struct {
	target ir.FleetTarget
	sd     sdTarget
}

// scope turns the raw SD document into the ordered, de-duplicated,
// filtered target list this fleet watches. Elements without a metrics path
// are dropped (nothing to scrape); the result is sorted by (database,
// branch) so every downstream ordering — printed lines, exported series,
// persisted records — is stable across ticks.
func (f *Fleet) scope(sdTargets []sdTarget) []scopedTarget {
	seen := make(map[ir.FleetTarget]bool, len(sdTargets))
	out := make([]scopedTarget, 0, len(sdTargets))
	for _, sd := range sdTargets {
		t := ir.FleetTarget{
			Database: sd.Labels[sdLabelDatabase],
			Branch:   sd.Labels[sdLabelBranch],
		}
		if t.Database == "" || t.Branch == "" || sd.Labels[sdLabelMetricsPath] == "" {
			continue
		}
		if f.branch != "" && t.Branch != f.branch {
			continue
		}
		if !f.filter.allows(t) {
			continue
		}
		if seen[t] {
			continue
		}
		seen[t] = true
		out = append(out, scopedTarget{target: t, sd: sd})
	}
	sort.Slice(out, func(i, j int) bool {
		if out[i].target.Database != out[j].target.Database {
			return out[i].target.Database < out[j].target.Database
		}
		return out[i].target.Branch < out[j].target.Branch
	})
	return out
}

// warnDiscover logs a swallowed org-discovery failure. ctx cancellation
// (normal shutdown) is not a warning.
func (f *Fleet) warnDiscover(ctx context.Context, err error) {
	if ctx.Err() != nil {
		return
	}
	f.logger.WarnContext(
		ctx,
		"fleet telemetry discovery failed; serving the last known fleet until it goes stale (advisory only)",
		slog.String("org", f.client.org),
		slog.String("err", err.Error()),
	)
}

// warnScrape logs a swallowed per-target scrape failure.
func (f *Fleet) warnScrape(ctx context.Context, t ir.FleetTarget, err error) {
	if ctx.Err() != nil {
		return
	}
	f.logger.WarnContext(
		ctx,
		"fleet telemetry poll failed for one target; serving its last snapshot until it goes stale (advisory only)",
		slog.String("database", t.Database),
		slog.String("branch", t.Branch),
		slog.String("err", err.Error()),
	)
}

// fleetFilter scopes the org fan-out. Patterns are [path.Match] globs
// evaluated against BOTH `database` and `database/branch`, so
// `--include-database 'shop-*'` selects every branch of the matching
// databases while `--include-database 'shop/main'` pins one branch.
//
// WART (named deliberately): unlike `--include-table`/`--exclude-table`
// (mutually exclusive, see migcore.TableFilter), include and exclude may be
// combined here, with EXCLUDE WINNING. A fleet watch's natural expression is
// "the whole prefix, minus these two" — forcing the operator to choose one
// list would make that unsayable — and the precedence rule (deny beats
// allow) is the unambiguous one.
type fleetFilter struct {
	include []string
	exclude []string
}

// newFleetFilter validates every pattern up front so a typo is a loud
// startup refusal rather than a silently empty fleet.
func newFleetFilter(include, exclude []string) (fleetFilter, error) {
	for _, p := range include {
		if _, err := path.Match(p, ""); err != nil {
			return fleetFilter{}, fmt.Errorf("telemetry: invalid --include-database pattern %q: %w", p, err)
		}
	}
	for _, p := range exclude {
		if _, err := path.Match(p, ""); err != nil {
			return fleetFilter{}, fmt.Errorf("telemetry: invalid --exclude-database pattern %q: %w", p, err)
		}
	}
	return fleetFilter{include: include, exclude: exclude}, nil
}

// allows reports whether the target participates in the fan-out.
func (f fleetFilter) allows(t ir.FleetTarget) bool {
	if len(f.include) > 0 && !f.matches(f.include, t) {
		return false
	}
	return !f.matches(f.exclude, t)
}

// matches reports whether any pattern matches the target's database name or
// its `database/branch` form. An invalid pattern is treated as "no match"
// (newFleetFilter rejects those up front; this branch is only reachable if a
// caller bypasses the constructor).
func (f fleetFilter) matches(patterns []string, t ir.FleetTarget) bool {
	for _, p := range patterns {
		if ok, err := path.Match(p, t.Database); err == nil && ok {
			return true
		}
		if ok, err := path.Match(p, t.String()); err == nil && ok {
			return true
		}
	}
	return false
}
