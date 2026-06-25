// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package telemetry

import (
	"context"
	"errors"
	"log/slog"
	"net/http"
	"strings"
	"sync"
	"time"

	"sluicesync.dev/sluice/internal/ir"
)

// Default poll cadence and clamp bounds. The cadence is plumbed FROM the
// caller (cmd/sluice passes the pipeline's telemetryPollInterval so there
// is one canonical value), but a zero or out-of-range value falls back to
// these. 60s matches the CONFIRMED PlanetScale metric granularity (the SD
// targets advertise __scrape_interval__=1m and sample timestamps advance
// exactly every 60s, probed live 2026-06-21), so polling faster only
// re-reads the same sample (ADR-0107 Phase 2 resolved open question 2).
const (
	defaultPollInterval = 60 * time.Second
	minPollInterval     = 10 * time.Second
	maxPollInterval     = 120 * time.Second
)

// Config configures a PlanetScale telemetry [Provider]. Org + the two
// service-token halves are the control-plane credential (distinct from the
// data-plane DSN, ADR-0107); Database/Branch select the target series. The
// remaining fields are injectable for tests (BaseURL, HTTPClient, Now) or
// have safe defaults.
type Config struct {
	Org      string // PlanetScale org slug (--planetscale-org)
	TokenID  string // service-token id (read_metrics_endpoints permission)
	Token    string // service-token secret (NEVER logged)
	Database string // target database to filter SD by (from the target DSN)
	Branch   string // target branch; defaults to "main" when empty

	// Engine is the target engine registry name ("mysql", "planetscale",
	// "postgres", …). It selects the per-engine metric-name table
	// ([metricNamesFor]) so a Postgres target reads `planetscale_volume_*` /
	// `planetscale_postgres_*` rather than the Vitess `vttablet_*` names. Empty
	// or any non-Postgres name ⇒ the MySQL/Vitess table (the default surface).
	Engine string

	// PollInterval is the background poll cadence; clamped to
	// [minPollInterval, maxPollInterval]. 0 ⇒ defaultPollInterval.
	PollInterval time.Duration

	// Freshness is the window Sample uses to age a cached snapshot out to
	// ok=false (see [ir.TargetHealthSnapshot.Fresh]). 0 ⇒ 3*PollInterval,
	// tolerating one missed poll before a consumer degrades.
	Freshness time.Duration

	// BaseURL overrides the SD endpoint host root (tests / self-host).
	// "" ⇒ https://api.planetscale.com.
	BaseURL string

	// HTTPClient is injected in tests; nil ⇒ a default client with a
	// per-request timeout.
	HTTPClient *http.Client

	// Now is injected for deterministic freshness in tests; nil ⇒ time.Now.
	Now func() time.Time

	// Logger receives poll-error WARNs; nil ⇒ slog.Default(). Poll errors
	// are logged and SWALLOWED — never propagated into the apply path.
	Logger *slog.Logger
}

const defaultBaseURL = "https://api.planetscale.com"

// Provider implements [ir.TargetTelemetry] against the PlanetScale metrics
// endpoint. It runs a background poll loop that refreshes a cached
// [ir.TargetHealthSnapshot]; Sample returns the cached value non-blocking,
// never doing a live round-trip. A poll error keeps the last good snapshot
// in place and lets [ir.TargetHealthSnapshot.Fresh] age it out — a provider
// outage degrades to "no signal", never an error that kills the run.
type Provider struct {
	client    *client
	database  string
	branch    string
	names     metricNames
	interval  time.Duration
	freshness time.Duration
	now       func() time.Time
	logger    *slog.Logger

	mu     sync.RWMutex
	cached ir.TargetHealthSnapshot
	haveOK bool // a poll has succeeded at least once

	cancel context.CancelFunc
	done   chan struct{}
}

// Compile-time proof the provider satisfies the engine-neutral seam.
var _ ir.TargetTelemetry = (*Provider)(nil)

// New constructs a Provider and STARTS its background poll loop, scoped to
// ctx (cancel ctx, or call Close, to stop it). It returns an error only for
// missing required credentials/identifiers — the loud opt-in-must-be-
// complete refusal; a network/parse failure on the FIRST poll is NOT an
// error here (the provider serves ok=false until a poll succeeds, exactly
// the degrade contract). The returned provider satisfies
// [ir.TargetTelemetry].
func New(ctx context.Context, cfg Config) (*Provider, error) {
	if cfg.Org == "" {
		return nil, errors.New("telemetry: PlanetScale org is required")
	}
	if cfg.TokenID == "" || cfg.Token == "" {
		return nil, errors.New("telemetry: PlanetScale metrics service-token id and secret are required")
	}
	if cfg.Database == "" {
		return nil, errors.New("telemetry: target database is required to select the metrics branch")
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
		// A per-request timeout bounds each poll leg; the loop's own ticker
		// owns the cadence. Kept short relative to the poll interval so one
		// slow poll can't pile up behind the next tick.
		httpClient = &http.Client{Timeout: 10 * time.Second}
	}
	baseURL := cfg.BaseURL
	if baseURL == "" {
		baseURL = defaultBaseURL
	}
	branch := cfg.Branch
	if branch == "" {
		branch = "main"
	}

	loopCtx, cancel := context.WithCancel(ctx)
	p := &Provider{
		client: &client{
			httpClient: httpClient,
			baseURL:    baseURL,
			org:        cfg.Org,
			tokenID:    cfg.TokenID,
			token:      cfg.Token,
		},
		database:  cfg.Database,
		branch:    branch,
		names:     metricNamesFor(cfg.Engine),
		interval:  interval,
		freshness: freshness,
		now:       now,
		logger:    logger,
		cancel:    cancel,
		done:      make(chan struct{}),
	}
	go p.run(loopCtx)
	return p, nil
}

// Sample returns the most-recent cached snapshot, non-blocking. ok=false
// when no poll has yet succeeded OR the cached snapshot has aged past the
// freshness window — the caller then degrades to its reactive path,
// exactly as if no provider were wired. It NEVER does a live round-trip.
func (p *Provider) Sample(ctx context.Context) (ir.TargetHealthSnapshot, bool) {
	// Honour ctx cancellation for the (non-blocking) read.
	if err := ctx.Err(); err != nil {
		return ir.TargetHealthSnapshot{}, false
	}
	p.mu.RLock()
	snap := p.cached
	have := p.haveOK
	p.mu.RUnlock()
	if !have || !snap.Fresh(p.now(), p.freshness) {
		return ir.TargetHealthSnapshot{}, false
	}
	return snap, true
}

// Close stops the background poll loop and waits for it to exit. Safe to
// call once; idempotent thereafter via the closed done channel.
func (p *Provider) Close() error {
	p.cancel()
	<-p.done
	return nil
}

// run is the background poll loop. It polls once immediately (so Sample
// warms up without waiting a full interval), then on every tick. A poll
// error is logged at WARN and swallowed — the last good snapshot stays in
// the cache and Fresh ages it out; a telemetry failure never propagates
// into the apply path.
func (p *Provider) run(ctx context.Context) {
	defer close(p.done)
	ticker := time.NewTicker(p.interval)
	defer ticker.Stop()
	p.pollOnce(ctx)
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			p.pollOnce(ctx)
		}
	}
}

// pollOnce does one SD + scrape + distill cycle and, on success, replaces
// the cached snapshot under the write lock. On any failure it logs WARN
// (token never included) and leaves the cache untouched.
func (p *Provider) pollOnce(ctx context.Context) {
	t, err := p.client.discover(ctx, p.database, p.branch)
	if err != nil {
		p.warnPoll(ctx, err)
		return
	}
	text, err := p.client.scrape(ctx, t)
	if err != nil {
		p.warnPoll(ctx, err)
		return
	}
	snap := distill(parsePromText(strings.NewReader(text)), p.names, p.now())
	p.mu.Lock()
	p.cached = snap
	p.haveOK = true
	p.mu.Unlock()
}

// warnPoll logs a swallowed poll error. ctx cancellation (normal shutdown)
// is NOT a warning — only a genuine telemetry failure is surfaced.
func (p *Provider) warnPoll(ctx context.Context, err error) {
	if ctx.Err() != nil {
		return
	}
	p.logger.WarnContext(
		ctx,
		"target telemetry poll failed; serving last snapshot until it goes stale (advisory only — apply is unaffected)",
		slog.String("database", p.database),
		slog.String("branch", p.branch),
		slog.String("err", err.Error()),
	)
}
