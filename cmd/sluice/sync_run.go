// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package main

import (
	"errors"
	"fmt"
	"log/slog"
	"os"
	"sort"
	"strings"
	"time"

	kms "github.com/go-viper/mapstructure/v2"
	"github.com/knadh/koanf/parsers/yaml"
	"github.com/knadh/koanf/providers/file"
	"github.com/knadh/koanf/v2"

	"sluicesync.dev/sluice/internal/config"
	"sluicesync.dev/sluice/internal/notify"
	"sluicesync.dev/sluice/internal/pipeline"
)

// SyncFleetConfig is the parsed `syncs.yaml` (ADR-0122 §3): a list of
// independent sync specs plus the fleet-wide restart policy. Loaded by
// [loadFleetConfig]; validated by [SyncFleetConfig.validate].
type SyncFleetConfig struct {
	Syncs   []SyncSpec    `koanf:"syncs"`
	Restart RestartConfig `koanf:"restart"`
}

// RestartConfig is the YAML form of [pipeline.RestartPolicy]. Zero
// fields fall back to the policy defaults (BackoffBase 1s, BackoffCap
// 30s, MaxConsecutiveFailures 0 = restart forever).
type RestartConfig struct {
	BackoffBase            time.Duration `koanf:"backoff-base"`
	BackoffCap             time.Duration `koanf:"backoff-cap"`
	HealthyRunThreshold    time.Duration `koanf:"healthy-run-threshold"`
	MaxConsecutiveFailures int           `koanf:"max-consecutive-failures"`
}

func (r RestartConfig) toPolicy() pipeline.RestartPolicy {
	return pipeline.RestartPolicy{
		BackoffBase:            r.BackoffBase,
		BackoffCap:             r.BackoffCap,
		HealthyRunThreshold:    r.HealthyRunThreshold,
		MaxConsecutiveFailures: r.MaxConsecutiveFailures,
	}
}

// SyncSpec is one supervised sync's config. It is a CURATED SUBSET of
// the `sync start` flag surface (ADR-0122 §3) — the per-sync knobs that
// matter for a fleet. Field keys are kebab-case to mirror the CLI flags
// operators already know. Process-global MySQL knobs (--mysql-sql-mode /
// --zero-date / VStream tuning) and per-sync PlanetScale telemetry are
// intentionally out of v1 (set once per process / a future addition).
type SyncSpec struct {
	StreamID     string `koanf:"stream-id"`
	SourceDriver string `koanf:"source-driver"`
	Source       string `koanf:"source"`
	TargetDriver string `koanf:"target-driver"`
	Target       string `koanf:"target"`

	SlotName     string `koanf:"slot-name"`
	TargetSchema string `koanf:"target-schema"`

	IncludeTable []string `koanf:"include-table"`
	ExcludeTable []string `koanf:"exclude-table"`
	TypeOverride []string `koanf:"type-override"`
	ExprOverride []string `koanf:"expr-override"`

	ApplyConcurrency int    `koanf:"apply-concurrency"`
	ApplyBatchSize   string `koanf:"apply-batch-size"`
	NoAutoTune       bool   `koanf:"no-auto-tune"`

	ApplyDelay       time.Duration `koanf:"apply-delay"`
	MaxBufferBytes   int64         `koanf:"max-buffer-bytes"`
	ApplyExecTimeout time.Duration `koanf:"apply-exec-timeout"`

	ApplyRetryAttempts    int           `koanf:"apply-retry-attempts"`
	ApplyRetryBackoffBase time.Duration `koanf:"apply-retry-backoff-base"`
	ApplyRetryBackoffCap  time.Duration `koanf:"apply-retry-backoff-cap"`

	MetricsListen     string        `koanf:"metrics-listen"`
	HeartbeatInterval time.Duration `koanf:"heartbeat-interval"`
	PollInterval      time.Duration `koanf:"poll-interval"`
	SchemaChanges     string        `koanf:"schema-changes"`

	// Notify sinks. The webhook/slack URLs and the SMTP password are
	// credentials; supply them via the SLUICE_NOTIFY_* env vars, not in
	// the committed YAML (same env-only contract as `sync start`).
	NotifyWebhook        string        `koanf:"notify-webhook"`
	NotifySlack          string        `koanf:"notify-slack"`
	NotifySyncLagSeconds float64       `koanf:"notify-sync-lag-seconds"`
	NotifyCooldown       time.Duration `koanf:"notify-cooldown"`

	NotifySMTPHost     string   `koanf:"notify-smtp-host"`
	NotifySMTPPort     int      `koanf:"notify-smtp-port"`
	NotifySMTPFrom     string   `koanf:"notify-smtp-from"`
	NotifySMTPTo       []string `koanf:"notify-smtp-to"`
	NotifySMTPTLS      string   `koanf:"notify-smtp-tls"`
	NotifySMTPAuth     string   `koanf:"notify-smtp-auth"`
	NotifySMTPUsername string   `koanf:"notify-smtp-username"`
}

// loadFleetConfig parses a syncs.yaml fleet config from path. Durations
// decode from strings ("5m", "30s") via the mapstructure duration hook.
func loadFleetConfig(path string) (*SyncFleetConfig, error) {
	if path == "" {
		return nil, errors.New("sync run: --config is required (path to a syncs.yaml fleet config)")
	}
	k := koanf.New(".")
	if err := k.Load(file.Provider(path), yaml.Parser()); err != nil {
		return nil, fmt.Errorf("sync run: load %q: %w", path, err)
	}
	var fleet SyncFleetConfig
	if err := k.UnmarshalWithConf("", &fleet, koanf.UnmarshalConf{
		Tag: "koanf",
		DecoderConfig: &kms.DecoderConfig{
			DecodeHook: kms.ComposeDecodeHookFunc(
				kms.StringToTimeDurationHookFunc(),
				kms.StringToSliceHookFunc(","),
			),
			TagName: "koanf",
		},
	}); err != nil {
		return nil, fmt.Errorf("sync run: parse %q: %w", path, err)
	}
	return &fleet, nil
}

// validate enforces the load-time fleet invariants: at least one sync,
// the required per-sync fields, fleet-wide stream-id uniqueness, and the
// Postgres slot-name uniqueness guard (ADR-0122 §4) — the data-corruption
// refusals. Returns the first violation it finds, named loudly.
func (f *SyncFleetConfig) validate() error {
	if len(f.Syncs) == 0 {
		return errors.New("sync run: no syncs configured (the `syncs:` list is empty)")
	}

	seenStreamID := make(map[string]int, len(f.Syncs))
	// slot collision keyed by resolved slot name across Postgres
	// sources. Conservative global-uniqueness; see ADR-0122 §4.
	seenSlot := make(map[string]string, len(f.Syncs))

	for i := range f.Syncs {
		s := &f.Syncs[i]
		who := s.describe(i)

		if s.StreamID == "" {
			return fmt.Errorf("sync run: %s: stream-id is required (it is the per-target position key + the fleet status row)", who)
		}
		if s.SourceDriver == "" || s.Source == "" {
			return fmt.Errorf("sync run: %s: source-driver and source (DSN) are required", who)
		}
		if s.TargetDriver == "" || s.Target == "" {
			return fmt.Errorf("sync run: %s: target-driver and target (DSN) are required", who)
		}

		// Duplicate stream-id: two syncs sharing one stream-id clobber
		// each other's sluice_cdc_state position row — the same
		// data-corruption class as a slot collision. Refuse loudly.
		if prev, ok := seenStreamID[s.StreamID]; ok {
			return fmt.Errorf(
				"sync run: duplicate stream-id %q (syncs #%d and #%d); each sync needs a distinct stream-id or they clobber each other's persisted position",
				s.StreamID, prev+1, i+1,
			)
		}
		seenStreamID[s.StreamID] = i

		// Slot-name uniqueness guard (ADR-0122 §4): a Postgres source
		// owns a single-consumer replication slot; two PG syncs sharing
		// a resolved slot name silently corrupt each other's stream.
		if isPostgresSourceDriver(s.SourceDriver) {
			slot := resolvedSlotName(s.SlotName)
			if prev, ok := seenSlot[slot]; ok {
				return fmt.Errorf(
					"sync run: Postgres syncs %q and %q both resolve to replication slot %q; set a distinct --slot-name on one of them (a shared slot corrupts both streams)",
					prev, s.StreamID, slot,
				)
			}
			seenSlot[slot] = s.StreamID
		}

		if err := validateRetryFlags(
			firstNonZeroInt(s.ApplyRetryAttempts, defaultApplyRetryAttempts),
			firstNonZeroDuration(s.ApplyRetryBackoffBase, defaultApplyRetryBackoffBase),
			firstNonZeroDuration(s.ApplyRetryBackoffCap, defaultApplyRetryBackoffCap),
		); err != nil {
			return fmt.Errorf("sync run: %s: %w", who, err)
		}
	}
	return nil
}

// describe names a sync for an error message: its stream-id when set,
// else its 1-based position.
func (s *SyncSpec) describe(i int) string {
	if s.StreamID != "" {
		return fmt.Sprintf("sync %q", s.StreamID)
	}
	return fmt.Sprintf("sync #%d", i+1)
}

// Per-sync fleet defaults, mirroring `sync start`'s flag defaults so a
// fleet sync behaves identically to the same flags on `sync start`.
const (
	defaultApplyBatchSize        = "auto"
	defaultMaxBufferBytes        = 67108864 // 64 MiB (ADR-0028)
	defaultApplyExecTimeout      = 60 * time.Second
	defaultHeartbeatInterval     = 60 * time.Second
	defaultSchemaChanges         = "forward"
	defaultApplyRetryAttempts    = 8
	defaultApplyRetryBackoffBase = 100 * time.Millisecond
	defaultApplyRetryBackoffCap  = 30 * time.Second
)

// sharedTargetGroups returns, keyed by resolved target endpoint, the
// stream-ids of every group of TWO OR MORE syncs that target the same
// server (and therefore share its connection budget). Coarse endpoint
// extraction; falls back to the full DSN string when a host can't be
// parsed (so distinct DSNs never collapse to one bucket). Pure so the
// shared-budget detection is unit-testable independent of logging.
func sharedTargetGroups(fleet *SyncFleetConfig) map[string][]string {
	byEndpoint := make(map[string][]string)
	for i := range fleet.Syncs {
		s := &fleet.Syncs[i]
		ep := dsnEndpoint(s.Target)
		byEndpoint[ep] = append(byEndpoint[ep], s.StreamID)
	}
	shared := make(map[string][]string)
	for ep, ids := range byEndpoint {
		if len(ids) >= 2 {
			shared[ep] = ids
		}
	}
	return shared
}

// warnSharedTargetBudget WARNs (does not refuse, ADR-0122 §5) when two
// or more syncs target the same server, since they share that target's
// connection budget.
func warnSharedTargetBudget(fleet *SyncFleetConfig) {
	shared := sharedTargetGroups(fleet)
	endpoints := make([]string, 0, len(shared))
	for ep := range shared {
		endpoints = append(endpoints, ep)
	}
	sort.Strings(endpoints) // deterministic WARN order
	for _, ep := range endpoints {
		slog.Warn(
			"sync run: multiple syncs share one target server; they share its connection budget — size apply-concurrency / max-target-connections accordingly (ADR-0122 §5)",
			slog.String("target_endpoint", ep),
			slog.Any("stream_ids", shared[ep]),
		)
	}
}

// SyncRunCmd is `sluice sync run --config syncs.yaml` (ADR-0122): the
// supervisor that runs N independent syncs in one process, each
// failure-isolated with bounded-backoff restart. The fleet config path
// is the global --config / -c flag (a syncs.yaml fleet config here, the
// list of sync specs + the restart policy).
type SyncRunCmd struct {
	DryRun bool `short:"n" help:"Validate the fleet config (required fields, stream-id + slot-name uniqueness, retry bounds) and print the resolved plan without starting any sync."`
}

// Run implements `sluice sync run`.
func (c *SyncRunCmd) Run(g *Globals) error {
	if g.Config == "" {
		return errors.New("sync run: --config is required (path to a syncs.yaml fleet config)")
	}
	fleet, err := loadFleetConfig(g.Config)
	if err != nil {
		return err
	}
	if err := fleet.validate(); err != nil {
		return err
	}

	if c.DryRun {
		return printFleetPlan(os.Stdout, fleet)
	}

	warnSharedTargetBudget(fleet)

	ctx := kongContext()
	supervised := make([]pipeline.SupervisedSync, 0, len(fleet.Syncs))
	for i := range fleet.Syncs {
		spec := &fleet.Syncs[i]
		streamer, err := buildStreamerFromSpec(spec)
		if err != nil {
			return fmt.Errorf("sync run: %s: %w", spec.describe(i), err)
		}
		supervised = append(supervised, pipeline.SupervisedSync{
			ID:     spec.StreamID,
			Runner: streamer,
		})
	}

	sup := pipeline.NewSupervisor(supervised, fleet.Restart.toPolicy())
	return sup.Run(ctx)
}

// buildStreamerFromSpec maps a SyncSpec to a *pipeline.Streamer, reusing
// the exact `sync start` helpers so a fleet sync behaves identically to
// the same flags on the standalone command.
func buildStreamerFromSpec(spec *SyncSpec) (*pipeline.Streamer, error) {
	source, err := resolveEngine(spec.SourceDriver)
	if err != nil {
		return nil, fmt.Errorf("source-driver: %w", err)
	}
	target, err := resolveEngine(spec.TargetDriver)
	if err != nil {
		return nil, fmt.Errorf("target-driver: %w", err)
	}

	if len(spec.IncludeTable) > 0 && len(spec.ExcludeTable) > 0 {
		return nil, errors.New("include-table and exclude-table are mutually exclusive")
	}
	filter, err := pipeline.NewTableFilter(spec.IncludeTable, spec.ExcludeTable)
	if err != nil {
		return nil, err
	}

	empty := &config.Config{}
	mappings, err := resolveMappings(spec.TypeOverride, empty)
	if err != nil {
		return nil, err
	}
	exprMappings, err := resolveExpressionMappings(spec.ExprOverride, empty)
	if err != nil {
		return nil, err
	}

	smtp := spec.smtpConfig()
	if err := smtp.Validate(); err != nil {
		return nil, err
	}

	applyBatchSize, err := resolveApplyBatchSize(firstNonEmpty(spec.ApplyBatchSize, defaultApplyBatchSize), target)
	if err != nil {
		return nil, fmt.Errorf("apply-batch-size: %w", err)
	}

	// connection-resilience (1): label connections with the stream-id.
	source = labelEngine(source, spec.StreamID)
	target = labelEngine(target, spec.StreamID)

	return &pipeline.Streamer{
		Source:             source,
		Target:             target,
		SourceDSN:          spec.Source,
		TargetDSN:          spec.Target,
		StreamID:           spec.StreamID,
		SlotName:           spec.SlotName,
		Mappings:           mappings,
		ExpressionMappings: exprMappings,
		Filter:             filter,
		TargetSchema:       spec.TargetSchema,
		ApplyBatchSize:     applyBatchSize,
		AutoTune:           !spec.NoAutoTune,
		ApplyConcurrency:   spec.ApplyConcurrency,
		ApplyDelay:         spec.ApplyDelay,
		MaxBufferBytes:     firstNonZeroInt64(spec.MaxBufferBytes, defaultMaxBufferBytes),
		ApplyExecTimeout:   firstNonZeroDuration(spec.ApplyExecTimeout, defaultApplyExecTimeout),

		ApplyRetryAttempts:    firstNonZeroInt(spec.ApplyRetryAttempts, defaultApplyRetryAttempts),
		ApplyRetryBackoffBase: firstNonZeroDuration(spec.ApplyRetryBackoffBase, defaultApplyRetryBackoffBase),
		ApplyRetryBackoffCap:  firstNonZeroDuration(spec.ApplyRetryBackoffCap, defaultApplyRetryBackoffCap),

		MetricsListen:     spec.MetricsListen,
		HeartbeatInterval: firstNonZeroDuration(spec.HeartbeatInterval, defaultHeartbeatInterval),
		PollInterval:      spec.PollInterval,
		SchemaChanges:     firstNonEmpty(spec.SchemaChanges, defaultSchemaChanges),

		NotifyWebhookURL:      spec.NotifyWebhook,
		NotifySlackWebhookURL: spec.NotifySlack,
		NotifySyncLagSeconds:  spec.NotifySyncLagSeconds,
		NotifyCooldown:        spec.NotifyCooldown,
		NotifySMTP:            smtp,

		BuildVersion: version,
		BuildCommit:  commit,
	}, nil
}

// smtpConfig assembles the [notify.SMTPConfig] from the spec's
// notify-smtp-* fields. The password is env-only (SLUICE_NOTIFY_SMTP_
// PASSWORD), shared across the fleet's SMTP sinks — same env-only
// contract as `sync start`.
func (s *SyncSpec) smtpConfig() notify.SMTPConfig {
	return notify.SMTPConfig{
		Host:     s.NotifySMTPHost,
		Port:     s.NotifySMTPPort,
		From:     s.NotifySMTPFrom,
		To:       s.NotifySMTPTo,
		Username: s.NotifySMTPUsername,
		Password: os.Getenv("SLUICE_NOTIFY_SMTP_PASSWORD"),
		TLS:      notify.TLSMode(firstNonEmpty(s.NotifySMTPTLS, "starttls")),
		Auth:     notify.SMTPAuth(firstNonEmpty(s.NotifySMTPAuth, "none")),
	}
}

// printFleetPlan renders the resolved fleet for --dry-run: one line per
// sync (stream-id, source→target, resolved slot) plus the restart policy.
func printFleetPlan(out *os.File, fleet *SyncFleetConfig) error {
	if _, err := fmt.Fprintf(out, "fleet: %d %s\n", len(fleet.Syncs), pluralize("sync", len(fleet.Syncs))); err != nil {
		return err
	}
	for i := range fleet.Syncs {
		s := &fleet.Syncs[i]
		slot := "-"
		if isPostgresSourceDriver(s.SourceDriver) {
			slot = resolvedSlotName(s.SlotName)
		}
		if _, err := fmt.Fprintf(
			out, "  %s\t%s://%s -> %s://%s\tslot=%s\n",
			s.StreamID, s.SourceDriver, dsnEndpoint(s.Source),
			s.TargetDriver, dsnEndpoint(s.Target), slot,
		); err != nil {
			return err
		}
	}
	p := fleet.Restart.toPolicy()
	_, err := fmt.Fprintf(
		out, "restart: backoff %s..%s, max-consecutive-failures=%d (0=unbounded)\n",
		firstNonZeroDuration(p.BackoffBase, time.Second),
		firstNonZeroDuration(p.BackoffCap, 30*time.Second),
		p.MaxConsecutiveFailures,
	)
	return err
}

// isPostgresSourceDriver reports whether the named source driver has a
// Postgres logical-replication-slot concept (the slot-uniqueness guard
// applies). Matches the "postgres" engine exactly — NOT "postgres-trigger"
// (the trigger-based CDC engine uses no replication slot and ignores
// slot-name, so guarding it would be a false refusal).
func isPostgresSourceDriver(name string) bool {
	return strings.EqualFold(name, "postgres")
}

// resolvedSlotName resolves a spec's slot-name to the effective slot
// (empty → the engine default sluice_slot), applying the sluice_ prefix
// convention, so the uniqueness guard compares the names the engine will
// actually create.
func resolvedSlotName(name string) string {
	if name == "" {
		return "sluice_slot"
	}
	return pipeline.ResolveSlotName(name)
}

// dsnEndpoint extracts a coarse host:port endpoint from a DSN for the
// shared-target-budget WARN and the dry-run plan. Best-effort: handles
// URL-form (scheme://[user[:pass]@]host[:port]/...) and keyword-form
// (host=... port=...). Falls back to the full DSN string when it can't
// find a host (so distinct DSNs never collapse to one bucket).
func dsnEndpoint(dsn string) string {
	if dsn == "" {
		return ""
	}
	// URL form: strip scheme, userinfo, then take up to the first
	// path/query separator.
	if i := strings.Index(dsn, "://"); i >= 0 {
		rest := dsn[i+3:]
		if at := strings.LastIndex(rest, "@"); at >= 0 {
			rest = rest[at+1:]
		}
		if j := strings.IndexAny(rest, "/?"); j >= 0 {
			rest = rest[:j]
		}
		if rest != "" {
			return rest
		}
	}
	// Keyword form: pull host= and port= tokens.
	var host, port string
	for _, tok := range strings.Fields(dsn) {
		switch {
		case strings.HasPrefix(strings.ToLower(tok), "host="):
			host = tok[len("host="):]
		case strings.HasPrefix(strings.ToLower(tok), "port="):
			port = tok[len("port="):]
		}
	}
	if host != "" {
		if port != "" {
			return host + ":" + port
		}
		return host
	}
	return dsn
}

func firstNonEmpty(v, fallback string) string {
	if v == "" {
		return fallback
	}
	return v
}

func firstNonZeroInt(v, fallback int) int {
	if v == 0 {
		return fallback
	}
	return v
}

func firstNonZeroInt64(v, fallback int64) int64 {
	if v == 0 {
		return fallback
	}
	return v
}

func firstNonZeroDuration(v, fallback time.Duration) time.Duration {
	if v == 0 {
		return fallback
	}
	return v
}
