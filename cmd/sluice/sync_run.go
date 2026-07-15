// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package main

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
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
	"sluicesync.dev/sluice/internal/diagnose"
	"sluicesync.dev/sluice/internal/notify"
	"sluicesync.dev/sluice/internal/pipeline"
	"sluicesync.dev/sluice/internal/pipeline/migcore"
	pstelemetry "sluicesync.dev/sluice/internal/planetscale/telemetry"
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
// operators already know. The per-sync MySQL zero-date policy landed as
// ADR-0127's `zero-date` key below; sql_mode is per-sync via the source/
// target DSN `?sql_mode=` (no key needed). Per-sync PlanetScale telemetry
// landed in ADR-0126 (the planetscale-* + PS-gated notify-* keys below).
type SyncSpec struct {
	StreamID     string `koanf:"stream-id"`
	SourceDriver string `koanf:"source-driver"`
	Source       string `koanf:"source"`
	TargetDriver string `koanf:"target-driver"`
	Target       string `koanf:"target"`

	SlotName     string `koanf:"slot-name"`
	TargetSchema string `koanf:"target-schema"`

	// ControlKeyspace is the per-sync --control-keyspace (the sidecar-keyspace
	// feature), mirroring `sync start --control-keyspace` for the fleet: on a
	// SHARDED MySQL/PlanetScale/Vitess TARGET the stream's vindex-less CDC
	// control tables must live in a separate UNSHARDED keyspace. Empty defers to
	// the SAME auto-detect the single-target flag uses (sharded target ⇒ pick the
	// sole unsharded sidecar keyspace; unsharded/non-Vitess target ⇒ the data
	// keyspace, unchanged). Set it to override. Threaded to the target engine via
	// applyControlKeyspace for BOTH `sync run` and `sync status --all`.
	ControlKeyspace string `koanf:"control-keyspace"`

	// ZeroDate is the per-sync MySQL zero/partial-date policy (ADR-0127):
	// error | null | epoch. Empty defers to the process-global --zero-date.
	// Sugar over the source DSN's `zero_date` param — it is validated at
	// config-load and merged into the MySQL SOURCE DSN as `?zero_date=`; an
	// explicit `zero_date` already in the DSN wins (the foundational
	// mechanism). Ignored (with a config-load refusal) for non-MySQL sources.
	ZeroDate string `koanf:"zero-date"`

	// InjectShardColumn / AllowCrossShardMerge mirror `sync start`'s
	// --inject-shard-column / --allow-cross-shard-merge (ADR-0048 / Bug 152):
	// the two ways to move a SHARDED source (a keyspace with >1 shard that
	// vtgate merges into one logical stream) into a PK/UNIQUE target without
	// tripping preflightCrossShardCollision. InjectShardColumn is the
	// structural fix — the same NAME=VALUE string the CLI flag takes; sluice
	// appends a per-shard discriminator column + composite PK so cross-shard
	// rows land disjoint. AllowCrossShardMerge is the "my keys are globally
	// unique across shards" override. They are mutually exclusive IN EFFECT
	// (the discriminator makes the override inert), so validateCrossShard
	// refuses BOTH set on one entry rather than let the operator believe an
	// inert override took effect.
	InjectShardColumn    string `koanf:"inject-shard-column"`
	AllowCrossShardMerge bool   `koanf:"allow-cross-shard-merge"`

	IncludeTable []string `koanf:"include-table"`
	ExcludeTable []string `koanf:"exclude-table"`
	TypeOverride []string `koanf:"type-override"`
	ExprOverride []string `koanf:"expr-override"`

	ApplyConcurrency int    `koanf:"apply-concurrency"`
	ApplyBatchSize   string `koanf:"apply-batch-size"`
	NoAutoTune       bool   `koanf:"no-auto-tune"`

	ApplyDelay time.Duration `koanf:"apply-delay"`

	// MaxBufferBytes, ApplyExecTimeout, and HeartbeatInterval (below) are
	// POINTER-typed (audit N-11) because an explicit 0 is documented-meaningful
	// on the matching `sync start` flags — --apply-exec-timeout and
	// --heartbeat-interval say "0 disables", and --max-buffer-bytes=0 means no
	// orchestrator cap (migcore.ApplyMaxBufferBytes skips the setter and the
	// engine's built-in batching default applies). nil = key omitted → the
	// `sync start` flag default; a present value — INCLUDING 0 — passes through
	// verbatim ([orDefault]), so the fleet key behaves byte-identically to the
	// flag. A value-typed field cannot tell "unset" from "explicit 0" (the
	// zero-value-collapse class): the old firstNonZero* coercion silently
	// turned an operator's `apply-exec-timeout: 0` into the 60s default.
	MaxBufferBytes   *int64         `koanf:"max-buffer-bytes"`
	ApplyExecTimeout *time.Duration `koanf:"apply-exec-timeout"`

	// The apply-retry-* trio is pointer-typed for N-11's REFUSAL side rather
	// than a meaningful 0: `sync start` refuses 0 as out of the ADR-0038
	// ranges ("1 = no retry"), so nil = key omitted → the ADR-0038 defaults,
	// while an explicit value — INCLUDING 0 — reaches validateRetryFlags
	// verbatim and is refused with the CLI's exact out-of-range message. The
	// old firstNonZero* coercion silently absorbed an explicit 0 into the
	// default instead — the same silent-config-inversion class as the
	// "0 disables" knobs above, cured the same way.
	ApplyRetryAttempts    *int           `koanf:"apply-retry-attempts"`
	ApplyRetryBackoffBase *time.Duration `koanf:"apply-retry-backoff-base"`
	ApplyRetryBackoffCap  *time.Duration `koanf:"apply-retry-backoff-cap"`

	MetricsListen string `koanf:"metrics-listen"`

	// HeartbeatInterval is pointer-typed for the same N-11 reason as
	// ApplyExecTimeout above: the flag documents "0 disables".
	HeartbeatInterval *time.Duration `koanf:"heartbeat-interval"`

	PollInterval  time.Duration `koanf:"poll-interval"`
	SchemaChanges string        `koanf:"schema-changes"`

	// Notify sinks. The webhook/slack URLs and the SMTP password are
	// credentials; supply them via the SLUICE_NOTIFY_* env vars, not in
	// the committed YAML (same env-only contract as `sync start`).
	NotifyWebhook        string        `koanf:"notify-webhook"`
	NotifySlack          string        `koanf:"notify-slack"`
	NotifySyncLagSeconds float64       `koanf:"notify-sync-lag-seconds"`
	NotifyCooldown       time.Duration `koanf:"notify-cooldown"`

	// NotifySchemaDrift toggles the ADR-0157 schema-drift alert per sync.
	// Default ON, but a plain bool in a YAML spec gets the Go zero value
	// (false) when omitted — the v0.99.51 trap — so it is a *bool: nil
	// (omitted) ⇒ enabled, an explicit `false` ⇒ disabled. See
	// [schemaDriftSuppressFromSpec].
	NotifySchemaDrift *bool `koanf:"notify-schema-drift"`

	// NotifySlotHealth toggles the ADR-0059/roadmap-64a slot-health alerts
	// per sync. Default ON; a *bool for the same v0.99.51 zero-value reason
	// as NotifySchemaDrift: nil (omitted) ⇒ enabled, an explicit `false` ⇒
	// disabled. See [slotHealthSuppressFromSpec].
	NotifySlotHealth *bool `koanf:"notify-slot-health"`

	NotifySMTPHost     string   `koanf:"notify-smtp-host"`
	NotifySMTPPort     int      `koanf:"notify-smtp-port"`
	NotifySMTPFrom     string   `koanf:"notify-smtp-from"`
	NotifySMTPTo       []string `koanf:"notify-smtp-to"`
	NotifySMTPTLS      string   `koanf:"notify-smtp-tls"`
	NotifySMTPAuth     string   `koanf:"notify-smtp-auth"`
	NotifySMTPUsername string   `koanf:"notify-smtp-username"`

	// OPTIONAL per-sync PlanetScale target-health telemetry (ADR-0126,
	// mirroring `sync start`'s --planetscale-* flags). Setting planetscale-org
	// enables it: sluice polls THIS sync's PlanetScale target off the apply
	// hot path, feeding the headroom clamp + sluice_target_* metrics + diagnose
	// + the PS-gated notify-* alerts below. The two secret fields are env-first
	// (ADR-0126 §2): empty in the spec ⇒ fall back to PLANETSCALE_METRICS_TOKEN_ID
	// / PLANETSCALE_METRICS_TOKEN — set the token once in the environment, only
	// planetscale-org/-branch/-db per sync. NEVER commit a token to the YAML.
	// All-or-nothing: planetscale-org without a resolvable token is refused at
	// validate(); unset ⇒ telemetry-off (byte-identical default sync).
	PlanetScaleOrg            string `koanf:"planetscale-org"`
	PlanetScaleMetricsTokenID string `koanf:"planetscale-metrics-token-id"`
	PlanetScaleMetricsToken   string `koanf:"planetscale-metrics-token"`
	PlanetScaleMetricsBranch  string `koanf:"planetscale-metrics-branch"`
	PlanetScaleMetricsDB      string `koanf:"planetscale-metrics-db"`

	SuppressTargetMetricsHistory bool `koanf:"suppress-target-metrics-history"`

	// PS-gated threshold alerts (ADR-0107 item 36, per-sync via ADR-0126).
	// Inert unless planetscale-org telemetry is wired AND a notify sink is set;
	// a threshold of 0 leaves its rule off. notify-lag-seconds is the PS
	// control-plane TARGET-INTERNAL replica lag — distinct from the ungated
	// notify-sync-lag-seconds (sluice's own end-to-end lag) above.
	NotifyStorageUtil         float64 `koanf:"notify-storage-util"`
	NotifyCPUUtil             float64 `koanf:"notify-cpu-util"`
	NotifyMemUtil             float64 `koanf:"notify-mem-util"`
	NotifyLagSeconds          float64 `koanf:"notify-lag-seconds"`
	NotifyStorageGrowthPerMin float64 `koanf:"notify-storage-growth-per-min"`
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
			// ErrorUnused makes an unknown/typo'd YAML key a LOUD load
			// failure instead of a silent drop (the trap: `allow-cross-shard-
			// merge` misspelled, or a `sync start` flag that isn't part of the
			// curated fleet subset, would otherwise be ignored and the operator
			// would never know their knob had no effect). Every key documented
			// in the syncs.yaml samples (ADR-0122 / ADR-0126, docs/, and the
			// fleet-operator skill) IS a SyncSpec / RestartConfig field, so this
			// only rejects genuinely-unsupported keys.
			ErrorUnused: true,
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

		// N-11: an omitted retry knob (nil) takes the ADR-0038 default and
		// never trips the range refusal; an explicit value — INCLUDING 0 —
		// is validated verbatim, so the fleet refuses exactly what the
		// `sync start` flags refuse (see the SyncSpec field comment).
		if err := validateRetryFlags(
			orDefault(s.ApplyRetryAttempts, defaultApplyRetryAttempts),
			orDefault(s.ApplyRetryBackoffBase, defaultApplyRetryBackoffBase),
			orDefault(s.ApplyRetryBackoffCap, defaultApplyRetryBackoffCap),
		); err != nil {
			return fmt.Errorf("sync run: %s: %w", who, err)
		}

		// PlanetScale telemetry opt-in is per-sync all-or-nothing (ADR-0126 §3,
		// mirroring `sync start`'s contract): planetscale-org without a
		// resolvable token-id + token (spec OR the shared env vars) is refused
		// loudly here at config-load — named by stream-id and missing field —
		// rather than silently running telemetry-off. A sync that sets none is
		// telemetry-off, byte-identical to today.
		if err := s.validateTelemetry(who); err != nil {
			return err
		}

		// Per-sync zero-date policy (ADR-0127): validated to the 3 values at
		// config-load, and refused on a non-MySQL source where it has no
		// meaning (rather than silently ignored).
		if err := s.validateZeroDate(who); err != nil {
			return err
		}

		// Per-sync schema-changes mode (ADR-0091): validated to the two
		// modes at config-load. The sole consumer treats anything that is
		// not "refuse" as "forward" (see pipeline.Streamer's
		// forwardSchemaEnabled), so a typo — `refused`, `off`, `no` — would
		// otherwise silently ENABLE DDL forwarding against explicit
		// operator intent. `sync start` is covered by the kong enum on
		// --schema-changes; the fleet YAML is the only other entry point.
		if err := s.validateSchemaChanges(who); err != nil {
			return err
		}

		// Cross-shard opt-ins (ADR-0048 / Bug 152): refuse a contradictory
		// inject-shard-column + allow-cross-shard-merge combination, and
		// surface a malformed inject-shard-column NAME=VALUE at config-load
		// (named by stream-id) rather than at cold-start.
		if err := s.validateCrossShard(who); err != nil {
			return err
		}
	}
	return nil
}

// validateCrossShard enforces the per-sync cross-shard contract. It refuses a
// fleet entry that sets BOTH inject-shard-column AND allow-cross-shard-merge —
// they are the two alternative ways past preflightCrossShardCollision and are
// mutually exclusive IN EFFECT (engaging the discriminator makes the merge
// override inert; see [pipeline.Migrator.AllowCrossShardMerge]). `sync start`
// lets inject-shard-column silently win when both are passed; the fleet is a
// CURATED config surface, so it refuses the contradiction loudly at config-load
// rather than let an operator believe an inert override took effect. It also
// validates the inject-shard-column NAME=VALUE shape up front so a malformed
// value is named by stream-id at load, not at the first cold-start.
func (s *SyncSpec) validateCrossShard(who string) error {
	if s.InjectShardColumn != "" && s.AllowCrossShardMerge {
		return fmt.Errorf(
			"sync run: %s: inject-shard-column and allow-cross-shard-merge are mutually exclusive — inject-shard-column adds a per-shard discriminator that already keeps cross-shard rows disjoint, so allow-cross-shard-merge would be inert; set exactly one",
			who,
		)
	}
	if s.InjectShardColumn != "" {
		if _, err := parseInjectShardColumn(s.InjectShardColumn); err != nil {
			return fmt.Errorf("sync run: %s: %w", who, err)
		}
	}
	return nil
}

// validateZeroDate enforces the per-sync zero-date contract (ADR-0127): an
// empty value defers to the process-global --zero-date; a set value must be
// one of error|null|epoch (mirroring the --zero-date enum) and applies only to
// a MySQL-family source. A value on a non-MySQL source is refused loudly at
// config-load so the operator isn't misled into thinking it took effect.
func (s *SyncSpec) validateZeroDate(who string) error {
	if s.ZeroDate == "" {
		return nil
	}
	switch s.ZeroDate {
	case "error", "null", "epoch":
	default:
		return fmt.Errorf(
			"sync run: %s: invalid zero-date %q (want one of: error, null, epoch)",
			who, s.ZeroDate,
		)
	}
	if !isMySQLSourceDriver(s.SourceDriver) {
		return fmt.Errorf(
			"sync run: %s: zero-date is a MySQL-source policy but source-driver is %q; remove zero-date or set a MySQL source (mysql/planetscale/vitess)",
			who, s.SourceDriver,
		)
	}
	return nil
}

// validateSchemaChanges enforces the per-sync schema-changes contract
// (ADR-0091): an empty value defers to the fleet default ("forward"); a set
// value must be forward|refuse, case-insensitive to mirror the consumer's
// EqualFold. Anything else is refused loudly at config-load — the consumer
// treats every non-"refuse" string as "forward", so an unvalidated typo
// would silently invert an operator's refuse-on-DDL intent.
func (s *SyncSpec) validateSchemaChanges(who string) error {
	switch {
	case s.SchemaChanges == "",
		strings.EqualFold(s.SchemaChanges, "forward"),
		strings.EqualFold(s.SchemaChanges, "refuse"):
		return nil
	}
	return fmt.Errorf(
		"sync run: %s: invalid schema-changes %q (want one of: forward, refuse)",
		who, s.SchemaChanges,
	)
}

// isMySQLSourceDriver reports whether the named source driver is a member of
// the MySQL engine family (vanilla MySQL or a VStream flavor) — the engines
// whose readers honor the `zero_date` DSN param (ADR-0127). Keyed on the
// registry name so a new MySQL flavor slots in by name.
func isMySQLSourceDriver(name string) bool {
	switch strings.ToLower(name) {
	case "mysql", "planetscale", "vitess":
		return true
	default:
		return false
	}
}

// applyZeroDateToSourceDSN merges the per-sync zero-date policy (ADR-0127)
// into a MySQL source DSN as the `zero_date` query param — the foundational
// mechanism the fleet `zero-date` key is sugar over. An explicit `zero_date`
// already present in the DSN WINS (the merge only appends when absent), so a
// hand-set DSN param is never clobbered. The query separator is detected
// after the last '@' so a '?'/'@' inside the password never confuses it. mode
// "" (the common case) returns the DSN unchanged.
func applyZeroDateToSourceDSN(dsn, mode string) string {
	if mode == "" {
		return dsn
	}
	tail := dsn
	if at := strings.LastIndex(dsn, "@"); at >= 0 {
		tail = dsn[at+1:]
	}
	if q := strings.IndexByte(tail, '?'); q >= 0 {
		if strings.Contains(tail[q+1:], "zero_date=") {
			return dsn // an explicit DSN param wins over the fleet key
		}
		return dsn + "&zero_date=" + mode
	}
	return dsn + "?zero_date=" + mode
}

// validateTelemetry enforces the per-sync PlanetScale telemetry all-or-nothing
// contract (ADR-0126 §3): if planetscale-org is set, BOTH the token-id and the
// token must resolve (from the spec or the PLANETSCALE_METRICS_TOKEN_ID /
// PLANETSCALE_METRICS_TOKEN env vars). The refusal names the offending sync and
// the missing field; the token value itself is never echoed.
func (s *SyncSpec) validateTelemetry(who string) error {
	if s.PlanetScaleOrg == "" {
		return nil // telemetry off — the zero value, unchanged.
	}
	p := s.resolveTelemetryParams()
	switch {
	case p.tokenID == "" && p.token == "":
		return fmt.Errorf(
			"sync run: %s: planetscale-org is set but no metrics service token is resolvable: set planetscale-metrics-token-id + planetscale-metrics-token in the spec, or the PLANETSCALE_METRICS_TOKEN_ID / PLANETSCALE_METRICS_TOKEN env vars (telemetry is opt-in and all-or-nothing — it never half-runs)",
			who,
		)
	case p.tokenID == "":
		return fmt.Errorf(
			"sync run: %s: planetscale-org is set but planetscale-metrics-token-id is missing (set it in the spec or the PLANETSCALE_METRICS_TOKEN_ID env var)",
			who,
		)
	case p.token == "":
		return fmt.Errorf(
			"sync run: %s: planetscale-org is set but planetscale-metrics-token is missing (set it in the spec or the PLANETSCALE_METRICS_TOKEN env var)",
			who,
		)
	}
	return nil
}

// resolveTelemetryParams gathers this sync's PlanetScale telemetry params for
// [buildTargetTelemetryProvider], resolving the two secret fields ENV-FIRST
// (ADR-0126 §2): an empty planetscale-metrics-token-id / -token in the spec
// falls back to the shared PLANETSCALE_METRICS_TOKEN_ID / PLANETSCALE_METRICS_TOKEN
// env vars (the same vars the single-sync flags read). The common one-org-one-
// token fleet sets the secret once in the environment and only planetscale-org
// (+ optional -branch/-db) per sync. The token is never logged.
func (s *SyncSpec) resolveTelemetryParams() telemetryParams {
	return telemetryParams{
		org:       s.PlanetScaleOrg,
		tokenID:   firstNonEmpty(s.PlanetScaleMetricsTokenID, os.Getenv("PLANETSCALE_METRICS_TOKEN_ID")),
		token:     firstNonEmpty(s.PlanetScaleMetricsToken, os.Getenv("PLANETSCALE_METRICS_TOKEN")),
		metricsDB: s.PlanetScaleMetricsDB,
		branch:    s.PlanetScaleMetricsBranch,
		targetDSN: s.Target,
		engine:    s.TargetDriver,
	}
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
	DryRun          bool   `short:"n" help:"Validate the fleet config (required fields, stream-id + slot-name uniqueness, retry bounds) and print the resolved plan without starting any sync."`
	DashboardListen string `help:"Serve a read-only fleet dashboard (HTML + a /api/fleet JSON API) on ADDR (e.g. :9300). Empty = off. NO AUTHENTICATION — bind to localhost or a trusted network only."`
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
	supervised, closeTelemetry, err := buildSupervisedFleet(ctx, fleet, g)
	if err != nil {
		return err
	}
	// ADR-0126: shut down every per-sync PlanetScale telemetry provider's poll
	// goroutine on fleet exit. Each provider is also scoped to ctx as a
	// backstop, but the explicit Close is the documented lifecycle.
	defer func() { _ = closeTelemetry() }()

	sup := pipeline.NewSupervisor(supervised, fleet.Restart.toPolicy())

	// Read-only fleet dashboard (ADR-0124): opt-in via --dashboard-listen.
	// A bind-time failure is LOUD-FATAL — the operator asked for the
	// dashboard, so refuse to start the fleet rather than silently run
	// without it (mirrors phaseStartMetricsServer's bind disposition). The
	// server reads only sup.Snapshot() — never the apply path. Empty addr
	// (the zero value) ⇒ off; never started under --dry-run.
	if c.DashboardListen != "" && !c.DryRun {
		dash, err := pipeline.NewDashboardServer(c.DashboardListen, sup)
		if err != nil {
			return err
		}
		if err := dash.Start(); err != nil {
			return err
		}
		defer func() { _ = dash.Close() }()
		slog.InfoContext(ctx, "sync run: fleet dashboard listening", slog.String("addr", c.DashboardListen))
	}

	// SIGHUP hot-reload (ADR-0122 §3): re-read + re-validate the config and
	// reconcile the live fleet without a process restart. POSIX-only — on
	// Windows installReloadHandler is a no-op. A bad reload is refused
	// loudly and the running fleet keeps going untouched.
	installReloadHandler(ctx, g.Config, sup, g)

	return sup.Run(ctx)
}

// buildSupervisedFleet maps a validated fleet config to the supervisor's
// input, building one Streamer per sync and stamping each with a stable
// fingerprint of its resolved spec so [pipeline.Supervisor.Reconcile] can
// detect a CHANGED sync across a hot-reload.
//
// For each telemetry-enabled sync (ADR-0126) it also builds a PlanetScale
// telemetry provider from that sync's resolved params and attaches it to THAT
// sync's Streamer.TargetTelemetry. The returned closer shuts every provider's
// poll goroutine down; the caller (`sync run`) defers it on fleet exit. The
// provider polls independently of the Streamer's run loop, so it persists
// across a sync's supervisor restarts — exactly as the single-sync provider
// outlives a reactive re-snapshot.
func buildSupervisedFleet(ctx context.Context, fleet *SyncFleetConfig, g *Globals) ([]pipeline.SupervisedSync, func() error, error) {
	supervised := make([]pipeline.SupervisedSync, 0, len(fleet.Syncs))
	var providers []*pstelemetry.Provider
	closeProviders := func() error {
		errs := make([]error, 0, len(providers))
		for _, p := range providers {
			errs = append(errs, p.Close())
		}
		return errors.Join(errs...)
	}
	for i := range fleet.Syncs {
		spec := &fleet.Syncs[i]
		streamer, err := buildStreamerFromSpec(ctx, spec, g)
		if err != nil {
			_ = closeProviders()
			return nil, nil, fmt.Errorf("sync run: %s: %w", spec.describe(i), err)
		}
		// ADR-0126: build + attach this sync's PlanetScale telemetry provider.
		// Returns (nil, nil) when the sync did not opt in (no planetscale-org)
		// ⇒ TargetTelemetry stays the zero nil interface ⇒ byte-identical
		// default sync. telemetryProviderOrNil avoids the typed-nil trap.
		provider, err := buildTargetTelemetryProvider(ctx, spec.resolveTelemetryParams())
		if err != nil {
			_ = closeProviders()
			return nil, nil, fmt.Errorf("sync run: %s: %w", spec.describe(i), err)
		}
		if provider != nil {
			providers = append(providers, provider)
			streamer.TargetTelemetry = telemetryProviderOrNil(provider)
		}
		supervised = append(supervised, pipeline.SupervisedSync{
			ID:          spec.StreamID,
			Runner:      streamer,
			Fingerprint: spec.fingerprint(),
		})
	}
	return supervised, closeProviders, nil
}

// reloadFleet re-reads the fleet config from path, RE-RUNS the same
// load-time validators the initial load used (required fields, stream-id
// + slot-name uniqueness — the data-corruption guards), and on success
// reconciles the live supervisor. THE load-bearing property: if the new
// config fails to parse or fails validation, reloadFleet returns the
// error WITHOUT calling Reconcile, so the running fleet keeps going on
// the old config unchanged. A malformed / colliding reloaded config can
// never take down or corrupt the live fleet.
func reloadFleet(ctx context.Context, path string, sup *pipeline.Supervisor, g *Globals) error {
	fleet, err := loadFleetConfig(path)
	if err != nil {
		return err
	}
	if err := fleet.validate(); err != nil {
		return err
	}
	warnSharedTargetBudget(fleet)
	supervised, closeTelemetry, err := buildSupervisedFleet(ctx, fleet, g)
	if err != nil {
		return err
	}
	if _, err := sup.Reconcile(supervised); err != nil {
		// Reconcile refused the reload (live fleet untouched): none of the
		// freshly built telemetry providers were adopted, so close them now to
		// avoid leaking their poll goroutines.
		_ = closeTelemetry()
		return err
	}
	// On a successful reconcile the adopted syncs' providers are live for the
	// fleet's lifetime. Providers built for an UNCHANGED sync (which Reconcile
	// discards in favour of the already-running streamer) are not closed here,
	// but every provider is scoped to ctx, so they all stop at fleet shutdown —
	// a bounded, advisory, failure-isolated duplicate-poll between a reload and
	// shutdown, never a permanent goroutine leak.
	return nil
}

// fingerprint is a stable hash of a resolved SyncSpec — every field that
// affects the built Streamer. The supervisor compares it per stream-id
// across a hot-reload to tell an UNCHANGED sync (leave running) from a
// CHANGED one (stop + restart with the new spec). JSON marshalling is
// order-stable for a flat struct, so two equal specs hash identically.
func (s *SyncSpec) fingerprint() string {
	b, err := json.Marshal(s)
	if err != nil {
		// A SyncSpec is plain data (strings / numbers / string slices) and
		// always marshals; fall back to a never-equal sentinel so a
		// (theoretical) failure forces a restart rather than a false match.
		return fmt.Sprintf("unmarshalable:%v", err)
	}
	sum := sha256.Sum256(b)
	return hex.EncodeToString(sum[:])
}

// buildStreamerFromSpec maps a SyncSpec to a *pipeline.Streamer, reusing
// the exact `sync start` helpers so a fleet sync behaves identically to
// the same flags on the standalone command. ctx scopes the one live probe
// the mapping may issue — the --control-keyspace auto-detect on a MySQL/
// VStream target (empty spec.ControlKeyspace + sharded target); every other
// target resolves it without connecting.
func buildStreamerFromSpec(ctx context.Context, spec *SyncSpec, g *Globals) (*pipeline.Streamer, error) {
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
	filter, err := migcore.NewTableFilter(spec.IncludeTable, spec.ExcludeTable)
	if err != nil {
		return nil, err
	}

	// Cross-shard opt-ins (ADR-0048 / Bug 152), threaded onto the Streamer the
	// same way `sync start` sets them. inject-shard-column is the NAME=VALUE
	// discriminator (parsed via the shared parser); allow-cross-shard-merge is
	// the globally-unique-key override. The load-time validateCrossShard guard
	// has already refused the contradictory both-set case and any malformed
	// NAME=VALUE, so a re-parse here can only succeed on the real fleet paths.
	shardSpec, err := parseInjectShardColumn(spec.InjectShardColumn)
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

	// Apply the fleet-wide value-fidelity flags (--mysql-sql-mode / --zero-date /
	// --sqlite-date-encoding) onto this sync's engines (task 2.5, replacing the
	// former process-wide globals main.go set for the whole `sync run`). Each
	// sync's per-spec DSN param still wins over these defaults — in particular the
	// per-spec zero-date is folded into the source DSN below, which overrides the
	// engine default the same way a per-command DSN param does.
	if source, err = applySourceEngineOptions(source, g); err != nil {
		return nil, err
	}
	if target, err = applyEngineOptions(target, g); err != nil {
		return nil, err
	}

	// --control-keyspace parity with `sync start` (task 1): resolve + record the
	// sidecar control keyspace on the MySQL/VStream target so a sharded-target
	// fleet sync routes its CDC control tables to the right unsharded keyspace.
	// Empty defers to the same auto-detect; a non-MySQL or unsharded target is
	// inert (returned unchanged).
	if target, err = applyControlKeyspace(ctx, target, spec.ControlKeyspace, spec.Target); err != nil {
		return nil, err
	}

	// Per-sync zero-date policy (ADR-0127): fold the `zero-date` key into the
	// MySQL source DSN as `?zero_date=` so the source reader picks it up. Only
	// MySQL-family sources honor it (validated at config-load); a hand-set DSN
	// param wins. Non-MySQL sources keep spec.Source byte-identical.
	sourceDSN := spec.Source
	if isMySQLSourceDriver(spec.SourceDriver) {
		sourceDSN = applyZeroDateToSourceDSN(spec.Source, spec.ZeroDate)
	}

	return &pipeline.Streamer{
		Source:             source,
		Target:             target,
		SourceDSN:          sourceDSN,
		TargetDSN:          spec.Target,
		StreamID:           spec.StreamID,
		SlotName:           spec.SlotName,
		Mappings:           mappings,
		ExpressionMappings: exprMappings,
		Filter:             filter,
		TargetSchema:       spec.TargetSchema,

		InjectShardColumn:    shardSpec,
		AllowCrossShardMerge: spec.AllowCrossShardMerge,

		ApplyBatchSize:   applyBatchSize,
		AutoTune:         !spec.NoAutoTune,
		ApplyConcurrency: spec.ApplyConcurrency,
		ApplyDelay:       spec.ApplyDelay,
		// N-11: nil = key omitted → the `sync start` flag default; an explicit
		// 0 passes through as 0 ("0 disables" / no orchestrator cap), exactly
		// as the same value on the `sync start` flag does.
		MaxBufferBytes:   orDefault(spec.MaxBufferBytes, defaultMaxBufferBytes),
		ApplyExecTimeout: orDefault(spec.ApplyExecTimeout, defaultApplyExecTimeout),

		ApplyRetryAttempts:    orDefault(spec.ApplyRetryAttempts, defaultApplyRetryAttempts),
		ApplyRetryBackoffBase: orDefault(spec.ApplyRetryBackoffBase, defaultApplyRetryBackoffBase),
		ApplyRetryBackoffCap:  orDefault(spec.ApplyRetryBackoffCap, defaultApplyRetryBackoffCap),

		MetricsListen:     spec.MetricsListen,
		HeartbeatInterval: orDefault(spec.HeartbeatInterval, defaultHeartbeatInterval),
		PollInterval:      spec.PollInterval,
		SchemaChanges:     firstNonEmpty(spec.SchemaChanges, defaultSchemaChanges),

		NotifyWebhookURL:      spec.NotifyWebhook,
		NotifySlackWebhookURL: spec.NotifySlack,
		NotifySyncLagSeconds:  spec.NotifySyncLagSeconds,
		NotifyCooldown:        spec.NotifyCooldown,
		NotifySMTP:            smtp,
		// ADR-0157: default-ON schema-drift alert; nil (omitted) ⇒ enabled.
		SuppressSchemaDriftNotify: schemaDriftSuppressFromSpec(spec.NotifySchemaDrift),
		// Roadmap 64a: default-ON slot-health alert; nil (omitted) ⇒ enabled.
		SuppressSlotHealthNotify: slotHealthSuppressFromSpec(spec.NotifySlotHealth),

		// ADR-0126: per-sync PlanetScale telemetry config. The provider itself
		// is built + attached in buildSupervisedFleet (it needs ctx + a poll
		// goroutine); these are the plain-data knobs. The PS-gated notify rules
		// stay inert unless a provider is wired AND a sink + threshold are set,
		// exactly as on `sync start` — so setting them here is always safe.
		SuppressTargetMetricsHistory: spec.SuppressTargetMetricsHistory,
		NotifyStorageUtil:            spec.NotifyStorageUtil,
		NotifyCPUUtil:                spec.NotifyCPUUtil,
		NotifyMemUtil:                spec.NotifyMemUtil,
		NotifyLagSeconds:             spec.NotifyLagSeconds,
		NotifyStorageGrowthPerMin:    spec.NotifyStorageGrowthPerMin,

		BuildVersion: version,
		BuildCommit:  commit,
	}, nil
}

// schemaDriftSuppressFromSpec maps the fleet spec's default-ON
// *bool NotifySchemaDrift to the streamer's opt-OUT
// SuppressSchemaDriftNotify (ADR-0157). A YAML spec that omits the key
// yields nil ⇒ the alert stays ENABLED (the zero-value-safe default holds
// for the fleet path, not just `sync start`); an explicit `false` disables
// it. Mirrors [SyncStartCmd.suppressSchemaDriftNotify] for the CLI path.
func schemaDriftSuppressFromSpec(notifySchemaDrift *bool) bool {
	return notifySchemaDrift != nil && !*notifySchemaDrift
}

// slotHealthSuppressFromSpec is the same default-ON *bool → opt-OUT
// mapping for the roadmap-64a slot-health alert (ADR-0059 implementation
// note): nil (omitted) ⇒ ENABLED, explicit `false` ⇒ suppressed. Mirrors
// [schemaDriftSuppressFromSpec] / [SyncStartCmd.suppressSlotHealthNotify].
func slotHealthSuppressFromSpec(notifySlotHealth *bool) bool {
	return notifySlotHealth != nil && !*notifySlotHealth
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
// Source/target DSNs pass through [diagnose.RedactDSN] so a --dry-run plan
// never leaks credentials — dsnEndpoint alone falls through to the raw DSN
// for a scheme-less go-sql-driver DSN (user:pw@tcp(host)/db, the common
// MySQL/PlanetScale shape), which would print the password.
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
			out, "  %s\t%s://%s -> %s://%s\tslot=%s\ttelemetry=%s\n",
			s.StreamID, s.SourceDriver, diagnose.RedactDSN(s.Source),
			s.TargetDriver, diagnose.RedactDSN(s.Target), slot, telemetryPlanLabel(s),
		); err != nil {
			return err
		}
	}
	p := fleet.Restart.toPolicy()
	// The firstNonZeroDuration here is DISPLAY-ONLY parity with
	// [pipeline.RestartPolicy]'s withDefaults (N-11 class (b)): zero is
	// documented as "fall back to the policy defaults" on RestartConfig and a
	// 0 backoff has no valid reading, so the plan shows the values the
	// supervisor will actually run with. The policy itself receives the raw
	// fields and applies the same defaults internally.
	_, err := fmt.Fprintf(
		out, "restart: backoff %s..%s, max-consecutive-failures=%d (0=unbounded)\n",
		firstNonZeroDuration(p.BackoffBase, time.Second),
		firstNonZeroDuration(p.BackoffCap, 30*time.Second),
		p.MaxConsecutiveFailures,
	)
	return err
}

// telemetryPlanLabel renders a sync's PlanetScale telemetry disposition for
// the --dry-run plan (ADR-0126 §5): "off" when no planetscale-org, else
// "org/db@branch" where db is the explicit planetscale-metrics-db or the one
// derived from the target DSN ("?" when neither yields a name), and branch is
// the configured branch or "main". It NEVER prints the token.
func telemetryPlanLabel(s *SyncSpec) string {
	if s.PlanetScaleOrg == "" {
		return "off"
	}
	db := s.PlanetScaleMetricsDB
	if db == "" {
		db = databaseFromDSN(s.Target)
	}
	if db == "" {
		db = "?"
	}
	return fmt.Sprintf("%s/%s@%s", s.PlanetScaleOrg, db, branchOrMainLabel(s.PlanetScaleMetricsBranch))
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
// URL-form (scheme://[user[:pass]@]host[:port]/...), keyword-form
// (host=... port=...), and the go-sql-driver form
// (user:pass@net(addr)/db). A genuinely unparseable DSN is REDACTED, never
// returned raw — this value is logged (shared-target WARN, status --all), so
// it must never carry a credential.
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
	// go-sql-driver form: [user[:pass]@][net[(addr)]]/dbname (the common
	// MySQL/PlanetScale shape, which matches neither branch above). Strip
	// the userinfo, pull the addr out of net(addr), drop the db — the raw
	// DSN carries the password and must never be returned (this value feeds
	// the shared-target-budget WARN and the `status --all` unreachable line).
	rest := dsn
	if at := strings.LastIndex(rest, "@"); at >= 0 {
		rest = rest[at+1:]
	}
	if open := strings.IndexByte(rest, '('); open >= 0 {
		if closeIdx := strings.IndexByte(rest, ')'); closeIdx > open {
			rest = rest[open+1 : closeIdx]
		}
	}
	if slash := strings.IndexByte(rest, '/'); slash >= 0 {
		rest = rest[:slash]
	}
	if rest != "" {
		return rest
	}
	// Genuinely unparseable: redact rather than leak a credential.
	return "<redacted-dsn>"
}

func firstNonEmpty(v, fallback string) string {
	if v == "" {
		return fallback
	}
	return v
}

// firstNonZeroDuration survives only for [printFleetPlan]'s display-side
// mirror of [pipeline.RestartPolicy]'s withDefaults; every per-sync knob now
// resolves through [orDefault] (audit N-11).
func firstNonZeroDuration(v, fallback time.Duration) time.Duration {
	if v == 0 {
		return fallback
	}
	return v
}

// orDefault resolves a pointer-typed fleet knob (audit N-11): nil — the YAML
// key was omitted — falls back to the `sync start` flag default; a present
// value, INCLUDING an explicit 0, passes through verbatim — to disable, for
// the knobs whose flags document "0 disables" / no cap, or into the ADR-0038
// range refusal, for the apply-retry-* knobs whose flags refuse 0.
func orDefault[T any](v *T, fallback T) T {
	if v == nil {
		return fallback
	}
	return *v
}
