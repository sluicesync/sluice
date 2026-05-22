// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package pipeline

// ADR-0054 Shape A Phase 2: live cross-shard DDL coordination — lease
// primitive.
//
// Cross-shard DDL coordination lives in a new per-target control table
// `sluice_shard_consolidation_lease` (one row per consolidated target
// table). The engines own the row schema (CREATE TABLE under
// EnsureControlTable in internal/engines/{postgres,mysql}/control_table.go,
// additive to sluice_cdc_state and sluice_cdc_schema_history). The
// LeaseManager defined here owns the state machine + heartbeat goroutine,
// dispatching SQL via a small engine-private surface
// [ShardConsolidationLeaseStore] that engines plug in by implementing the
// interface on their ChangeApplier (probed by type-assertion, same
// shape as [shardPreflightProber]).
//
// The state machine (per ADR-0054 §1):
//
//	ABSENT → HELD (Acquire INSERTs a row with lease_expires_at = now+TTL)
//	HELD → APPLIED (Apply UPDATES applied_at + ddl_checksum + version)
//	HELD → EXPIRED (heartbeat stops → lease_expires_at <= now)
//	EXPIRED → HELD (takeover-stream conditional UPDATE; runs probe-and-record)
//
// Hybrid TTL + heartbeat semantics (DP-A): a holder goroutine writes
// lease_expires_at = now + LeaseDuration every RetryPeriod. A holder
// that misses its RenewDeadline considers itself failed and exits the
// apply path (the caller's ctx is left untouched; the lease will
// simply expire and another stream can take over).
//
// Probe-and-record on takeover (DP-C) is implemented in
// shard_consolidation_probe.go (Phase 2c); this file's Acquire surfaces
// the takeover signal via [LeaseTakeover] on the returned Lease so the
// caller can dispatch the right probe.

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"log/slog"
	"strings"
	"sync"
	"time"
	"unicode"

	"github.com/orware/sluice/internal/ir"
)

// Default lease timing per ADR-0054 §2 / DP-A. Mirror Kubernetes
// leader-election semantics; the K8s defaults (15/10/2) are tuned for
// control-plane components, sluice relaxes them slightly because the
// failure mode is operator-visible (stream pause) rather than
// control-plane-visible.
const (
	DefaultLeaseDuration = 30 * time.Second
	DefaultRenewDeadline = 20 * time.Second
	DefaultRetryPeriod   = 10 * time.Second
)

// LeaseState classifies the observed state of a lease row at a moment
// in time. Used by [LeaseManager.Observe] for status output and by
// Acquire's pre-check. See ADR-0054 §1 for the state transitions.
type LeaseState int

const (
	// LeaseStateAbsent — no row for the target table. First-shard
	// path: Acquire INSERTs a fresh row.
	LeaseStateAbsent LeaseState = iota

	// LeaseStateHeld — a row exists with lease_expires_at > now() and
	// applied_at IS NULL. Some stream is mid-apply. Observers wait;
	// peer streams attempting Acquire see contention and back off.
	LeaseStateHeld

	// LeaseStateExpired — a row exists with lease_expires_at <= now()
	// and applied_at IS NULL. Takeover-eligible. Acquire runs the
	// probe-and-record path (Phase 2c).
	LeaseStateExpired

	// LeaseStateApplied — a row exists with applied_at IS NOT NULL.
	// The DDL has landed; observers verify ddl_checksum matches and
	// advance their schema-version cursor (no re-apply).
	LeaseStateApplied
)

// String renders a LeaseState for logs and status JSON.
func (s LeaseState) String() string {
	switch s {
	case LeaseStateAbsent:
		return "absent"
	case LeaseStateHeld:
		return "held"
	case LeaseStateExpired:
		return "expired"
	case LeaseStateApplied:
		return "applied"
	}
	return "unknown"
}

// LeaseObservation is the snapshot a peer stream loads from the lease
// row to decide whether to apply, wait, or refuse loudly. Returned by
// [LeaseManager.Observe].
type LeaseObservation struct {
	// State classifies the row at observation time.
	State LeaseState

	// HolderStreamID is the lease_holder_stream_id when State is Held
	// or Expired (i.e. some stream once held it). Empty in Absent.
	HolderStreamID string

	// ExpiresAt is the lease_expires_at when known. Zero in Absent.
	ExpiresAt time.Time

	// DDLText is the recorded ddl_text. Populated when a previous
	// holder reached the apply phase; empty before that.
	DDLText string

	// DDLChecksum is the recorded SHA-256 hex; non-empty only when
	// State is Applied.
	DDLChecksum string

	// AppliedSchemaVersion is the boundary version the holder
	// recorded. Zero before Apply.
	AppliedSchemaVersion int64

	// AppliedAt is the wall-clock at which the holder's UPDATE
	// committed. Zero unless State is Applied.
	AppliedAt time.Time
}

// ShardConsolidationLeaseRow is the engine-exchange row type for the
// lease primitive. The canonical definition lives in `ir` so engines
// can implement [ir.ShardConsolidationLeaseStore] without importing
// pipeline (which would create a cycle).
//
// Aliased here so existing pipeline code can keep referring to the
// type by its short name.
type ShardConsolidationLeaseRow = ir.ShardConsolidationLeaseRow

// ShardConsolidationLeaseStore is the engine-implemented surface the
// LeaseManager uses. Canonical definition in `ir` (engine packages
// implement it without importing pipeline).
type ShardConsolidationLeaseStore = ir.ShardConsolidationLeaseStore

// NormalizeDDLText applies the ADR-0054 §3 normalization to a DDL
// statement before checksumming: strip leading/trailing whitespace,
// collapse runs of internal whitespace to a single space, lowercase
// reserved-class (alphabetic) tokens. Mirrors the shape ADR-0049's
// SchemaSignature.Equal applies for IR-derived structural fingerprints
// (whitespace + case normalization on the structural surface).
//
// The normalization is intentionally conservative: it removes cosmetic
// differences operators are likely to introduce by hand (extra spaces,
// case variation on reserved words) without touching identifier
// quoting, literal values, or operator-meaningful punctuation. Tokens
// that contain non-letters pass through verbatim so quoted identifiers
// `"Foo Bar"` and string literals 'X = Y' aren't case-folded.
func NormalizeDDLText(raw string) string {
	trimmed := strings.TrimSpace(raw)
	if trimmed == "" {
		return ""
	}
	// Collapse whitespace + lowercase alphabetic tokens. A token is a
	// maximal run of non-whitespace characters. A token composed
	// entirely of ASCII letters is lowercased; mixed tokens pass
	// through (preserves quoted identifiers and literals).
	var b strings.Builder
	b.Grow(len(trimmed))
	first := true
	for _, tok := range strings.Fields(trimmed) {
		if !first {
			b.WriteByte(' ')
		}
		first = false
		if isAllLetters(tok) {
			b.WriteString(strings.ToLower(tok))
		} else {
			b.WriteString(tok)
		}
	}
	return b.String()
}

// isAllLetters reports whether s is non-empty and composed solely of
// ASCII letters. Conservative: a token like `foo,` (with trailing
// comma) returns false and passes through unchanged.
func isAllLetters(s string) bool {
	if s == "" {
		return false
	}
	for _, r := range s {
		if !unicode.IsLetter(r) {
			return false
		}
	}
	return true
}

// ChecksumDDLText computes the SHA-256 hex of the normalized form of
// raw. Used as the lease row's ddl_checksum. Two operators (across
// shards) who issue the same logical DDL — modulo whitespace and
// reserved-keyword case — produce the same checksum.
func ChecksumDDLText(raw string) string {
	norm := NormalizeDDLText(raw)
	sum := sha256.Sum256([]byte(norm))
	return hex.EncodeToString(sum[:])
}

// LeaseConfig collects the timing knobs operator-tunable via the
// --shard-coordination-* CLI flags. The zero value uses the ADR-0054
// §2 defaults (30s / 20s / 10s).
type LeaseConfig struct {
	// LeaseDuration is the TTL written into lease_expires_at on every
	// heartbeat-extend. Zero falls back to [DefaultLeaseDuration].
	LeaseDuration time.Duration

	// RenewDeadline is the time-budget a holder has to write a
	// successful heartbeat before considering itself failed. Zero
	// falls back to [DefaultRenewDeadline]. Must be < LeaseDuration
	// (otherwise the holder can lose the lease before noticing).
	RenewDeadline time.Duration

	// RetryPeriod is the cadence the holder writes heartbeats at.
	// Zero falls back to [DefaultRetryPeriod]. Must be <
	// RenewDeadline (otherwise the holder's missed-heartbeat
	// detection can't fire before the renew window closes).
	RetryPeriod time.Duration
}

// Resolved returns a copy of cfg with zero fields filled in from the
// ADR-0054 defaults.
func (cfg LeaseConfig) Resolved() LeaseConfig {
	out := cfg
	if out.LeaseDuration <= 0 {
		out.LeaseDuration = DefaultLeaseDuration
	}
	if out.RenewDeadline <= 0 {
		out.RenewDeadline = DefaultRenewDeadline
	}
	if out.RetryPeriod <= 0 {
		out.RetryPeriod = DefaultRetryPeriod
	}
	return out
}

// Validate checks the LeaseConfig invariants: LeaseDuration >
// RenewDeadline > RetryPeriod > 0. Returns a precise error naming the
// offending pair so the operator can correct it without consulting
// the docs.
func (cfg LeaseConfig) Validate() error {
	r := cfg.Resolved()
	if r.RetryPeriod <= 0 {
		return fmt.Errorf("--shard-coordination-retry-period=%s must be > 0", r.RetryPeriod)
	}
	if r.RenewDeadline <= r.RetryPeriod {
		return fmt.Errorf(
			"--shard-coordination-renew-deadline=%s must be > --shard-coordination-retry-period=%s "+
				"(holder needs room to retry a failed heartbeat before the renew window closes)",
			r.RenewDeadline, r.RetryPeriod,
		)
	}
	if r.LeaseDuration <= r.RenewDeadline {
		return fmt.Errorf(
			"--shard-coordination-lease-duration=%s must be > --shard-coordination-renew-deadline=%s "+
				"(TTL must outlast the renew budget or the holder loses the lease before noticing failure)",
			r.LeaseDuration, r.RenewDeadline,
		)
	}
	return nil
}

// LeaseManager owns the ADR-0054 lease state machine. One instance per
// stream (Streamer constructs it after engaging Shape A live
// coordination); methods are safe for concurrent calls only across
// different leases — a single Lease handle is single-goroutine on the
// holder side (heartbeat + apply share it).
type LeaseManager struct {
	store    ShardConsolidationLeaseStore
	streamID string
	cfg      LeaseConfig
	now      func() time.Time // injectable clock for tests
}

// NewLeaseManager constructs a LeaseManager that drives store on
// behalf of streamID. The streamID must be globally unique across all
// peer streams (the lease's holder identity).
//
// Returns an error when cfg fails [LeaseConfig.Validate]. Zero-value
// cfg uses the ADR-0054 §2 defaults.
func NewLeaseManager(store ShardConsolidationLeaseStore, streamID string, cfg LeaseConfig) (*LeaseManager, error) {
	if store == nil {
		return nil, errors.New("pipeline: NewLeaseManager: store is nil")
	}
	if strings.TrimSpace(streamID) == "" {
		return nil, errors.New("pipeline: NewLeaseManager: streamID is empty")
	}
	if err := cfg.Validate(); err != nil {
		return nil, fmt.Errorf("pipeline: NewLeaseManager: %w", err)
	}
	return &LeaseManager{
		store:    store,
		streamID: streamID,
		cfg:      cfg.Resolved(),
		now:      time.Now,
	}, nil
}

// Lease is a held lease handle. The owning goroutine (lease-holder
// streamer) calls Apply when the DDL completes on the target. The
// heartbeat goroutine extends lease_expires_at on the configured
// RetryPeriod cadence; Apply or ctx cancellation stops it.
//
// Lease is single-goroutine on the owner side: Apply and the
// background heartbeat goroutine are the only writers. A peer stream
// observing the lease does not hold a Lease — it uses
// LeaseManager.Observe directly.
type Lease struct {
	mgr        *LeaseManager
	tableName  string
	stopCh     chan struct{}
	doneCh     chan struct{}
	finalizeMu sync.Mutex
	finalized  bool

	// takeover is true when Acquire took over an expired row whose
	// ddl_text was populated but applied_at was NULL. The caller's
	// probe-and-record path (Phase 2c) keys off this to decide
	// re-apply vs just-record.
	takeover bool

	// priorDDLText carries the previous (crashed) holder's recorded
	// ddl_text on takeover. Empty when takeover is false.
	priorDDLText string
}

// Takeover reports whether this lease acquisition took over an
// expired-but-not-recorded prior lease (the lease_holder crashed mid-
// apply on a non-transactional-DDL engine). When true, the caller must
// run probe-and-record (Phase 2c) before re-applying.
func (l *Lease) Takeover() bool {
	if l == nil {
		return false
	}
	return l.takeover
}

// PriorDDLText returns the prior (crashed) holder's recorded ddl_text
// when [Takeover] is true. Empty otherwise.
func (l *Lease) PriorDDLText() string {
	if l == nil {
		return ""
	}
	return l.priorDDLText
}

// TableName returns the consolidated target table this lease guards.
func (l *Lease) TableName() string {
	if l == nil {
		return ""
	}
	return l.tableName
}

// StreamID returns the holder stream-id.
func (l *Lease) StreamID() string {
	if l == nil || l.mgr == nil {
		return ""
	}
	return l.mgr.streamID
}

// Acquire attempts to take the lease for tableName, returning a held
// Lease on success. Starts the heartbeat goroutine before returning;
// the goroutine extends lease_expires_at on the configured
// RetryPeriod until Apply() or ctx cancellation.
//
// On contention (another stream holds the lease, not yet expired),
// returns nil + a wrapped error tagged with [ErrLeaseContended] so the
// caller can poll-with-backoff.
//
// When a prior holder's expired-but-not-recorded row is taken over,
// Lease.Takeover() returns true and Lease.PriorDDLText() returns the
// recorded text; the caller's probe-and-record path uses these.
//
// ddlText is recorded into the row before the heartbeat goroutine
// starts so a crashed holder leaves a usable signal for the next
// takeover-stream.
func (m *LeaseManager) Acquire(ctx context.Context, tableName, ddlText string) (*Lease, error) {
	if m == nil {
		return nil, errors.New("pipeline: LeaseManager.Acquire: receiver is nil")
	}
	if strings.TrimSpace(tableName) == "" {
		return nil, errors.New("pipeline: LeaseManager.Acquire: tableName is empty")
	}

	expires := m.now().Add(m.cfg.LeaseDuration)
	acquired, current, err := m.store.TryAcquireLease(ctx, tableName, m.streamID, expires)
	if err != nil {
		return nil, fmt.Errorf("pipeline: lease acquire %q: %w", tableName, err)
	}
	if !acquired {
		state := classifyLeaseRow(current, m.now())
		return nil, fmt.Errorf("%w: lease for %q is %s (holder %q expires %s)",
			ErrLeaseContended, tableName, state, current.LeaseHolderStreamID, current.LeaseExpiresAt.Format(time.RFC3339))
	}

	// Acquire succeeded. If the previous row was expired with a
	// populated ddl_text and applied_at NULL, this is a takeover; the
	// caller's probe-and-record path takes the right action via
	// Lease.Takeover().
	takeover := current.DDLText != "" && !current.HasAppliedAt

	// Record the new DDL text the lease-holder is about to apply.
	// Surface the recorded value as the prior text on takeover —
	// callers don't get to see two records overlap; the lease-row's
	// ddl_text reflects what the CURRENT holder is doing, the prior
	// text is preserved in the returned Lease.priorDDLText for the
	// probe.
	priorDDLText := ""
	if takeover {
		priorDDLText = current.DDLText
	}
	if ddlText != "" {
		if recorded, err := m.store.RecordDDLText(ctx, tableName, m.streamID, ddlText); err != nil {
			return nil, fmt.Errorf("pipeline: lease record ddl %q: %w", tableName, err)
		} else if !recorded {
			return nil, fmt.Errorf("%w: lease for %q taken over between acquire and ddl-record",
				ErrLeaseContended, tableName)
		}
	}

	lease := &Lease{
		mgr:          m,
		tableName:    tableName,
		stopCh:       make(chan struct{}),
		doneCh:       make(chan struct{}),
		takeover:     takeover,
		priorDDLText: priorDDLText,
	}
	go m.heartbeatLoop(ctx, lease)
	slog.InfoContext(
		ctx, "shard consolidation lease acquired",
		"table", tableName,
		"stream_id", m.streamID,
		"takeover", takeover,
		"expires_at", expires.Format(time.RFC3339),
	)
	return lease, nil
}

// Heartbeat performs one synchronous heartbeat-extend on the lease.
// Returns nil on success, [ErrLeaseLost] when the lease has been
// taken over by another stream. Exposed for tests and for the
// integration suite's crash-injection matrix; production code relies
// on the heartbeat goroutine started by Acquire.
func (m *LeaseManager) Heartbeat(ctx context.Context, l *Lease) error {
	if l == nil {
		return errors.New("pipeline: LeaseManager.Heartbeat: lease is nil")
	}
	expires := m.now().Add(m.cfg.LeaseDuration)
	extended, err := m.store.HeartbeatLease(ctx, l.tableName, m.streamID, expires)
	if err != nil {
		return fmt.Errorf("pipeline: lease heartbeat %q: %w", l.tableName, err)
	}
	if !extended {
		return fmt.Errorf("%w: lease for %q taken over", ErrLeaseLost, l.tableName)
	}
	return nil
}

// Apply finalizes the lease: records applied_at + ddl_checksum +
// applied_schema_version, then stops the heartbeat goroutine.
//
// The caller must run the actual ALTER on the target before calling
// Apply (or, on a transactional-DDL engine like PG, in the same tx
// that wraps the FinalizeLeaseApply UPDATE if engine support is
// added later). For v1 Phase 2 the holder runs the ALTER, then calls
// Apply; the gap between ALTER-commit and Apply is the
// non-transactional-DDL hazard ADR-0054 §4's probe-and-record
// handles.
//
// Returns nil on success. [ErrLeaseLost] when the lease has been
// taken over between heartbeat-loop and finalize. Apply is idempotent
// on the caller's side: a second Apply returns nil without
// re-issuing the UPDATE.
func (m *LeaseManager) Apply(ctx context.Context, l *Lease, version int64, ddlText, ddlChecksum string) error {
	if l == nil {
		return errors.New("pipeline: LeaseManager.Apply: lease is nil")
	}
	l.finalizeMu.Lock()
	already := l.finalized
	l.finalized = true
	l.finalizeMu.Unlock()
	if already {
		return nil
	}
	finalized, err := m.store.FinalizeLeaseApply(ctx, l.tableName, m.streamID, ddlText, ddlChecksum, version)
	// Stop the heartbeat goroutine regardless of outcome — if the
	// finalize failed the caller has to surface the error, but the
	// lease is no longer ours to extend.
	close(l.stopCh)
	<-l.doneCh
	if err != nil {
		return fmt.Errorf("pipeline: lease apply %q: %w", l.tableName, err)
	}
	if !finalized {
		return fmt.Errorf("%w: lease for %q taken over before finalize", ErrLeaseLost, l.tableName)
	}
	slog.InfoContext(
		ctx, "shard consolidation lease applied",
		"table", l.tableName,
		"stream_id", m.streamID,
		"version", version,
		"ddl_checksum", ddlChecksum,
	)
	return nil
}

// Release stops the heartbeat goroutine without recording an apply.
// Used by the caller's error paths (DDL failed; probe inconsistent)
// so the lease expires on its TTL rather than getting renewed
// indefinitely. Idempotent — safe to call after Apply (no-op).
func (m *LeaseManager) Release(_ context.Context, l *Lease) {
	if l == nil {
		return
	}
	l.finalizeMu.Lock()
	already := l.finalized
	l.finalized = true
	l.finalizeMu.Unlock()
	if already {
		return
	}
	close(l.stopCh)
	<-l.doneCh
}

// Observe loads the current lease-row for tableName and classifies it
// into a [LeaseObservation]. Peer streams (the N-1 shards not holding
// the lease) call this to decide whether to wait, advance, or refuse
// loudly. Tolerant of the row being absent (returns
// LeaseStateAbsent).
func (m *LeaseManager) Observe(ctx context.Context, tableName string) (LeaseObservation, error) {
	row, ok, err := m.store.ObserveLease(ctx, tableName)
	if err != nil {
		return LeaseObservation{}, fmt.Errorf("pipeline: lease observe %q: %w", tableName, err)
	}
	if !ok {
		return LeaseObservation{State: LeaseStateAbsent}, nil
	}
	return LeaseObservation{
		State:                classifyLeaseRow(row, m.now()),
		HolderStreamID:       row.LeaseHolderStreamID,
		ExpiresAt:            row.LeaseExpiresAt,
		DDLText:              row.DDLText,
		DDLChecksum:          row.DDLChecksum,
		AppliedSchemaVersion: row.AppliedSchemaVersion,
		AppliedAt:            row.AppliedAt,
	}, nil
}

// classifyLeaseRow maps a loaded row to its LeaseState given the
// caller's "now" clock. Splits ABSENT/HELD/EXPIRED/APPLIED per
// ADR-0054 §1 state-machine semantics.
func classifyLeaseRow(row ShardConsolidationLeaseRow, now time.Time) LeaseState {
	if row.HasAppliedAt {
		return LeaseStateApplied
	}
	if !row.HasLeaseExpiresAt {
		// Defensive: a half-populated row (acquire INSERT raced with
		// a peer reader) is treated as ABSENT so peer streams retry
		// rather than wedging.
		return LeaseStateAbsent
	}
	if row.LeaseExpiresAt.After(now) {
		return LeaseStateHeld
	}
	return LeaseStateExpired
}

// heartbeatLoop runs in a goroutine for the lifetime of a held lease,
// extending lease_expires_at every RetryPeriod. Exits on stopCh
// (Apply / Release called) or when the holder loses the lease.
//
// Per ADR-0054 §2 / DP-A: the holder considers itself failed if a
// heartbeat hasn't succeeded within RenewDeadline. The
// failed-heartbeat path doesn't *do* anything (we can't cancel the
// caller's ctx — that's the caller's job); it just stops trying and
// logs. The caller's ctx eventually drives the apply path to exit,
// or a peer stream observes the expired row and takes over.
func (m *LeaseManager) heartbeatLoop(ctx context.Context, l *Lease) {
	defer close(l.doneCh)
	cfg := m.cfg
	tick := time.NewTicker(cfg.RetryPeriod)
	defer tick.Stop()

	lastSuccess := m.now()
	for {
		select {
		case <-l.stopCh:
			return
		case <-ctx.Done():
			return
		case <-tick.C:
			hbCtx, cancel := context.WithTimeout(ctx, cfg.RenewDeadline)
			err := m.Heartbeat(hbCtx, l)
			cancel()
			now := m.now()
			if err == nil {
				lastSuccess = now
				slog.DebugContext(
					ctx, "shard consolidation lease heartbeat extended",
					"table", l.tableName,
					"stream_id", m.streamID,
				)
				continue
			}
			if errors.Is(err, ErrLeaseLost) {
				slog.WarnContext(
					ctx, "shard consolidation lease lost (taken over by peer)",
					"table", l.tableName,
					"stream_id", m.streamID,
					"error", err,
				)
				return
			}
			if now.Sub(lastSuccess) >= cfg.RenewDeadline {
				slog.WarnContext(
					ctx, "shard consolidation lease heartbeat exceeded renew-deadline",
					"table", l.tableName,
					"stream_id", m.streamID,
					"since_last_success", now.Sub(lastSuccess).String(),
					"error", err,
				)
				return
			}
			slog.WarnContext(
				ctx, "shard consolidation lease heartbeat failed (within renew-deadline; will retry)",
				"table", l.tableName,
				"stream_id", m.streamID,
				"error", err,
			)
		}
	}
}

// ErrLeaseContended is returned by Acquire when another stream
// currently holds the lease. The caller polls-with-backoff via the
// observer path (peer observers do not retry-acquire; they Observe
// and apply when they see State=Applied).
var ErrLeaseContended = errors.New("pipeline: shard consolidation lease contended")

// ErrLeaseLost is returned by Heartbeat/Apply when the lease has been
// taken over by another stream between operations. The lease-holder's
// only safe response is to exit the apply path and let the takeover
// stream proceed (the takeover's probe-and-record will reconcile).
var ErrLeaseLost = errors.New("pipeline: shard consolidation lease lost")

// ErrLeaseChecksumMismatch is returned when an observer sees a lease
// row in APPLIED state whose ddl_checksum differs from the observer's
// own checksum of its observed DDL. The silent-divergence hazard the
// ADR's loud-failure tenet exists to surface.
var ErrLeaseChecksumMismatch = errors.New("pipeline: shard consolidation lease ddl checksum mismatch")
