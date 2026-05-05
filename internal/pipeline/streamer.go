package pipeline

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net/url"
	"strings"

	"github.com/orware/sluice/internal/config"
	"github.com/orware/sluice/internal/ir"
	"github.com/orware/sluice/internal/translate"
)

// publicationEnsurer is the optional engine-side surface for engines
// that need a publication (or analogous CDC-source-side scope object)
// established before snapshot capture / CDC start. Postgres
// implements it (Bug 13, ADR-0021); MySQL does not.
//
// Tables is the post-filter source-table list — schema-qualifying is
// the engine's job. Empty tables means "fall back to the engine's
// default scope" (FOR ALL TABLES on PG); the streamer never passes
// nil — when the schema is empty, [coldStart] returns before this
// is called.
type publicationEnsurer interface {
	EnsurePublication(ctx context.Context, dsn string, tables []string) error
}

// lsnTrackerProvider is the optional applier-side surface for
// engines that produce applied-LSN feedback (Bug 15, ADR-0020). The
// applier owns the tracker; the streamer fetches it via this
// interface and hands it to the matching CDC reader via
// [lsnTrackerAttacher].
//
// Returns an opaque value (typed `any`) so the pipeline package
// stays free of engine-specific types. The matching CDC reader
// type-asserts internally — only same-engine pairs (PG applier ↔
// PG reader) actually wire anything; cross-engine pairs harmlessly
// hand an unrelated value to the attacher and the attacher's type-
// assertion fails closed.
type lsnTrackerProvider interface {
	LSNTracker() any
}

// lsnTrackerAttacher is the optional CDC-reader-side surface for
// engines that consume applied-LSN feedback (Bug 15, ADR-0020). On
// a successful type-assertion of the opaque tracker to its native
// shape, the reader keeps a pointer and uses it on its keepalive
// path; on failure it ignores the value and falls back to streamed-
// LSN keepalives.
type lsnTrackerAttacher interface {
	AttachLSNTracker(t any)
}

// Streamer is the long-running orchestrator: it captures a consistent
// source snapshot (cold start) or resumes from a previously-persisted
// position (warm resume), runs the bulk-copy phase if needed, then
// streams ongoing changes to a [ir.ChangeApplier] until ctx is
// cancelled.
//
// Each applied change writes its source position into the target's
// sluice_cdc_state table inside the same transaction as the data
// write — progress and data move together, per ADR-0007. A restart
// looks up the persisted position and skips the snapshot+bulk-copy
// phase entirely; combined with the applier's idempotency on retry,
// every event lands on the target exactly once.
//
// The simple-mode counterpart is [Migrator]; the two share the
// schema-apply + bulk-copy phases via [runBulkCopy] and diverge
// after that step (Migrator returns; Streamer keeps streaming).
type Streamer struct {
	// Source is the engine the source DSN belongs to. Must declare
	// CDC support (Capabilities().CDC != ir.CDCNone).
	Source ir.Engine

	// Target is the engine the target DSN belongs to. May be the
	// same as Source for same-engine streams.
	Target ir.Engine

	// SourceDSN, TargetDSN are the engine-native connection strings.
	SourceDSN string
	TargetDSN string

	// StreamID is the position-table key for this stream. When
	// empty, the Streamer auto-generates one from source+target
	// engine names and DSN host info. Operator-supplied IDs let
	// multiple concurrent streams share a target without clobbering
	// each other's position.
	StreamID string

	// Applier is optional. When nil, the Streamer auto-opens one
	// via Target.OpenChangeApplier(ctx, TargetDSN). Tests inject a
	// stub; production callers leave it nil. When non-nil, the
	// Streamer assumes the caller owns the applier's lifecycle and
	// does NOT call Close on it.
	Applier ir.ChangeApplier

	// Mappings is the per-column type-override list from sluice.yaml.
	// Consumed only on the cold-start path, where the schema-apply
	// phase needs the rewritten types. Warm resume reuses the target
	// schema as-is, so the field is ignored on that branch.
	Mappings []config.Mapping

	// DryRun, when true, prints what Run would do (cold-start vs
	// warm-resume, source schema summary or persisted-position
	// token) and returns without opening the snapshot stream,
	// applying any data, or modifying the target's control table.
	// Symmetric with the Migrator's existing DryRun flag.
	//
	// The position lookup against the target's control table still
	// happens — that's a read, not a write, and it's the only way
	// to tell the operator "this is a cold start" vs "this would
	// resume from <position>". The control table itself is NOT
	// created on dry-run; the lookup uses the tolerant readPosition
	// path that returns "no row" when the table doesn't exist yet.
	DryRun bool

	// Filter selects which source tables participate in the
	// stream. Applied to the cold-start schema (so bulk-copy and
	// schema-apply only see allowed tables) and to the dispatch
	// loop (so CDC events for excluded tables are dropped before
	// the applier sees them). The empty filter keeps every table.
	//
	// Caveat: position only advances when an event is applied. A
	// stream that consists entirely of dropped events for a long
	// time accumulates position lag bounded by the source-side
	// WAL/binlog retention. In practice every workload mixes
	// allowed and dropped events and the next applied event
	// advances the position past the dropped ones.
	Filter TableFilter

	// ForceColdStart, when true, skips the cold-start pre-flight
	// check that refuses a fresh stream into a target with
	// pre-existing rows. The check protects against Bug 9 (cold-
	// start hangs after a killed-mid-copy run leaves partial dest
	// data behind); this flag is the explicit override for the
	// rare case of bulk-copying into a populated table. Ignored on
	// the warm-resume path — that branch doesn't bulk-copy.
	ForceColdStart bool

	// ApplyBatchSize is the upper bound on changes per target
	// transaction. 0 or 1 means one-change-per-tx (the conservative
	// v0.3.x default). Larger values amortise per-tx commit
	// overhead at the cost of a larger replay-on-crash window. The
	// applier's idempotent semantics (ADR-0010) make the replay
	// safe; the position-and-data atomicity (ADR-0007) is preserved
	// per batch — the position of the last applied change in a
	// batch is written in the same tx as the batch's data writes.
	//
	// Schema-change events (Truncate today; AddColumn / DropColumn
	// when the IR grows them) flush the in-progress batch and
	// apply alone so the applier's column-type cache is scoped per
	// schema epoch. The cap is an upper bound, not a target —
	// small streams don't accumulate.
	//
	// Engines that don't implement [ir.BatchedChangeApplier] fall
	// back to per-change Apply regardless of this field; in
	// practice every shipping engine implements it (see ADR-0017).
	ApplyBatchSize int
}

// Run executes a snapshot+CDC stream. See [Streamer] for the full
// flow.
//
// Returns nil on clean ctx cancellation; non-nil on any phase
// failure. Resources (snapshot stream, target writers, applier)
// are released before return regardless of outcome.
func (s *Streamer) Run(ctx context.Context) error {
	if err := s.validate(); err != nil {
		return err
	}
	streamID := s.resolveStreamID()
	slog.InfoContext(ctx, "stream starting", slog.String("stream_id", streamID))

	// ---- 1. Open / wire the applier first ----
	applier, ownsApplier, err := s.openApplier(ctx)
	if err != nil {
		return err
	}
	if ownsApplier {
		defer closeIf(applier)
	}

	// ---- 2. Ensure the control table exists ----
	// Skip on dry-run — that's a write, and dry-run is read-only.
	// ReadPosition below tolerates a missing control table by
	// returning ok=false (same as "no row").
	if !s.DryRun {
		if err := applier.EnsureControlTable(ctx); err != nil {
			return wrapWithHint(PhaseSchemaApply, fmt.Errorf("pipeline: ensure control table: %w", err))
		}
	}

	// ---- 2.5. Clear any leftover stop signal from a previous run ----
	// Without this, `sluice sync stop` leaves stop_requested_at set
	// after the streamer drains and exits; the next `sync start`
	// would then see the stale flag and exit within the first poll
	// interval (Bug 11 in v0.3.2 testing). Skip on dry-run for the
	// same read-only reason as EnsureControlTable above.
	if !s.DryRun {
		if err := applier.ClearStopRequested(ctx, streamID); err != nil {
			return wrapWithHint(PhaseSchemaApply, fmt.Errorf("pipeline: clear stop signal: %w", err))
		}
	}

	// ---- 3. Look up the persisted position ----
	persisted, found, err := applier.ReadPosition(ctx, streamID)
	if err != nil {
		return wrapWithHint(PhaseCDC, fmt.Errorf("pipeline: read position: %w", err))
	}

	// ---- 3.5. Dry-run: print plan and exit before any state mutation. ----
	if s.DryRun {
		return s.logDryRunPlan(ctx, streamID, persisted, found)
	}

	// ---- 3.6. Fetch the applier's LSN-feedback tracker (if any) ----
	// Slot-ack-after-apply (Bug 15, ADR-0020): the postgres applier
	// exposes a tracker the matching CDC reader reads from on its
	// keepalive path. The tracker is opaque (typed `any`) so the
	// pipeline package stays engine-neutral; the matching reader's
	// AttachLSNTracker type-asserts internally. Cross-engine pairs
	// (PG applier → MySQL reader, etc.) harmlessly hand a value the
	// reader doesn't recognise; nothing breaks because the reader's
	// fallback path (streamed-LSN keepalive) is correct for engines
	// without an async-batched apply layer.
	var lsnTracker any
	if provider, ok := applier.(lsnTrackerProvider); ok {
		lsnTracker = provider.LSNTracker()
	}

	// ---- 4. Branch: cold start vs warm resume ----
	var changes <-chan ir.Change
	if found {
		changes, err = s.warmResume(ctx, persisted, lsnTracker)
	} else {
		changes, err = s.coldStart(ctx, lsnTracker)
	}
	if err != nil {
		return err
	}
	if changes == nil {
		// coldStart returns (nil, nil) when the source schema is
		// empty — nothing to do.
		return nil
	}

	// ---- 5. Apply ----
	// The dispatch-side filter wraps `changes` with a goroutine
	// that drops events whose qualified name doesn't pass the
	// filter. No-op pass-through when the filter is empty.
	//
	// applyCtx is the context the apply loop sees. It's a child of
	// the caller's ctx, plus an extra cancel hook the stop-signal
	// poll goroutine can pull. Cancelling applyCtx triggers the
	// applier's existing context.Canceled return path (which the
	// loop below treats as "clean exit" — same shape as a Ctrl-C).
	applyCtx, cancelApply := context.WithCancel(ctx)
	defer cancelApply()
	s.startStopSignalPoll(applyCtx, applier, streamID, cancelApply)

	filtered := filterChanges(applyCtx, changes, s.Filter)
	if err := s.dispatchApply(applyCtx, applier, streamID, filtered); err != nil {
		if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
			return nil
		}
		return wrapWithHint(PhaseCDC, fmt.Errorf("pipeline: apply changes: %w", err))
	}
	return nil
}

// dispatchApply routes the change channel to the applier's batched
// or per-change Apply path. When ApplyBatchSize > 1 and the applier
// implements [ir.BatchedChangeApplier], the batched path runs;
// otherwise the per-change path runs (preserving v0.3.x semantics
// bit-for-bit).
//
// The optional-interface probe means engines that don't yet
// implement the batched form keep working — type assertion fails
// silently and we fall through to Apply. ADR-0017 covers the
// design choice.
func (s *Streamer) dispatchApply(ctx context.Context, applier ir.ChangeApplier, streamID string, changes <-chan ir.Change) error {
	if s.ApplyBatchSize > 1 {
		if batched, ok := applier.(ir.BatchedChangeApplier); ok {
			slog.DebugContext(ctx, "applier: batched apply enabled",
				slog.String("stream_id", streamID),
				slog.Int("apply_batch_size", s.ApplyBatchSize),
			)
			return batched.ApplyBatch(ctx, streamID, changes, s.ApplyBatchSize)
		}
		slog.WarnContext(ctx, "applier: --apply-batch-size requested but applier does not implement BatchedChangeApplier; falling back to per-change apply",
			slog.String("stream_id", streamID),
			slog.Int("apply_batch_size", s.ApplyBatchSize),
		)
	}
	return applier.Apply(ctx, streamID, changes)
}

// startStopSignalPoll wires the optional stop-signal poll goroutine
// when the applier supports it. The goroutine reads the control
// row's stop flag every few seconds; when set, it cancels applyCtx
// so the apply loop drains the in-flight change and exits cleanly.
//
// Test stubs that don't implement stopFlagReader skip the poll
// entirely — the existing Ctrl-C / ctx-cancel path remains the only
// way to stop those streams, which matches their pre-stop-signal
// behavior.
func (s *Streamer) startStopSignalPoll(applyCtx context.Context, applier ir.ChangeApplier, streamID string, cancelApply context.CancelFunc) {
	reader, ok := applier.(stopFlagReader)
	if !ok {
		slog.DebugContext(applyCtx, "stop-signal poll skipped: applier does not implement ReadStopRequested",
			slog.String("stream_id", streamID),
		)
		return
	}
	slog.DebugContext(applyCtx, "stop-signal poll started",
		slog.String("stream_id", streamID),
	)
	go pollStopSignal(applyCtx, reader, streamID, cancelApply)
}

// logDryRunPlan describes what Run would do without doing it via
// structured slog records. Cold-start logs the source schema summary
// so operators can catch missing-tables / unexpected-column-counts
// before the migration starts; warm-resume logs the persisted
// position token (truncated for readability) so operators can see
// whether the stream is positioned where they expect.
//
// The source schema read for cold-start is the only source-side
// touch the dry-run does — same level of access the regular
// cold-start would do, just without then opening the snapshot
// stream or starting CDC.
func (s *Streamer) logDryRunPlan(ctx context.Context, streamID string, persisted ir.Position, found bool) error {
	slog.InfoContext(ctx, "dry run: stream plan",
		slog.String("source", s.Source.Name()),
		slog.String("source_host", redactedHost(s.SourceDSN)),
		slog.String("target", s.Target.Name()),
		slog.String("target_host", redactedHost(s.TargetDSN)),
		slog.String("stream_id", streamID),
	)
	if found {
		slog.InfoContext(ctx, "dry run: warm resume from persisted position",
			slog.String("stream_id", streamID),
			slog.String("position_token", truncateDryRunToken(persisted.Token, 80)),
		)
		return nil
	}
	slog.InfoContext(ctx, "dry run: cold start — would capture snapshot, bulk-copy, then start CDC",
		slog.String("stream_id", streamID),
	)

	sr, err := s.Source.OpenSchemaReader(ctx, s.SourceDSN)
	if err != nil {
		return wrapWithHint(PhaseConnect, fmt.Errorf("pipeline: dry-run: open source schema reader: %w", err))
	}
	defer closeIf(sr)
	schema, err := sr.ReadSchema(ctx)
	if err != nil {
		return wrapWithHint(PhaseConnect, fmt.Errorf("pipeline: dry-run: read source schema: %w", err))
	}
	if len(schema.Tables) == 0 {
		slog.InfoContext(ctx, "dry run: source schema has no tables — nothing to stream")
		return nil
	}
	if err := applyTableFilter(ctx, schema, s.Filter); err != nil {
		return err
	}
	if _, err := translate.ApplyMappings(schema, s.Mappings); err != nil {
		return fmt.Errorf("pipeline: dry-run: apply mappings: %w", err)
	}
	slog.InfoContext(ctx, "dry run: tables to bulk-copy and tail via CDC",
		slog.Int("tables", len(schema.Tables)),
	)
	for _, t := range schema.Tables {
		// secondary_indexes excludes the primary key (reported via
		// primary_key) — see migrate.go logPlan for the rationale.
		slog.InfoContext(ctx, "dry run: table",
			slog.String("name", t.Name),
			slog.Int("columns", len(t.Columns)),
			slog.Bool("primary_key", t.PrimaryKey != nil),
			slog.Int("secondary_indexes", len(t.Indexes)),
			slog.Int("foreign_keys", len(t.ForeignKeys)),
		)
	}
	return nil
}

// truncateDryRunToken trims a position token to maxLen characters
// with an ellipsis when longer. Position tokens are JSON blobs that
// can run hundreds of bytes; the dry-run output stays scannable.
func truncateDryRunToken(token string, maxLen int) string {
	if len(token) <= maxLen {
		return token
	}
	return token[:maxLen-1] + "…"
}

// openApplier returns the applier to use plus a flag indicating
// whether the Streamer owns its lifecycle. Owns => Streamer must
// Close it. Borrowed => caller is responsible.
func (s *Streamer) openApplier(ctx context.Context) (ir.ChangeApplier, bool, error) {
	if s.Applier != nil {
		return s.Applier, false, nil
	}
	a, err := s.Target.OpenChangeApplier(ctx, s.TargetDSN)
	if err != nil {
		return nil, false, wrapWithHint(PhaseConnect, fmt.Errorf("pipeline: open target change applier: %w", err))
	}
	return a, true, nil
}

// warmResume opens a CDC reader on the source and starts streaming
// from the persisted position. No snapshot, no bulk-copy.
//
// lsnTracker is the opaque applied-LSN feedback channel (Bug 15,
// ADR-0020). Attached to the reader before StreamChanges so the
// keepalive path uses applied-LSN from the very first ack — no
// window where the slot could advance past un-applied work just
// because the reader was constructed before the tracker was
// passed through. nil tracker means the engine doesn't support
// LSN feedback (the pre-v0.5.0 shape) or the applier isn't a
// matching engine; the reader falls back to streamed-LSN.
//
// Warm resume reuses the publication scope established at cold
// start; we don't re-read the schema or re-call EnsurePublication
// here. Defence-in-depth lives in the applier's dispatch path
// (skip-with-warning on unknown tables).
func (s *Streamer) warmResume(ctx context.Context, persisted ir.Position, lsnTracker any) (<-chan ir.Change, error) {
	slog.InfoContext(ctx, "warm resume from persisted position",
		slog.String("position_token", persisted.Token),
	)
	cdc, err := s.Source.OpenCDCReader(ctx, s.SourceDSN)
	if err != nil {
		return nil, wrapWithHint(PhaseCDC, fmt.Errorf("pipeline: open cdc reader: %w", err))
	}
	if lsnTracker != nil {
		if attacher, ok := cdc.(lsnTrackerAttacher); ok {
			attacher.AttachLSNTracker(lsnTracker)
		}
	}
	// CDC reader's Close is async (cancellation-driven), but we
	// don't have a clean handle to call it from here. Streamer.Run's
	// returning will cancel ctx; the pump exits and closes the
	// channel.
	changes, err := cdc.StreamChanges(ctx, persisted)
	if err != nil {
		closeIf(cdc)
		return nil, wrapWithHint(PhaseCDC, fmt.Errorf("pipeline: start cdc: %w", err))
	}
	return changes, nil
}

// coldStart performs the original §4 flow: read schema → ensure
// publication scope → snapshot → bulk-copy → start CDC from
// snapshot's position.
//
// lsnTracker is the opaque applied-LSN feedback channel (Bug 15,
// ADR-0020) — attached to the snapshot stream's CDC reader before
// StreamChanges so the keepalive path uses applied-LSN from the
// first ack onwards.
func (s *Streamer) coldStart(ctx context.Context, lsnTracker any) (<-chan ir.Change, error) {
	sr, err := s.Source.OpenSchemaReader(ctx, s.SourceDSN)
	if err != nil {
		return nil, wrapWithHint(PhaseConnect, fmt.Errorf("pipeline: open source schema reader: %w", err))
	}
	schema, err := sr.ReadSchema(ctx)
	closeIf(sr)
	if err != nil {
		return nil, wrapWithHint(PhaseConnect, fmt.Errorf("pipeline: read source schema: %w", err))
	}
	if len(schema.Tables) == 0 {
		slog.InfoContext(ctx, "source schema has no tables; nothing to stream")
		return nil, nil
	}

	// Prune by table filter before mappings + bulk-copy so the
	// excluded tables never reach the target schema-apply phase.
	if err := applyTableFilter(ctx, schema, s.Filter); err != nil {
		return nil, err
	}

	// ---- Scope the source-side publication to the filtered table
	// list (Bug 13, ADR-0021). On engines that don't have
	// publications (MySQL), this is a no-op; on Postgres, this is
	// what stops a CREATE TABLE on the source mid-sync from
	// crashing the applier with "table public.X has no columns".
	// Run BEFORE OpenSnapshotStream so the snapshot's slot pins a
	// catalog snapshot that already has the scoped publication.
	if pe, ok := s.Source.(publicationEnsurer); ok {
		tables := tableNamesForPublication(schema)
		if err := pe.EnsurePublication(ctx, s.SourceDSN, tables); err != nil {
			return nil, wrapWithHint(PhaseConnect, fmt.Errorf("pipeline: ensure publication scope: %w", err))
		}
	}

	// Apply per-column type overrides before the schema-write phase
	// sees the schema. Warm resume skips this step — by then the
	// target schema is already shaped from the cold-start run.
	schema, err = translate.ApplyMappings(schema, s.Mappings)
	if err != nil {
		return nil, fmt.Errorf("pipeline: apply mappings: %w", err)
	}

	stream, err := s.Source.OpenSnapshotStream(ctx, s.SourceDSN)
	if err != nil {
		return nil, wrapWithHint(PhaseSnapshot, fmt.Errorf("pipeline: open snapshot stream: %w", err))
	}
	// stream.Close is deferred by the caller indirectly via
	// Streamer.Run's defer chain — we keep the handle alive past
	// this function so the snapshot+CDC pair stays valid.
	slog.InfoContext(ctx, "cold start; snapshot captured",
		slog.String("position_token", stream.Position.Token),
	)

	sw, err := s.Target.OpenSchemaWriter(ctx, s.TargetDSN)
	if err != nil {
		_ = stream.Close()
		return nil, wrapWithHint(PhaseConnect, fmt.Errorf("pipeline: open target schema writer: %w", err))
	}
	rw, err := s.Target.OpenRowWriter(ctx, s.TargetDSN)
	if err != nil {
		closeIf(sw)
		_ = stream.Close()
		return nil, wrapWithHint(PhaseConnect, fmt.Errorf("pipeline: open target row writer: %w", err))
	}

	// Cold-start pre-flight: refuse if any target table already
	// contains data. See preflight.go for the rationale (Bug 9).
	// Streamer's cold-start branch is the analogue of Migrator's
	// non-resume cold-start path; warm-resume doesn't run bulk-copy
	// and is therefore not gated by this check.
	if err := preflightColdStart(ctx, schema, rw, s.ForceColdStart); err != nil {
		closeIf(rw)
		closeIf(sw)
		_ = stream.Close()
		return nil, err
	}

	if err := runBulkCopy(ctx, schema, stream.Rows, sw, rw); err != nil {
		closeIf(rw)
		closeIf(sw)
		_ = stream.Close()
		return nil, err
	}
	closeIf(rw)
	closeIf(sw)
	slog.InfoContext(ctx, "bulk-copy complete; entering CDC mode")

	if lsnTracker != nil {
		if attacher, ok := stream.Changes.(lsnTrackerAttacher); ok {
			attacher.AttachLSNTracker(lsnTracker)
		}
	}

	changes, err := stream.Changes.StreamChanges(ctx, stream.Position)
	if err != nil {
		_ = stream.Close()
		return nil, wrapWithHint(PhaseCDC, fmt.Errorf("pipeline: start cdc: %w", err))
	}
	// stream stays alive for the rest of Run; cleanup happens via
	// the function's defer chain when ctx cancels and pump exits.
	// We don't have a clean way to defer here while returning the
	// channel; the OS reclaims connections at process exit, and ctx
	// cancellation tears down the goroutines that hold them.
	return changes, nil
}

// tableNamesForPublication returns the bare table names from a
// post-filter schema, in declaration order. Used by the publication-
// scope step (Bug 13, ADR-0021) — schema-qualifying happens in the
// engine because schema is an engine-side concept (PG namespaces vs.
// MySQL databases vs. future engines).
func tableNamesForPublication(schema *ir.Schema) []string {
	if schema == nil {
		return nil
	}
	out := make([]string, 0, len(schema.Tables))
	for _, t := range schema.Tables {
		out = append(out, t.Name)
	}
	return out
}

// validate enforces the required-fields contract.
func (s *Streamer) validate() error {
	switch {
	case s.Source == nil:
		return errors.New("pipeline: Streamer.Source engine is nil")
	case s.Target == nil:
		return errors.New("pipeline: Streamer.Target engine is nil")
	case s.SourceDSN == "":
		return errors.New("pipeline: Streamer.SourceDSN is empty")
	case s.TargetDSN == "":
		return errors.New("pipeline: Streamer.TargetDSN is empty")
	case s.Source.Capabilities().CDC == ir.CDCNone:
		return fmt.Errorf("pipeline: Streamer.Source engine %q declares CDC=None", s.Source.Name())
	}
	return nil
}

// resolveStreamID returns the operator-supplied StreamID if non-
// empty; otherwise generates a deterministic ID from source+target
// engine names and DSN host info (passwords stripped). The result
// is length-bounded to fit VARCHAR(255) on the MySQL control table.
func (s *Streamer) resolveStreamID() string {
	if s.StreamID != "" {
		return s.StreamID
	}
	id := fmt.Sprintf("%s://%s -> %s://%s",
		s.Source.Name(), redactedHost(s.SourceDSN),
		s.Target.Name(), redactedHost(s.TargetDSN))
	if len(id) > 255 {
		id = id[:255]
	}
	return id
}

// redactedHost extracts a "host:port" (or "host") fragment from the
// DSN, dropping passwords and other connection params. Both URI
// (postgres://, mysql://) and KV-pair (libpq, MySQL DSN) forms are
// accepted; falls back to "" on parse failure rather than leaking
// sensitive material.
func redactedHost(dsn string) string {
	// URI form, e.g. "postgres://u:p@host:5432/db?...".
	if strings.HasPrefix(dsn, "postgres://") || strings.HasPrefix(dsn, "postgresql://") {
		if u, err := url.Parse(dsn); err == nil {
			return u.Host
		}
		return ""
	}
	// MySQL DSN form, e.g. "user:pass@tcp(host:port)/dbname?params".
	// Pull out the part inside tcp(...) if present.
	if at := strings.Index(dsn, "@tcp("); at >= 0 {
		body := dsn[at+5:]
		if end := strings.Index(body, ")"); end >= 0 {
			return body[:end]
		}
	}
	// libpq KV form, e.g. "host=localhost port=5432 user=...".
	host, port := "", ""
	for _, tok := range strings.Fields(dsn) {
		k, v, ok := strings.Cut(tok, "=")
		if !ok {
			continue
		}
		switch strings.ToLower(k) {
		case "host":
			host = v
		case "port":
			port = v
		}
	}
	if host == "" {
		return ""
	}
	if port != "" {
		return host + ":" + port
	}
	return host
}
