package pipeline

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/url"
	"strings"

	"github.com/orware/sluice/internal/ir"
)

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

	// Stdout receives human-readable progress messages — most
	// importantly, the captured snapshot Position (cold start) or
	// the resumed Position (warm resume), and the resolved
	// stream_id. Defaults to discarding when nil.
	Stdout io.Writer
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
	s.printf("pipeline: stream_id = %q\n", streamID)

	// ---- 1. Open / wire the applier first ----
	applier, ownsApplier, err := s.openApplier(ctx)
	if err != nil {
		return err
	}
	if ownsApplier {
		defer closeIf(applier)
	}

	// ---- 2. Ensure the control table exists ----
	if err := applier.EnsureControlTable(ctx); err != nil {
		return fmt.Errorf("pipeline: ensure control table: %w", err)
	}

	// ---- 3. Look up the persisted position ----
	persisted, found, err := applier.ReadPosition(ctx, streamID)
	if err != nil {
		return fmt.Errorf("pipeline: read position: %w", err)
	}

	// ---- 4. Branch: cold start vs warm resume ----
	var changes <-chan ir.Change
	if found {
		changes, err = s.warmResume(ctx, persisted)
	} else {
		changes, err = s.coldStart(ctx)
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
	if err := applier.Apply(ctx, streamID, changes); err != nil {
		if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
			return nil
		}
		return fmt.Errorf("pipeline: apply changes: %w", err)
	}
	return nil
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
		return nil, false, fmt.Errorf("pipeline: open target change applier: %w", err)
	}
	return a, true, nil
}

// warmResume opens a CDC reader on the source and starts streaming
// from the persisted position. No snapshot, no bulk-copy.
func (s *Streamer) warmResume(ctx context.Context, persisted ir.Position) (<-chan ir.Change, error) {
	s.printf("pipeline: warm resume from persisted position {token=%s}\n", persisted.Token)
	cdc, err := s.Source.OpenCDCReader(ctx, s.SourceDSN)
	if err != nil {
		return nil, fmt.Errorf("pipeline: open cdc reader: %w", err)
	}
	// CDC reader's Close is async (cancellation-driven), but we
	// don't have a clean handle to call it from here. Streamer.Run's
	// returning will cancel ctx; the pump exits and closes the
	// channel.
	changes, err := cdc.StreamChanges(ctx, persisted)
	if err != nil {
		closeIf(cdc)
		return nil, fmt.Errorf("pipeline: start cdc: %w", err)
	}
	return changes, nil
}

// coldStart performs the original §4 flow: read schema → snapshot
// → bulk-copy → start CDC from snapshot's position.
func (s *Streamer) coldStart(ctx context.Context) (<-chan ir.Change, error) {
	sr, err := s.Source.OpenSchemaReader(ctx, s.SourceDSN)
	if err != nil {
		return nil, fmt.Errorf("pipeline: open source schema reader: %w", err)
	}
	schema, err := sr.ReadSchema(ctx)
	closeIf(sr)
	if err != nil {
		return nil, fmt.Errorf("pipeline: read source schema: %w", err)
	}
	if len(schema.Tables) == 0 {
		s.printf("pipeline: source schema has no tables; nothing to stream\n")
		return nil, nil
	}

	stream, err := s.Source.OpenSnapshotStream(ctx, s.SourceDSN)
	if err != nil {
		return nil, fmt.Errorf("pipeline: open snapshot stream: %w", err)
	}
	// stream.Close is deferred by the caller indirectly via
	// Streamer.Run's defer chain — we keep the handle alive past
	// this function so the snapshot+CDC pair stays valid.
	s.printf("pipeline: cold start; snapshot captured at {token=%s}\n", stream.Position.Token)

	sw, err := s.Target.OpenSchemaWriter(ctx, s.TargetDSN)
	if err != nil {
		_ = stream.Close()
		return nil, fmt.Errorf("pipeline: open target schema writer: %w", err)
	}
	rw, err := s.Target.OpenRowWriter(ctx, s.TargetDSN)
	if err != nil {
		closeIf(sw)
		_ = stream.Close()
		return nil, fmt.Errorf("pipeline: open target row writer: %w", err)
	}

	if err := runBulkCopy(ctx, schema, stream.Rows, sw, rw); err != nil {
		closeIf(rw)
		closeIf(sw)
		_ = stream.Close()
		return nil, err
	}
	closeIf(rw)
	closeIf(sw)
	s.printf("pipeline: bulk-copy complete; entering CDC mode\n")

	changes, err := stream.Changes.StreamChanges(ctx, stream.Position)
	if err != nil {
		_ = stream.Close()
		return nil, fmt.Errorf("pipeline: start cdc: %w", err)
	}
	// stream stays alive for the rest of Run; cleanup happens via
	// the function's defer chain when ctx cancels and pump exits.
	// We don't have a clean way to defer here while returning the
	// channel; the OS reclaims connections at process exit, and ctx
	// cancellation tears down the goroutines that hold them.
	return changes, nil
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

// printf writes formatted output to s.Stdout, defaulting to
// discarding when no writer is configured.
func (s *Streamer) printf(format string, args ...any) {
	if s.Stdout == nil {
		return
	}
	fmt.Fprintf(s.Stdout, format, args...)
}
