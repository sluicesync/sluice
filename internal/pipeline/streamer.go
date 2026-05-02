package pipeline

import (
	"context"
	"errors"
	"fmt"
	"io"

	"github.com/orware/sluice/internal/ir"
)

// Streamer is the long-running orchestrator: it captures a consistent
// source snapshot, runs the bulk-copy phase against it, then streams
// ongoing changes to a [ir.ChangeApplier] until ctx is cancelled. The
// snapshot's logical clock and the CDC stream's start position are
// the same point — bulk-copy and CDC together cover every row exactly
// once, with no gap and no duplicates.
//
// The simple-mode counterpart is [Migrator]; the two share the
// schema-apply + bulk-copy phases via [runBulkCopy] and diverge only
// after that step (Migrator returns; Streamer keeps streaming).
//
// Streamer.Run does not retain state between calls; instantiate two
// values to run two streams. Concurrent calls on the same Streamer
// are not supported.
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

	// Applier consumes the [ir.Change] events the CDC stream produces
	// after bulk-copy. Required (non-nil); a nil applier silently
	// drops data, which sluice refuses to do — misconfiguration in
	// continuous-sync mode is data loss waiting to happen.
	Applier ir.ChangeApplier

	// Stdout receives human-readable progress messages — most
	// importantly, the captured snapshot Position so the operator
	// can re-pass it for resume until persistent positions land
	// (roadmap §5). Defaults to os.Stdout when nil; tests supply a
	// buffer or io.Discard.
	Stdout io.Writer
}

// Run executes a snapshot+CDC stream:
//
//  1. Read source schema (regular connection — outside the snapshot tx).
//  2. Open the source's [ir.SnapshotStream], capturing a position.
//  3. Apply schema phase 1 → bulk-copy → indexes → constraints on the
//     target, reading from the snapshot-pinned RowReader.
//  4. Start CDC from the snapshot's position; pipe events into Applier.
//  5. Run until ctx is cancelled (clean shutdown), the change channel
//     closes (upstream torn down), or a phase returns an error.
//
// Returns nil on clean ctx cancellation; non-nil on any phase failure.
// Resources (snapshot stream, target writers) are released before
// return regardless of outcome.
func (s *Streamer) Run(ctx context.Context) error {
	if err := s.validate(); err != nil {
		return err
	}

	// ---- 1. Source schema (regular connection) ----
	sr, err := s.Source.OpenSchemaReader(ctx, s.SourceDSN)
	if err != nil {
		return fmt.Errorf("pipeline: open source schema reader: %w", err)
	}
	schema, err := sr.ReadSchema(ctx)
	closeIf(sr)
	if err != nil {
		return fmt.Errorf("pipeline: read source schema: %w", err)
	}
	if len(schema.Tables) == 0 {
		s.printf("pipeline: source schema has no tables; nothing to stream\n")
		return nil
	}

	// ---- 2. Snapshot capture ----
	stream, err := s.Source.OpenSnapshotStream(ctx, s.SourceDSN)
	if err != nil {
		return fmt.Errorf("pipeline: open snapshot stream: %w", err)
	}
	defer func() { _ = stream.Close() }()
	s.printf("pipeline: snapshot captured at position {engine=%s, token=%s}\n",
		stream.Position.Engine, stream.Position.Token)

	// ---- 3. Target writers + bulk copy through the snapshot view ----
	sw, err := s.Target.OpenSchemaWriter(ctx, s.TargetDSN)
	if err != nil {
		return fmt.Errorf("pipeline: open target schema writer: %w", err)
	}
	defer closeIf(sw)

	rw, err := s.Target.OpenRowWriter(ctx, s.TargetDSN)
	if err != nil {
		return fmt.Errorf("pipeline: open target row writer: %w", err)
	}
	defer closeIf(rw)

	if err := runBulkCopy(ctx, schema, stream.Rows, sw, rw); err != nil {
		return err
	}
	s.printf("pipeline: bulk-copy complete; entering CDC mode\n")

	// ---- 4. CDC stream → applier ----
	changes, err := stream.Changes.StreamChanges(ctx, stream.Position)
	if err != nil {
		return fmt.Errorf("pipeline: start cdc: %w", err)
	}

	if err := s.Applier.Apply(ctx, changes); err != nil {
		// Treat ctx cancellation as clean shutdown; anything else is
		// a real error.
		if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
			return nil
		}
		return fmt.Errorf("pipeline: apply changes: %w", err)
	}
	return nil
}

// validate enforces the required-fields contract. Errors here indicate
// caller bugs; surface them clearly before any I/O happens.
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
	case s.Applier == nil:
		return errors.New("pipeline: Streamer.Applier is nil (continuous-sync mode requires an applier — silent change discarding is not allowed)")
	case s.Source.Capabilities().CDC == ir.CDCNone:
		return fmt.Errorf("pipeline: Streamer.Source engine %q declares CDC=None", s.Source.Name())
	}
	return nil
}

// printf writes formatted output to s.Stdout, defaulting to discarding
// when no writer is configured.
func (s *Streamer) printf(format string, args ...any) {
	if s.Stdout == nil {
		return
	}
	fmt.Fprintf(s.Stdout, format, args...)
}
