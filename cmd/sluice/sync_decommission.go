// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package main

import (
	"fmt"
	"io"
	"os"

	"sluicesync.dev/sluice/internal/ir"
	"sluicesync.dev/sluice/internal/pipeline"
	"sluicesync.dev/sluice/internal/pipeline/migcore"
	"sluicesync.dev/sluice/internal/sluicecode"
)

// SyncDecommissionCmd retires a FINISHED stream's durable footprint
// (audit 2026-07-23 DEVEX-3 / Q3). The objects live on BOTH sides —
// the replication slot and per-stream publication on the SOURCE, the
// control row on the TARGET — hence the cross-DSN shape `sync start`
// uses, unlike `sync stop`/`status` which only touch the target.
//
// Why this exists as product surface: a finished wave's slot pins WAL
// on the source for the rest of a multi-week staged migration (slot
// invalidation has been field-observed at a few hundred MB on small
// managed instances) and, since v0.99.289's existence-semantics scope
// guard, blocks every later differently-scoped cold start. The
// staged-wave guide's step 5 used to be raw psql; this is that step.
//
// Engine scope: Postgres sources get slot + recorded per-stream
// publication removal (never the shared `sluice_pub`). MySQL-family
// sources have no source-side objects (the binlog is the stream) —
// decommission clears the control row and says so. Trigger-CDC
// sources keep their change-log table and triggers: those are SHARED
// across streams (`sluice trigger prune` bounds the log,
// `sluice trigger teardown` removes them once no streams remain).
type SyncDecommissionCmd struct {
	SourceDriver string `help:"Source engine name (e.g. postgres). See 'sluice engines'." required:"" placeholder:"NAME" group:"source"`
	Source       string `help:"Source database DSN (where the stream's replication slot and publication live)." required:"" env:"SLUICE_SOURCE" placeholder:"DSN" group:"source"`
	TargetDriver string `help:"Target engine name (e.g. mysql, postgres). See 'sluice engines'." required:"" placeholder:"NAME" group:"target"`
	Target       string `help:"Target database DSN (where the stream's control row lives)." required:"" env:"SLUICE_TARGET" placeholder:"DSN" group:"target"`

	StreamID string `help:"Stream identifier to decommission. Must be stopped ('sluice sync stop --wait'); a stream with an active slot is refused." required:"" placeholder:"ID"`
	DryRun   bool   `help:"Report what would be removed without touching either database."`
	Yes      bool   `help:"Confirm this destructive operation. Required (except with --dry-run): a decommissioned stream can never warm-resume." short:"y"`

	ControlKeyspace string `name:"control-keyspace" help:"MySQL/PlanetScale/Vitess target only: the unsharded sidecar keyspace the stream's control tables live in (see 'sync start --control-keyspace'). Omit to auto-detect on a sharded target. Empty + unsharded/non-Vitess target = the default keyspace." placeholder:"KEYSPACE"`
}

// Run implements `sluice sync decommission`.
func (s *SyncDecommissionCmd) Run(_ *Globals) error {
	// Non-interactive by contract: refuse loudly without --yes rather
	// than prompt (the `slot drop` precedent). --dry-run is exempt —
	// it touches nothing, and previewing should be frictionless.
	if !s.Yes && !s.DryRun {
		return &sluicecode.CodedError{
			Code: sluicecode.CodeConfirmationRequired,
			Hint: "pass --yes (or -y) to confirm, or --dry-run to preview",
			Err: fmt.Errorf(
				"decommissioning stream %q is destructive: it drops the stream's replication slot and per-stream publication on the source and clears its control row on the target — the stream can never warm-resume after",
				s.StreamID,
			),
		}
	}

	target, err := resolveEngine(s.TargetDriver)
	if err != nil {
		return fmt.Errorf("--target-driver: %w", err)
	}
	source, err := resolveEngine(s.SourceDriver)
	if err != nil {
		return fmt.Errorf("--source-driver: %w", err)
	}

	ctx := kongContext()
	if target, err = applyControlKeyspace(ctx, target, s.ControlKeyspace, s.Target); err != nil {
		return err
	}
	applier, err := target.OpenChangeApplier(ctx, s.Target)
	if err != nil {
		return fmt.Errorf("open target applier: %w", err)
	}
	defer func() {
		if c, ok := applier.(io.Closer); ok {
			_ = c.Close()
		}
	}()

	// Source side: only engines with replication slots get a manager;
	// everything else (MySQL family, trigger-CDC) takes the
	// control-row-only path — the source is never even dialed.
	var slots ir.SlotManager
	if opener, ok := source.(ir.SlotManagerOpener); ok {
		slots, err = opener.OpenSlotManager(ctx, s.Source)
		if err != nil {
			// Same connect-phase hint routing as openSlotManager (Bug
			// 196 residual): an AAAA-only managed host must carry the
			// coded IPv6-only remedy here too.
			return migcore.WrapWithHint(migcore.PhaseConnect, fmt.Errorf("open slot manager: %w", err))
		}
		defer func() { _ = slots.Close() }()
	}

	rep, err := pipeline.DecommissionStream(ctx, applier, slots, s.StreamID, s.DryRun)
	if rep != nil {
		renderDecommissionReport(os.Stdout, rep)
	}
	return err
}

// renderDecommissionReport prints one line per object — removed,
// already gone, or deliberately left alone — plus a closing line. The
// per-object accounting is the command's contract: after a partial
// failure the operator must know exactly what remains.
func renderDecommissionReport(w io.Writer, rep *pipeline.DecommissionReport) {
	did := func(present, past string) string {
		if rep.DryRun {
			return "[dry-run] would " + present
		}
		return past
	}
	switch {
	case rep.SlotDropped:
		fmt.Fprintf(w, "source: %s replication slot %q\n", did("drop", "dropped"), rep.SlotName)
	case rep.SlotAlreadyAbsent:
		fmt.Fprintf(w, "source: replication slot %q already absent\n", rep.SlotName)
	default:
		fmt.Fprintf(w, "source: no replication slot removed — %s\n", rep.SlotSkipped)
	}
	switch {
	case rep.PublicationDropped:
		fmt.Fprintf(w, "source: %s per-stream publication %q\n", did("drop", "dropped"), rep.PublicationName)
	case rep.PublicationAlreadyAbsent:
		fmt.Fprintf(w, "source: per-stream publication %q already absent\n", rep.PublicationName)
	default:
		fmt.Fprintf(w, "source: no publication removed — %s\n", rep.PublicationSkipped)
	}
	if rep.ControlRowCleared {
		fmt.Fprintf(w, "target: %s control row for stream %q\n", did("clear", "cleared"), rep.StreamID)
	} else {
		fmt.Fprintf(w, "target: control row for stream %q KEPT (re-run to finish the removals above)\n", rep.StreamID)
	}
	switch {
	case rep.DryRun:
		fmt.Fprintln(w, "dry run: nothing was changed")
	case rep.ControlRowCleared:
		fmt.Fprintf(w, "stream %q decommissioned; it can no longer warm-resume\n", rep.StreamID)
	}
}
