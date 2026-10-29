//go:build integration

// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package pipeline

import (
	"context"
	"testing"
	"time"

	"sluicesync.dev/sluice/internal/engines/mysql"
	"sluicesync.dev/sluice/internal/engines/postgres"
	"sluicesync.dev/sluice/internal/ir"
)

// mixedVersionTarget is the target engine as a drained `schema add-table`
// sees it against an IDLE stream run by an older binary after this release
// recorded the stream's stop — the one state, named in the CHANGELOG's
// Compatibility section, in which a current add-table still runs beside a
// running stream: the liveness record reads stopped, and the stream's
// position, as the add reads it, never moves.
//
// Tests that pin what a running stream does when a table is added under it
// (a live-added record it then meets) used to reach that state with a
// drained add the check did not stop; it stops it now
// (DRAINED-ADD-TABLE-STREAM-NOT-STOPPED), so they reach it through this
// emulation instead. Freezing the position matters on the test rigs whose
// source and target share a server: there the running stream reads the
// add's own target writes as filtered events and advances its position,
// which a separate-server target would not.
func mixedVersionTarget(t *testing.T, eng ir.Engine) ir.Engine {
	t.Helper()
	switch e := eng.(type) {
	case mysql.Engine:
		return &mixedVersionMySQLTarget{Engine: e}
	case postgres.Engine:
		return &mixedVersionPGTarget{Engine: e}
	}
	t.Fatalf("RIG: no mixed-version target for engine %q", eng.Name())
	return nil
}

// frozenPosition is the first position the add read, shared by an
// applier's copies (the wrappers have value receivers).
type frozenPosition struct {
	set   bool
	pos   ir.Position
	found bool
}

func (f *frozenPosition) read(ctx context.Context, read func(context.Context, string) (ir.Position, bool, error), streamID string) (ir.Position, bool, error) {
	if !f.set {
		pos, found, err := read(ctx, streamID)
		if err != nil {
			return pos, found, err
		}
		*f = frozenPosition{set: true, pos: pos, found: found}
	}
	return f.pos, f.found, nil
}

func stoppedLiveness() ir.StreamLiveness {
	return ir.StreamLiveness{Found: true, Recorded: true, Stopped: true, Now: time.Now()}
}

type mixedVersionMySQLTarget struct{ mysql.Engine }

type mixedVersionMySQLApplier struct {
	*mysql.ChangeApplier
	frozen *frozenPosition
}

func (e *mixedVersionMySQLTarget) OpenChangeApplier(ctx context.Context, dsn string) (ir.ChangeApplier, error) {
	a, err := e.Engine.OpenChangeApplier(ctx, dsn)
	if err != nil {
		return nil, err
	}
	return mixedVersionMySQLApplier{ChangeApplier: a.(*mysql.ChangeApplier), frozen: &frozenPosition{}}, nil
}

func (a mixedVersionMySQLApplier) ReadStreamLiveness(context.Context, string) (ir.StreamLiveness, error) {
	return stoppedLiveness(), nil
}

func (a mixedVersionMySQLApplier) ReadPosition(ctx context.Context, streamID string) (ir.Position, bool, error) {
	return a.frozen.read(ctx, a.ChangeApplier.ReadPosition, streamID)
}

type mixedVersionPGTarget struct{ postgres.Engine }

type mixedVersionPGApplier struct {
	*postgres.ChangeApplier
	frozen *frozenPosition
}

func (e *mixedVersionPGTarget) OpenChangeApplier(ctx context.Context, dsn string) (ir.ChangeApplier, error) {
	a, err := e.Engine.OpenChangeApplier(ctx, dsn)
	if err != nil {
		return nil, err
	}
	return mixedVersionPGApplier{ChangeApplier: a.(*postgres.ChangeApplier), frozen: &frozenPosition{}}, nil
}

func (a mixedVersionPGApplier) ReadStreamLiveness(context.Context, string) (ir.StreamLiveness, error) {
	return stoppedLiveness(), nil
}

func (a mixedVersionPGApplier) ReadPosition(ctx context.Context, streamID string) (ir.Position, bool, error) {
	return a.frozen.read(ctx, a.ChangeApplier.ReadPosition, streamID)
}
