// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package migcore

import (
	"context"
	"errors"
	"log/slog"

	"sluicesync.dev/sluice/internal/sluicecode"
)

// DirectDDLProber is implemented by a target writer that can answer, by
// issuing one, whether the target is accepting direct DDL right now.
//
// Only the MySQL engine implements it, and the probe is only meaningful
// against a PlanetScale/Vitess target where safe migrations can refuse DDL
// outright. Every other writer is skipped and pays one type assertion.
type DirectDDLProber interface {
	ProbeDirectDDL(ctx context.Context) error
}

// PreflightDirectDDL refuses, before the schema phase, a target that is not
// accepting direct DDL.
//
// # Why this exists
//
// An operator's real PlanetScale MySQL migration hit the safe-migrations
// refusal partway into the run, disabled safe migrations, re-ran, hit it
// again because disabling had not propagated yet, and then needed --resume
// because the first attempt had already created part of the schema. Three
// of their five reported stumbling blocks are that one sequence. Every step
// of it is avoidable by asking the question first.
//
// # The answers are asymmetric, and this preflight only acts on one of them
//
// A refusal is conclusive and is turned into a refusal here. A success is
// NOT conclusive — safe migrations propagates asynchronously to each
// gateway, so an accepted DDL means "this gateway, this connection, this
// moment" and nothing about the branch. So a pass is silent: sluice never
// announces that safe migrations is off, because there is no independent
// evidence for that claim and an all-clear nobody can back is worse than no
// check. See the engine-side doc on ProbeDirectDDL for the full reasoning,
// including why the API's safe_migrations flag is not a substitute (it is
// the value that is ahead of reality during propagation, which is precisely
// the window that cost the operator a cycle).
//
// # A probe that cannot RUN is not a verdict
//
// Any failure that is not the coded safe-migrations refusal is logged at
// WARN and the run proceeds. This preflight exists to convert a late, messy
// failure into an early clean one; it must never become a new way for a
// working configuration to be refused. The real DDL a moment later is still
// the correctness floor.
func PreflightDirectDDL(ctx context.Context, rw any, mode string) error {
	prober, ok := rw.(DirectDDLProber)
	if !ok {
		return nil
	}
	err := prober.ProbeDirectDDL(ctx)
	if err == nil {
		return nil
	}
	var coded *sluicecode.CodedError
	if errors.As(err, &coded) && coded.Code == sluicecode.CodePSDirectDDLBlocked {
		return err
	}
	slog.Warn("direct-DDL preflight could not complete; continuing (the schema phase remains the real check)",
		"mode", mode, "err", err)
	return nil
}
