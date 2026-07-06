// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

// DSN validation pre-flight: refuse a driver/host mismatch before any
// work begins.
//
// Some engine flavors can tell, from the DSN string alone, that they are
// the wrong driver for the endpoint the operator pointed them at. The
// motivating case is the vanilla `mysql` driver aimed at a PlanetScale
// host (*.connect.psdb.cloud): its binlog CDC and LOAD DATA cold-copy
// are both blocked by Vitess, so the run fails obscurely partway through
// the copy. The engine surfaces the mismatch via the optional
// [ir.DSNValidator] surface; this pre-flight consults it for the source
// and the target at the very top of migrate and sync so the mismatch
// fails loudly, up front, naming the driver flag to fix.
//
// It needs no connection — only the engines and their DSNs — so it runs
// before any reader/writer is opened. Engines that don't implement
// [ir.DSNValidator] are a silent no-op.

package pipeline

import (
	"fmt"

	"sluicesync.dev/sluice/internal/ir"
	"sluicesync.dev/sluice/internal/sluicecode"
)

// preflightDSNValidation checks the source and the target DSNs against
// their engines' optional [ir.DSNValidator] surface. On the first side
// whose engine refuses its DSN it returns a [sluicecode.CodedError]
// (SLUICE-E-DRIVER-HOST-MISMATCH, a refusal → exit 3) whose message is
// prefixed with the role ("source" / "target") — the engine's own
// message is role-agnostic — and whose hint names the exact driver flag
// to pass. A side whose engine doesn't implement the surface is skipped;
// with no implementers this is a no-op.
func preflightDSNValidation(source ir.Engine, sourceDSN string, target ir.Engine, targetDSN string) error {
	sides := []struct {
		role   string
		engine ir.Engine
		dsn    string
	}{
		{"source", source, sourceDSN},
		{"target", target, targetDSN},
	}
	for _, side := range sides {
		validator, ok := side.engine.(ir.DSNValidator)
		if !ok {
			continue
		}
		if err := validator.ValidateDSN(side.dsn); err != nil {
			return &sluicecode.CodedError{
				Code: sluicecode.CodeDriverHostMismatch,
				Hint: "pass --" + side.role + "-driver planetscale",
				Err:  fmt.Errorf("%s: %w", side.role, err),
			}
		}
	}
	return nil
}
