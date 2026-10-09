// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package migcore

import (
	"context"
	"errors"
	"fmt"
	"time"

	"sluicesync.dev/sluice/internal/sluicecode"
)

// ErrOrCancel attributes a failure to the run's cancellation when the run's
// context has ended, and returns err unchanged otherwise.
//
// # Why
//
// A context that ends while a database call is in flight does not reliably
// come back as a context error. pgx (v5.10) cancels an in-flight call by
// moving the socket's deadline into the past; its read path normalizes that
// to the context error, but its WRITE path returns a bare `write failed: ...
// i/o timeout`; the connection it closed on that cancel answers the next
// statement of the same transaction, through database/sql, with `driver: bad
// connection`; and a dial that times out on the deadline pgx derived from
// ctx's can return before ctx's own timer has fired (see [ctxEnded]).
// Measured against a real Postgres: 6,000 deadline-bounded probes gave 1
// write-path timeout and 23 bad connections. None of those unwraps to
// context.Canceled or DeadlineExceeded, so a run that documents a clean stop
// on cancel reported a network-looking failure instead (v0.157.0 CI,
// TestBroker_CancelStorm_Postgres, twice: at the keyless door's probe and at
// the --at-chain-id position write).
//
// # The rule
//
// When ctx has ended and err is not already the context's error, the result
// unwraps to the context's error and carries err's text — NOT err itself, so
// nothing a layer above matches on (a coded refusal, a marker type) can be
// reached through a failure the cancel caused. Two kinds of error are a
// verdict in their own right and pass through untouched even then:
//
//   - a coded sluice REFUSAL — a [sluicecode.CodedError] whose code is
//     [sluicecode.ClassRefusal]: a definite judgment must never be masked by
//     a cancel that happened to land beside it. A [sluicecode.ClassRuntime]
//     code is NOT one: those are what [WrapWithHint] stamps on a driver or
//     connection failure by matching its text (SLUICE-E-CONNECT-*, the
//     phase catch-alls), so a mid-call cancel arrives exactly that way —
//     the broker's tick wraps its failure with the CDC phase hint;
//   - a [CancelVerdict]: an error that already IS the run's report of the
//     cancel (the broker's BROKER-INCREMENTAL-PARTIAL and
//     BROKER-COLD-START-PARTIAL), which must reach the exit code as a failure.
//
// The cost, stated: a genuine failure that races a cancel is reported as the
// cancel. That is the right trade only where the caller's contract for a
// cancel is "a stop", and only for a failure that is not one of the verdicts
// above — which is why the broker applies this at its single return path and
// chain restore, whose cancel is a failure either way, does not.
func ErrOrCancel(ctx context.Context, err error) error {
	if err == nil {
		return nil
	}
	ctxErr := ctxEnded(ctx)
	if ctxErr == nil || errors.Is(err, ctxErr) {
		return err
	}
	if isCodedRefusal(err) {
		return err
	}
	var verdict CancelVerdict
	if errors.As(err, &verdict) {
		return err
	}
	//nolint:errorlint // err is deliberately NOT wrapped: see the doc above
	return fmt.Errorf("%w (the run was cancelled while a call was in flight; it reported: %v)", ctxErr, err)
}

// isCodedRefusal reports whether err carries a refusal-class sluice code.
func isCodedRefusal(err error) bool {
	ce, ok := sluicecode.FromError(err)
	if !ok {
		return false
	}
	info, ok := sluicecode.Describe(ce.Code)
	return ok && info.Class == sluicecode.ClassRefusal
}

// CancelVerdict marks an error that is the run's own report of a
// cancellation — a cancel that left work the operator must know about — so
// [ErrOrCancel] never re-attributes it to the bare cancel.
type CancelVerdict interface {
	error
	CancelVerdict()
}

// ctxEnded is ctx.Err(), plus DeadlineExceeded once ctx's deadline has
// passed even if its timer has not fired yet. The driver derives its own
// socket and dial deadlines from ctx's, so a dial can time out first and
// return while ctx.Err() still reads nil — seen as `failed to connect: dial
// error: timeout` by TestFindReplayKeylessTables_RealPostgresCancelIsTheCancel.
func ctxEnded(ctx context.Context) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if d, ok := ctx.Deadline(); ok && !time.Now().Before(d) {
		return context.DeadlineExceeded
	}
	return nil
}
