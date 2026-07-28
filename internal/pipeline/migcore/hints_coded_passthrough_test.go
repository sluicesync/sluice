// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package migcore

import (
	"errors"
	"fmt"
	"testing"

	"sluicesync.dev/sluice/internal/sluicecode"
)

// TestWrapWithHint_PreservesAnAlreadyCodedError is the item-91 / Bug-213
// pin: the hint registry matches on message SUBSTRINGS, so an error that
// already carries a precise code will also match the phase catch-all it
// travels through. Re-coding it there costs two things at once — the code
// operators are told to match on, and the process exit status, since
// [sluicecode.CodedError.ExitCode] is derived from the code's class.
//
// The observed instance: a deferrable upsert-key refusal reached
// `schema add-table` as SLUICE-E-BULKCOPY-TABLE-FAILED with exit 1, where
// `sync start` and chain restore reported SLUICE-E-TARGET-DEFERRABLE-KEY
// with exit 3 — a caller matching on the documented code, which is what
// the error-codes table tells them to do, would not have matched it.
func TestWrapWithHint_PreservesAnAlreadyCodedError(t *testing.T) {
	// The real refusal shape: coded at the engine, then wrapped by the
	// bulk-copy caller with the prefix the catch-all entry matches on.
	refusal := sluicecode.Wrap(
		sluicecode.CodeTargetDeferrableKey,
		"recreate the target constraint as immediate",
		errors.New(`postgres: table "public"."orders": primary key is DEFERRABLE`),
	)
	wrapped := fmt.Errorf("pipeline: copy table %q: %w", "orders", refusal)

	// Guard the premise: if the catch-all stops matching this text the test
	// would pass for the wrong reason, having exercised no re-coding risk
	// at all.
	if _, matched := matchErrorHint(PhaseBulkCopy, wrapped); !matched {
		t.Fatal("the bulk-copy catch-all no longer matches the copy-table wrapper; " +
			"this test would be vacuous — re-point it at whatever entry now catches this shape")
	}

	got := WrapWithHint(PhaseBulkCopy, wrapped)

	var coded *sluicecode.CodedError
	if !errors.As(got, &coded) {
		t.Fatalf("result carries no code at all: %v", got)
	}
	if coded.Code != sluicecode.CodeTargetDeferrableKey {
		t.Errorf("code = %s; want %s — the phase catch-all replaced a specific refusal's code",
			coded.Code, sluicecode.CodeTargetDeferrableKey)
	}
	if got, want := coded.ExitCode(), sluicecode.ExitRefusal; got != want {
		t.Errorf("ExitCode() = %d; want %d — a refusal that exits like a runtime failure is "+
			"indistinguishable from one to any caller branching on status", got, want)
	}
	// The generic hint is not merely redundant here, it is wrong: the
	// bulk-copy catch-all says earlier tables are missing their secondary
	// indexes, and a refusal copied nothing.
	if coded.Hint == "" {
		t.Error("hint was dropped entirely; the refusal's own remedy should survive")
	}
	if got := coded.Hint; got == bulkCopyCatchAllHint(t) {
		t.Errorf("hint = the bulk-copy catch-all's; want the refusal's own remedy")
	}
}

// TestWrapWithHint_StillCodesAnUncodedError is the other direction, and the
// reason the fix is a passthrough rather than a removal: the hint layer's
// whole purpose is to attach a code and a remedy to a bare engine error.
// Suppressing that would trade one reporting defect for a worse one.
func TestWrapWithHint_StillCodesAnUncodedError(t *testing.T) {
	bare := fmt.Errorf("pipeline: copy table %q: %w", "orders",
		errors.New("pq: deadlock detected"))

	got := WrapWithHint(PhaseBulkCopy, bare)

	var coded *sluicecode.CodedError
	if !errors.As(got, &coded) {
		t.Fatalf("an uncoded error came back uncoded; the hint layer stopped working: %v", got)
	}
	if coded.Code != sluicecode.CodeBulkCopyTableFailed {
		t.Errorf("code = %s; want %s", coded.Code, sluicecode.CodeBulkCopyTableFailed)
	}
	if coded.Hint == "" {
		t.Error("no hint attached to an uncoded bulk-copy failure")
	}
}

// bulkCopyCatchAllHint reads the catch-all's hint out of the registry so
// the assertion above compares against the real string rather than a copy
// that would drift silently when the registry is reworded.
func bulkCopyCatchAllHint(t *testing.T) string {
	t.Helper()
	h, ok := matchErrorHint(PhaseBulkCopy, errors.New(`pipeline: copy table "x": boom`))
	if !ok {
		t.Fatal("bulk-copy catch-all entry not found in the registry")
	}
	return h.hint
}
