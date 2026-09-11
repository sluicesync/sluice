// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package main

import (
	"errors"
	"fmt"
	"strings"
	"testing"

	"sluicesync.dev/sluice/internal/sluicecode"
)

// An operator who ran a real migration reported that "the errors are pretty
// dense and hard to parse". Investigating that turned up a specific cause:
// sluice had TWO hint mechanisms with OPPOSITE human visibility.
//
// migcore.WrapWithHint folds its hint into the error TEXT, so it reaches the
// terminal. sluicecode.CodedError's Hint did not — CodedError.Error() returns
// only the inner error, so the remedy appeared only in the structured slog
// record, which is the dense machine-facing line an operator skims past. The
// last line of output, where anyone actually looks, carried the diagnosis and
// no fix.
//
// 242 of the tree's 243 coded construction sites carry a Hint — 18 of 19
// &sluicecode.CodedError{…} literals plus all 224 sluicecode.Wrap(code,
// hint, err) call sites, none of which pass an empty hint — and for
// several it holds the ONLY statement of what to do.
func TestWithHintForHumans(t *testing.T) {
	t.Parallel()

	t.Run("a coded error's remedy reaches the prose kong prints", func(t *testing.T) {
		t.Parallel()
		err := &sluicecode.CodedError{
			Code: sluicecode.CodeDriverHostMismatch,
			Hint: "pass --source-driver planetscale",
			Err:  errors.New("source: the DSN host is a PlanetScale endpoint"),
		}
		got := withHintForHumans(err).Error()
		if !strings.Contains(got, "pass --source-driver planetscale") {
			t.Errorf("the remedy did not reach the human-facing text, so the operator sees a diagnosis "+
				"with no fix: %q", got)
		}
		if !strings.Contains(got, "PlanetScale endpoint") {
			t.Errorf("the original diagnosis was lost: %q", got)
		}
	})

	t.Run("the code stays reachable, so the exit code is unchanged", func(t *testing.T) {
		t.Parallel()
		// The wrap must not break errors.As — a refusal that stopped
		// exiting 3 because its hint was appended would be a worse bug
		// than the one this fixes.
		in := &sluicecode.CodedError{
			Code: sluicecode.CodeTargetShardKeyNotInUpsertKey,
			Hint: "add the shard-key column(s) to the PRIMARY KEY",
			Err:  errors.New("refused"),
		}
		out := withHintForHumans(in)
		ce, ok := sluicecode.FromError(out)
		if !ok || ce.Code != sluicecode.CodeTargetShardKeyNotInUpsertKey {
			t.Fatalf("the coded error stopped being reachable through the wrap: %v", out)
		}
		if ce.ExitCode() != sluicecode.ExitRefusal {
			t.Errorf("exit code changed to %d; a refusal must still exit %d",
				ce.ExitCode(), sluicecode.ExitRefusal)
		}
	})

	t.Run("a hint already in the text is not repeated", func(t *testing.T) {
		t.Parallel()
		// migcore.WrapWithHint errors already end in their own hint block.
		// Appending a second one is exactly the density being complained
		// about.
		hint := "use --resume to continue"
		err := &sluicecode.CodedError{
			Code: sluicecode.CodeDriverHostMismatch,
			Hint: hint,
			Err:  fmt.Errorf("copy table failed\nhint: %s", hint),
		}
		got := withHintForHumans(err).Error()
		if n := strings.Count(got, hint); n != 1 {
			t.Errorf("hint appears %d times, want 1 — duplicating it is the density this fix exists to "+
				"reduce: %q", n, got)
		}
	})

	t.Run("uncoded errors and nil pass through untouched", func(t *testing.T) {
		t.Parallel()
		plain := errors.New("connection reset")
		if got := withHintForHumans(plain); got.Error() != plain.Error() {
			t.Errorf("an uncoded error was modified: %q", got)
		}
		if withHintForHumans(nil) != nil {
			t.Error("nil did not pass through")
		}
		// A coded error with no hint has nothing to add.
		noHint := &sluicecode.CodedError{Code: sluicecode.CodeDriverHostMismatch, Err: errors.New("boom")}
		if got := withHintForHumans(noHint).Error(); got != "boom" {
			t.Errorf("a hintless coded error was modified: %q", got)
		}
	})
}
