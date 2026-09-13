// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package pipeline

import (
	"errors"
	"io"
	"testing"
)

// terminalTestError is a minimal [ir.TerminalError] whose WRAPPED CAUSE is
// exactly what the retry heuristics look for. That combination is the point:
// a terminal error whose cause looks transient is the shape that defeated the
// classifier in production.
type terminalTestError struct{ err error }

func (e *terminalTestError) Error() string  { return e.err.Error() }
func (e *terminalTestError) Unwrap() error  { return e.err }
func (e *terminalTestError) Terminal() bool { return true }

// TestIsRetriableChunkOpenError_TerminalBeatsEveryHeuristic pins the ordering
// that cost a production run 26 pointless replays.
//
// The chunk-open / raw-copy classifier is layered and heuristic by design: it
// honours an engine's RetriableError, then falls back to stdlib sentinels
// (driver.ErrBadConn, io.EOF), net.Error timeouts, and finally a text
// allow-list of driver/OS connection-drop shapes. That layering is what lets a
// new transport failure be ridden out without teaching every engine about it.
//
// It also meant an engine could not say "no". The Postgres engine refuses a
// copy whose exported SNAPSHOT-PINNED connection died — no replay can ever
// succeed, because the snapshot lives in a transaction on that connection —
// and that refusal carried no RetriableError. It was replayed anyway: the
// refusal wrapped its cause with %w, the cause was an EOF, and
// errors.Is(err, io.EOF) is true through a wrap.
//
// [ir.TerminalError] is the primitive that fixes it, and ORDER is the whole
// contract: it must be consulted before every transient test, including
// before RetriableError, so an error that somehow satisfies both is treated as
// terminal. Refusing to retry is the safe direction — a wrong terminal verdict
// costs one loud failure, a wrong retriable verdict costs the operator the
// entire retry budget and then fails anyway.
func TestIsRetriableChunkOpenError_TerminalBeatsEveryHeuristic(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name  string
		cause error
	}{
		{"cause is io.EOF", io.EOF},
		{"cause carries a connection-drop text shape", errors.New("write: broken pipe")},
		{"cause is a joined chain of both", errors.Join(io.EOF, errors.New("invalid connection"))},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			term := &terminalTestError{err: tc.cause}

			// Sanity: without the terminal assertion this cause WOULD be
			// judged retriable, or the cell proves nothing.
			if !isRetriableChunkOpenError(tc.cause) {
				t.Fatalf("fixture is wrong: the bare cause %v is not judged retriable, so the "+
					"terminal assertion is not actually being tested", tc.cause)
			}

			if isRetriableChunkOpenError(term) {
				t.Fatalf("a TERMINAL error was judged retriable because its wrapped cause (%v) looks "+
					"transient — the retry will replay something that can never succeed", tc.cause)
			}
		})
	}
}

// TestIsRetriableChunkOpenError_StillRetriesOrdinaryTransients is the
// anti-over-match arm: the terminal check must not have broken the retry path
// it sits in front of.
func TestIsRetriableChunkOpenError_StillRetriesOrdinaryTransients(t *testing.T) {
	t.Parallel()

	for _, err := range []error{io.EOF, errors.New("connection reset by peer")} {
		if !isRetriableChunkOpenError(err) {
			t.Errorf("an ordinary transient (%v) is no longer retriable — the terminal check has "+
				"swallowed the class it was supposed to sit in front of", err)
		}
	}
}
