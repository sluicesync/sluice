// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package migcore

import (
	"context"
	"errors"
	"fmt"
	"testing"

	"sluicesync.dev/sluice/internal/ir"
)

type stickyErrReader struct{ err error }

func (stickyErrReader) ReadRows(context.Context, *ir.Table) (<-chan ir.Row, error) { return nil, nil }
func (r stickyErrReader) Err() error                                               { return r.err }

// TestReaderStreamErr_ContextErrorIsAnInterruption pins GC-41 (i)'s root
// change: a reader whose sticky error is a context error stopped because
// the context it was read under ended, so its stream is short — never a
// clean end. Before, both arms returned nil, and a stopped whole-table
// copy was recorded COMPLETE. Every arm keeps the cause reachable for
// errors.Is, because callers classify a stop by it.
func TestReaderStreamErr_ContextErrorIsAnInterruption(t *testing.T) {
	table := &ir.Table{Name: "t"}
	for _, cause := range []error{context.Canceled, context.DeadlineExceeded} {
		for _, shape := range []struct {
			name string
			err  error
		}{
			{"bare", cause},
			{"wrapped", fmt.Errorf("postgres: rows iteration: %w", cause)},
		} {
			t.Run(fmt.Sprintf("%v/%s", cause, shape.name), func(t *testing.T) {
				err := ReaderStreamErr(stickyErrReader{err: shape.err}, table)
				if !errors.Is(err, ErrCopyInterrupted) {
					t.Fatalf("a stream cut by %v was forgiven (got %v): its caller records the table COMPLETE", cause, err)
				}
				if !errors.Is(err, cause) {
					t.Errorf("the interruption lost its cause %v: %v", cause, err)
				}
			})
		}
	}
	// Controls: a clean reader is still clean, and a Bug-68 decode failure
	// is still a failure and NOT an interruption.
	if err := ReaderStreamErr(stickyErrReader{}, table); err != nil {
		t.Errorf("control: a clean reader reported %v", err)
	}
	decode := errors.New(`postgres: column "c": bad value`)
	if err := ReaderStreamErr(stickyErrReader{err: decode}, table); !errors.Is(err, decode) || errors.Is(err, ErrCopyInterrupted) {
		t.Errorf("control: a decode failure came back as %v", err)
	}
}

// TestSourceEnd_ConfirmNeedsANaturalEndAndALiveRun grades the verdict's
// truth table: both facts must hold, and a natural end recorded while the
// run was already stopped does not count.
func TestSourceEnd_ConfirmNeedsANaturalEndAndALiveRun(t *testing.T) {
	live := context.Background()
	stopped, cancel := context.WithCancel(context.Background())
	cancel()

	var never SourceEnd
	if err := never.Confirm(live, "t"); !errors.Is(err, ErrCopyInterrupted) {
		t.Errorf("a source never reported ended was confirmed: %v", err)
	}

	var drained SourceEnd
	drained.Reached(live)
	if err := drained.Confirm(live, "t"); err != nil {
		t.Errorf("a drained source on a live run was refused: %v", err)
	}
	if err := drained.Confirm(stopped, "t"); !errors.Is(err, ErrCopyInterrupted) || !errors.Is(err, context.Canceled) {
		t.Errorf("a drained source on a stopped run was confirmed (a stage downstream may have dropped rows): %v", err)
	}

	var lateEnd SourceEnd
	lateEnd.Reached(stopped)
	if err := lateEnd.Confirm(live, "t"); !errors.Is(err, ErrCopyInterrupted) {
		t.Errorf("a close observed after the stop counted as a natural end: %v", err)
	}

	// The writer-consumption clause, over the channels the writer was
	// handed: closed and empty passes; a row left behind, or a channel
	// that never closed, does not.
	consumed := make(chan ir.Row)
	close(consumed)
	leftBehind := make(chan ir.Row, 1)
	leftBehind <- ir.Row{"id": 1}
	close(leftBehind)
	neverClosed := make(chan ir.Row)
	if err := drained.Confirm(live, "t", consumed, consumed); err != nil {
		t.Errorf("channels consumed to their close were refused: %v", err)
	}
	if err := drained.Confirm(live, "t", consumed, leftBehind); !errors.Is(err, ErrCopyInterrupted) {
		t.Errorf("a writer that left a handed row unread was confirmed: %v", err)
	}
	if err := drained.Confirm(live, "t", neverClosed); !errors.Is(err, ErrCopyInterrupted) {
		t.Errorf("a writer that returned before its input closed was confirmed: %v", err)
	}
}
