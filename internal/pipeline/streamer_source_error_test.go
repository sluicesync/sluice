// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package pipeline

import (
	"context"
	"errors"
	"fmt"
	"testing"
)

// TestSurfaceSourceError_NilFnIsNoOp covers the v0.45.x-and-earlier
// reader case: a CDC reader that doesn't expose Err() — or wasn't
// captured at all because [coldStart] / [warmResume] hadn't reached
// the StreamChanges call — produces a nil sourceErrFn, and the
// surfacing path must no-op. Otherwise the runOnce branch would
// nil-deref or return a spurious error.
//
// GitHub issue #19.
func TestSurfaceSourceError_NilFnIsNoOp(t *testing.T) {
	if got := surfaceSourceError(nil); got != nil {
		t.Errorf("surfaceSourceError(nil) = %v; want nil", got)
	}
}

// TestSurfaceSourceError_NilReturnIsNoOp covers the normal-EOF path:
// the pump exited because the source ran out of changes / operator
// invoked graceful stop, and the reader's stored Err is nil. The
// surfacing path must not invent an error.
func TestSurfaceSourceError_NilReturnIsNoOp(t *testing.T) {
	fn := func() error { return nil }
	if got := surfaceSourceError(fn); got != nil {
		t.Errorf("surfaceSourceError(returns-nil) = %v; want nil", got)
	}
}

// TestSurfaceSourceError_ContextCancellationFiltered covers the
// outer-ctx-cancelled path. The pump's internal check filters
// context.Canceled before storing — but engine implementations may
// store wrapped or co-occurring cancellation errors anyway. The
// surfacing path filters these to avoid spurious retries when the
// parent has already decided to shut down.
func TestSurfaceSourceError_ContextCancellationFiltered(t *testing.T) {
	cases := []struct {
		name string
		err  error
	}{
		{"bare context.Canceled", context.Canceled},
		{"bare context.DeadlineExceeded", context.DeadlineExceeded},
		{"wrapped context.Canceled", fmt.Errorf("mysql: cdc: get event: %w", context.Canceled)},
		{"wrapped context.DeadlineExceeded", fmt.Errorf("postgres: cdc: receive: %w", context.DeadlineExceeded)},
	}
	for _, c := range cases {
		c := c
		t.Run(c.name, func(t *testing.T) {
			fn := func() error { return c.err }
			if got := surfaceSourceError(fn); got != nil {
				t.Errorf("surfaceSourceError(ctx-cancel shape %q) = %v; want nil", c.name, got)
			}
		})
	}
}

// TestSurfaceSourceError_TransientReturned covers the GitHub #19
// happy path: the pump stored a non-cancellation error (e.g. a
// classifyReaderError-wrapped retriable shape from a connection
// reset), and the surfacing path returns the error verbatim so
// runWithRetry can errors.As it against ir.RetriableError.
func TestSurfaceSourceError_TransientReturned(t *testing.T) {
	sentinel := errors.New("read tcp: connection reset by peer")
	fn := func() error { return sentinel }
	got := surfaceSourceError(fn)
	if !errors.Is(got, sentinel) {
		t.Errorf("surfaceSourceError(transient) lost the underlying error; got %v; want chained to %v", got, sentinel)
	}
}
