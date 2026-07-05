// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package pipeline

import (
	"context"
	"sync"

	"sluicesync.dev/sluice/internal/ir"
	"sluicesync.dev/sluice/internal/pipeline/migcore"
	"sluicesync.dev/sluice/internal/redact"
)

// redactRows wraps a row channel with the operator-configured
// redaction policy. Each row read from src is passed through
// [migcore.RedactRow] before being forwarded to the returned channel.
//
// When reg is nil or empty, the function returns src verbatim
// (zero-cost passthrough) — no goroutine is spawned and the
// returned errFn always reports nil. This is the load-bearing
// fast path for the no-redactions case so default operators pay
// nothing for the feature.
//
// When at least one rule is registered, a goroutine reads from
// src, applies migcore.RedactRow in-place, and forwards. On a redaction
// error (strategy refusal), the goroutine:
//
//  1. Stores the error so errFn returns it.
//  2. Closes the output channel.
//  3. Returns. The downstream consumer (writer / applier) sees
//     the channel close and exits cleanly; the caller checks
//     errFn after that exit to surface the redaction error.
//
// The error-propagation contract is *intentionally* not via ctx
// cancellation: a downstream WriteRows seeing a redacted channel
// close cleanly is a legitimate "no more rows" signal; the caller
// disambiguates by checking errFn() AFTER WriteRows returns nil.
//
//nolint:gocritic // named results would conflict with the bidi `out` channel built internally
func redactRows(
	ctx context.Context,
	src <-chan ir.Row,
	reg *redact.Registry,
	schema, table string,
	cols []*ir.Column,
	pkColumns []string,
	streamID string,
) (<-chan ir.Row, func() error) {
	if reg.Empty() {
		return src, func() error { return nil }
	}
	// Standard bounded buffer ([rowChanBuffer], perf-parity matrix row 6):
	// an unbuffered relay here re-introduced a rendezvous hop into the
	// bulk-copy hot path whenever redaction was engaged, defeating the
	// 64-buffer discipline of the surrounding pipeline. The buffer lets
	// redact and the downstream write overlap; back-pressure is preserved
	// once it fills.
	out := make(chan ir.Row, rowChanBuffer)
	var (
		mu    sync.Mutex
		fnErr error
	)
	storeFn := func(err error) {
		mu.Lock()
		defer mu.Unlock()
		if fnErr == nil {
			fnErr = err
		}
	}
	errFn := func() error {
		mu.Lock()
		defer mu.Unlock()
		return fnErr
	}
	go func() {
		defer close(out)
		for {
			select {
			case <-ctx.Done():
				return
			case row, ok := <-src:
				if !ok {
					return
				}
				if err := migcore.RedactRow(reg, schema, table, row, cols, pkColumns, streamID); err != nil {
					storeFn(err)
					return
				}
				select {
				case out <- row:
				case <-ctx.Done():
					return
				}
			}
		}
	}()
	return out, errFn
}
