// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package pipeline

import (
	"context"
	"sync"

	"sluicesync.dev/sluice/internal/ir"
	"sluicesync.dev/sluice/internal/pipeline/migcore"
)

// shardStampRow stamps the operator-supplied discriminator
// (`name=value`, supplied via ADR-0048's `--inject-shard-column
// NAME=VALUE`) onto a single row before it reaches the bulk-copy
// writer. Empty name is a no-op fast path — the bulk-copy hot
// path stays zero-cost when Shape A isn't engaged.
//
// The row map is mutated in place (not copied) because the row is
// owned by the caller's read goroutine and not aliased after this
// call — same contract as [migcore.RedactRow]. Stamping unconditionally
// overwrites any existing value at row[name]: the column is a
// sluice-injected column on the consolidated target, so the
// source's row stream should never carry a key with that name in
// the first place (the IR-pass refuses upfront if it would
// collide with a source column).
func shardStampRow(row ir.Row, name string, value any) {
	if name == "" {
		return
	}
	row[name] = value
}

// shardStampRows wraps a row channel with the Shape-A
// discriminator stamper. Each row read from src has
// row[shardName]=shardValue set before being forwarded — the
// value half of the schema-pass / value-wrap split (ADR-0048
// Decision 1, DP-1 resolved 2026-05-21 to option (a)). Mirrors
// the [redactRows] contract exactly: zero-cost passthrough when
// the discriminator isn't engaged, single goroutine when it is,
// a separate `func() error` returns a captured failure (always
// nil today; reserved for the symmetry-with-redactRows shape so
// future stamping validation can grow a refusal path without
// changing the call sites).
//
// Zero-cost passthrough: when shardName is empty, returns src
// unchanged — no goroutine, no allocation, no per-row predicate
// in the hot path. Composes alongside [redactRows] in copyTable
// (chain through the streaming pipeline; the redaction wrap is
// applied first per the existing precedent, the shard stamp
// after).
//
//nolint:gocritic // named results would conflict with the bidi `out` channel built internally
func shardStampRows(
	ctx context.Context,
	src <-chan ir.Row,
	shardName string,
	shardValue any,
) (<-chan ir.Row, func() error) {
	if shardName == "" {
		return src, func() error { return nil }
	}
	// Standard bounded buffer ([migcore.RowChanBuffer], perf-parity matrix row 6):
	// same rationale as the [redactRows] tee — an unbuffered relay made the
	// stamp a rendezvous hop in the bulk-copy hot path when Shape A was
	// engaged. Buffered, stamping and the downstream write overlap.
	out := make(chan ir.Row, migcore.RowChanBuffer)
	var (
		mu    sync.Mutex
		fnErr error
	)
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
				shardStampRow(row, shardName, shardValue)
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
