// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package postgres

import (
	"context"
	"fmt"

	"sluicesync.dev/sluice/internal/ir"
)

// chanCopySource adapts an [ir.Row] channel to pgx's [CopyFromSource]
// interface (Next / Values / Err). The streaming write contract on
// the IR side is push-driven over a channel; pgx's COPY API is
// pull-driven through a source object — this is the bridge.
//
// Lifecycle: one source per [RowWriter.WriteRows] invocation. The
// constructor captures ctx so [Next] can honor cancellation; pgx's
// [CopyFromSource.Next] doesn't take a ctx parameter, so storing it
// is the only way to make iteration cancellable. (revive flags
// "context-as-argument" in this pattern; the linter exemption lives
// next to the struct.)
//
//nolint:containedctx // pgx CopyFromSource.Next takes no ctx; storing it is required for cancellation.
type chanCopySource struct {
	ctx     context.Context
	rows    <-chan ir.Row
	cols    []*ir.Column // non-generated columns, computed once — invariant per table
	current []any        // values for the row Next just dequeued; reused across calls
	err     error        // sticky; once non-nil, Next returns false on every call
}

// newChanCopySource builds a source bound to the given context,
// table schema, and row channel. The source does not retain
// ownership of any of these — the caller is still responsible for
// the channel's lifecycle.
//
// The non-generated column filter is hoisted here: it is invariant
// per table, and computing it inside Next cost one slice allocation
// + filter walk per row (billions of transient allocs on a large
// cross-engine copy). The values slice is likewise allocated once
// and reused — pgx's CopyFrom copies what it needs before the next
// Next call (see the Values doc comment).
func newChanCopySource(ctx context.Context, table *ir.Table, rows <-chan ir.Row) *chanCopySource {
	cols := nonGeneratedColumns(table.Columns)
	return &chanCopySource{ctx: ctx, rows: rows, cols: cols, current: make([]any, len(cols))}
}

// Next blocks until a row arrives on the channel, the channel
// closes, ctx cancels, or value preparation errors. Returns true
// when [Values] has a fresh row to return; false otherwise (with
// [Err] revealing the cause when relevant — channel close is a
// nil-Err case).
//
// Generated columns are skipped so the value list stays in lockstep
// with the column list passed to pgx.CopyFrom (also filtered).
func (s *chanCopySource) Next() bool {
	if s.err != nil {
		return false
	}
	select {
	case row, ok := <-s.rows:
		if !ok {
			return false
		}
		for i, col := range s.cols {
			v, err := prepareValue(row[col.Name], col.Type)
			if err != nil {
				s.err = fmt.Errorf("column %q: %w", col.Name, err)
				return false
			}
			s.current[i] = v
		}
		return true
	case <-s.ctx.Done():
		s.err = s.ctx.Err()
		return false
	}
}

// Values returns the values for the row [Next] just dequeued. The
// slice is reused across calls; pgx's CopyFrom is documented to
// copy what it needs before the next [Next] call.
func (s *chanCopySource) Values() ([]any, error) {
	return s.current, nil
}

// Err returns the sticky error from the most recent [Next] that
// returned false. Channel-closed (clean drain) is reported as nil.
func (s *chanCopySource) Err() error {
	return s.err
}

// sliceCopySource adapts a buffered []ir.Row chunk to pgx's
// [CopyFromSource] interface for the grow-gate-engaged chunked-COPY path
// (roadmap item 38). It is the slice-backed twin of [chanCopySource]: the
// chunked writer buffers a bounded chunk of rows so a CopyFrom that fails on
// a storage-grow transient can be REPLAYED, and this source feeds that same
// buffered slice into pgx — re-runnable from the start on each replay.
//
// LOAD-BEARING: it shares the SAME per-value encoding as [chanCopySource] —
// the exact same nonGeneratedColumns filter and the exact same prepareValue
// call per cell. There is deliberately ONE encoding path, so a chunked write
// produces byte-identical target rows to the monolithic single-CopyFrom path
// (the value-fidelity / Bug-74 discipline).
type sliceCopySource struct {
	rows    []ir.Row
	cols    []*ir.Column // non-generated columns, computed once — invariant per table
	idx     int          // next row to deliver; reset to 0 to replay the chunk
	current []any        // values for the row Next just dequeued; reused across calls
	err     error        // sticky; once non-nil, Next returns false on every call
}

// newSliceCopySource builds a source over a buffered chunk for the given
// table schema. The non-generated column filter is hoisted once (invariant
// per table) and the values slice is allocated once and reused, mirroring
// [newChanCopySource]. The source does not retain ownership of rows — the
// chunked writer owns the buffer and may reuse this source's chunk across a
// replay by constructing a fresh source over the same slice.
func newSliceCopySource(table *ir.Table, rows []ir.Row) *sliceCopySource {
	cols := nonGeneratedColumns(table.Columns)
	return &sliceCopySource{rows: rows, cols: cols, current: make([]any, len(cols))}
}

// Next advances to the next buffered row. Returns false at end of chunk
// (a clean drain, nil Err) or on a value-preparation error (sticky in Err).
// Generated columns are skipped so the value list stays in lockstep with the
// column list passed to pgx.CopyFrom (also filtered) — identical to
// [chanCopySource.Next].
func (s *sliceCopySource) Next() bool {
	if s.err != nil {
		return false
	}
	if s.idx >= len(s.rows) {
		return false
	}
	row := s.rows[s.idx]
	s.idx++
	for i, col := range s.cols {
		v, err := prepareValue(row[col.Name], col.Type)
		if err != nil {
			s.err = fmt.Errorf("column %q: %w", col.Name, err)
			return false
		}
		s.current[i] = v
	}
	return true
}

// Values returns the values for the row [Next] just dequeued. The slice is
// reused across calls; pgx's CopyFrom copies what it needs before the next
// [Next] call.
func (s *sliceCopySource) Values() ([]any, error) {
	return s.current, nil
}

// Err returns the sticky error from the most recent [Next] that returned
// false. End-of-chunk (clean drain) is reported as nil.
func (s *sliceCopySource) Err() error {
	return s.err
}
