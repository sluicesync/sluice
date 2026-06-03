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
	table   *ir.Table
	current []any // values for the row Next just dequeued
	err     error // sticky; once non-nil, Next returns false on every call
}

// newChanCopySource builds a source bound to the given context,
// table schema, and row channel. The source does not retain
// ownership of any of these — the caller is still responsible for
// the channel's lifecycle.
func newChanCopySource(ctx context.Context, table *ir.Table, rows <-chan ir.Row) *chanCopySource {
	return &chanCopySource{ctx: ctx, table: table, rows: rows}
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
		cols := nonGeneratedColumns(s.table.Columns)
		values := make([]any, len(cols))
		for i, col := range cols {
			v, err := prepareValue(row[col.Name], col.Type)
			if err != nil {
				s.err = fmt.Errorf("column %q: %w", col.Name, err)
				return false
			}
			values[i] = v
		}
		s.current = values
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
