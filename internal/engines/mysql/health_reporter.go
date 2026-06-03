// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package mysql

import (
	"context"
	"errors"
	"fmt"

	"sluicesync.dev/sluice/internal/ir"
)

// SourceCurrentPosition implements [ir.HealthReporter]. Returns the
// source's current GTID-executed set as a string. Used by
// `sluice sync health` to compute lag relative to the target's
// tracked position.
//
// Returns a Position with Engine=mysql, Token=GTID-set string. The
// token is comparable to the StreamStatus.Position.Token values
// surfaced by `ListStreams` for binlog-mode streams (which encode
// GTID sets the same way). VStream-mode streams encode position
// differently; the comparison is still operator-meaningful (both
// strings move forward over time) but byte-arithmetic doesn't apply.
//
// **Note: MySQL deliberately doesn't implement [ir.BytesLagReporter].**
// GTID sets are set-membership-comparable rather than byte-distance-
// comparable; cross-GTID-set arithmetic would require parsing GTID
// sets and computing transaction-count differences, which isn't
// operator-meaningful as a single integer. Operators using MySQL
// should compare the source-position and target-position tokens
// directly via the verbose health output (or `sluice sync status`).
func (r *SchemaReader) SourceCurrentPosition(ctx context.Context) (ir.Position, error) {
	if r.db == nil {
		return ir.Position{}, errors.New("mysql: SourceCurrentPosition: reader not opened")
	}
	var gtidSet string
	// SELECT @@global.gtid_executed returns the union of all GTIDs
	// known to the server. On binlog-disabled or read-replica setups
	// this may be empty; the caller treats empty-token as "source has
	// no position to report" and surfaces that case.
	if err := r.db.QueryRowContext(ctx, `SELECT @@global.gtid_executed`).Scan(&gtidSet); err != nil {
		return ir.Position{}, fmt.Errorf("mysql: SourceCurrentPosition: %w", err)
	}
	return ir.Position{Engine: "mysql", Token: gtidSet}, nil
}
