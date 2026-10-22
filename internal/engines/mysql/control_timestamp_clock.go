// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package mysql

import (
	"context"

	"sluicesync.dev/sluice/internal/ir"
)

var _ ir.ControlTimestampClock = (*ChangeApplier)(nil)

// ControlTimestampsComparable implements [ir.ControlTimestampClock]: always
// false. Both updated_at columns are on one clock (TIMESTAMP, stored in UTC),
// but sluice_cdc_state.updated_at is ON UPDATE CURRENT_TIMESTAMP, so it moves
// on every UPDATE that changes the row — requestStop, clearStopRequested
// (which a start runs before the VStream copy door), the live-added and
// table-set writes — not only on a position write. Its order against the
// progress record therefore says nothing about when the position was written,
// and a frozen mid-copy checkpoint touched by any of those writes would read
// as the later, finished anchor. A caller must treat the comparison as
// unproven here.
func (a *ChangeApplier) ControlTimestampsComparable(context.Context) (bool, error) {
	return false, nil
}
