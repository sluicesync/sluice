// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package mysql

import (
	"context"

	"sluicesync.dev/sluice/internal/ir"
)

var _ ir.ControlTimestampClock = (*ChangeApplier)(nil)

// ControlTimestampsComparable implements [ir.ControlTimestampClock]: always
// true. sluice_cdc_state.updated_at and sluice_migrate_state.updated_at are
// both TIMESTAMP ... DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP on
// every release that writes a sync cold start's header (checked at v0.149.0
// and now), and MySQL stores a TIMESTAMP in UTC and renders both columns in
// the reading session's zone, so their order is the order of the writes.
// Their resolution is one second: two writes in the same second compare
// equal, which a caller must treat as unordered.
func (a *ChangeApplier) ControlTimestampsComparable(context.Context) (bool, error) {
	return true, nil
}
