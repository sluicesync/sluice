// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package postgres

import (
	"context"
	"fmt"

	"sluicesync.dev/sluice/internal/ir"
)

var _ ir.ControlTimestampClock = (*ChangeApplier)(nil)

// ControlTimestampsComparable implements [ir.ControlTimestampClock].
//
// Both columns are naive TIMESTAMPs, so their digits carry whatever clock
// wrote them. sluice_migrate_state has stored UTC digits on every release
// that writes a sync cold start's header (v0.149.0 wrote
// timezone('utc', now()) explicitly; later releases lean on UTC defaults that
// EnsureControlTable re-points). sluice_cdc_state has stored UTC digits since
// v0.156.5 (utcNowSQL) and, before that, CURRENT_TIMESTAMP cast to a naive
// TIMESTAMP: the writing session's local time. The two agree only when that
// session's offset was zero, so this answers whether THIS session's offset is
// zero.
//
// UNVERIFIED PREMISE: a row an older release wrote came from a session with
// the same UTC offset as this one (the same server and DSN). A DSN that sets
// a different timezone than the one the older run used breaks it, and
// nothing here can see that.
func (a *ChangeApplier) ControlTimestampsComparable(ctx context.Context) (bool, error) {
	var offset float64
	if err := a.db.QueryRowContext(ctx, "SELECT EXTRACT(TIMEZONE FROM pg_catalog.now())").Scan(&offset); err != nil {
		return false, fmt.Errorf("postgres: read the session's UTC offset: %w", err)
	}
	return offset == 0, nil
}
