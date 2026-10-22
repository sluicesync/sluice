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
// session's offset was zero, so this answers whether THIS session's zone is
// at offset zero all year: at 1 January and 1 July of this year (a
// zone such as Europe/London or Europe/Lisbon is at zero only in winter, and
// an older row may have been written in summer). Only a position write sets
// sluice_cdc_state.updated_at on Postgres (checked at v0.149.0 and HEAD: the
// stop-request, live-added and table-set writes leave it alone), so on a
// comparable clock its order is the order of the position writes.
//
// UNVERIFIED PREMISE: a row an older release wrote came from a session with
// the same UTC offset as this one (the same server and DSN). A DSN that sets
// a different timezone than the one the older run used breaks it, and
// nothing here can see that.
func (a *ChangeApplier) ControlTimestampsComparable(ctx context.Context) (bool, error) {
	// Two fixed instants rather than now(): the answer must not depend on the
	// season the check happens to run in.
	var jan, jul float64
	const q = "SELECT EXTRACT(TIMEZONE FROM pg_catalog.date_trunc('year', pg_catalog.now())), " +
		"EXTRACT(TIMEZONE FROM pg_catalog.date_trunc('year', pg_catalog.now()) + interval '6 months')"
	if err := a.db.QueryRowContext(ctx, q).Scan(&jan, &jul); err != nil {
		return false, fmt.Errorf("postgres: read the session zone's UTC offsets: %w", err)
	}
	return jan == 0 && jul == 0, nil
}
