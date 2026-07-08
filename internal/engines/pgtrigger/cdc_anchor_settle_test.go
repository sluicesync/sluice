// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package pgtrigger

import (
	"strings"
	"testing"
)

// TestSettleQueries_StayInXID8Domain is the settle-wait sibling of
// TestPollQuery_ComparesInXID8Domain / TestAnchorQuery_ComparesInXID8Domain:
// every settle query must carry txid values through the ::text::bigint
// (or ::text::xid8) casts so they stay in the 64-bit epoch-carrying
// domain on the driver's int64 path, and must never touch the 32-bit
// epoch-less row xmin (the epoch-wrap silent-gap class).
func TestSettleQueries_StayInXID8Domain(t *testing.T) {
	for name, q := range map[string]string{
		"txidUpperBoundQuery": txidUpperBoundQuery,
		"settleWaitQuery":     settleWaitQuery,
	} {
		if !strings.Contains(q, "::text::bigint") {
			t.Errorf("%s lost the xid8 → bigint domain cast:\n%s", name, q)
		}
	}
	if !strings.Contains(txidUpperBoundQuery, "pg_current_xact_id()") {
		t.Errorf("txidUpperBoundQuery must ASSIGN a fresh xid (pg_current_xact_id) — a snapshot's xmax is latestCompletedXid+1 and does NOT bound running txns:\n%s", txidUpperBoundQuery)
	}
}

// TestSettleWaitQuery_SeesRunnersAboveXmax pins the live-probed trap
// (PG 16, 2026-07-08): a snapshot's xmax is latestCompletedXid+1, so
// the ONLY running transaction sits AT xmax and appears in NO xip list
// — a wait built on pg_snapshot_xip alone silently skips it, reopening
// the exact hole the settle wait exists to close. The query must
// therefore union the snapshot's xmin (which PG keeps at the oldest
// still-RUNNING txid) with the xip members, bounded below the assigned
// upper bound.
func TestSettleWaitQuery_SeesRunnersAboveXmax(t *testing.T) {
	if !strings.Contains(settleWaitQuery, "pg_snapshot_xmin(pg_current_snapshot())") {
		t.Errorf("settleWaitQuery lost the xmin arm — pg_snapshot_xip alone misses runners at/above xmax:\n%s", settleWaitQuery)
	}
	if !strings.Contains(settleWaitQuery, "pg_snapshot_xip(pg_current_snapshot())") {
		t.Errorf("settleWaitQuery lost the xip arm:\n%s", settleWaitQuery)
	}
	if !strings.Contains(settleWaitQuery, "x < $1") {
		t.Errorf("settleWaitQuery lost the upper-bound filter:\n%s", settleWaitQuery)
	}
	// The wait must stay observable in pg_stat_activity (tests and
	// operators key on the marker).
	if !strings.Contains(settleWaitQuery, "sluice-anchor-settle-wait") {
		t.Errorf("settleWaitQuery lost its pg_stat_activity marker:\n%s", settleWaitQuery)
	}
}
