//go:build integration

// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package postgres

import (
	"context"
	"testing"
	"time"
)

// TestControlTimestampsComparable_FollowsTheSessionZone pins the premise
// behind ControlTimestampsComparable on a real server: a session on UTC
// answers true, and one whose TimeZone is not UTC (where a release before
// v0.156.5 wrote sluice_cdc_state.updated_at in local digits) answers false,
// so the VStream copy-completion door leaves the comparison unproven there
// rather than ordering two clocks.
func TestControlTimestampsComparable_FollowsTheSessionZone(t *testing.T) {
	dsn, cleanup := newSharedPGDB(t, "ctclock_db")
	defer cleanup()
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	for _, tc := range []struct {
		zone string
		want bool
	}{{"UTC", true}, {"America/New_York", false}, {"Asia/Kolkata", false}} {
		a, err := Engine{}.OpenChangeApplier(ctx, dsn+"&timezone="+tc.zone)
		if err != nil {
			t.Fatalf("%s: open applier: %v", tc.zone, err)
		}
		got, err := a.(*ChangeApplier).ControlTimestampsComparable(ctx)
		_ = a.(*ChangeApplier).Close()
		if err != nil || got != tc.want {
			t.Errorf("session zone %s: comparable = %v (err %v), want %v", tc.zone, got, err, tc.want)
		}
	}
}
