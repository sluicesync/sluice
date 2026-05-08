// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package postgres

import (
	"errors"
	"testing"
)

func TestIsPermissionDenied(t *testing.T) {
	cases := []struct {
		name string
		err  error
		want bool
	}{
		{"nil", nil, false},
		{"unrelated", errors.New("connection reset"), false},
		{"permission denied verbatim", errors.New("ERROR: permission denied for table pg_stat_replication"), true},
		{"sqlstate 42501", errors.New("ERROR: SQLSTATE 42501"), true},
		{"wrapped permission denied", errors.New("query failed: permission denied"), true},
	}
	for _, c := range cases {
		c := c
		t.Run(c.name, func(t *testing.T) {
			got := isPermissionDenied(c.err)
			if got != c.want {
				t.Errorf("got %v; want %v", got, c.want)
			}
		})
	}
}

// TestWalKeepSizeWarnThreshold pins the soft-warning floor so a future
// change requires a deliberate threshold revisit. 64 MB is the PG
// default; the threshold is at the default so only setups that
// deliberately dialed wal_keep_size DOWN trigger the warning.
func TestWalKeepSizeWarnThreshold(t *testing.T) {
	if walKeepSizeWarnThresholdMB != 64 {
		t.Errorf("walKeepSizeWarnThresholdMB = %d; want 64 (PG default)", walKeepSizeWarnThresholdMB)
	}
}
