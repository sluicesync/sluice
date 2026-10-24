// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package mysql

import (
	"strings"
	"testing"
)

// TestStreamLivenessQueries_Shape: every liveness write suppresses
// updated_at's ON UPDATE bump and honours the control keyspace, and the
// running stamp overwrites a stopped marker. The real-server half is
// TestStreamLivenessStore_LeavesUpdatedAtAlone.
func TestStreamLivenessQueries_Shape(t *testing.T) {
	q := (&ChangeApplier{controlKeyspace: "ctl"}).streamLivenessQueries()
	for name, stmt := range map[string]string{"stamp": q.Stamp, "stop": q.Stop} {
		if !strings.Contains(stmt, "updated_at = updated_at") {
			t.Errorf("%s stamp %q does not pin updated_at; its ON UPDATE would move on every stamp", name, stmt)
		}
		if !strings.Contains(stmt, "`ctl`.") {
			t.Errorf("%s stamp %q ignores the control keyspace", name, stmt)
		}
	}
	if strings.Contains(q.Stamp, "<> 0") {
		t.Errorf("the running stamp must overwrite a stopped marker (an attempt's start): %q", q.Stamp)
	}
}
