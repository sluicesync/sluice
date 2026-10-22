// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package mysql

import (
	"context"
	"testing"
)

// TestControlTimestampsComparable_NeverOnMySQL pins that a MySQL-family
// target never offers its updated_at columns as a position-write order: the
// cdc-state column is ON UPDATE CURRENT_TIMESTAMP and moves on requestStop,
// clearStopRequested and the table-set writes too, so a frozen mid-copy
// checkpoint touched by any of them would read as the finished anchor.
func TestControlTimestampsComparable_NeverOnMySQL(t *testing.T) {
	ok, err := (&ChangeApplier{}).ControlTimestampsComparable(context.Background())
	if err != nil || ok {
		t.Fatalf("ControlTimestampsComparable = %v, %v; want false, nil", ok, err)
	}
}
