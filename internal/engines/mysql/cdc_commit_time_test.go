// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package mysql

import (
	"testing"
	"time"

	"github.com/go-mysql-org/go-mysql/replication"
	"vitess.io/vitess/go/vt/proto/binlogdata"
)

// TestBinlogEventCommitTime pins the units conversion for the sync-lag metric
// (roadmap item 45): the binlog header Timestamp is UTC seconds-since-epoch,
// and a zero header timestamp (artificial events) maps to the zero time
// (unknown), never to the epoch.
func TestBinlogEventCommitTime(t *testing.T) {
	const epoch int64 = 1_750_000_000 // 2025-06-15T...
	got := binlogEventCommitTime(&replication.EventHeader{Timestamp: uint32(epoch)})
	if want := time.Unix(epoch, 0).UTC(); !got.Equal(want) {
		t.Errorf("binlogEventCommitTime = %v; want %v", got, want)
	}
	if got.Location() != time.UTC {
		t.Errorf("binlogEventCommitTime location = %v; want UTC", got.Location())
	}
	if !binlogEventCommitTime(&replication.EventHeader{Timestamp: 0}).IsZero() {
		t.Error("zero header timestamp did not map to the zero time")
	}
	if !binlogEventCommitTime(nil).IsZero() {
		t.Error("nil header did not map to the zero time")
	}
}

// TestVStreamEventCommitTime pins the units conversion for the VStream path:
// VEvent.Timestamp is UTC seconds; zero/nil map to the zero time (unknown).
func TestVStreamEventCommitTime(t *testing.T) {
	const epoch int64 = 1_750_000_000
	got := vstreamEventCommitTime(&binlogdata.VEvent{Timestamp: epoch})
	if want := time.Unix(epoch, 0).UTC(); !got.Equal(want) {
		t.Errorf("vstreamEventCommitTime = %v; want %v", got, want)
	}
	if !vstreamEventCommitTime(&binlogdata.VEvent{Timestamp: 0}).IsZero() {
		t.Error("zero VEvent timestamp did not map to the zero time")
	}
	if !vstreamEventCommitTime(nil).IsZero() {
		t.Error("nil VEvent did not map to the zero time")
	}
}
