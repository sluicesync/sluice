// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

//go:build integration

package pipeline

import (
	"testing"

	"sluicesync.dev/sluice/internal/pipeline/backup"
)

// observeRestoreChunkDispatch / observeRestoreDispatch install the carved-out
// backup package's exported restore-dispatch observer seams and return the last
// (parallelism, reason) they saw. Mirrors of the backup package's own test
// copies, duplicated so the root-resident restore-parallel integration tests
// keep their fixtures without importing the backup test tree.

func observeRestoreChunkDispatch(t *testing.T) (gotParallelism *int, gotReason *string) {
	t.Helper()
	p, r := 0, ""
	backup.RestoreChunkDispatchObserver = func(chunkParallelism int, reason string) {
		p, r = chunkParallelism, reason
	}
	t.Cleanup(func() { backup.RestoreChunkDispatchObserver = nil })
	return &p, &r
}

func observeRestoreDispatch(t *testing.T) (gotParallelism *int, gotReason *string) {
	t.Helper()
	p, r := 0, ""
	backup.RestoreDispatchObserver = func(tableParallelism int, reason string) {
		p, r = tableParallelism, reason
	}
	t.Cleanup(func() { backup.RestoreDispatchObserver = nil })
	return &p, &r
}
