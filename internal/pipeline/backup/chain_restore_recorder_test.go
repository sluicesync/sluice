// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package backup

import (
	"context"
	"sync"

	"sluicesync.dev/sluice/internal/ir"
)

// chainRestoreRecorderEngine extends restoreRecorderEngine with a recording
// change-applier so chain-restore tests can assert every applied event.
// Mirror of the pipeline-root test copy, duplicated so the carved-out backup
// test tree does not import root's.
type chainRestoreRecorderEngine struct {
	*restoreRecorderEngine
	mu       sync.Mutex
	applied  []ir.Change
	applierC *chainRestoreRecordingApplier
}

func (e *chainRestoreRecorderEngine) OpenChangeApplier(_ context.Context, _ string) (ir.ChangeApplier, error) {
	if e.applierC == nil {
		e.applierC = &chainRestoreRecordingApplier{owner: e}
	}
	return e.applierC, nil
}

type chainRestoreRecordingApplier struct {
	owner *chainRestoreRecorderEngine
}

func (a *chainRestoreRecordingApplier) EnsureControlTable(_ context.Context) error { return nil }

func (a *chainRestoreRecordingApplier) ReadPosition(_ context.Context, _ string) (ir.Position, bool, error) {
	return ir.Position{}, false, nil
}

func (a *chainRestoreRecordingApplier) ListStreams(_ context.Context) ([]ir.StreamStatus, error) {
	return nil, nil
}

func (a *chainRestoreRecordingApplier) Apply(_ context.Context, _ string, changes <-chan ir.Change) error {
	for c := range changes {
		a.owner.mu.Lock()
		a.owner.applied = append(a.owner.applied, c)
		a.owner.mu.Unlock()
	}
	return nil
}

func (a *chainRestoreRecordingApplier) RequestStop(context.Context, string) error { return nil }

func (a *chainRestoreRecordingApplier) ClearStopRequested(context.Context, string) error { return nil }

func (a *chainRestoreRecordingApplier) Close() error { return nil }
