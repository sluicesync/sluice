// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package mysql

import (
	"context"
	"testing"
	"time"

	"github.com/orware/sluice/internal/ir"
)

// fakeProvider is a minimal [ir.BatchSizeProvider] for testing the
// applier's optional-surface wiring (ADR-0052) without standing up an
// AIMD controller.
type fakeProvider struct {
	next     int
	hits     int
	hintRows int
	hintByte int64
	hintCap  int64
	hintHits int
}

func (f *fakeProvider) NextBatchSize() int {
	f.hits++
	return f.next
}

func (f *fakeProvider) NoteByteCapDominant(_ context.Context, rows int, bytes, byteCap int64) {
	f.hintHits++
	f.hintRows = rows
	f.hintByte = bytes
	f.hintCap = byteCap
}

// fakeObserver is a minimal [ir.BatchObserver] for testing the
// applier's optional-surface wiring.
type fakeObserver struct {
	calls    int
	lastErr  error
	lastRows int
	lastLat  time.Duration
}

func (f *fakeObserver) ObserveBatch(_ context.Context, latency time.Duration, rows int, err error) {
	f.calls++
	f.lastLat = latency
	f.lastRows = rows
	f.lastErr = err
}

func TestChangeApplier_SetBatchSizeProvider(t *testing.T) {
	// Unit-level: just confirm the setter stores the value and the
	// applier exposes the same ir.BatchSizeProviderSetter shape the
	// streamer probes for.
	a := &ChangeApplier{}
	var setter ir.BatchSizeProviderSetter = a
	p := &fakeProvider{next: 42}
	setter.SetBatchSizeProvider(p)
	if a.batchSizeProvider == nil {
		t.Fatalf("SetBatchSizeProvider: stored value is nil")
	}
	if got := a.batchSizeProvider.NextBatchSize(); got != 42 {
		t.Fatalf("provider NextBatchSize via applier field = %d; want 42", got)
	}
	// Nil clears the wiring.
	setter.SetBatchSizeProvider(nil)
	if a.batchSizeProvider != nil {
		t.Fatalf("SetBatchSizeProvider(nil): expected to clear; got %v", a.batchSizeProvider)
	}
}

func TestChangeApplier_SetBatchObserver(t *testing.T) {
	a := &ChangeApplier{}
	var setter ir.BatchObserverSetter = a
	o := &fakeObserver{}
	setter.SetBatchObserver(o)
	if a.batchObserver == nil {
		t.Fatalf("SetBatchObserver: stored value is nil")
	}
	// Invoke through the applier field to confirm the same observer
	// is reachable.
	a.batchObserver.ObserveBatch(context.Background(), 5*time.Millisecond, 7, nil)
	if o.calls != 1 || o.lastRows != 7 || o.lastLat != 5*time.Millisecond {
		t.Fatalf("observer call = %+v; want calls=1 rows=7 lat=5ms", o)
	}
	setter.SetBatchObserver(nil)
	if a.batchObserver != nil {
		t.Fatalf("SetBatchObserver(nil): expected to clear")
	}
}

// TestChangeApplier_ImplementsAIMDInterfaces is a compile-time
// guarantee that the MySQL applier exposes both optional-surface
// setters the streamer probes for. A future refactor that drops
// either setter would break this assertion at build time, which is
// the loud-failure shape we want.
func TestChangeApplier_ImplementsAIMDInterfaces(_ *testing.T) {
	var _ ir.BatchSizeProviderSetter = (*ChangeApplier)(nil)
	var _ ir.BatchObserverSetter = (*ChangeApplier)(nil)
}
