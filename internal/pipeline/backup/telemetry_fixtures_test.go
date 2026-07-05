// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package backup

import (
	"context"
	"time"

	"sluicesync.dev/sluice/internal/ir"
)

// fakeTelemetry is a scriptable [ir.TargetTelemetry] returning a fixed
// snapshot + ok flag; tests mutate the fields between calls to model
// staleness / saturation transitions. Mirror of the pipeline-root test copy,
// duplicated so the carved-out backup test tree does not import root's.
type fakeTelemetry struct {
	snap ir.TargetHealthSnapshot
	ok   bool
}

func (f *fakeTelemetry) Sample(context.Context) (ir.TargetHealthSnapshot, bool) {
	return f.snap, f.ok
}

func freshNow() time.Time { return time.Now() }
