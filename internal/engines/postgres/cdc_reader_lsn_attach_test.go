// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package postgres

import "testing"

// TestCDCReader_AttachLSNTracker_EngagesViaAnyInterface is the runtime
// regression pin for the ADR-0121 silent-loss discovery: the PG reader's
// AttachLSNTracker previously took the concrete *lsnTracker, which did NOT
// satisfy the pipeline's engine-neutral attacher interface (AttachLSNTracker
// (any)), so the streamer's type-assertion failed SILENTLY and the slot-ack-
// after-apply tracker (Bug 15 / ADR-0020) was never attached on the cold-start
// / warm-resume paths — the keepalive fell back to acking the DECODED LSN,
// letting confirmed_flush_lsn advance past un-applied changes and silently
// losing them on a crash-resume when the reader ran ahead of the applier.
//
// The compile-time pin (var _ interface{ AttachLSNTracker(any) }) guards the
// signature; this asserts the WIRING actually engages at runtime — the gap the
// confirmed-flush invariant test missed by attaching the tracker manually.
func TestCDCReader_AttachLSNTracker_EngagesViaAnyInterface(t *testing.T) {
	// A *lsnTracker handed through the any-typed method must engage.
	r := &CDCReader{}
	tr := newLSNTracker()
	r.AttachLSNTracker(tr)
	if r.appliedLSN != tr {
		t.Fatal("AttachLSNTracker(*lsnTracker) did not engage appliedLSN — slot-ack-after-apply would silently no-op (ADR-0121)")
	}

	// A non-tracker value (e.g. a cross-engine applier's feedback handle) is
	// ignored, not panicked — the reader falls back to streamed-LSN keepalives.
	other := &CDCReader{}
	other.AttachLSNTracker("not a tracker")
	if other.appliedLSN != nil {
		t.Error("AttachLSNTracker(non-tracker) should be ignored, leaving appliedLSN nil")
	}

	// Structural pin: the reader satisfies the pipeline's exact attacher shape.
	var _ interface{ AttachLSNTracker(any) } = (*CDCReader)(nil)
}
