// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

// Package streamlivenesstest holds the engine-independent contract for the
// sluice_cdc_state liveness column (DRAINED-ADD-TABLE-RUNNING-STREAM-UNCHECKED),
// run by each engine's integration tests against a real server so both
// appliers are held to one contract.
package streamlivenesstest

import (
	"context"
	"testing"
	"time"

	"sluicesync.dev/sluice/internal/ir"
)

// Raw is the caller's own SQL on the fixture's control table, so the test
// never reads or writes the column through the store under test: Set writes
// stream s1's liveness column (nil = NULL), Drop drops the control table.
type Raw struct {
	Set  func(ms *int64)
	Drop func()
}

// RoundTrip runs the liveness contract on stream s1, whose row exists in a
// control table that predates the column. It ends by dropping the table.
func RoundTrip(ctx context.Context, t *testing.T, store ir.StreamLivenessStore, raw Raw) {
	t.Helper()
	read := func(stream string) ir.StreamLiveness {
		t.Helper()
		l, err := store.ReadStreamLiveness(ctx, stream)
		if err != nil {
			t.Fatalf("read %s: %v", stream, err)
		}
		// The clock is the server's, read in the same statement. The
		// container shares the host's kernel clock, so it is near ours.
		if d := time.Since(l.Now); l.Found && l.Recorded && (d > time.Minute || d < -time.Minute) {
			t.Fatalf("read %s: server now %v is %v from this host's", stream, l.Now, d)
		}
		return l
	}
	// stamp runs one stamp and requires it to report wantStamped: whether
	// the stream's row took it.
	stamp := func(stream string, wantStamped bool) {
		t.Helper()
		stamped, err := store.StampStreamAlive(ctx, stream)
		if err != nil {
			t.Fatalf("stamp %s: %v", stream, err)
		}
		if stamped != wantStamped {
			t.Fatalf("stamp %s reported stamped=%v; want %v", stream, stamped, wantStamped)
		}
	}
	running := func(l ir.StreamLiveness) bool {
		return l.Found && l.Recorded && !l.Stopped && l.Age() > -time.Minute && l.Age() < time.Minute
	}

	// A control table an older release created: no column, so no record.
	if l := read("s1"); !l.Found || l.Recorded {
		t.Fatalf("pre-column table: %+v; want Found, not Recorded", l)
	}

	// A stamp adds the column and stamps: a running stream, just now.
	stamp("s1", true)
	if l := read("s1"); !running(l) {
		t.Fatalf("after a stamp: %+v; want a running stamp written just now", l)
	}

	// No row: a new stream. Stamps on it are no-ops, not errors.
	if l := read("nope"); l.Found {
		t.Fatalf("missing row: %+v; want not Found", l)
	}
	stamp("nope", false)
	if err := store.RecordStreamStopped(ctx, "nope"); err != nil {
		t.Fatalf("stop on a missing row: %v", err)
	}
	if l := read("nope"); l.Found {
		t.Fatalf("a stamp created a row: %+v", l)
	}

	// A recorded stop reads as Stopped; the next stamp (the next run's
	// attempt) overwrites it.
	if err := store.RecordStreamStopped(ctx, "s1"); err != nil {
		t.Fatalf("stop: %v", err)
	}
	if l := read("s1"); !l.Stopped {
		t.Fatalf("after the stop: %+v; want Stopped", l)
	}
	stamp("s1", true)
	if l := read("s1"); l.Stopped || !running(l) {
		t.Fatalf("after a stamp over the stop: %+v; want running", l)
	}

	// An old stamp (a crashed stream's, or an idle one's) reads as running,
	// not stopped, whatever its age.
	old := time.Now().Add(-time.Hour).UnixMilli()
	raw.Set(&old)
	if l := read("s1"); !l.Recorded || l.Stopped || l.Age() < 59*time.Minute {
		t.Fatalf("an hour-old stamp: %+v; want recorded, not stopped, an hour old", l)
	}
	// A value no stamp writes is an error, never a guess.
	bad := int64(-5)
	raw.Set(&bad)
	if _, err := store.ReadStreamLiveness(ctx, "s1"); err == nil {
		t.Fatal("a negative stored value read without error")
	}
	raw.Set(nil)
	if l := read("s1"); !l.Found || l.Recorded {
		t.Fatalf("NULL: %+v; want Found, not Recorded", l)
	}

	// No control table at all: nothing found, and stamps are no-ops.
	raw.Drop()
	if l := read("s1"); l.Found {
		t.Fatalf("no control table: %+v; want not Found", l)
	}
	stamp("s1", false)
}
