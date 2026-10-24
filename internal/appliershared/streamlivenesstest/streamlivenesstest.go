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
	stamp := func(stream string, wantStamped bool) ir.StreamStamp {
		t.Helper()
		st, err := store.StampStreamAlive(ctx, stream)
		if err != nil {
			t.Fatalf("stamp %s: %v", stream, err)
		}
		if st.Stamped != wantStamped {
			t.Fatalf("stamp %s reported stamped=%v; want %v", stream, st.Stamped, wantStamped)
		}
		return st
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
	over := stamp("s1", true)
	if l := read("s1"); l.Stopped || !running(l) {
		t.Fatalf("after a stamp over the stop: %+v; want running", l)
	}
	if !over.ReplacedStop() {
		t.Fatalf("a stamp over the stopped marker reported %+v; want ReplacedStop", over)
	}

	// Ownership: a run that stamped over a stop and never streamed puts the
	// stop back by compare-and-set on its own stamp...
	if ok, err := store.RestoreStreamStopped(ctx, "s1", over); err != nil || !ok {
		t.Fatalf("restore over its own stamp: restored=%v err=%v; want restored", ok, err)
	}
	if l := read("s1"); !l.Stopped {
		t.Fatalf("after restoring its own stamp: %+v; want Stopped", l)
	}
	// ...but never over a value anyone else wrote since (another run's stamp
	// or position write): the running record survives.
	mine := stamp("s1", true)
	if !mine.ReplacedStop() {
		t.Fatalf("second stamp over the restored stop reported %+v; want ReplacedStop", mine)
	}
	theirs := stamp("s1", true) // another run, stamping over mine
	if !theirs.Replaced(mine) || theirs.AliveMillis == mine.AliveMillis {
		t.Fatalf("a stamp over %+v reported %+v; want it to replace that exact value with a different one", mine, theirs)
	}
	if ok, err := store.RestoreStreamStopped(ctx, "s1", mine); err != nil || ok {
		t.Fatalf("restore over a stamp another run made since: restored=%v err=%v; want not restored", ok, err)
	}
	if l := read("s1"); !running(l) {
		t.Fatalf("after a refused restore: %+v; want the other run's running stamp", l)
	}
	// A stamp never writes the value it replaces, even over one from the
	// future (a skewed clock), so the compare-and-set can tell them apart.
	future := time.Now().Add(time.Hour).UnixMilli()
	raw.Set(&future)
	if st := stamp("s1", true); st.AliveMillis <= future || !st.Replaced(ir.StreamStamp{Stamped: true, AliveMillis: future}) {
		t.Fatalf("a stamp over a future value %d wrote %+v; want a larger value replacing it", future, st)
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
