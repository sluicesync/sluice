// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package mysql

import (
	"context"
	"errors"
	"strings"
	"testing"

	"google.golang.org/grpc"
	"vitess.io/vitess/go/vt/proto/topodata"
	"vitess.io/vitess/go/vt/proto/vtgate"
	"vitess.io/vitess/go/vt/proto/vtgateservice"

	"sluicesync.dev/sluice/internal/ir"
)

// errVStreamClient is a VitessClient whose VStream always errors, so a test can
// drive StreamChanges PAST its guard/reset and observe a clean error rather than
// a nil-client panic.
type errVStreamClient struct {
	vtgateservice.VitessClient // embedded nil: every other method panics
}

func (errVStreamClient) VStream(context.Context, *vtgate.VStreamRequest, ...grpc.CallOption) (vtgateservice.Vitess_VStreamClient, error) {
	return nil, errors.New("vstream boom")
}

func newGuardTestReader() *vstreamCDCReader {
	return &vstreamCDCReader{
		keyspace:   "ks",
		shards:     []string{"-"}, // preset so StreamChanges skips shard discovery
		tabletType: topodata.TabletType_PRIMARY,
		client:     errVStreamClient{},
	}
}

// TestVStreamCDCReader_StreamChangesRefusesSecondCall pins the audit-2026-08-19
// LOW: StreamChanges must refuse a second call (the standalone counterpart of
// the snapshot stream's pumpStarted guard). Without it a second call overwrites
// streamerCancel/pumpDone and leaks the first pump. The erroring fake lets the
// mutation (guard removed) surface as the VStream error instead of "already
// called", so the assertion catches it.
func TestVStreamCDCReader_StreamChangesRefusesSecondCall(t *testing.T) {
	r := newGuardTestReader()
	r.streamStarted = true // simulate a stream already open

	_, err := r.StreamChanges(context.Background(), ir.Position{})
	if err == nil || !strings.Contains(err.Error(), "already called") {
		t.Fatalf("second StreamChanges must be refused with 'already called'; got %v", err)
	}
}

// TestVStreamCDCReader_StreamChangesResetsStaleErr pins the reset half: a fresh
// StreamChanges clears any stale r.err before opening, so Err() cannot report a
// spent failure from a prior stream. The open then fails (erroring fake), but
// that path returns the error directly without setErr, so r.err stays the
// freshly-cleared nil — proving the reset ran. Mutation (drop the reset): the
// stale error survives and Err() returns it.
func TestVStreamCDCReader_StreamChangesResetsStaleErr(t *testing.T) {
	r := newGuardTestReader()
	stale := errors.New("a spent error from a prior stream")
	r.err = stale

	if _, err := r.StreamChanges(context.Background(), ir.Position{}); err == nil {
		t.Fatal("expected the erroring fake VStream to fail the open")
	}
	if got := r.Err(); got != nil {
		t.Fatalf("StreamChanges did not reset a stale r.err: Err() = %v; want nil", got)
	}
}
