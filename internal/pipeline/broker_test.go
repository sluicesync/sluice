// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package pipeline

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"sluicesync.dev/sluice/internal/ir"
	"sluicesync.dev/sluice/internal/pipeline/blobcodec"
)

// TestEncodeDecodeBrokerPosition pins the round-trip contract for the
// broker's synthetic position-shape: encodeBrokerPosition produces a
// JSON token decodeBrokerPosition can round-trip.
func TestEncodeDecodeBrokerPosition(t *testing.T) {
	pos := encodeBrokerPosition("file:///tmp/chain", "abc123def4567890")
	if pos.Engine != BackupBrokerPositionEngine {
		t.Errorf("engine = %q; want %q", pos.Engine, BackupBrokerPositionEngine)
	}
	if pos.Token == "" {
		t.Fatal("token is empty")
	}
	// Bug 39 fix: the token JSON carries an `_engine` field so the
	// broker sentinel survives the engine appliers' round-trip
	// discard of [ir.Position.Engine].
	if !strings.Contains(pos.Token, `"_engine":"backup-broker"`) {
		t.Errorf("token does not embed _engine sentinel: %s", pos.Token)
	}
	tok, err := decodeBrokerPosition(pos)
	if err != nil {
		t.Fatalf("decodeBrokerPosition: %v", err)
	}
	if tok.Engine != BackupBrokerPositionEngine {
		t.Errorf("decoded Engine = %q; want %q", tok.Engine, BackupBrokerPositionEngine)
	}
	if tok.ChainURL != "file:///tmp/chain" {
		t.Errorf("ChainURL = %q; want %q", tok.ChainURL, "file:///tmp/chain")
	}
	if tok.LastAppliedBackupID != "abc123def4567890" {
		t.Errorf("LastAppliedBackupID = %q; want %q", tok.LastAppliedBackupID, "abc123def4567890")
	}
}

// TestDecodeBrokerPosition_RejectsNonBrokerToken rejects positions
// whose token doesn't carry the broker's _engine sentinel — e.g. a
// live CDC stream's row written by a non-broker writer. Bug 39 fix:
// the discriminator moved from [ir.Position.Engine] to the JSON
// envelope's `_engine` field because the engine appliers' ReadPosition
// discards Position.Engine on read.
func TestDecodeBrokerPosition_RejectsNonBrokerToken(t *testing.T) {
	// Live CDC stream row shape (no _engine field).
	pos := ir.Position{Engine: "postgres", Token: `{"slot":"sluice_slot"}`}
	_, err := decodeBrokerPosition(pos)
	if err == nil {
		t.Fatal("err = nil; want refusal of non-broker token")
	}
}

// TestIsBrokerToken_RoundTrip pins the discriminator behaviour at the
// engine-applier round-trip boundary: a broker-written token always
// reads back as a broker token, regardless of what
// [ir.Position.Engine] looks like (the appliers stomp on it). Bug 39
// fix: this is the warm-resume gating predicate.
func TestIsBrokerToken_RoundTrip(t *testing.T) {
	// Simulate the broker writing a position …
	written := encodeBrokerPosition("file:///tmp/chain", "abc123")
	// … and the PG applier reading it back: token preserved, but
	// Engine clobbered to "postgres".
	readBack := ir.Position{Engine: "postgres", Token: written.Token}
	if !isBrokerToken(readBack) {
		t.Errorf("isBrokerToken = false on round-tripped broker token; want true")
	}
}

// TestIsBrokerToken_LegacyRow pins backward-compat: a v0.19.x-shaped
// row that doesn't carry the _engine field is treated as non-broker.
// Bug 39 fix doesn't break existing live-CDC writers.
func TestIsBrokerToken_LegacyRow(t *testing.T) {
	cases := []struct {
		name string
		pos  ir.Position
	}{
		{"empty", ir.Position{}},
		{"opaque", ir.Position{Engine: "mysql", Token: "MySQL56/abc:1-100"}},
		{"pg-json-no-engine", ir.Position{Engine: "postgres", Token: `{"slot":"x","lsn":"0/16B0000"}`}},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			if isBrokerToken(c.pos) {
				t.Errorf("isBrokerToken = true; want false on legacy non-broker shape")
			}
		})
	}
}

// TestRewritePosition_AllChangeShapes verifies rewritePosition copies
// the supplied position onto every concrete Change variant.
func TestRewritePosition_AllChangeShapes(t *testing.T) {
	target := ir.Position{Engine: "backup-broker", Token: "T"}
	for name, c := range map[string]ir.Change{
		"insert":   ir.Insert{Position: ir.Position{Token: "old"}, Schema: "s", Table: "t", Row: ir.Row{"id": 1}},
		"update":   ir.Update{Position: ir.Position{Token: "old"}, Schema: "s", Table: "t", After: ir.Row{"id": 1}},
		"delete":   ir.Delete{Position: ir.Position{Token: "old"}, Schema: "s", Table: "t"},
		"truncate": ir.Truncate{Position: ir.Position{Token: "old"}, Schema: "s", Table: "t"},
		"txbegin":  ir.TxBegin{Position: ir.Position{Token: "old"}},
		"txcommit": ir.TxCommit{Position: ir.Position{Token: "old"}},
	} {
		got, err := rewritePosition(c, target)
		if err != nil {
			t.Fatalf("%s: rewritePosition returned error: %v", name, err)
		}
		if got.Pos() != target {
			t.Errorf("%s: pos = %+v; want %+v", name, got.Pos(), target)
		}
	}
}

// TestSyncFromBackup_Validate_RequiresFields checks the sanity-check
// on Run's first step: empty fields produce clear errors.
func TestSyncFromBackup_Validate_RequiresFields(t *testing.T) {
	dir := t.TempDir()
	store, _ := blobcodec.NewLocalStore(dir)
	cases := []struct {
		name string
		b    SyncFromBackup
		want string
	}{
		{"no target", SyncFromBackup{Store: store, TargetDSN: "x", StreamID: "s"}, "Target engine is nil"},
		{"no targetdsn", SyncFromBackup{Target: stubTargetEngine{}, Store: store, StreamID: "s"}, "TargetDSN is empty"},
		{"no store", SyncFromBackup{Target: stubTargetEngine{}, TargetDSN: "x", StreamID: "s"}, "Store is nil"},
		{"no streamid", SyncFromBackup{Target: stubTargetEngine{}, TargetDSN: "x", Store: store}, "StreamID is empty"},
		{
			"reset+atchain",
			SyncFromBackup{
				Target: stubTargetEngine{}, TargetDSN: "x", Store: store, StreamID: "s",
				ResetTargetData: true, AtChainID: "abc",
			},
			"mutually exclusive",
		},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			err := c.b.validate()
			if err == nil {
				t.Fatalf("err = nil; want %q", c.want)
			}
			if !strings.Contains(err.Error(), c.want) {
				t.Errorf("err = %v; want %q substring", err, c.want)
			}
		})
	}
}

// TestBrokerState_RoundTrip pins the JSON shape of broker_state.json:
// write a state, read it back, observe equal fields.
func TestBrokerState_RoundTrip(t *testing.T) {
	dir := t.TempDir()
	store, _ := blobcodec.NewLocalStore(dir)
	now := time.Now().UTC().Truncate(time.Second)
	state := &brokerState{
		PID:         12345,
		Host:        "test.example.com",
		StreamID:    "test-stream",
		StartedAt:   now,
		LastApplyAt: now.Add(time.Minute),
	}
	if err := writeBrokerState(context.Background(), store, "broker_state.json", state); err != nil {
		t.Fatalf("writeBrokerState: %v", err)
	}
	got, err := readBrokerState(context.Background(), store, "broker_state.json")
	if err != nil {
		t.Fatalf("readBrokerState: %v", err)
	}
	if got.PID != state.PID || got.Host != state.Host || got.StreamID != state.StreamID {
		t.Errorf("round-trip mismatch: got %+v want %+v", got, state)
	}
	if !got.StartedAt.Equal(state.StartedAt) {
		t.Errorf("StartedAt mismatch: got %v want %v", got.StartedAt, state.StartedAt)
	}
}

// TestBrokerState_ReadMissingReturnsNil pins the cold-start tolerance
// of readBrokerState: a missing file returns (nil, nil), not an error.
func TestBrokerState_ReadMissingReturnsNil(t *testing.T) {
	dir := t.TempDir()
	store, _ := blobcodec.NewLocalStore(dir)
	got, err := readBrokerState(context.Background(), store, "missing.json")
	if err != nil {
		t.Fatalf("readBrokerState: %v", err)
	}
	if got != nil {
		t.Errorf("got non-nil state %+v; want nil", got)
	}
}

// TestWriteBrokerStateMergeHeartbeat_PreservesStop pins the v0.19.1
// clobber-fix shape applied to the broker side: a heartbeat write
// against a state file that already carries StopRequestedAt copies
// the stop forward + reports it.
func TestWriteBrokerStateMergeHeartbeat_PreservesStop(t *testing.T) {
	dir := t.TempDir()
	store, _ := blobcodec.NewLocalStore(dir)
	now := time.Now().UTC().Truncate(time.Second)
	stopT := now.Add(-time.Minute)
	prior := &brokerState{
		PID: 1, Host: "h", StreamID: "s",
		StartedAt: now, LastApplyAt: now,
		StopRequestedAt: &stopT,
	}
	if err := writeBrokerState(context.Background(), store, "broker_state.json", prior); err != nil {
		t.Fatalf("writeBrokerState: %v", err)
	}
	heartbeat := &brokerState{
		PID: 1, Host: "h", StreamID: "s",
		StartedAt: now, LastApplyAt: now.Add(time.Second),
	}
	stopObserved, err := writeBrokerStateMergeHeartbeat(context.Background(), store, "broker_state.json", heartbeat)
	if err != nil {
		t.Fatalf("writeBrokerStateMergeHeartbeat: %v", err)
	}
	if !stopObserved {
		t.Error("stopObserved = false; want true (concurrent stop should be reported)")
	}
	got, err := readBrokerState(context.Background(), store, "broker_state.json")
	if err != nil {
		t.Fatalf("readBrokerState: %v", err)
	}
	if got.StopRequestedAt == nil {
		t.Fatal("StopRequestedAt is nil; want preserved across heartbeat")
	}
}

// TestRequestSyncFromBackupStop_RefusesMissingFile mirrors
// RequestStreamStop: with no broker_state.json, the stop refuses
// rather than silently writing a phantom file.
func TestRequestSyncFromBackupStop_RefusesMissingFile(t *testing.T) {
	dir := t.TempDir()
	store, _ := blobcodec.NewLocalStore(dir)
	_, err := RequestSyncFromBackupStop(context.Background(), store, time.Now())
	if err == nil {
		t.Fatal("err = nil; want refusal")
	}
	if !strings.Contains(err.Error(), "no broker_state.json") {
		t.Errorf("err = %v; want 'no broker_state.json' guidance", err)
	}
}

// TestBrokerStopRegistry exercises the in-process stop channel
// lifecycle.
func TestBrokerStopRegistry(t *testing.T) {
	dir := t.TempDir()
	store, _ := blobcodec.NewLocalStore(dir)

	ch, deregister := registerBrokerStopChan(store)
	defer deregister()
	select {
	case <-ch:
		t.Fatal("channel closed before notify")
	default:
	}
	if !notifyBrokerStop(store) {
		t.Error("notifyBrokerStop = false; want true (registered)")
	}
	select {
	case <-ch:
	default:
		t.Error("channel not closed after notifyBrokerStop")
	}
}

// TestBrokerStopRegistry_NoEntryReturnsFalse — the cross-process case
// where no broker is registered in this process.
func TestBrokerStopRegistry_NoEntryReturnsFalse(t *testing.T) {
	dir := t.TempDir()
	store, _ := blobcodec.NewLocalStore(dir)
	if notifyBrokerStop(store) {
		t.Error("notifyBrokerStop = true with no entry; want false")
	}
}

// stubTargetEngine is a minimal ir.Engine for validate-shape tests
// that don't run anything against a real database.
type stubTargetEngine struct{}

func (stubTargetEngine) Name() string                  { return "stub-target" }
func (stubTargetEngine) Capabilities() ir.Capabilities { return ir.Capabilities{} }
func (stubTargetEngine) OpenSchemaReader(_ context.Context, _ string) (ir.SchemaReader, error) {
	return nil, errors.New("stub")
}

func (stubTargetEngine) OpenSchemaWriter(_ context.Context, _ string) (ir.SchemaWriter, error) {
	return nil, errors.New("stub")
}

func (stubTargetEngine) OpenRowReader(_ context.Context, _ string) (ir.RowReader, error) {
	return nil, errors.New("stub")
}

func (stubTargetEngine) OpenRowWriter(_ context.Context, _ string) (ir.RowWriter, error) {
	return nil, errors.New("stub")
}

func (stubTargetEngine) OpenCDCReader(_ context.Context, _ string) (ir.CDCReader, error) {
	return nil, errors.New("stub")
}

func (stubTargetEngine) OpenChangeApplier(_ context.Context, _ string) (ir.ChangeApplier, error) {
	return nil, errors.New("stub")
}

func (stubTargetEngine) OpenSnapshotStream(_ context.Context, _ string) (*ir.SnapshotStream, error) {
	return nil, errors.New("stub")
}
