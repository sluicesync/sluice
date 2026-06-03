// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package pipeline

import (
	"context"
	"strings"
	"testing"
	"time"

	"sluicesync.dev/sluice/internal/ir"
)

// TestStreamState_RoundTrip verifies the JSON round-trip of the state
// file shape: write, read, fields preserved.
func TestStreamState_RoundTrip(t *testing.T) {
	dir := t.TempDir()
	store, _ := NewLocalStore(dir)

	then := time.Date(2026, 5, 8, 10, 0, 0, 0, time.UTC)
	stop := then.Add(5 * time.Minute)
	want := &streamState{
		PID:             1234,
		Host:            "alpha.example.com",
		StartedAt:       then,
		LastRolloverAt:  then.Add(time.Minute),
		StopRequestedAt: &stop,
	}
	if err := writeStreamState(context.Background(), store, "manifests/stream_state.json", want); err != nil {
		t.Fatalf("writeStreamState: %v", err)
	}
	got, err := readStreamState(context.Background(), store, "manifests/stream_state.json")
	if err != nil {
		t.Fatalf("readStreamState: %v", err)
	}
	if got == nil {
		t.Fatal("got nil; want round-tripped state")
	}
	if got.PID != want.PID || got.Host != want.Host {
		t.Errorf("pid/host = %d/%q; want %d/%q", got.PID, got.Host, want.PID, want.Host)
	}
	if !got.StartedAt.Equal(want.StartedAt) {
		t.Errorf("started_at = %v; want %v", got.StartedAt, want.StartedAt)
	}
	if !got.LastRolloverAt.Equal(want.LastRolloverAt) {
		t.Errorf("last_rollover_at = %v; want %v", got.LastRolloverAt, want.LastRolloverAt)
	}
	if got.StopRequestedAt == nil || !got.StopRequestedAt.Equal(stop) {
		t.Errorf("stop_requested_at = %v; want %v", got.StopRequestedAt, stop)
	}
}

// TestStreamState_ReadMissing returns (nil, nil) when no file exists.
func TestStreamState_ReadMissing(t *testing.T) {
	dir := t.TempDir()
	store, _ := NewLocalStore(dir)
	got, err := readStreamState(context.Background(), store, "manifests/stream_state.json")
	if err != nil {
		t.Errorf("err = %v; want nil for missing file", err)
	}
	if got != nil {
		t.Errorf("got = %+v; want nil for missing file", got)
	}
}

// TestPreflightStreamState_NoExistingFile_ClearStart verifies the
// fresh-start path: no state file, preflight succeeds.
func TestPreflightStreamState_NoExistingFile_ClearStart(t *testing.T) {
	dir := t.TempDir()
	store, _ := NewLocalStore(dir)
	b := &BackupStream{Store: store}
	err := b.preflightStreamState(context.Background(), "manifests/stream_state.json", time.Minute, 1, "h", time.Now())
	if err != nil {
		t.Errorf("preflight on empty store err = %v; want nil", err)
	}
}

// TestPreflightStreamState_StaleState_TakesOver verifies a stale state
// file (last_rollover_at older than 2*window ago) is overridden with a
// WARN log; preflight succeeds without --force.
func TestPreflightStreamState_StaleState_TakesOver(t *testing.T) {
	dir := t.TempDir()
	store, _ := NewLocalStore(dir)
	now := time.Date(2026, 5, 8, 10, 0, 0, 0, time.UTC)
	stale := &streamState{
		PID: 999, Host: "old.example.com",
		StartedAt:      now.Add(-time.Hour),
		LastRolloverAt: now.Add(-time.Hour),
	}
	_ = writeStreamState(context.Background(), store, "manifests/stream_state.json", stale)
	b := &BackupStream{Store: store}
	err := b.preflightStreamState(context.Background(), "manifests/stream_state.json", time.Minute, 1, "new.example.com", now)
	if err != nil {
		t.Errorf("preflight on stale state err = %v; want nil", err)
	}
}

// TestPreflightStreamState_FreshConflict_Refuses verifies a fresh
// state file from a different (pid, host) is refused with an
// operator-actionable error.
func TestPreflightStreamState_FreshConflict_Refuses(t *testing.T) {
	dir := t.TempDir()
	store, _ := NewLocalStore(dir)
	now := time.Date(2026, 5, 8, 10, 0, 0, 0, time.UTC)
	fresh := &streamState{
		PID: 999, Host: "other.example.com",
		StartedAt:      now.Add(-30 * time.Second),
		LastRolloverAt: now.Add(-10 * time.Second), // very fresh
	}
	_ = writeStreamState(context.Background(), store, "manifests/stream_state.json", fresh)
	b := &BackupStream{Store: store}
	err := b.preflightStreamState(context.Background(), "manifests/stream_state.json", time.Minute, 1, "us.example.com", now)
	if err == nil {
		t.Fatal("err = nil; want refusal on fresh-conflict")
	}
	if !strings.Contains(err.Error(), "stream is already running") {
		t.Errorf("err = %v; want 'stream is already running' guidance", err)
	}
	if !strings.Contains(err.Error(), "--force") {
		t.Errorf("err = %v; want '--force' override hint", err)
	}
}

// TestPreflightStreamState_ForceOverride_Bypasses verifies --force
// makes a fresh-conflict succeed (operator-confirmed takeover).
func TestPreflightStreamState_ForceOverride_Bypasses(t *testing.T) {
	dir := t.TempDir()
	store, _ := NewLocalStore(dir)
	now := time.Date(2026, 5, 8, 10, 0, 0, 0, time.UTC)
	fresh := &streamState{
		PID: 999, Host: "other.example.com",
		StartedAt:      now.Add(-30 * time.Second),
		LastRolloverAt: now.Add(-10 * time.Second),
	}
	_ = writeStreamState(context.Background(), store, "manifests/stream_state.json", fresh)
	b := &BackupStream{Store: store, Force: true}
	err := b.preflightStreamState(context.Background(), "manifests/stream_state.json", time.Minute, 1, "us.example.com", now)
	if err != nil {
		t.Errorf("preflight with --force err = %v; want nil", err)
	}
}

// TestPreflightStreamState_SameProcessRestart_Bypasses verifies a
// re-run from the SAME (pid, host) doesn't trip the concurrent-writer
// check (operator's pid was reused after a clean restart).
func TestPreflightStreamState_SameProcessRestart_Bypasses(t *testing.T) {
	dir := t.TempDir()
	store, _ := NewLocalStore(dir)
	now := time.Date(2026, 5, 8, 10, 0, 0, 0, time.UTC)
	prev := &streamState{
		PID: 1234, Host: "us.example.com",
		StartedAt:      now.Add(-10 * time.Second),
		LastRolloverAt: now.Add(-5 * time.Second),
	}
	_ = writeStreamState(context.Background(), store, "manifests/stream_state.json", prev)
	b := &BackupStream{Store: store}
	err := b.preflightStreamState(context.Background(), "manifests/stream_state.json", time.Minute, 1234, "us.example.com", now)
	if err != nil {
		t.Errorf("same-process preflight err = %v; want nil", err)
	}
}

// TestWriteStreamStateMergeHeartbeat_PreservesStop pins the Bug 37 fix:
// the heartbeat write must NOT clobber a concurrent stop_requested_at
// written by RequestStreamStop in the race window. Pre-fix the in-memory
// `state` (no StopRequestedAt) overwrote whatever was on disk; post-fix
// the merge helper does a read-modify-write that copies StopRequestedAt
// forward.
func TestWriteStreamStateMergeHeartbeat_PreservesStop(t *testing.T) {
	dir := t.TempDir()
	store, _ := NewLocalStore(dir)
	t0 := time.Date(2026, 5, 8, 10, 0, 0, 0, time.UTC)
	stop := t0.Add(-time.Second) // operator wrote stop just before our heartbeat

	// Simulate the operator's RequestStreamStop having landed already:
	// state file carries stop_requested_at + the previous heartbeat.
	priorOnDisk := &streamState{
		PID: 1234, Host: "h",
		StartedAt:       t0.Add(-time.Minute),
		LastRolloverAt:  t0.Add(-30 * time.Second),
		StopRequestedAt: &stop,
	}
	if err := writeStreamState(context.Background(), store, DefaultStreamStateFilename, priorOnDisk); err != nil {
		t.Fatalf("seed: %v", err)
	}

	// Stream's heartbeat-write payload: in-memory state, no
	// StopRequestedAt (the stream's main goroutine doesn't track that
	// field). Pre-fix this would have clobbered the operator's stop.
	heartbeat := &streamState{
		PID: 1234, Host: "h",
		StartedAt:      t0.Add(-time.Minute),
		LastRolloverAt: t0,
	}
	stopObserved, err := writeStreamStateMergeHeartbeat(context.Background(), store, DefaultStreamStateFilename, heartbeat)
	if err != nil {
		t.Fatalf("writeStreamStateMergeHeartbeat: %v", err)
	}
	if !stopObserved {
		t.Errorf("stopObserved = false; want true (concurrent stop_requested_at present on disk)")
	}

	got, err := readStreamState(context.Background(), store, DefaultStreamStateFilename)
	if err != nil {
		t.Fatalf("readStreamState: %v", err)
	}
	if got.StopRequestedAt == nil {
		t.Fatal("stop_requested_at = nil after heartbeat merge; want preserved (Bug 37 clobber bug)")
	}
	if !got.StopRequestedAt.Equal(stop) {
		t.Errorf("stop_requested_at = %v; want %v (operator's value)", *got.StopRequestedAt, stop)
	}
	if !got.LastRolloverAt.Equal(t0) {
		t.Errorf("last_rollover_at = %v; want %v (heartbeat advanced it)", got.LastRolloverAt, t0)
	}
}

// TestWriteStreamStateMergeHeartbeat_NoStopReturnsFalse pins the
// happy-path heartbeat: when no concurrent stop is on disk, the merge
// returns stopObserved=false and the file simply gets the new
// LastRolloverAt.
func TestWriteStreamStateMergeHeartbeat_NoStopReturnsFalse(t *testing.T) {
	dir := t.TempDir()
	store, _ := NewLocalStore(dir)
	t0 := time.Date(2026, 5, 8, 10, 0, 0, 0, time.UTC)

	prior := &streamState{
		PID: 1234, Host: "h",
		StartedAt:      t0.Add(-time.Minute),
		LastRolloverAt: t0.Add(-time.Second),
	}
	if err := writeStreamState(context.Background(), store, DefaultStreamStateFilename, prior); err != nil {
		t.Fatalf("seed: %v", err)
	}

	heartbeat := &streamState{
		PID: 1234, Host: "h",
		StartedAt:      t0.Add(-time.Minute),
		LastRolloverAt: t0,
	}
	stopObserved, err := writeStreamStateMergeHeartbeat(context.Background(), store, DefaultStreamStateFilename, heartbeat)
	if err != nil {
		t.Fatalf("writeStreamStateMergeHeartbeat: %v", err)
	}
	if stopObserved {
		t.Errorf("stopObserved = true; want false (no concurrent stop on disk)")
	}
	got, _ := readStreamState(context.Background(), store, DefaultStreamStateFilename)
	if got.StopRequestedAt != nil {
		t.Errorf("stop_requested_at = %v; want nil (none was set)", *got.StopRequestedAt)
	}
}

// TestRequestStreamStop_Roundtrip verifies the cross-machine stop path:
// write stop_requested_at, observe via readStreamStopRequested.
func TestRequestStreamStop_Roundtrip(t *testing.T) {
	dir := t.TempDir()
	store, _ := NewLocalStore(dir)
	now := time.Date(2026, 5, 8, 10, 0, 0, 0, time.UTC)
	prev := &streamState{
		PID: 1234, Host: "h",
		StartedAt:      now.Add(-time.Hour),
		LastRolloverAt: now.Add(-time.Minute),
	}
	if err := writeStreamState(context.Background(), store, DefaultStreamStateFilename, prev); err != nil {
		t.Fatalf("seed: %v", err)
	}
	got, err := RequestStreamStop(context.Background(), store, now)
	if err != nil {
		t.Fatalf("RequestStreamStop: %v", err)
	}
	if got.PID != prev.PID {
		t.Errorf("returned prev pid = %d; want %d", got.PID, prev.PID)
	}
	stopReq, err := readStreamStopRequested(context.Background(), store, DefaultStreamStateFilename)
	if err != nil {
		t.Fatalf("readStreamStopRequested: %v", err)
	}
	if stopReq == nil {
		t.Fatal("stop_requested_at is nil; want set")
	}
	if !stopReq.Equal(now.UTC()) {
		t.Errorf("stop_requested_at = %v; want %v", *stopReq, now.UTC())
	}
}

// TestRequestStreamStop_NoStateFile errors clearly when no stream is
// running.
func TestRequestStreamStop_NoStateFile(t *testing.T) {
	dir := t.TempDir()
	store, _ := NewLocalStore(dir)
	_, err := RequestStreamStop(context.Background(), store, time.Now())
	if err == nil {
		t.Fatal("err = nil; want error when no state file present")
	}
	if !strings.Contains(err.Error(), "no stream is running") {
		t.Errorf("err = %v; want 'no stream is running' guidance", err)
	}
}

// TestRequestStreamStop_Idempotent verifies re-issuing stop preserves
// the original timestamp (doesn't reset the clock).
func TestRequestStreamStop_Idempotent(t *testing.T) {
	dir := t.TempDir()
	store, _ := NewLocalStore(dir)
	t0 := time.Date(2026, 5, 8, 10, 0, 0, 0, time.UTC)
	t1 := t0.Add(5 * time.Minute)

	prev := &streamState{
		PID: 1, Host: "h",
		LastRolloverAt: t0.Add(-time.Minute),
	}
	if err := writeStreamState(context.Background(), store, DefaultStreamStateFilename, prev); err != nil {
		t.Fatalf("seed: %v", err)
	}
	if _, err := RequestStreamStop(context.Background(), store, t0); err != nil {
		t.Fatalf("first stop: %v", err)
	}
	if _, err := RequestStreamStop(context.Background(), store, t1); err != nil {
		t.Fatalf("second stop: %v", err)
	}
	got, err := readStreamStopRequested(context.Background(), store, DefaultStreamStateFilename)
	if err != nil {
		t.Fatalf("read: %v", err)
	}
	if got == nil || !got.Equal(t0.UTC()) {
		t.Errorf("stop_requested_at after re-issue = %v; want %v (first stop's timestamp preserved)", got, t0.UTC())
	}
}

// TestBackupStream_Run_RefusesFreshConcurrentWriter exercises the
// preflight refusal end-to-end through Run.
func TestBackupStream_Run_RefusesFreshConcurrentWriter(t *testing.T) {
	dir := t.TempDir()
	store, _ := NewLocalStore(dir)

	parent := &ir.Manifest{
		FormatVersion: ir.BackupFormatVersion,
		CreatedAt:     time.Now().UTC(),
		SourceEngine:  "postgres",
		Schema:        &ir.Schema{},
		Kind:          ir.BackupKindFull,
		EndPosition:   ir.Position{Engine: "postgres", Token: `{"slot":"sluice_slot","lsn":"0/100"}`},
	}
	parent.BackupID = ir.ComputeBackupID(parent)
	writeParentFullManifest(t, store, parent)

	now := time.Now().UTC()
	prev := &streamState{
		PID: 999, Host: "other.example.com",
		StartedAt:      now.Add(-30 * time.Second),
		LastRolloverAt: now.Add(-5 * time.Second), // fresh
	}
	_ = writeStreamState(context.Background(), store, DefaultStreamStateFilename, prev)

	src := &fakeCDCEngine{name: "postgres", schemaSequence: []*ir.Schema{{}}}
	stream := &BackupStream{
		Source:         src,
		SourceDSN:      "src",
		Store:          store,
		ParentRef:      parent.BackupID,
		RolloverWindow: time.Minute,
		pidHostFn:      func() (int, string) { return 1, "us.example.com" },
		Now:            func() time.Time { return now },
	}
	err := stream.Run(context.Background())
	if err == nil {
		t.Fatal("err = nil; want concurrent-writer refusal")
	}
	if !strings.Contains(err.Error(), "stream is already running") {
		t.Errorf("err = %v; want concurrent-writer guidance", err)
	}
}
