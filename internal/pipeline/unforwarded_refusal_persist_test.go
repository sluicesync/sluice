// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package pipeline

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"
	"unicode/utf8"

	"sluicesync.dev/sluice/internal/ir"
	irbackup "sluicesync.dev/sluice/internal/ir/backup"
	"sluicesync.dev/sluice/internal/pipeline/blobcodec"
	"sluicesync.dev/sluice/internal/pipeline/migcore"
)

// engineRefusal stands in for a CDC reader's UNFORWARDED-SCHEMA-CHANGE
// refusal: both engines build theirs as ir.WithMarker(<prose>,
// ir.ErrUnforwardedSchemaChange) (pinned engine-side by
// TestUnforwardedChangeError_WrapsTheSentinel), so the pipeline sees exactly
// this shape.
func engineRefusal(what string) error {
	return ir.WithMarker(fmt.Errorf("postgres: cdc: UNFORWARDED-SCHEMA-CHANGE on public.t: %s", what), ir.ErrUnforwardedSchemaChange)
}

// fakeRefusalStore is an ir.ChangeApplier that implements only the
// persisted-refusal surface; every other method panics via the nil embed.
type fakeRefusalStore struct {
	ir.ChangeApplier
	msg       string
	has       bool
	readErr   error
	cleared   int
	recorded  []string
	ensureErr error
	ensured   int
}

func (f *fakeRefusalStore) EnsureUnforwardedRefusalStorage(context.Context) error {
	f.ensured++
	return f.ensureErr
}

func (f *fakeRefusalStore) RecordUnforwardedRefusal(_ context.Context, _, msg string) error {
	f.recorded = append(f.recorded, msg)
	f.msg, f.has = msg, true
	return nil
}

func (f *fakeRefusalStore) ReadUnforwardedRefusal(context.Context, string) (msg string, ok bool, err error) {
	return f.msg, f.has, f.readErr
}

func (f *fakeRefusalStore) ClearUnforwardedRefusal(context.Context, string) error {
	f.cleared++
	f.msg, f.has = "", false
	return nil
}

var _ ir.UnforwardedRefusalStore = (*fakeRefusalStore)(nil)

// TestUnforwardedRefusal_SentinelSurvivesThePipelineWrapping: the recorder
// and the fleet key on errors.Is, so every wrapper a refusal travels through
// on its way out of Run must keep the sentinel reachable — the backup
// stream's rollover wrap, the phase hint, and connectHint.
func TestUnforwardedRefusal_SentinelSurvivesThePipelineWrapping(t *testing.T) {
	base := engineRefusal(`ADD CONSTRAINT "u" UNIQUE (name)`)
	wrapped := map[string]error{
		"bare":            base,
		"rollover wrap":   migcore.WrapWithHint(migcore.PhaseCDC, fmt.Errorf("stream: rollover %d: %w", 3, base)),
		"cdc reader wrap": fmt.Errorf("cdc reader: %w", base),
		"connect hint":    connectHint(base),
		"phase hint":      migcore.WrapWithHint(migcore.PhaseCDC, base),
	}
	for name, err := range wrapped {
		if !errors.Is(err, ir.ErrUnforwardedSchemaChange) {
			t.Errorf("%s: errors.Is(ErrUnforwardedSchemaChange) = false: %v", name, err)
		}
		if !freshUnforwardedRefusal(err) {
			t.Errorf("%s: not classified as a fresh refusal to record", name)
		}
	}
	if freshUnforwardedRefusal(errors.New("postgres: cdc: something else")) || freshUnforwardedRefusal(nil) {
		t.Error("a non-refusal error (or nil) was classified as a refusal to record")
	}
	replay := &recordedUnforwardedRefusalError{where: "w", recorded: "r", remedy: "x"}
	if !errors.Is(replay, ir.ErrUnforwardedSchemaChange) || !ir.IsTerminal(replay) {
		t.Error("the startup door's refusal must wrap the sentinel and be terminal")
	}
	if freshUnforwardedRefusal(connectHint(replay)) {
		t.Error("the door's own replay was classified as fresh; it would be re-recorded nested inside itself on every restart")
	}
}

// TestStreamerUnforwardedDoor_ZeroValueRefuses: a Streamer built without
// the acknowledgement (every non-CLI construction) refuses on a recorded
// refusal, names the flag and the remedy, quotes the record, and leaves it
// in place.
func TestStreamerUnforwardedDoor_ZeroValueRefuses(t *testing.T) {
	store := &fakeRefusalStore{msg: `UNFORWARDED-SCHEMA-CHANGE on public.t: ADD CONSTRAINT "u" UNIQUE (name)`, has: true}
	err := (&Streamer{}).phaseRefuseRecordedUnforwardedChange(context.Background(), store, "s1")
	if err == nil {
		t.Fatal("the zero-value Streamer proceeded past a recorded refusal")
	}
	if !errors.Is(err, ir.ErrUnforwardedSchemaChange) || !ir.IsTerminal(err) {
		t.Errorf("refusal is not a terminal ErrUnforwardedSchemaChange: %v", err)
	}
	for _, want := range []string{"UNFORWARDED-SCHEMA-CHANGE", unforwardedRefusalAckFlag, `ADD CONSTRAINT "u" UNIQUE (name)`, "apply the same change to the target", `stream "s1"`} {
		if !strings.Contains(err.Error(), want) {
			t.Errorf("refusal missing %q: %v", want, err)
		}
	}
	if store.cleared != 0 || !store.has {
		t.Error("a refusing start cleared the record")
	}
}

// TestStreamerUnforwardedDoor_AckClearsAndIsConsumed: the acknowledgement
// clears the record, lets the start proceed, and is spent — the same
// Streamer restarted after a new refusal refuses again.
func TestStreamerUnforwardedDoor_AckClearsAndIsConsumed(t *testing.T) {
	store := &fakeRefusalStore{msg: "UNFORWARDED-SCHEMA-CHANGE: recorded", has: true}
	s := &Streamer{AcceptUnforwardedSchemaChange: unforwardedRefusalFingerprint(store.msg)}
	if err := s.phaseRefuseRecordedUnforwardedChange(context.Background(), store, "s1"); err != nil {
		t.Fatalf("acknowledged start refused: %v", err)
	}
	if store.cleared != 1 || store.has {
		t.Fatalf("acknowledged start did not clear the record (cleared=%d has=%v)", store.cleared, store.has)
	}
	if s.AcceptUnforwardedSchemaChange != "" {
		t.Error("the acknowledgement was not consumed")
	}
	store.msg, store.has = "UNFORWARDED-SCHEMA-CHANGE: a second one", true
	if err := s.phaseRefuseRecordedUnforwardedChange(context.Background(), store, "s1"); !errors.Is(err, ir.ErrUnforwardedSchemaChange) {
		t.Errorf("a later refusal in the same process was pre-accepted: %v", err)
	}
}

// TestStreamerUnforwardedDoor_NothingRecorded proceeds without touching the
// store; a read failure refuses rather than starting unchecked.
func TestStreamerUnforwardedDoor_NothingRecorded(t *testing.T) {
	store := &fakeRefusalStore{}
	if err := (&Streamer{AcceptUnforwardedSchemaChange: "0123456789ab"}).phaseRefuseRecordedUnforwardedChange(context.Background(), store, "s1"); err != nil {
		t.Fatalf("no record, yet refused: %v", err)
	}
	if store.cleared != 0 {
		t.Error("cleared a record that did not exist")
	}
	store.readErr = errors.New("boom")
	if err := (&Streamer{}).phaseRefuseRecordedUnforwardedChange(context.Background(), store, "s1"); err == nil || !strings.Contains(err.Error(), "boom") {
		t.Errorf("a failed read started the stream unchecked: %v", err)
	}
}

// TestStreamerRecordUnforwardedRefusal records only a FRESH refusal, in its
// storable form, and never the door's own replay or an unrelated error.
func TestStreamerRecordUnforwardedRefusal(t *testing.T) {
	s := &Streamer{}
	store := &fakeRefusalStore{}
	s.recordUnforwardedRefusal(context.Background(), store, "s1", nil)
	s.recordUnforwardedRefusal(context.Background(), store, "s1", errors.New("unrelated"))
	s.recordUnforwardedRefusal(context.Background(), store, "s1", &recordedUnforwardedRefusalError{recorded: "old"})
	if len(store.recorded) != 0 {
		t.Fatalf("recorded %q; want nothing for nil / unrelated / replay", store.recorded)
	}
	refusal := migcore.WrapWithHint(migcore.PhaseCDC, engineRefusal("bad\x00byte"))
	s.recordUnforwardedRefusal(context.Background(), store, "s1", refusal)
	if len(store.recorded) != 1 {
		t.Fatalf("recorded %d times; want 1", len(store.recorded))
	}
	if got := store.recorded[0]; !strings.HasPrefix(got, "[recorded ") || !strings.HasSuffix(got, storableUnforwardedRefusal(refusal.Error())) || strings.ContainsRune(got, 0) {
		t.Errorf("recorded %q; want the storable form of the refusal", got)
	}
}

// TestStorableUnforwardedRefusal: the record must land on both engines'
// text columns (valid UTF-8, no NUL) and be capped on a rune boundary, with
// the cut marked.
func TestStorableUnforwardedRefusal(t *testing.T) {
	short := "UNFORWARDED-SCHEMA-CHANGE: café ✓"
	if got := storableUnforwardedRefusal(short); got != short {
		t.Errorf("short message changed: %q", got)
	}
	if got := storableUnforwardedRefusal("a\xffb\x00c"); !utf8.ValidString(got) || strings.ContainsRune(got, 0) {
		t.Errorf("invalid UTF-8 / NUL not made storable: %q", got)
	}
	// Every width of rune straddling the cut.
	for _, r := range []string{"é", "✓", "😀"} {
		long := strings.Repeat("x", unforwardedRefusalMaxLen-2) + strings.Repeat(r, 50)
		got := storableUnforwardedRefusal(long)
		if len(got) > unforwardedRefusalMaxLen || !utf8.ValidString(got) || !strings.HasSuffix(got, "…") {
			t.Errorf("rune %q: len=%d valid=%v; want <= %d bytes, valid UTF-8, ellipsis-marked", r, len(got), utf8.ValidString(got), unforwardedRefusalMaxLen)
		}
	}
}

// refusingCDCEngine is a CDC source whose reader ends every stream with the
// engines' refusal shape (surfaced through the reader's Err, as the real
// pgoutput / binlog pumps do), and counts reader opens so a test can prove
// the startup door refused BEFORE a pump opened.
type refusingCDCEngine struct {
	fakeCDCEngine
	refuse bool
	opens  int
}

func (e *refusingCDCEngine) OpenCDCReader(_ context.Context, _ string) (ir.CDCReader, error) {
	e.opens++
	return &refusingCDCReader{refuse: e.refuse}, nil
}

type refusingCDCReader struct{ refuse bool }

func (r *refusingCDCReader) StreamChanges(context.Context, ir.Position) (<-chan ir.Change, error) {
	out := make(chan ir.Change)
	close(out)
	return out, nil
}

func (r *refusingCDCReader) Err() error {
	if r.refuse {
		return engineRefusal(`ADD CONSTRAINT "u" UNIQUE (name)`)
	}
	return nil
}

func (r *refusingCDCReader) Close() error { return nil }

// TestBackupStream_UnforwardedRefusalSurvivesARestart is the supervisor-free
// restart of `backup stream run`: a run that ends on the refusal records it
// in the stream-state file, a NEW BackupStream against the same destination
// refuses before opening a pump, and one started with the acknowledgement
// proceeds and clears the record.
func TestBackupStream_UnforwardedRefusalSurvivesARestart(t *testing.T) {
	dir := t.TempDir()
	store, _ := blobcodec.NewLocalStore(dir)
	parent := &irbackup.Manifest{
		FormatVersion: irbackup.BackupFormatVersion,
		CreatedAt:     time.Now().UTC(),
		SourceEngine:  "postgres",
		Schema:        &ir.Schema{},
		Kind:          irbackup.BackupKindFull,
		EndPosition:   ir.Position{Engine: "postgres", Token: `{"slot":"sluice_slot","lsn":"0/100"}`},
	}
	parent.BackupID = irbackup.ComputeBackupID(parent)
	writeParentFullManifest(t, store, parent)

	newStream := func(src *refusingCDCEngine, ack string) *BackupStream {
		return &BackupStream{
			Source:                        src,
			SourceDSN:                     "src",
			Store:                         store,
			ParentRef:                     parent.BackupID,
			RolloverWindow:                time.Minute,
			AcceptUnforwardedSchemaChange: ack,
			// The same (pid, host): a restart the concurrent-writer check
			// admits immediately, so this test reaches the door.
			pidHostFn: func() (int, string) { return 7, "h" },
		}
	}
	src := func(refuse bool) *refusingCDCEngine {
		return &refusingCDCEngine{fakeCDCEngine: fakeCDCEngine{name: "postgres", schemaSequence: []*ir.Schema{{}}}, refuse: refuse}
	}

	first := src(true)
	if err := newStream(first, "").Run(context.Background()); !errors.Is(err, ir.ErrUnforwardedSchemaChange) {
		t.Fatalf("first run: %v; want the refusal", err)
	}
	state, err := readStreamState(context.Background(), store, DefaultStreamStateFilename)
	if err != nil || state == nil || !strings.Contains(state.UnforwardedRefusal, `ADD CONSTRAINT "u" UNIQUE (name)`) {
		t.Fatalf("refusal not recorded in the stream state: %+v (err %v)", state, err)
	}

	restarted := src(false)
	err = newStream(restarted, "").Run(context.Background())
	if !errors.Is(err, ir.ErrUnforwardedSchemaChange) || !strings.Contains(err.Error(), unforwardedRefusalAckFlag) ||
		!strings.Contains(err.Error(), "take a new full backup") {
		t.Fatalf("restart without the acknowledgement: %v; want the recorded refusal replayed", err)
	}
	if restarted.opens != 0 {
		t.Errorf("the refusing restart opened %d CDC reader(s); the door must run before the pump", restarted.opens)
	}
	if state, _ := readStreamState(context.Background(), store, DefaultStreamStateFilename); state == nil || state.UnforwardedRefusal == "" {
		t.Fatal("a refusing restart dropped the record")
	}

	wrongAck := src(false)
	if err := newStream(wrongAck, "000000000000").Run(context.Background()); !errors.Is(err, ir.ErrUnforwardedSchemaChange) || !strings.Contains(err.Error(), "names a different refusal") {
		t.Fatalf("restart with a mismatched fingerprint: %v; want the replay naming the mismatch", err)
	}
	if wrongAck.opens != 0 {
		t.Errorf("a mismatched acknowledgement opened %d CDC reader(s)", wrongAck.opens)
	}

	acked := src(false)
	if err := newStream(acked, unforwardedRefusalFingerprint(state.UnforwardedRefusal)).Run(context.Background()); err != nil {
		t.Fatalf("acknowledged restart: %v", err)
	}
	if acked.opens == 0 {
		t.Error("acknowledged restart never opened the pump")
	}
	if state, _ := readStreamState(context.Background(), store, DefaultStreamStateFilename); state == nil || state.UnforwardedRefusal != "" {
		t.Errorf("acknowledged restart left the record: %+v", state)
	}
}

// TestStreamStateHeartbeat_PreservesTheRecordedRefusal: the heartbeat's
// read-modify-write must never drop a recorded refusal — its in-memory
// state predates the record.
func TestStreamStateHeartbeat_PreservesTheRecordedRefusal(t *testing.T) {
	dir := t.TempDir()
	store, _ := blobcodec.NewLocalStore(dir)
	ctx := context.Background()
	const path = "manifests/stream_state.json"
	if err := writeStreamState(ctx, store, path, &streamState{PID: 1, Host: "h", UnforwardedRefusal: "recorded"}); err != nil {
		t.Fatal(err)
	}
	if _, err := writeStreamStateMergeHeartbeat(ctx, store, path, &streamState{PID: 1, Host: "h", LastRolloverAt: time.Now()}); err != nil {
		t.Fatal(err)
	}
	got, err := readStreamState(ctx, store, path)
	if err != nil || got == nil || got.UnforwardedRefusal != "recorded" {
		t.Errorf("heartbeat dropped the recorded refusal: %+v (err %v)", got, err)
	}
}

// TestStreamerUnforwardedDoor_MismatchedFingerprintRefuses pins the
// 2026-09-23 second-pass review's finding 4: the acknowledgement is bound to
// the refusal it names. A fingerprint of a different refusal — the shape of
// a flag left in a systemd ExecStart that meets the NEXT refusal on the next
// automatic restart — refuses, keeps the record, and says why.
func TestStreamerUnforwardedDoor_MismatchedFingerprintRefuses(t *testing.T) {
	store := &fakeRefusalStore{msg: "UNFORWARDED-SCHEMA-CHANGE: the second refusal", has: true}
	stale := unforwardedRefusalFingerprint("UNFORWARDED-SCHEMA-CHANGE: the first refusal")
	err := (&Streamer{AcceptUnforwardedSchemaChange: stale}).phaseRefuseRecordedUnforwardedChange(context.Background(), store, "s1")
	if !errors.Is(err, ir.ErrUnforwardedSchemaChange) {
		t.Fatalf("a stale acknowledgement was applied to a different refusal: %v", err)
	}
	for _, want := range []string{"names a different refusal", stale, unforwardedRefusalFingerprint(store.msg)} {
		if !strings.Contains(err.Error(), want) {
			t.Errorf("refusal missing %q: %v", want, err)
		}
	}
	if store.cleared != 0 || !store.has {
		t.Error("a mismatched acknowledgement cleared the record")
	}
}

// TestStreamerUnforwardedDoor_StorageUnavailableFailsTheStart pins the
// docs-sweep finding: a target whose control table lacks the column and
// cannot gain it (a `--schema-already-applied` table owned by another role,
// a PlanetScale safe-migrations branch) must fail the START loudly. Before,
// the start proceeded, the first refusal could not be recorded, and the
// restart after it accepted the change silently.
func TestStreamerUnforwardedDoor_StorageUnavailableFailsTheStart(t *testing.T) {
	store := &fakeRefusalStore{ensureErr: errors.New("ALTER TABLE sluice_cdc_state ADD COLUMN unforwarded_refusal: must be owner of table")}
	err := (&Streamer{}).phaseRefuseRecordedUnforwardedChange(context.Background(), store, "s1")
	if err == nil || !strings.Contains(err.Error(), "must be owner of table") || !strings.Contains(err.Error(), "cannot record") {
		t.Fatalf("a target that cannot record a refusal started anyway: %v", err)
	}
	if errors.Is(err, ir.ErrUnforwardedSchemaChange) {
		t.Error("the storage failure wraps the refusal sentinel; the fleet would treat a configuration fault as a recorded refusal")
	}
	ok := &fakeRefusalStore{}
	if err := (&Streamer{}).phaseRefuseRecordedUnforwardedChange(context.Background(), ok, "s1"); err != nil || ok.ensured != 1 {
		t.Errorf("healthy storage: err=%v ensured=%d; want nil and one ensure", err, ok.ensured)
	}
}

// TestRecordedUnforwardedRefusalText_IdenticalRefusalsGetDistinctFingerprints
// pins the third-pass review's finding 1: the same refusal recorded twice —
// the same change recurring later — must not share a fingerprint, or an
// acknowledgement left in a service definition from the first time clears
// the second.
func TestRecordedUnforwardedRefusalText_IdenticalRefusalsGetDistinctFingerprints(t *testing.T) {
	refusal := fmt.Errorf("%w on public.t: row level security DISABLED", ir.ErrUnforwardedSchemaChange)
	first := recordedUnforwardedRefusalText(refusal)
	time.Sleep(time.Millisecond)
	second := recordedUnforwardedRefusalText(refusal)
	if unforwardedRefusalFingerprint(first) == unforwardedRefusalFingerprint(second) {
		t.Fatalf("two recordings of the same refusal share fingerprint %s; a stale acknowledgement would clear the recurrence", unforwardedRefusalFingerprint(first))
	}
	if !strings.Contains(first, "UNFORWARDED-SCHEMA-CHANGE") || !strings.Contains(first, "row level security DISABLED") {
		t.Errorf("the recorded text lost the refusal: %q", first)
	}
}
