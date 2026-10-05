// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package pipeline

// F-E1-SEVERED-TAIL-REPLAY producer pins: a backup window never ends inside a
// source transaction. Every exit of the two capture lanes is driven here with
// a transaction OPEN at the moment it fires, against a CDC feed the test
// controls change by change:
//
//	exit                         lane          verdict pinned
//	in-process stop signal       stream        drains to the TxCommit, commits there
//	stream_state.json stop file  stream        drains to the TxCommit, commits there
//	stop drain, time budget      stream        abandons: no manifest, no ack
//	stop drain, change budget    stream        abandons: no manifest, no ack
//	source channel close         stream        abandons: no manifest, no ack
//	ctx cancel (SIGTERM)         stream        abandons, though a chunk of the
//	                                           open transaction was already stored
//	ctx cancel, drain-flush fails stream       abandons (the adjacent EndPos defect)
//	source channel close         incremental   refuses loudly, no manifest
//
// The deadline and the count/byte caps already closed only at a boundary;
// TestBackupStream_RolloverOnMaxChanges and the incremental window tests hold
// those. The real-engine matrix is stream_severed_tail_integration_test.go.

import (
	"context"
	"errors"
	"io"
	"strings"
	"sync"
	"testing"
	"time"

	"sluicesync.dev/sluice/internal/ir"
	irbackup "sluicesync.dev/sluice/internal/ir/backup"
	"sluicesync.dev/sluice/internal/pipeline/blobcodec"
	"sluicesync.dev/sluice/internal/pipeline/lineage"
)

// feedCDCEngine is a fakeCDCEngine whose change stream the test feeds one
// change at a time. Both hops are unbuffered, so a send on feed returning
// means the stream has RECEIVED the change before it — the test always knows
// how far the window has read.
type feedCDCEngine struct {
	fakeCDCEngine
	feed chan ir.Change

	mu   sync.Mutex
	acks []ir.Position
}

func (e *feedCDCEngine) OpenCDCReader(context.Context, string) (ir.CDCReader, error) {
	return &feedCDCReader{e: e}, nil
}

type feedCDCReader struct{ e *feedCDCEngine }

func (r *feedCDCReader) StreamChanges(ctx context.Context, _ ir.Position) (<-chan ir.Change, error) {
	out := make(chan ir.Change)
	go func() {
		defer close(out)
		for {
			select {
			case <-ctx.Done():
				return
			case c, ok := <-r.e.feed:
				if !ok {
					return
				}
				select {
				case out <- c:
				case <-ctx.Done():
					return
				}
			}
		}
	}()
	return out, nil
}

func (r *feedCDCReader) Close() error { return nil }

// ReleaseSlotAckTo records every chain-ack release (the slotAckReleaser
// surface): an abandoned window must release NOTHING.
func (r *feedCDCReader) ReleaseSlotAckTo(pos ir.Position) error {
	r.e.mu.Lock()
	defer r.e.mu.Unlock()
	r.e.acks = append(r.e.acks, pos)
	return nil
}

func (e *feedCDCEngine) ackCount() int {
	e.mu.Lock()
	defer e.mu.Unlock()
	return len(e.acks)
}

type severedFixture struct {
	t      *testing.T
	store  *blobcodec.LocalStore
	eng    *feedCDCEngine
	stream *BackupStream
	parent *irbackup.Manifest
}

func newSeveredFixture(t *testing.T) *severedFixture {
	t.Helper()
	store, err := blobcodec.NewLocalStore(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	schema := &ir.Schema{Tables: []*ir.Table{{
		Name: "kl", Columns: []*ir.Column{{Name: "v", Type: ir.Integer{Width: 64}}},
	}}}
	parent := &irbackup.Manifest{
		FormatVersion: irbackup.BackupFormatVersion,
		CreatedAt:     time.Now().UTC().Add(-time.Hour),
		SourceEngine:  "postgres",
		Schema:        schema,
		Kind:          irbackup.BackupKindFull,
		EndPosition:   posTok(1),
		PartialState:  irbackup.BackupStateComplete,
	}
	parent.BackupID = irbackup.ComputeBackupID(parent)
	writeParentFullManifest(t, store, parent)
	eng := &feedCDCEngine{
		fakeCDCEngine: fakeCDCEngine{name: "postgres", schemaSequence: []*ir.Schema{schema}},
		feed:          make(chan ir.Change),
	}
	return &severedFixture{
		t: t, store: store, eng: eng, parent: parent,
		stream: &BackupStream{
			Source:             eng,
			SourceDSN:          "src",
			Store:              store,
			ParentRef:          parent.BackupID,
			RolloverWindow:     time.Hour, // only the exit under test closes the window
			RolloverMaxChanges: 1 << 30,
			RolloverMaxBytes:   1 << 40,
			ChunkChanges:       2, // chunks are STORED inside the open transaction
			SluiceVersion:      "test",
			pidHostFn:          func() (int, string) { return 1, "h" },
		},
	}
}

func (f *severedFixture) run(ctx context.Context) <-chan error {
	done := make(chan error, 1)
	go func() { done <- f.stream.Run(ctx) }()
	return done
}

func (f *severedFixture) send(cs ...ir.Change) {
	f.t.Helper()
	for _, c := range cs {
		select {
		case f.eng.feed <- c:
		case <-time.After(10 * time.Second):
			f.t.Fatalf("the stream stopped reading at %T %+v", c, c.Pos())
		}
	}
}

// offer sends what the stream will still read, without failing when it has
// already exited: the drain tests grade the COMMITTED WINDOW, so a stream
// that stopped reading early must reach that assertion, not this helper.
func (f *severedFixture) offer(cs ...ir.Change) {
	for _, c := range cs {
		select {
		case f.eng.feed <- c:
		case <-time.After(2 * time.Second):
			return
		}
	}
}

// tx is a complete transaction at positions base..base+n+1.
func tx(base, n int) []ir.Change {
	out := []ir.Change{ir.TxBegin{Position: posTok(base)}}
	for i := 1; i <= n; i++ {
		out = append(out, ir.Insert{Position: posTok(base + i), Table: "kl", Row: ir.Row{"v": int64(base + i)}})
	}
	return append(out, ir.TxCommit{Position: posTok(base + n + 1)})
}

func (f *severedFixture) wait(done <-chan error) {
	f.t.Helper()
	select {
	case err := <-done:
		if err != nil {
			f.t.Fatalf("stream.Run: %v", err)
		}
	case <-time.After(30 * time.Second):
		f.t.Fatal("stream.Run did not return")
	}
}

func (f *severedFixture) incrementals() []*irbackup.Manifest {
	f.t.Helper()
	records, err := lineage.ListAllManifestsViaWalk(context.Background(), f.store)
	if err != nil {
		f.t.Fatal(err)
	}
	var out []*irbackup.Manifest
	for _, r := range records {
		if r.Manifest.Kind == irbackup.BackupKindIncremental {
			out = append(out, r.Manifest)
		}
	}
	return out
}

// decoded returns the committed incremental's change stream.
func (f *severedFixture) decoded(m *irbackup.Manifest) []ir.Change {
	f.t.Helper()
	var out []ir.Change
	for _, c := range m.ChangeChunks {
		src, err := f.store.Get(context.Background(), c.File)
		if err != nil {
			f.t.Fatal(err)
		}
		rd, err := blobcodec.NewChangeChunkReader(src, c.SHA256, nil, blobcodec.DefaultCodec, nil)
		if err != nil {
			f.t.Fatal(err)
		}
		for {
			ch, err := rd.ReadChange()
			if errors.Is(err, io.EOF) {
				break
			}
			if err != nil {
				f.t.Fatal(err)
			}
			out = append(out, ch)
		}
		_ = rd.Close()
	}
	return out
}

// requireClosedAtCommit is the committed-window half of the rule: exactly one
// incremental, its last recorded change a TxCommit, its EndPosition that
// commit's position, and every change sent up to it present.
func (f *severedFixture) requireClosedAtCommit(wantChanges int, commitPos ir.Position) {
	f.t.Helper()
	incs := f.incrementals()
	if len(incs) != 1 {
		f.t.Fatalf("committed incrementals = %d; want 1", len(incs))
	}
	got := f.decoded(incs[0])
	if len(got) != wantChanges {
		f.t.Fatalf("committed window carries %d changes; want %d (the whole open transaction)", len(got), wantChanges)
	}
	if _, ok := got[len(got)-1].(ir.TxCommit); !ok {
		f.t.Fatalf("committed window ends on %T, not a TxCommit — the chain ends inside a source transaction", got[len(got)-1])
	}
	if incs[0].EndPosition != commitPos {
		f.t.Fatalf("EndPosition = %+v; want the TxCommit's %+v", incs[0].EndPosition, commitPos)
	}
}

// requireAbandoned is the other half: no manifest, no acknowledgement.
func (f *severedFixture) requireAbandoned() {
	f.t.Helper()
	if incs := f.incrementals(); len(incs) != 0 {
		f.t.Fatalf("a window that ends inside a source transaction was COMMITTED (%d incrementals, EndPosition %+v)",
			len(incs), incs[0].EndPosition)
	}
	if n := f.eng.ackCount(); n != 0 {
		f.t.Fatalf("an abandoned window released the chain ack %d time(s); the source may discard what the next run must re-read", n)
	}
}

func (f *severedFixture) stopFile() {
	f.t.Helper()
	ctx := context.Background()
	st, err := readStreamState(ctx, f.store, DefaultStreamStateFilename)
	if err != nil || st == nil {
		f.t.Fatalf("read stream state: %v (state %v)", err, st)
	}
	now := time.Now().UTC()
	st.StopRequestedAt = &now
	if err := writeStreamState(ctx, f.store, DefaultStreamStateFilename, st); err != nil {
		f.t.Fatal(err)
	}
}

// openTx sends a complete transaction and then the head of a second one:
// TxBegin(10) + rows 11..13. With ChunkChanges=2 a chunk of the open
// transaction is already stored when this returns.
func (f *severedFixture) openTx() {
	f.send(tx(2, 2)...)                // 4 changes, positions 2..5
	f.send(tx(10, 4)[:4]...)           // TxBegin(10), 11, 12, 13
	time.Sleep(100 * time.Millisecond) // let the window record row 13
}

func TestBackupStream_StopInsideTransaction_InProcess_DrainsToCommit(t *testing.T) {
	f := newSeveredFixture(t)
	done := f.run(context.Background())
	f.openTx()
	if !notifyStreamStop(f.store) {
		t.Fatal("no stream registered for the in-process stop")
	}
	time.Sleep(200 * time.Millisecond) // the stop is observed with the transaction open
	f.offer(tx(10, 4)[4:]...)          // row 14, TxCommit(15)
	f.wait(done)
	f.requireClosedAtCommit(4+6, posTok(15))
}

func TestBackupStream_StopInsideTransaction_StopFile_DrainsToCommit(t *testing.T) {
	f := newSeveredFixture(t)
	done := f.run(context.Background())
	f.openTx()
	f.stopFile()
	time.Sleep(streamStopPollInterval + 500*time.Millisecond) // the poll observes it, transaction open
	f.offer(tx(10, 4)[4:]...)
	f.wait(done)
	f.requireClosedAtCommit(4+6, posTok(15))
}

func TestBackupStream_StopInsideTransaction_TimeBudgetAbandons(t *testing.T) {
	f := newSeveredFixture(t)
	f.stream.StopTransactionDrainTimeout = 300 * time.Millisecond
	done := f.run(context.Background())
	f.openTx()
	if !notifyStreamStop(f.store) {
		t.Fatal("no stream registered for the in-process stop")
	}
	f.wait(done) // the commit never arrives
	f.requireAbandoned()
}

func TestBackupStream_StopInsideTransaction_ChangeBudgetAbandons(t *testing.T) {
	f := newSeveredFixture(t)
	f.stream.StopTransactionDrainMaxChanges = 2
	done := f.run(context.Background())
	f.openTx()
	if !notifyStreamStop(f.store) {
		t.Fatal("no stream registered for the in-process stop")
	}
	time.Sleep(200 * time.Millisecond)
	// Two more rows of the still-open transaction exhaust the budget.
	f.send(ir.Insert{Position: posTok(14), Table: "kl", Row: ir.Row{"v": int64(14)}},
		ir.Insert{Position: posTok(15), Table: "kl", Row: ir.Row{"v": int64(15)}})
	f.wait(done)
	f.requireAbandoned()
}

func TestBackupStream_SourceCloseInsideTransaction_Abandons(t *testing.T) {
	f := newSeveredFixture(t)
	done := f.run(context.Background())
	f.openTx()
	close(f.eng.feed)
	f.wait(done)
	f.requireAbandoned()
}

func TestBackupStream_CancelInsideTransaction_Abandons(t *testing.T) {
	f := newSeveredFixture(t)
	ctx, cancel := context.WithCancel(context.Background())
	done := f.run(ctx)
	f.openTx()
	cancel()
	f.wait(done)
	f.requireAbandoned()
}

// TestBackupStream_CancelAtBoundary_Commits is the non-regression direction:
// the SIGTERM drain still commits a window that stands at a boundary, with
// its EndPosition at the last change it stored.
func TestBackupStream_CancelAtBoundary_Commits(t *testing.T) {
	f := newSeveredFixture(t)
	ctx, cancel := context.WithCancel(context.Background())
	done := f.run(ctx)
	f.send(tx(2, 2)...)
	f.send(tx(10, 3)...) // positions 10..14, ends on TxCommit(14)
	time.Sleep(100 * time.Millisecond)
	cancel()
	f.wait(done)
	f.requireClosedAtCommit(4+5, posTok(14))
}

// failingChunkStore fails every change-chunk Put once armed, so the
// ctx-cancel drain's last flush fails.
type failingChunkStore struct {
	*blobcodec.LocalStore
	mu    sync.Mutex
	armed bool
}

func (s *failingChunkStore) Put(ctx context.Context, path string, r io.Reader) error {
	s.mu.Lock()
	armed := s.armed
	s.mu.Unlock()
	if armed && strings.Contains(path, "_changes") {
		return errors.New("injected chunk put failure")
	}
	return s.LocalStore.Put(ctx, path, r)
}

// TestBackupStream_CancelDrainFlushFails_Abandons is the adjacent defect the
// severed-tail trace found: the ctx-cancel drain WARNed "in-flight chunk
// dropped" when its last flush failed and committed the earlier chunks with
// an EndPosition processChange had already moved past them — a manifest
// claiming changes no stored chunk carries. Now it abandons.
func TestBackupStream_CancelDrainFlushFails_Abandons(t *testing.T) {
	f := newSeveredFixture(t)
	fs := &failingChunkStore{LocalStore: f.store}
	f.stream.Store = fs
	f.stream.ChunkChanges = 100 // nothing stored before the cancel
	ctx, cancel := context.WithCancel(context.Background())
	done := f.run(ctx)
	f.send(tx(2, 2)...)
	time.Sleep(100 * time.Millisecond)
	fs.mu.Lock()
	fs.armed = true
	fs.mu.Unlock()
	cancel()
	f.wait(done)
	f.requireAbandoned()
}

// TestBackupStream_CancelDrainFlushFailsAfterStoredChunks_Abandons is the
// sharper half: the window stands at a boundary (the TxCommit has been read),
// chunks holding the transaction's TxBegin and rows are ALREADY stored, and
// only the last chunk — the one carrying the TxCommit — fails to store.
// Committing the stored chunks would end the chain inside that transaction
// with nothing open in the in-memory state to say so; the failed flush is
// the only witness, and it must abandon.
func TestBackupStream_CancelDrainFlushFailsAfterStoredChunks_Abandons(t *testing.T) {
	f := newSeveredFixture(t)
	fs := &failingChunkStore{LocalStore: f.store}
	f.stream.Store = fs
	ctx, cancel := context.WithCancel(context.Background())
	done := f.run(ctx)
	f.send(tx(2, 2)...)  // chunks [B2 I3] [I4 C5] stored
	f.send(tx(10, 1)...) // chunk [B10 I11] stored; C12 buffered
	time.Sleep(100 * time.Millisecond)
	fs.mu.Lock()
	fs.armed = true
	fs.mu.Unlock()
	cancel()
	f.wait(done)
	f.requireAbandoned()
}

// TestChangeChunkBuffer_EndPosMovesOnlyWhenStored pins the adjacent defect's
// own fix at the unit: a change written into the OPEN chunk does not move the
// window's EndPos; storing the chunk does. Before, processChange moved EndPos
// per change, so any exit that committed without storing the open chunk
// recorded an EndPosition no stored chunk carries.
func TestChangeChunkBuffer_EndPosMovesOnlyWhenStored(t *testing.T) {
	store, err := blobcodec.NewLocalStore(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	b := &BackupStream{Source: &fakeCDCEngine{name: "postgres"}, segStore: store, segCodec: blobcodec.DefaultCodec}
	m := &irbackup.Manifest{StartPosition: posTok(1), CreatedAt: time.Now().UTC()}
	cb := &changeChunkBuffer{b: b, sealer: b, manifest: m, runNamespace: changeChunkRunNamespace(m)}
	var (
		out  captureOutcome
		inTx bool
	)
	for _, c := range tx(2, 2) {
		if _, err := cb.processChange(context.Background(), c, &out, &inTx, false, 100, 0, 0); err != nil {
			t.Fatal(err)
		}
	}
	if out.EndPos != (ir.Position{}) {
		t.Fatalf("EndPos moved to %+v before any chunk was stored", out.EndPos)
	}
	if err := cb.flushTo(context.Background(), &out); err != nil {
		t.Fatal(err)
	}
	if out.EndPos != posTok(5) {
		t.Fatalf("EndPos after the chunk was stored = %+v; want the last stored change's %+v", out.EndPos, posTok(5))
	}
}

// TestIncrementalBackup_SourceCloseInsideTransaction_Refuses is the one-shot
// lane's channel-close exit: a stream that ends between a TxBegin and its
// TxCommit refuses loudly with the marker and writes no manifest.
func TestIncrementalBackup_SourceCloseInsideTransaction_Refuses(t *testing.T) {
	store, err := blobcodec.NewLocalStore(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	schema := &ir.Schema{Tables: []*ir.Table{{Name: "kl", Columns: []*ir.Column{{Name: "v", Type: ir.Integer{Width: 64}}}}}}
	parent := &irbackup.Manifest{
		FormatVersion: irbackup.BackupFormatVersion,
		CreatedAt:     time.Now().UTC().Add(-time.Hour),
		SourceEngine:  "postgres",
		Schema:        schema,
		Kind:          irbackup.BackupKindFull,
		EndPosition:   posTok(1),
		PartialState:  irbackup.BackupStateComplete,
	}
	parent.BackupID = irbackup.ComputeBackupID(parent)
	writeParentFullManifest(t, store, parent)
	changes := append(tx(2, 2), tx(10, 4)[:4]...) // a complete transaction, then an open one
	src := &fakeCDCEngine{name: "postgres", schemaSequence: []*ir.Schema{schema}, cdcChanges: changes}
	inc := &IncrementalBackup{
		Source: src, SourceDSN: "src", Store: store, ParentRef: parent.BackupID,
		Window: time.Hour, ChunkChanges: 2, SluiceVersion: "test",
	}
	err = inc.Run(context.Background())
	if err == nil || !strings.Contains(err.Error(), abandonedWindowMarker) {
		t.Fatalf("Run = %v; want the %s refusal", err, abandonedWindowMarker)
	}
	records, _ := lineage.ListAllManifestsViaWalk(context.Background(), store)
	for _, r := range records {
		if r.Manifest.Kind == irbackup.BackupKindIncremental {
			t.Fatalf("an incremental ending inside a source transaction was committed: EndPosition %+v", r.Manifest.EndPosition)
		}
	}
}
