// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package pipeline

// Bug 300 at every place a change stream meets [migcore.EmptyTxRuns]: the two
// capture lanes (`backup incremental`, `backup stream`), the two replay
// producers (the broker, chain restore) and the live stream's stage. The rule
// itself is pinned in migcore; these pin that each consumer applies it, keeps
// its tail, and — on the replay side — keeps the ADR-0191 ordinals of the raw
// stream. The real-server half is empty_tx_runs_integration_test.go.

import (
	"context"
	"fmt"
	"reflect"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"sluicesync.dev/sluice/internal/ir"
	irbackup "sluicesync.dev/sluice/internal/ir/backup"
	"sluicesync.dev/sluice/internal/pipeline/backup"
	"sluicesync.dev/sluice/internal/pipeline/blobcodec"
	"sluicesync.dev/sluice/internal/pipeline/lineage"
)

// emptyTxScript builds a change stream from a script — B TxBegin, C
// TxCommit, r a row — where event i carries position lsn 0/(0x200+i), so a
// decoded or restored stream names exactly which input events it holds.
func emptyTxScript(script string) []ir.Change {
	var out []ir.Change
	for i, ch := range strings.ReplaceAll(script, " ", "") {
		p := ir.Position{Engine: "postgres", Token: fmt.Sprintf(`{"slot":"sluice_slot","lsn":"0/%X"}`, 0x200+i)}
		switch ch {
		case 'B':
			out = append(out, ir.TxBegin{Position: p})
		case 'C':
			out = append(out, ir.TxCommit{Position: p})
		case 'r':
			out = append(out, ir.Insert{Position: p, Table: "users", Row: ir.Row{"id": int64(i)}})
		default:
			panic("emptyTxScript: unknown event " + string(ch))
		}
	}
	return out
}

// scriptIndex recovers the script index from an [emptyTxScript] position.
func scriptIndex(t *testing.T, c ir.Change) int {
	t.Helper()
	var lsn int
	if _, err := fmt.Sscanf(c.Pos().Token, `{"slot":"sluice_slot","lsn":"0/%X"}`, &lsn); err != nil {
		t.Fatalf("%T carries position %q, not an emptyTxScript one: %v", c, c.Pos().Token, err)
	}
	return lsn - 0x200
}

func scriptIndices(t *testing.T, cs []ir.Change) string {
	t.Helper()
	parts := make([]string, len(cs))
	for i, c := range cs {
		parts[i] = fmt.Sprint(scriptIndex(t, c))
	}
	return strings.Join(parts, " ")
}

// emptyTxWindow is the capture fixture: a run of five empty transactions, a
// real one, and a run of three. The window must record the last of each run
// and the real transaction — events 8 9 | 10 11 12 | 17 18.
const (
	emptyTxWindow     = "BCBCBCBCBC BrC BCBCBC"
	emptyTxWindowKept = "8 9 10 11 12 17 18"
)

// decodedWindow returns the incremental's recorded change stream.
func decodedWindow(t *testing.T, store *blobcodec.LocalStore, m *irbackup.Manifest) []ir.Change {
	t.Helper()
	f := &severedFixture{t: t, store: store}
	return f.decoded(m)
}

// TestCaptureLanes_RecordOnlyTheLastEmptyTransactionOfARun is Bug 300's
// writer half, on BOTH capture lanes: an empty source transaction followed by
// another is not recorded, the window still ends — EndPosition and the chunk
// tail alike — on the last empty transaction of a trailing run, and the chunk
// RowCount counts what was recorded. The legacy row runs the incremental lane
// with the test seam that writes the pre-fix shape, which is both the
// anti-vacuity check (the fixture really carries runs) and the "before".
func TestCaptureLanes_RecordOnlyTheLastEmptyTransactionOfARun(t *testing.T) {
	changes := emptyTxScript(emptyTxWindow)
	wantEnd := changes[len(changes)-1].Pos()
	every := make([]string, len(changes))
	for i := range changes {
		every[i] = fmt.Sprint(i)
	}
	for _, lane := range []struct {
		name   string
		legacy bool
		want   string
		run    func(t *testing.T, store *blobcodec.LocalStore, parent *irbackup.Manifest, src *fakeCDCEngine)
	}{
		{"backup incremental", false, emptyTxWindowKept, runIncrementalLane(false)},
		{"backup incremental, pre-fix shape", true, strings.Join(every, " "), runIncrementalLane(true)},
		{"backup stream", false, emptyTxWindowKept, runRolloverLane},
	} {
		t.Run(lane.name, func(t *testing.T) {
			store, err := blobcodec.NewLocalStore(t.TempDir())
			if err != nil {
				t.Fatal(err)
			}
			before, _ := ddlWindowSchemas()
			parent := writeDDLWindowParent(t, store, before)
			src := &fakeCDCEngine{
				name: "postgres", schemaSequence: []*ir.Schema{before},
				cdcChanges: changes, cdcExpectedFromOK: true,
			}
			lane.run(t, store, parent, src)

			incr := findIncremental(t, store)
			got := decodedWindow(t, store, incr)
			if s := scriptIndices(t, got); s != lane.want {
				t.Errorf("the window recorded events [%s]; want [%s]", s, lane.want)
			}
			if n := manifestChangeRecordCount(incr); n != int64(len(got)) {
				t.Errorf("the chunks' RowCount sums to %d; the chunks decode to %d records", n, len(got))
			}
			if incr.EndPosition != wantEnd {
				t.Errorf("EndPosition = %+v; want the trailing run's last commit %+v", incr.EndPosition, wantEnd)
			}
			chain, err := lineage.BuildLineageChain(context.Background(), store, nil)
			if err != nil {
				t.Fatal(err)
			}
			if err := backup.NewSeveredTransactionDoor(store, nil, nil).Check(context.Background(), chain); err != nil {
				t.Errorf("the severed-transaction door refused the window: %v", err)
			}
		})
	}
}

func runIncrementalLane(legacy bool) func(t *testing.T, store *blobcodec.LocalStore, parent *irbackup.Manifest, src *fakeCDCEngine) {
	return func(t *testing.T, store *blobcodec.LocalStore, parent *irbackup.Manifest, src *fakeCDCEngine) {
		t.Helper()
		now := time.Date(2026, 10, 9, 11, 0, 0, 0, time.UTC)
		b := &IncrementalBackup{
			Source: src, SourceDSN: "src", Store: store, ParentRef: parent.BackupID,
			Window: 5 * time.Minute, ChunkChanges: 4, SluiceVersion: "test",
			Now: func() time.Time { return now }, clockNow: func() time.Time { return now },
			recordEveryEmptyTx: legacy,
		}
		if err := b.Run(context.Background()); err != nil {
			t.Fatalf("IncrementalBackup.Run: %v", err)
		}
	}
}

func runRolloverLane(t *testing.T, store *blobcodec.LocalStore, parent *irbackup.Manifest, src *fakeCDCEngine) {
	t.Helper()
	now := time.Date(2026, 10, 9, 11, 0, 0, 0, time.UTC)
	stream := &BackupStream{
		Source: src, SourceDSN: "src", Store: store, ParentRef: parent.BackupID,
		RolloverWindow: 5 * time.Minute, ChunkChanges: 4, SluiceVersion: "test",
		Now: func() time.Time { return now }, clockNow: func() time.Time { return now },
		pidHostFn:       func() (int, string) { return 12345, "test-host" },
		streamStatePath: DefaultStreamStateFilename,
	}
	if err := stream.Run(context.Background()); err != nil {
		t.Fatalf("BackupStream.Run: %v", err)
	}
}

// legacyEmptyTxChunks is an incremental the pre-fix writer could have written:
// runs of empty transactions on both sides of real ones, one run spanning a
// chunk boundary. Ordinals (= script indices):
//
//	chunk 0:  0 B  1 r  2 C  3 B  4 C  5 B  6 C
//	chunk 1:  7 B  8 C  9 B 10 r 11 C 12 B 13 C
//	chunk 2: 14 B 15 C
var legacyEmptyTxChunks = []string{"BrCBCBC", "BCBrCBC", "BC"}

// legacyEmptyTxBoundary is, per ordinal, the frontier boundary its token
// names over the RAW stream (ADR-0191 §3.2), written out by hand: -1 the
// parent, a TxCommit its own ordinal, every other event the boundary before
// its transaction.
var legacyEmptyTxBoundary = []int64{-1, -1, 2, 2, 4, 4, 6, 6, 8, 8, 8, 11, 11, 13, 13, 15}

// legacyEmptyTxLink writes legacyEmptyTxChunks as an incremental's chunks.
func legacyEmptyTxLink(t *testing.T, store irbackup.Store) lineage.SegmentRecord {
	t.Helper()
	all := emptyTxScript(strings.Join(legacyEmptyTxChunks, ""))
	chunks := make([][]ir.Change, 0, len(legacyEmptyTxChunks))
	at := 0
	for _, c := range legacyEmptyTxChunks {
		chunks = append(chunks, all[at:at+len(c)])
		at += len(c)
	}
	link := frontierLink(t, store, "X", chunks)
	link.Manifest.StartPosition = ir.Position{Engine: "postgres", Token: `{"slot":"sluice_slot","lsn":"0/100"}`}
	return link
}

// TestBrokerReplay_CollapsesLegacyEmptyTxRunsKeepingOrdinals is Bug 300's
// broker half: on a chain written WITH every empty transaction, the producer
// withholds all but the last of each run — including a run that spans a chunk
// boundary and one a resume lands inside — and every event it does emit
// carries exactly the frontier token the raw stream gives its ordinal, so a
// position persisted from it means what it always meant.
func TestBrokerReplay_CollapsesLegacyEmptyTxRunsKeepingOrdinals(t *testing.T) {
	store, err := blobcodec.NewLocalStore(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	link := legacyEmptyTxLink(t, store)
	script := strings.Join(legacyEmptyTxChunks, "")
	// The broker rewrites every position to its frontier token, so an emitted
	// event is named by its kind, its row id and that token: B@6 is a TxBegin
	// whose transaction starts after ordinal 6. Which empty transaction of a
	// run survived is visible in its tokens — the run's last is B@6 C@8; any
	// other would be B@2 C@4 or B@4 C@6.
	render := func(ords ...int) string {
		parts := make([]string, len(ords))
		for i, o := range ords {
			kind := string(script[o])
			if kind == "r" {
				kind = fmt.Sprintf("r%d", o)
			}
			parts[i] = fmt.Sprintf("%s@%d", kind, legacyEmptyTxBoundary[o])
		}
		return strings.Join(parts, " ")
	}
	for _, tc := range []struct {
		skip int64
		want string
	}{
		{-1, render(0, 1, 2, 7, 8, 9, 10, 11, 14, 15)},
		{4, render(7, 8, 9, 10, 11, 14, 15)}, // resumed inside the first run
		{8, render(9, 10, 11, 14, 15)},
		{13, render(14, 15)}, // resumed inside the last run
	} {
		t.Run(fmt.Sprintf("resumed through %d", tc.skip), func(t *testing.T) {
			got, err := emitted(t, store, &link, tc.skip)
			if err != nil {
				t.Fatal(err)
			}
			parts := make([]string, len(got))
			for i, c := range got {
				kind := map[string]string{"ir.TxBegin": "B", "ir.TxCommit": "C"}[fmt.Sprintf("%T", c)]
				if ins, ok := c.(ir.Insert); ok {
					kind = fmt.Sprintf("r%v", ins.Row["id"])
				}
				parts[i] = fmt.Sprintf("%s@%d", kind, boundaryOf(t, c.Pos()))
			}
			if s := strings.Join(parts, " "); s != tc.want {
				t.Fatalf("the broker emitted [%s]; want [%s]", s, tc.want)
			}
		})
	}
}

// TestChainRestore_CollapsesLegacyEmptyTxRuns is the chain-restore half: the
// same legacy incremental, restored, reaches the applier with each run
// collapsed to its last transaction and its tail intact.
func TestChainRestore_CollapsesLegacyEmptyTxRuns(t *testing.T) {
	store, err := blobcodec.NewLocalStore(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	all := emptyTxScript(strings.Join(legacyEmptyTxChunks, ""))
	full := makeManifest(t, irbackup.BackupKindFull, nil, "0/100")
	full.BackupID = irbackup.ComputeBackupID(full)
	if err := lineage.WriteManifestAt(context.Background(), store, lineage.ManifestFileName, full); err != nil {
		t.Fatal(err)
	}
	_ = lineage.UpdateLineageForManifestBestEffort(context.Background(), store, full, lineage.ManifestFileName, blobcodec.CodecGzip)
	incr := makeManifest(t, irbackup.BackupKindIncremental, full, "0/20F")
	at := 0
	for i, c := range legacyEmptyTxChunks {
		incr.ChangeChunks = append(incr.ChangeChunks,
			writeTestChangeChunk(t, store, fmt.Sprintf("chunks/_changes/test/changes-%d.jsonl.gz", i), all[at:at+len(c)]))
		at += len(c)
	}
	if incr.EndPosition != all[len(all)-1].Pos() {
		t.Fatalf("fixture: EndPosition %+v is not the last change's %+v", incr.EndPosition, all[len(all)-1].Pos())
	}
	incr.BackupID = irbackup.ComputeBackupID(incr)
	if err := lineage.WriteManifestAt(context.Background(), store, "manifests/incr-0001.json", incr); err != nil {
		t.Fatal(err)
	}
	_ = lineage.UpdateLineageForManifestBestEffort(context.Background(), store, incr, "manifests/incr-0001.json", blobcodec.CodecGzip)

	tgt := &chainRestoreRecorderEngine{restoreRecorderEngine: newRestoreRecorderEngine("postgres")}
	if err := (&backup.ChainRestore{Target: tgt, TargetDSN: "tgt", Store: store}).Run(context.Background()); err != nil {
		t.Fatalf("ChainRestore.Run: %v", err)
	}
	tgt.mu.Lock()
	got := append([]ir.Change(nil), tgt.applied...)
	tgt.mu.Unlock()
	if s := scriptIndices(t, got); s != "0 1 2 7 8 9 10 11 14 15" {
		t.Fatalf("chain restore applied [%s]; want [0 1 2 7 8 9 10 11 14 15]", s)
	}
}

// fakeClock is a settable clock for the live stage's policy.
type fakeClock struct {
	mu  sync.Mutex
	now time.Time
}

func (c *fakeClock) Now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.now
}

func (c *fakeClock) Advance(d time.Duration) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.now = c.now.Add(d)
}

// stagePolicy is a live policy for tests; never disables a bound.
func stagePolicy(maxRun int, linger, maxHold time.Duration, now func() time.Time) liveEmptyTxPolicy {
	return liveEmptyTxPolicy{maxRun: maxRun, linger: linger, maxHold: maxHold, now: now}
}

const never = time.Hour // a bound a fixture must not reach

// TestLiveEmptyTxStage pins the live stream's stage. Under backlog it
// collapses each run, bounded by MaxRun; a held empty transaction with
// nothing behind it is released once the linger passes — so an idle stream
// persists every boundary as before, late by at most the linger — while a
// withheld begin, which persists nothing, waits for its transaction's next
// event. The hold cap and the cancel are pinned below.
func TestLiveEmptyTxStage(t *testing.T) {
	collect := func(in chan ir.Change, maxRun int) []ir.Change {
		var got []ir.Change
		for c := range coalesceEmptyTxRuns(context.Background(), in, stagePolicy(maxRun, never, never, time.Now)) {
			got = append(got, c)
		}
		return got
	}
	receive := func(t *testing.T, out <-chan ir.Change, want ir.Change, within time.Duration) {
		t.Helper()
		select {
		case c := <-out:
			if !reflect.DeepEqual(c, want) {
				t.Fatalf("got %#v; want %#v", c, want)
			}
		case <-time.After(within):
			t.Fatalf("%#v never arrived", want)
		}
	}

	t.Run("backlog", func(t *testing.T) {
		changes := emptyTxScript(emptyTxWindow)
		in := make(chan ir.Change, len(changes))
		for _, c := range changes {
			in <- c
		}
		close(in)
		if s := scriptIndices(t, collect(in, defaultLiveEmptyTxPolicy().maxRun)); s != emptyTxWindowKept {
			t.Errorf("emitted [%s]; want [%s]", s, emptyTxWindowKept)
		}
	})

	t.Run("backlog bounded by MaxRun", func(t *testing.T) {
		const pairs, maxRun = 25, 10
		changes := emptyTxScript(strings.Repeat("BC", pairs))
		in := make(chan ir.Change, len(changes))
		for _, c := range changes {
			in <- c
		}
		close(in)
		// Pairs 10 and 21 by the bound, pair 24 at the close.
		if s := scriptIndices(t, collect(in, maxRun)); s != "20 21 42 43 48 49" {
			t.Errorf("emitted [%s]; want [20 21 42 43 48 49]", s)
		}
	})

	t.Run("idle", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		in := make(chan ir.Change)
		out := coalesceEmptyTxRuns(ctx, in, stagePolicy(1000, 20*time.Millisecond, never, time.Now))
		events := emptyTxScript("BC B")
		for _, c := range events {
			in <- c // no close and no further event: the source goes idle
		}
		// The held empty transaction is released on the linger, though a
		// begin is withheld behind it.
		receive(t, out, events[0], 10*time.Second)
		receive(t, out, events[1], 10*time.Second)
		// The withheld begin is not: it persists nothing.
		select {
		case c := <-out:
			t.Fatalf("a withheld begin was released with nothing after it: %#v", c)
		case <-time.After(200 * time.Millisecond):
		}
		row := ir.Insert{Position: events[2].Pos(), Table: "users", Row: ir.Row{"id": int64(9)}}
		in <- row
		receive(t, out, events[2], 10*time.Second)
		receive(t, out, row, 10*time.Second)
	})
}

// TestLiveEmptyTxStage_HoldCapBoundsARunTheLingerKeepsResetting is the
// pre-land review's F1. Foreign transactions that keep arriving inside the
// linger reset it every time, so without a cap the position was persisted
// only at maxRun — ~100 s at ~10 tx/s. The cap is measured from the run's
// FIRST held transaction and never reset. Deterministic: the clock is fake
// and the linger is out of reach, so only the cap can release anything.
func TestLiveEmptyTxStage_HoldCapBoundsARunTheLingerKeepsResetting(t *testing.T) {
	clock := &fakeClock{now: time.Date(2026, 10, 10, 0, 0, 0, 0, time.UTC)}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	in := make(chan ir.Change, 16) // buffered: a release blocks the stage on out while the test is still sending
	var pushed atomic.Int64
	policy := stagePolicy(1000, never, time.Second, clock.Now)
	policy.pushed = func() { pushed.Add(1) }
	out := coalesceEmptyTxRuns(ctx, in, policy)
	pairs := emptyTxScript(strings.Repeat("BC", 4))
	sent := int64(0)
	// sendPair sends pair i and waits until the stage has stamped both
	// events, so the clock is advanced only BETWEEN pairs.
	sendPair := func(i int) {
		t.Helper()
		in <- pairs[2*i]
		in <- pairs[2*i+1]
		sent += 2
		deadline := time.Now().Add(10 * time.Second)
		for pushed.Load() < sent {
			if time.Now().After(deadline) {
				t.Fatalf("the stage stamped %d of %d events", pushed.Load(), sent)
			}
			time.Sleep(time.Millisecond)
		}
	}
	quiet := func(why string) {
		t.Helper()
		select {
		case c := <-out:
			t.Fatalf("%s, yet %#v was released", why, c)
		case <-time.After(150 * time.Millisecond):
		}
	}

	sendPair(0) // the run begins
	clock.Advance(600 * time.Millisecond)
	sendPair(1) // inside the cap: pair 0 is dropped, pair 1 held
	quiet("the run is 600 ms old with a 1 s cap")

	clock.Advance(500 * time.Millisecond)
	// 1.1 s after the run began: at pair 2's begin the cap releases the HELD
	// pair 1, and the stage blocks on out until it is read — so pair 2 is
	// sent without waiting, and stamped after the read.
	in <- pairs[4]
	in <- pairs[5]
	sent += 2
	for _, want := range pairs[2:4] {
		select {
		case c := <-out:
			if !reflect.DeepEqual(c, want) {
				t.Fatalf("the cap released %#v; want the run's held transaction %#v", c, want)
			}
		case <-time.After(10 * time.Second):
			t.Fatal("a run held past its cap was never released: a source busy just inside the linger would hold the position back until maxRun")
		}
	}

	// The release ended the run: the next one is timed from its own start.
	sendPair(3)
	clock.Advance(600 * time.Millisecond)
	quiet("a new run began 600 ms ago")
}

// TestLiveEmptyTxStage_CancelDropsWhatIsHeld pins the stage's cancel: the
// output closes (the goroutine has returned — the close is its last act), and
// the held empty transaction is NOT emitted, exactly as a crash at that point
// would not have persisted it.
func TestLiveEmptyTxStage_CancelDropsWhatIsHeld(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	in := make(chan ir.Change)
	out := coalesceEmptyTxRuns(ctx, in, stagePolicy(1000, never, never, time.Now))
	for _, c := range emptyTxScript("BC B") {
		in <- c
	}
	cancel()
	select {
	case c, ok := <-out:
		if ok {
			t.Fatalf("a cancelled stage emitted %#v", c)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("the stage's output never closed after the cancel: its goroutine leaked")
	}
}
