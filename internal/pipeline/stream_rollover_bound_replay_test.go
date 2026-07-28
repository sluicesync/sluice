// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

// Roadmap item 98 — item 92's defect in the streaming orchestrator. A
// resumed CDC pump opens by RE-DELIVERING the transaction the parent
// segment ends on, and both rollover caps count what that replay
// carries: `--rollover-max-changes` counts change EVENTS (transaction
// framing included) and `--rollover-max-bytes` counts the bytes those
// events buffer. A tight cap was therefore satisfiable by the replay
// alone — the rollover closed at its own start position having captured
// nothing new, and still committed a chain-linked, correctly-stamped
// manifest reporting a non-zero change count.
//
// The mechanism is item 92's, ground-truthed there on real PG 16 and not
// re-derived here: PostgreSQL's logical decoding begins at
// max(requested_lsn, confirmed_flush_lsn) and skips only transactions
// whose commit record is STRICTLY BEFORE that point, so restarting at a
// parent EndPosition — exactly a commit LSN — replays that whole
// transaction, and every pgoutput event of a transaction carries that
// transaction's commit LSN.
//
// These pins lock the fix at the orchestrator level, for BOTH caps:
// neither may close a rollover that has not advanced past its start
// position, and both must still bite promptly once it HAS advanced.
// The real-wire half is stream_rollover_bound_replay_pg_integration_test.go.

package pipeline

import (
	"context"
	"testing"
	"time"

	"sluicesync.dev/sluice/internal/ir"
	irbackup "sluicesync.dev/sluice/internal/ir/backup"
	"sluicesync.dev/sluice/internal/pipeline/blobcodec"
	"sluicesync.dev/sluice/internal/pipeline/lineage"
)

// item98 positions. The tokens are zero-padded so orderedFakeCDCEngine's
// string compare is the native order (see incremental_maxchanges_replay_test.go).
const (
	item98ParentEnd = "0000000100" // the parent segment's EndPosition
	item98NewTx1    = "0000000200" // the first genuinely new transaction
	item98NewTx2    = "0000000300" // the second
)

// item98Stream is one scripted stream run's configuration. Exactly one
// of maxChanges / maxBytes is the cap under test; the other is set far
// out of reach so the pin names which bound closed the rollover.
//
// chunkChanges matters for the byte cap: the ceiling is compared against
// out.TotalBytes plus the in-flight buffer's length, and the codec
// (gzip) writes nothing into that buffer until the chunk closes — so the
// byte cap can only re-evaluate at a chunk-flush boundary. One chunk per
// transaction makes it fire on the transaction that crosses it.
type item98Stream struct {
	maxChanges   int
	maxBytes     int64
	chunkChanges int
}

// runItem98Stream drives one BackupStream against a fake source that
// plays `changes` and then closes the channel (the unit-test stand-in
// for "source-side end of stream"), and returns the committed rollover
// manifests in chain order.
func runItem98Stream(t *testing.T, src ir.Engine, parentEnd ir.Position, cfg item98Stream) []*irbackup.Manifest {
	t.Helper()
	store, err := blobcodec.NewLocalStore(t.TempDir())
	if err != nil {
		t.Fatalf("NewLocalStore: %v", err)
	}
	parent := &irbackup.Manifest{
		FormatVersion: irbackup.BackupFormatVersion,
		CreatedAt:     time.Date(2026, 7, 28, 10, 0, 0, 0, time.UTC),
		SourceEngine:  "postgres",
		Schema:        item92Schema(),
		Kind:          irbackup.BackupKindFull,
		EndPosition:   parentEnd,
		PartialState:  irbackup.BackupStateComplete,
	}
	parent.BackupID = irbackup.ComputeBackupID(parent)
	writeParentFullManifest(t, store, parent)

	now := time.Date(2026, 7, 28, 11, 0, 0, 0, time.UTC)
	stream := &BackupStream{
		Source:             src,
		SourceDSN:          "src",
		Store:              store,
		ParentRef:          parent.BackupID,
		RolloverWindow:     5 * time.Minute, // pinned clock: never fires
		RolloverMaxChanges: cfg.maxChanges,
		RolloverMaxBytes:   cfg.maxBytes,
		ChunkChanges:       cfg.chunkChanges,
		SluiceVersion:      "test",
		Now:                func() time.Time { return now },
		clockNow:           func() time.Time { return now },
		pidHostFn:          func() (int, string) { return 12345, "test-host" },
		streamStatePath:    DefaultStreamStateFilename,
	}
	if err := stream.Run(context.Background()); err != nil {
		t.Fatalf("stream.Run: %v", err)
	}

	records, err := lineage.ListAllManifestsViaWalk(context.Background(), store)
	if err != nil {
		t.Fatalf("ListAllManifestsViaWalk: %v", err)
	}
	var incrementals []*irbackup.Manifest
	for _, r := range records {
		if r.Manifest.Kind == irbackup.BackupKindIncremental {
			incrementals = append(incrementals, r.Manifest)
		}
	}
	if len(incrementals) == 0 {
		t.Fatal("no rollover manifest committed")
	}
	sortIncrementalsByChain(t, incrementals, parent.BackupID)
	return incrementals
}

// item98Changes returns the captured change-event count of one rollover.
func item98Changes(m *irbackup.Manifest) int64 {
	var n int64
	for _, c := range m.ChangeChunks {
		n += c.RowCount
	}
	return n
}

// item98ChainChanges sums the change events across every rollover.
func item98ChainChanges(ms []*irbackup.Manifest) int64 {
	var n int64
	for _, m := range ms {
		n += item98Changes(m)
	}
	return n
}

// item98ReplayScript is the stream a resumed pump delivers: the parent's
// boundary transaction re-delivered in full, then two genuinely new
// transactions.
func item98ReplayScript() []ir.Change {
	out := append(item92Tx(item98ParentEnd, "already-in-the-parent@example.com"), item92Tx(item98NewTx1, "carol@example.com")...)
	return append(out, item92Tx(item98NewTx2, "dave@example.com")...)
}

// item98Sources is the engine-shape matrix every cap pin runs across.
// Both real CDC engines implement the comparator; the fallback arm
// covers an engine that does not.
func item98Sources() []struct {
	name string
	src  func(*fakeCDCEngine) ir.Engine
} {
	return []struct {
		name string
		src  func(*fakeCDCEngine) ir.Engine
	}{
		{
			name: "engine implements PositionMonotonicChecker",
			src:  func(f *fakeCDCEngine) ir.Engine { return orderedFakeCDCEngine{fakeCDCEngine: f} },
		},
		{
			name: "engine without a comparator falls back to position inequality",
			src:  func(f *fakeCDCEngine) ir.Engine { return f },
		},
	}
}

func item98FakeSource(changes []ir.Change) *fakeCDCEngine {
	return &fakeCDCEngine{
		name:              "postgres",
		schemaSequence:    []*ir.Schema{item92Schema()},
		cdcChanges:        changes,
		cdcExpectedFromOK: true,
	}
}

// TestBackupStream_RolloverCapsWillNotCloseOnTheReplayedBoundaryTx is the
// item-98 pin, run for BOTH caps because both count what the replay
// carries and both dispatch on the same window-closing decision. The cap
// is satisfied by the replayed boundary transaction alone; the rollover
// must nonetheless run on until it has a transaction of its own, and
// must then close at THAT boundary rather than draining the stream.
func TestBackupStream_RolloverCapsWillNotCloseOnTheReplayedBoundaryTx(t *testing.T) {
	caps := []struct {
		name string
		cfg  item98Stream
	}{
		// The replayed transaction is three events, so a cap of 2 is
		// satisfied by the replay by itself.
		{"--rollover-max-changes", item98Stream{maxChanges: 2, maxBytes: 1 << 40, chunkChanges: 1000}},
		// One chunk per transaction, and a ceiling of 1 byte: the
		// replay's own flushed chunk is enough to satisfy it.
		{"--rollover-max-bytes", item98Stream{maxChanges: 1_000_000, maxBytes: 1, chunkChanges: 3}},
	}
	for _, c := range caps {
		for _, s := range item98Sources() {
			t.Run(c.name+"/"+s.name, func(t *testing.T) {
				incrs := runItem98Stream(t, s.src(item98FakeSource(item98ReplayScript())), item92Pos(item98ParentEnd), c.cfg)
				first := incrs[0]

				if first.StartPosition != item92Pos(item98ParentEnd) {
					t.Fatalf("rollover 1 StartPosition = %+v; want %+v", first.StartPosition, item92Pos(item98ParentEnd))
				}
				// The defect: the rollover closed at its own start position.
				if first.EndPosition == first.StartPosition {
					t.Errorf("rollover 1 EndPosition == StartPosition (%+v) — the cap closed the rollover on the replayed boundary transaction, so the segment carries nothing the parent does not (roadmap item 98)", first.EndPosition)
				}
				if want := item92Pos(item98NewTx1); first.EndPosition != want {
					t.Errorf("rollover 1 EndPosition = %+v; want %+v (close at the FIRST new transaction's commit)", first.EndPosition, want)
				}
				// Non-vacuity: the cap must still bite. Draining the whole
				// stream would have reached newTx2 and counted 9 events.
				if got := item98Changes(first); got != 6 {
					t.Errorf("rollover 1 captured %d change events; want 6 (the 3-event replay plus the 3-event new transaction) — %d means the cap stopped biting", got, got)
				}
				// The replay is KEPT, not filtered: when the parent's own
				// window ended mid-transaction (this orchestrator's eager
				// stop exit and its ctx-cancel drain can both do that) the
				// replay is the only thing carrying the severed tail.
				if got := item98ChainChanges(incrs); got != 9 {
					t.Errorf("chain captured %d change events across %d rollovers; want the full 9 — the replayed events must be kept, not dropped", got, len(incrs))
				}
			})
		}
	}
}

// TestBackupStream_RolloverCapsStillCloseWithoutAReplay is the other half
// of the non-vacuity pair: when the pump delivers no replay (every event
// is already past the parent's position), the advancement condition must
// be transparent — each cap closes exactly where it did before the fix.
func TestBackupStream_RolloverCapsStillCloseWithoutAReplay(t *testing.T) {
	changes := append(item92Tx(item98NewTx1, "carol@example.com"), item92Tx(item98NewTx2, "dave@example.com")...)
	caps := []struct {
		name string
		cfg  item98Stream
	}{
		{"--rollover-max-changes", item98Stream{maxChanges: 2, maxBytes: 1 << 40, chunkChanges: 1000}},
		{"--rollover-max-bytes", item98Stream{maxChanges: 1_000_000, maxBytes: 1, chunkChanges: 3}},
	}
	for _, c := range caps {
		t.Run(c.name, func(t *testing.T) {
			incrs := runItem98Stream(t, orderedFakeCDCEngine{fakeCDCEngine: item98FakeSource(changes)}, item92Pos(item98ParentEnd), c.cfg)
			if len(incrs) != 2 {
				t.Fatalf("rollovers = %d; want 2 (one per transaction) — the advancement condition must not delay a stream that never saw a replay", len(incrs))
			}
			if want := item92Pos(item98NewTx1); incrs[0].EndPosition != want {
				t.Errorf("rollover 1 EndPosition = %+v; want %+v (the cap must close at the first tx boundary at-or-after it)", incrs[0].EndPosition, want)
			}
			if got := item98Changes(incrs[0]); got != 3 {
				t.Errorf("rollover 1 captured %d change events; want 3 (one transaction)", got)
			}
		})
	}
}

// TestBackupStream_ReplayOnlyRolloverStillCommitsWhenTheWindowEndsOtherwise
// pins the deliberate exemption: only the two caps carry the advancement
// condition. Every other window exit — the wall-clock deadline, the stop
// signal, the ctx-cancel drain, and the source-closed path exercised here
// — must still close a replay-only rollover, or a resumed stream over a
// quiet source would never commit and never heartbeat.
func TestBackupStream_ReplayOnlyRolloverStillCommitsWhenTheWindowEndsOtherwise(t *testing.T) {
	replayOnly := item92Tx(item98ParentEnd, "already-in-the-parent@example.com")
	incrs := runItem98Stream(
		t,
		orderedFakeCDCEngine{fakeCDCEngine: item98FakeSource(replayOnly)},
		item92Pos(item98ParentEnd),
		item98Stream{maxChanges: 2, maxBytes: 1, chunkChanges: 3},
	)
	if len(incrs) != 1 {
		t.Fatalf("rollovers = %d; want 1 (the source-closed exit must still commit what it drained)", len(incrs))
	}
	if incrs[0].EndPosition != incrs[0].StartPosition {
		t.Errorf("EndPosition = %+v; want it to equal StartPosition %+v — a rollover holding only the replay has genuinely not advanced, and the exempt exits must commit it honestly rather than invent a position",
			incrs[0].EndPosition, incrs[0].StartPosition)
	}
	if got := item98Changes(incrs[0]); got != 3 {
		t.Errorf("captured %d change events; want 3 — the replayed events are kept, not filtered", got)
	}
}
