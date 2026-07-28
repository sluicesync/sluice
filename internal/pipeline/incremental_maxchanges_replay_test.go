// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

// Roadmap item 92: a resumed CDC stream opens by RE-DELIVERING the
// transaction the parent segment ends on, and `--max-changes` counts
// change EVENTS (transaction framing included). A tight bound was
// therefore satisfiable by that replay alone: the window closed at its
// own start position having captured nothing new, and still wrote a
// chain-linked, correctly-stamped segment reporting a non-zero change
// count. Ground truth (PG 16, an incremental resumed at the parent's
// EndPosition 0/25CC498 with --max-changes 2):
//
//	capture TxBegin      pos 0/25CC498  total_before=0 in_tx=true
//	capture SchemaSnapshot pos 0/0      total_before=1 in_tx=true
//	capture Insert       pos 0/25CC498  total_before=2 in_tx=true
//	capture TxCommit     pos 0/25CC498  total_before=3 in_tx=false
//	branch: maxChanges close total=4 max=2 last_pos=0/25CC498
//	→ manifest start == end == 0/25CC498, chunks=1, changes=4
//
// The slot's confirmed_flush_lsn trailed the request by two committed
// transactions in that run and PostgreSQL still replayed only the
// boundary one — so the requested LSN is the floor and delivered
// positions never regress below it.
//
// These pins lock the fix shape at the orchestrator level: the bound may
// not close a window that has not advanced past its start position, and
// it must still bite promptly once the window HAS advanced.

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

// orderedFakeCDCEngine is [fakeCDCEngine] plus the optional
// [ir.PositionMonotonicChecker] surface both real CDC engines
// implement. Its positions are zero-padded decimal LSNs so the native
// order is a plain string compare.
type orderedFakeCDCEngine struct {
	*fakeCDCEngine
}

func (orderedFakeCDCEngine) PrecedesOrEqual(a, b ir.Position) (bool, error) {
	return a.Token <= b.Token, nil
}

var _ ir.PositionMonotonicChecker = orderedFakeCDCEngine{}

// item92Pos builds a fake ordered position.
func item92Pos(lsn string) ir.Position {
	return ir.Position{Engine: "postgres", Token: lsn}
}

// item92Tx returns the three events of a one-row transaction committing
// at lsn — the shape a Postgres source produces, where every event of a
// transaction carries that transaction's commit LSN.
func item92Tx(lsn, email string) []ir.Change {
	p := item92Pos(lsn)
	return []ir.Change{
		ir.TxBegin{Position: p},
		ir.Insert{Position: p, Table: "users", Row: ir.Row{"id": int64(1), "email": email}},
		ir.TxCommit{Position: p},
	}
}

// runItem92Incremental drives one incremental against a fake source
// whose stream begins with a replay of parentEnd's transaction, and
// returns the manifest it wrote.
func runItem92Incremental(t *testing.T, src ir.Engine, parentEnd ir.Position, maxChanges int) *irbackup.Manifest {
	t.Helper()
	dir := t.TempDir()
	store, err := blobcodec.NewLocalStore(dir)
	if err != nil {
		t.Fatalf("NewLocalStore: %v", err)
	}
	parent := &irbackup.Manifest{
		FormatVersion: irbackup.BackupFormatVersion,
		CreatedAt:     time.Date(2026, 7, 27, 10, 0, 0, 0, time.UTC),
		SourceEngine:  "postgres",
		Schema:        item92Schema(),
		Kind:          irbackup.BackupKindFull,
		EndPosition:   parentEnd,
		PartialState:  irbackup.BackupStateComplete,
	}
	parent.BackupID = irbackup.ComputeBackupID(parent)
	writeParentFullManifest(t, store, parent)

	now := time.Date(2026, 7, 27, 11, 0, 0, 0, time.UTC)
	b := &IncrementalBackup{
		Source:        src,
		SourceDSN:     "src",
		Store:         store,
		ParentRef:     parent.BackupID,
		Window:        5 * time.Minute,
		MaxChanges:    maxChanges,
		ChunkChanges:  1000,
		SluiceVersion: "test",
		Now:           func() time.Time { return now },
		clockNow:      func() time.Time { return now },
	}
	if err := b.Run(context.Background()); err != nil {
		t.Fatalf("IncrementalBackup.Run: %v", err)
	}
	records, err := lineage.ListAllManifestsViaWalk(context.Background(), store)
	if err != nil {
		t.Fatalf("ListAllManifestsViaWalk: %v", err)
	}
	for _, r := range records {
		if r.Manifest.Kind == irbackup.BackupKindIncremental {
			return r.Manifest
		}
	}
	t.Fatal("no incremental manifest written")
	return nil
}

// item92Schema is the fake source's schema, shared by every case.
func item92Schema() *ir.Schema {
	return &ir.Schema{Tables: []*ir.Table{{
		Name:    "users",
		Columns: []*ir.Column{{Name: "id", Type: ir.Integer{Width: 64}}},
	}}}
}

// TestIncrementalBackup_MaxChangesWillNotCloseOnTheReplayedBoundaryTx is
// the item-92 pin. The bound (2) is satisfied by the replayed boundary
// transaction alone; the window must nonetheless run on until it has
// captured a transaction of its own, and must then close at THAT
// boundary rather than draining the whole stream.
func TestIncrementalBackup_MaxChangesWillNotCloseOnTheReplayedBoundaryTx(t *testing.T) {
	const (
		parentEnd = "0000000100" // the parent segment's EndPosition
		newTx1    = "0000000200" // the first genuinely new transaction
		newTx2    = "0000000300" // must NOT be captured: the bound bites first
	)

	changes := append(item92Tx(parentEnd, "already-in-the-parent@example.com"), item92Tx(newTx1, "carol@example.com")...)
	changes = append(changes, item92Tx(newTx2, "dave@example.com")...)

	cases := []struct {
		name string
		src  func(*fakeCDCEngine) ir.Engine
	}{
		{
			// The production shape: PG and MySQL both implement the
			// comparator, so ordering is semantic (an LSN compare)
			// rather than a byte compare of the position token.
			name: "engine implements PositionMonotonicChecker",
			src:  func(f *fakeCDCEngine) ir.Engine { return orderedFakeCDCEngine{fakeCDCEngine: f} },
		},
		{
			// Fallback path for an engine without the comparator:
			// position inequality. It reaches the same verdict here
			// because the replayed events carry the parent's exact
			// position.
			name: "engine without a comparator falls back to position inequality",
			src:  func(f *fakeCDCEngine) ir.Engine { return f },
		},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			src := c.src(&fakeCDCEngine{
				name:              "postgres",
				schemaSequence:    []*ir.Schema{item92Schema()},
				cdcChanges:        changes,
				cdcExpectedFromOK: true,
			})
			incr := runItem92Incremental(t, src, item92Pos(parentEnd), 2)

			if incr.StartPosition != item92Pos(parentEnd) {
				t.Fatalf("StartPosition = %+v; want %+v", incr.StartPosition, item92Pos(parentEnd))
			}
			// The defect: the window closed at its own start position.
			if incr.EndPosition == incr.StartPosition {
				t.Errorf("EndPosition == StartPosition (%+v) — the window closed on the replayed boundary transaction and captured nothing new (roadmap item 92)", incr.EndPosition)
			}
			if want := item92Pos(newTx1); incr.EndPosition != want {
				t.Errorf("EndPosition = %+v; want %+v (close at the FIRST new transaction's commit)", incr.EndPosition, want)
			}
			// Non-vacuity: the bound must still bite. Draining the whole
			// stream would have reached newTx2 and counted 9 events.
			var captured int64
			for _, ch := range incr.ChangeChunks {
				captured += ch.RowCount
			}
			if captured != 6 {
				t.Errorf("captured change events = %d; want 6 (the 3-event replay plus the 3-event new transaction — %d means the bound stopped biting)", captured, captured)
			}
		})
	}
}

// TestIncrementalBackup_MaxChangesStillClosesWithoutAReplay is the other
// half of the non-vacuity pair: when the stream carries no replay (every
// event is already past the parent's position), the advancement
// condition must be transparent — the bound closes exactly where it did
// before the item-92 fix.
func TestIncrementalBackup_MaxChangesStillClosesWithoutAReplay(t *testing.T) {
	const (
		parentEnd = "0000000100"
		newTx1    = "0000000200"
		newTx2    = "0000000300"
	)
	changes := append(item92Tx(newTx1, "carol@example.com"), item92Tx(newTx2, "dave@example.com")...)

	src := orderedFakeCDCEngine{fakeCDCEngine: &fakeCDCEngine{
		name:              "postgres",
		schemaSequence:    []*ir.Schema{item92Schema()},
		cdcChanges:        changes,
		cdcExpectedFromOK: true,
	}}
	incr := runItem92Incremental(t, src, item92Pos(parentEnd), 2)

	if want := item92Pos(newTx1); incr.EndPosition != want {
		t.Errorf("EndPosition = %+v; want %+v (the bound must close at the first tx boundary at-or-after the cap)", incr.EndPosition, want)
	}
	var captured int64
	for _, ch := range incr.ChangeChunks {
		captured += ch.RowCount
	}
	if captured != 3 {
		t.Errorf("captured change events = %d; want 3 (one transaction) — the advancement condition must not delay a window that never saw a replay", captured)
	}
}

// TestWindowAdvancedPast pins the predicate itself across the shapes the
// orchestrator hands it: the replay (equal), forward progress, the
// Postgres SchemaSnapshot anchored at 0/0 (which must not un-advance a
// window), the "from now" parent with no recorded EndPosition, and the
// no-comparator fallback.
func TestWindowAdvancedPast(t *testing.T) {
	ordered := orderedFakeCDCEngine{fakeCDCEngine: &fakeCDCEngine{name: "postgres"}}
	plain := &fakeCDCEngine{name: "postgres"}
	start := item92Pos("0000000100")

	cases := []struct {
		name string
		src  ir.Engine
		pos  ir.Position
		want bool
	}{
		{"replayed boundary event carries the start position", ordered, start, false},
		{"a genuinely later position", ordered, item92Pos("0000000200"), true},
		{"a lower position (PG anchors a SchemaSnapshot at 0/0)", ordered, item92Pos("0000000000"), false},
		{"a position-less event", ordered, ir.Position{}, false},
		{"no comparator: equal position", plain, start, false},
		{"no comparator: different position", plain, item92Pos("0000000200"), true},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			if got := windowAdvancedPast(c.src, start, c.pos); got != c.want {
				t.Errorf("windowAdvancedPast(%+v, %+v) = %v; want %v", start, c.pos, got, c.want)
			}
		})
	}

	// A "from now" start (a v0.16.x parent with no EndPosition) has no
	// boundary transaction to replay, so everything is new.
	if !windowAdvancedPast(ordered, ir.Position{}, item92Pos("0000000000")) {
		t.Error("windowAdvancedPast with an empty start = false; want true (nothing can be a replay of a from-now start)")
	}
}
