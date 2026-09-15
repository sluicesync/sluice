// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package sqlitetrigger

import (
	"context"
	"errors"
	"strings"
	"testing"

	"sluicesync.dev/sluice/internal/engines/sqlite"
	"sluicesync.dev/sluice/internal/ir"
	irbackup "sluicesync.dev/sluice/internal/ir/backup"
)

// Roadmap item 163 on the two transports. The local file is the executor
// the argument is PROVEN on; the D1 backend runs the identical function
// over the mock /query server, so what is pinned there is that the D1
// path takes the same anchor at the same point — derived from the local
// proof, not measured on a live D1.

// TestBackupSnapshot_AnchorIsMaxIDBeforeTheSweep pins the load-bearing
// ordering on the local transport: the snapshot's Position is the change
// log's MAX(id) read BEFORE the row reader opens, so a change committed
// AFTER the anchor is BOTH in the sweep (over-replay, safe) and above the
// anchor (replayed by the incremental) — never in neither.
func TestBackupSnapshot_AnchorIsMaxIDBeforeTheSweep(t *testing.T) {
	path := newSourceFile(t, `CREATE TABLE t (id INTEGER PRIMARY KEY, n INTEGER)`)
	if _, err := Setup(bg(), path, SetupOptions{Tables: []string{"t"}}); err != nil {
		t.Fatalf("Setup: %v", err)
	}
	for i := 1; i <= 3; i++ {
		exec(t, path, `INSERT INTO t (id, n) VALUES (?, ?)`, i, i)
	}
	snap, err := (Engine{}).OpenBackupSnapshot(bg(), path, irbackup.SnapshotOptions{})
	if err != nil {
		t.Fatalf("OpenBackupSnapshot: %v", err)
	}
	defer func() { _ = snap.Close() }()

	p, ok, err := decodePos(snap.Position)
	if err != nil || !ok {
		t.Fatalf("decode backup position: ok=%v err=%v", ok, err)
	}
	if p.LastID != 3 {
		t.Errorf("backup anchor last_id=%d; want 3 (MAX(id) at open)", p.LastID)
	}
	if snap.Position.Engine != EngineName {
		t.Errorf("position engine = %q; want %q (the trigger codec's own tag)", snap.Position.Engine, EngineName)
	}
	// The position must be one the incremental's resume path decodes: the
	// prune/registry decoder is the same codec the poller uses.
	if id, err := AppliedLastID(snap.Position.Token); err != nil || id != 3 {
		t.Errorf("AppliedLastID(token) = (%d, %v); want (3, nil)", id, err)
	}

	// A row committed AFTER the anchor and BEFORE the sweep reads the table:
	// the over-replay case. It is in the sweep (below) and its change-log id
	// (4) is above the anchor (3), so an incremental resuming at 3 replays it
	// idempotently. What must never happen is the reverse — an id ≤ anchor
	// missing from the sweep — and that cannot be constructed here because
	// the reader opens strictly after the anchor read.
	exec(t, path, `INSERT INTO t (id, n) VALUES (4, 4)`)
	rows, err := snap.Rows.ReadRows(bg(), &ir.Table{
		Name:       "t",
		Columns:    []*ir.Column{{Name: "id", Type: ir.Integer{}}, {Name: "n", Type: ir.Integer{}}},
		PrimaryKey: &ir.Index{Columns: []ir.IndexColumn{{Column: "id"}}, Unique: true},
	})
	if err != nil {
		t.Fatalf("ReadRows: %v", err)
	}
	var ids []int64
	for r := range rows {
		ids = append(ids, r["id"].(int64))
	}
	if err := snap.Rows.Err(); err != nil {
		t.Fatalf("Rows.Err: %v", err)
	}
	if len(ids) != 4 {
		t.Errorf("sweep read ids %v; want all four (the post-anchor row is in the sweep AND above the anchor)", ids)
	}
}

// TestBackupSnapshot_RefusesChainSlot pins the `--chain-slot` refusal on
// both transports: there is no slot to persist, and the refusal fires
// before any executor opens (the D1 mock sees no request).
func TestBackupSnapshot_RefusesChainSlot(t *testing.T) {
	m := &mockD1{exists: true, maxID: "7"}
	conn := startMockD1(t, m)
	for _, tc := range []struct {
		name string
		b    backend
	}{
		{"local", localBackend(newSourceFile(t, `CREATE TABLE t (id INTEGER PRIMARY KEY)`))},
		{"d1", d1TestBackend(conn, &ir.Schema{})},
	} {
		t.Run(tc.name, func(t *testing.T) {
			_, err := openBackupSnapshot(bg(), tc.b, irbackup.SnapshotOptions{PersistChainSlot: true})
			if err == nil || !strings.Contains(err.Error(), "--chain-slot") {
				t.Fatalf("openBackupSnapshot with PersistChainSlot = %v; want a refusal naming --chain-slot", err)
			}
		})
	}
	if len(m.bodies) != 0 {
		t.Errorf("the D1 mock saw %d request(s) before the --chain-slot refusal; want 0", len(m.bodies))
	}
}

// TestBackupSnapshot_RefusesWithoutSetup pins that a source with no change
// log refuses the snapshot loudly (the orchestrator then falls back and the
// capturer reports unavailable — see the next test) rather than recording a
// position nothing captures into.
func TestBackupSnapshot_RefusesWithoutSetup(t *testing.T) {
	path := newSourceFile(t, `CREATE TABLE t (id INTEGER PRIMARY KEY)`)
	_, err := (Engine{}).OpenBackupSnapshot(bg(), path, irbackup.SnapshotOptions{})
	if err == nil || !strings.Contains(err.Error(), "trigger setup") {
		t.Fatalf("OpenBackupSnapshot without setup = %v; want the change-log-absent refusal naming `trigger setup`", err)
	}
}

// orderProbeColdStart is a cold-start engine whose OpenRowReader records
// how many requests the D1 mock had seen when the reader was opened, so
// the test can assert the anchor read (a MAX(id) request) preceded it.
type orderProbeColdStart struct {
	stubColdStart
	m           *mockD1
	seenAtOpen  int
	openedTimes int
}

func (p *orderProbeColdStart) OpenRowReader(context.Context, string) (ir.RowReader, error) {
	p.m.mu.Lock()
	p.seenAtOpen = len(p.m.bodies)
	p.m.mu.Unlock()
	p.openedTimes++
	return noRows{}, nil
}

// noRows is an ir.RowReader that yields nothing; the D1 pin is about
// ordering, not rows.
type noRows struct{}

func (noRows) ReadRows(context.Context, *ir.Table) (<-chan ir.Row, error) {
	ch := make(chan ir.Row)
	close(ch)
	return ch, nil
}

func (noRows) Err() error { return nil }

// TestD1BackupSnapshot_AnchorsBeforeTheSweep pins the D1 transport: the
// same openBackupSnapshot reads MAX(id) over /query BEFORE it opens the
// cold-start reader, and encodes it under the d1-trigger family's codec.
// Live-D1 behaviour is DERIVED from this and the local proof, not measured
// in this release.
func TestD1BackupSnapshot_AnchorsBeforeTheSweep(t *testing.T) {
	m := &mockD1{exists: true, maxID: "7"}
	conn := startMockD1(t, m)
	probe := &orderProbeColdStart{m: m}
	b := d1TestBackend(conn, &ir.Schema{})
	b.coldStart = probe

	snap, err := openBackupSnapshot(bg(), b, irbackup.SnapshotOptions{})
	if err != nil {
		t.Fatalf("openBackupSnapshot over D1: %v", err)
	}
	defer func() { _ = snap.Close() }()
	p, ok, err := decodePos(snap.Position)
	if err != nil || !ok {
		t.Fatalf("decode D1 backup position: ok=%v err=%v", ok, err)
	}
	if p.LastID != 7 {
		t.Errorf("D1 backup anchor last_id=%d; want 7 (the mock's MAX(id))", p.LastID)
	}
	if probe.openedTimes != 1 {
		t.Fatalf("cold-start reader opened %d times; want 1", probe.openedTimes)
	}
	if probe.seenAtOpen == 0 {
		t.Error("the row reader opened before any /query request — the anchor must be read BEFORE the sweep")
	}
	sawMax := false
	for _, body := range m.bodies[:probe.seenAtOpen] {
		if strings.Contains(body, "MAX(id)") {
			sawMax = true
		}
	}
	if !sawMax {
		t.Errorf("no MAX(id) request preceded the row reader's open; requests before open: %v", m.bodies[:probe.seenAtOpen])
	}
}

// TestCaptureBackupPosition_IsUnavailableOnTheFallbackDoor pins the
// post-sweep capturer on both transports: it never records a position
// (see unavailableBackupPosition for why), reporting ErrPositionUnavailable
// with the remedy that fits — `trigger setup` when the change log is
// absent, "fix the cause and re-run" when it is present — so the
// orchestrator records an empty EndPosition and the chain refuses rather
// than gaps.
func TestCaptureBackupPosition_IsUnavailableOnTheFallbackDoor(t *testing.T) {
	withSetup := newSourceFile(t, `CREATE TABLE t (id INTEGER PRIMARY KEY)`)
	if _, err := Setup(bg(), withSetup, SetupOptions{Tables: []string{"t"}}); err != nil {
		t.Fatalf("Setup: %v", err)
	}
	withoutSetup := newSourceFile(t, `CREATE TABLE t (id INTEGER PRIMARY KEY)`)
	d1Present := startMockD1(t, &mockD1{exists: true, maxID: "7"})
	d1Absent := startMockD1(t, &mockD1{exists: false})

	for _, tc := range []struct {
		name        string
		open        func() (ir.SchemaReader, error)
		wantMention string
	}{
		{"local, change log present", func() (ir.SchemaReader, error) { return (Engine{}).OpenSchemaReader(bg(), withSetup) }, "re-run `backup full`"},
		{"local, change log absent", func() (ir.SchemaReader, error) { return (Engine{}).OpenSchemaReader(bg(), withoutSetup) }, "trigger setup"},
		{"d1, change log present", func() (ir.SchemaReader, error) {
			return &D1SchemaReader{b: d1TestBackend(d1Present, &ir.Schema{})}, nil
		}, "re-run `backup full`"},
		{"d1, change log absent", func() (ir.SchemaReader, error) {
			return &D1SchemaReader{b: d1TestBackend(d1Absent, &ir.Schema{})}, nil
		}, "trigger setup"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			sr, err := tc.open()
			if err != nil {
				t.Fatalf("open schema reader: %v", err)
			}
			defer func() { _ = closeReader(sr) }()
			capturer, ok := sr.(irbackup.PositionCapturer)
			if !ok {
				t.Fatalf("%T does not implement irbackup.PositionCapturer", sr)
			}
			pos, err := capturer.CaptureBackupPosition(bg(), "")
			if !errors.Is(err, irbackup.ErrPositionUnavailable) {
				t.Fatalf("CaptureBackupPosition = (%+v, %v); want errors.Is ErrPositionUnavailable", pos, err)
			}
			if pos != (ir.Position{}) {
				t.Errorf("position = %+v; want empty alongside the unavailable error", pos)
			}
			if !strings.Contains(err.Error(), tc.wantMention) {
				t.Errorf("err = %v; want the remedy to mention %q", err, tc.wantMention)
			}
		})
	}
}

// TestSchemaReader_PromotesTheComposedSurfaces pins that wrapping did not
// narrow the reader: the composed sqlite reader's Close still reaches the
// orchestrator's io.Closer probe, and the wrapper is the concrete type the
// capabilities pin names. (The D1 wrapper's composed surface is exercised
// by OpenD1SchemaReader against a live D1 only; its Close is a no-op.)
func TestSchemaReader_PromotesTheComposedSurfaces(t *testing.T) {
	path := newSourceFile(t, `CREATE TABLE t (id INTEGER PRIMARY KEY)`)
	sr, err := (Engine{}).OpenSchemaReader(bg(), path)
	if err != nil {
		t.Fatalf("OpenSchemaReader: %v", err)
	}
	wrapped, ok := sr.(*SchemaReader)
	if !ok {
		t.Fatalf("OpenSchemaReader returned %T; want *SchemaReader", sr)
	}
	if _, ok := sr.(interface{ Close() error }); !ok {
		t.Fatal("the wrapper lost the composed reader's Close")
	}
	if _, ok := any(wrapped.SchemaReader).(*sqlite.SchemaReader); !ok {
		t.Fatalf("embedded reader is %T; want *sqlite.SchemaReader", wrapped.SchemaReader)
	}
	if err := closeReader(sr); err != nil {
		t.Fatalf("Close: %v", err)
	}
}
