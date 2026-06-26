// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package mysql

import (
	"context"
	"database/sql"
	"errors"
	"testing"
	"time"

	"sluicesync.dev/sluice/internal/ir"
)

// --- ADR-0111 native-MySQL resumable cold-copy: unit pins ---

// TestVerifyCDCAnchorUnchanged is THE value-fidelity guard test (ADR-0111 §3,
// the #1 correctness requirement): the CDC anchor MUST stay at the ORIGINAL,
// earliest position P across a re-snapshot recovery. Advancing it to P′ would
// SKIP P→P′ changes on already-completed tables — silent loss. This pins that
// verifyCDCAnchorUnchanged accepts an unchanged anchor and LOUDLY refuses ANY
// drift of the anchor fields (file, pos, gtid, mode, uuid).
func TestVerifyCDCAnchorUnchanged(t *testing.T) {
	p := binlogPos{Mode: positionModeFilePos, File: "mysql-bin.000007", Pos: 4567, ServerUUID: "uuid-A"}

	t.Run("unchanged anchor passes", func(t *testing.T) {
		if err := verifyCDCAnchorUnchanged(p, p); err != nil {
			t.Fatalf("identical anchors rejected: %v", err)
		}
	})

	// Each field drift = a would-be silent-loss anchor advance → must refuse.
	cases := map[string]binlogPos{
		"advanced pos (the P→P′ silent-loss class)": {Mode: positionModeFilePos, File: "mysql-bin.000007", Pos: 9999, ServerUUID: "uuid-A"},
		"rotated file":                   {Mode: positionModeFilePos, File: "mysql-bin.000009", Pos: 4567, ServerUUID: "uuid-A"},
		"different uuid (instance swap)": {Mode: positionModeFilePos, File: "mysql-bin.000007", Pos: 4567, ServerUUID: "uuid-B"},
		"mode flip to gtid":              {Mode: positionModeGTID, GTIDSet: "uuid:1-100"},
	}
	for name, drifted := range cases {
		drifted := drifted
		t.Run("refuses "+name, func(t *testing.T) {
			if err := verifyCDCAnchorUnchanged(p, drifted); err == nil {
				t.Fatalf("anchor drift (%s) accepted; want a LOUD refusal (silent-loss guard, ADR-0111 §3)", name)
			}
		})
	}

	t.Run("gtid anchor unchanged passes, advanced gtid refused", func(t *testing.T) {
		g := binlogPos{Mode: positionModeGTID, GTIDSet: "uuid:1-100"}
		if err := verifyCDCAnchorUnchanged(g, g); err != nil {
			t.Fatalf("identical gtid anchors rejected: %v", err)
		}
		g2 := binlogPos{Mode: positionModeGTID, GTIDSet: "uuid:1-200"}
		if err := verifyCDCAnchorUnchanged(g, g2); err == nil {
			t.Fatal("advanced gtid set accepted; want a LOUD refusal")
		}
	})
}

// TestConcurrentReader_AnchorNeverMutatesUnderRecovery proves the reader's
// recorded anchor + anchorToken survive a simulated re-snapshot recovery
// UNCHANGED even though the inner connections were swapped — the in-memory
// counterpart of the §3 invariant. We drive recoverViaResnapshot with a fake
// resnapshot fn that returns a DIFFERENT P′ (a later file) and assert the
// anchor the reader will hand to CDC is still the ORIGINAL P.
func TestConcurrentReader_AnchorNeverMutatesUnderRecovery(t *testing.T) {
	origAnchor := binlogPos{Mode: positionModeFilePos, File: "mysql-bin.000005", Pos: 100, ServerUUID: "uuid-A"}
	tok, err := encodeBinlogPos(origAnchor)
	if err != nil {
		t.Fatalf("encode anchor: %v", err)
	}
	r := newConcurrentBinlogRows(nil, [][]string{{"a"}, {"b"}}, "db", nil, zeroDateInherit)
	r.anchor = origAnchor
	r.anchorToken = tok.Token
	r.anchorSet = true
	// Fake re-snapshot: returns a LATER position P′ (mysql-bin.000005 still
	// present so no purge), and nil conns/db (we only assert the anchor — the
	// swap of nil readers is harmless here).
	r.resnapshot = func(_ context.Context) ([]*sql.Conn, *sql.DB, string, uint32, error) {
		return nil, nil, "mysql-bin.000005", 9999, nil
	}
	// binlogFileBefore needs a *sql.DB; with db==nil the file_pos purge branch
	// would panic — so override the anchor to GTID mode for THIS pure-anchor
	// assertion (GTID skips the file purge probe). The file_pos purge path is
	// covered by the integration test against a real DB.
	r.anchor.Mode = positionModeGTID
	r.anchor.GTIDSet = "uuid:1-50"
	r.anchor.File = ""
	r.anchor.Pos = 0
	gtok, _ := encodeBinlogPos(r.anchor)
	r.anchorToken = gtok.Token

	before := r.anchor
	if rerr := r.recoverViaResnapshot(context.Background()); rerr != nil {
		t.Fatalf("recoverViaResnapshot: %v", rerr)
	}
	if r.anchor != before {
		t.Fatalf("anchor MUTATED across recovery: before=%+v after=%+v (silent-loss: CDC would skip P→P′ on completed tables)", before, r.anchor)
	}
	if r.anchorToken != gtok.Token {
		t.Fatalf("anchorToken mutated across recovery: %q != %q", r.anchorToken, gtok.Token)
	}
}

// TestConcurrentReader_RecoveryDecision pins the per-table recovery decision
// (skip-completed / resume-keyed-from-cursor / restart-keyless) the in-memory
// cursor drives: a completed table is skipped, a keyed in-progress table
// resumes from its last-PK cursor, a keyless table restarts from the start.
func TestConcurrentReader_RecoveryDecision(t *testing.T) {
	r := newConcurrentBinlogRows(nil, [][]string{{"done", "keyed"}, {"keyless"}}, "db", nil, zeroDateInherit)

	// done: emitted some rows then completed → skip on recovery.
	r.noteRowEmitted("done", true, []any{int64(10)})
	r.noteTableComplete("done", true)
	// keyed: emitted up to pk=42, not complete → resume WHERE pk > 42.
	r.noteRowEmitted("keyed", true, []any{int64(7)})
	r.noteRowEmitted("keyed", true, []any{int64(42)})
	// keyless: emitted rows, no PK tracked, not complete → restart from start.
	r.noteRowEmitted("keyless", false, nil)

	if c := r.cursorFor("done"); !c.complete {
		t.Error("completed table not marked complete (would be re-read on recovery)")
	}
	keyed := r.cursorFor("keyed")
	if keyed.complete {
		t.Error("in-progress keyed table marked complete")
	}
	if !keyed.keyed {
		t.Error("keyed table not flagged keyed")
	}
	if len(keyed.lastPK) != 1 || keyed.lastPK[0].(int64) != 42 {
		t.Errorf("keyed cursor = %v; want last-emitted pk [42] (resume WHERE pk > 42)", keyed.lastPK)
	}
	keyless := r.cursorFor("keyless")
	if keyless.keyed {
		t.Error("keyless table flagged keyed (would attempt cursor resume on a no-PK table)")
	}
	if len(keyless.lastPK) != 0 {
		t.Errorf("keyless cursor has a lastPK %v; want none (restart-from-start contract)", keyless.lastPK)
	}
}

// TestTableHasOrderablePK_FamilyMatrix pins the keyed-vs-keyless decision
// across EVERY orderable PK type family AND the non-orderable families (the
// Bug 74 "pin the class, not the representative" discipline): the recovery's
// keyed-resume vs keyless-restart dispatch hinges on this, so a missed family
// would silently route a resumable keyed table to the lossy keyless restart.
func TestTableHasOrderablePK_FamilyMatrix(t *testing.T) {
	mk := func(pkType ir.Type) *ir.Table {
		return &ir.Table{
			Name:    "t",
			Columns: []*ir.Column{{Name: "id", Type: pkType}},
			PrimaryKey: &ir.Index{
				Columns: []ir.IndexColumn{{Column: "id"}},
			},
		}
	}

	orderable := map[string]ir.Type{
		"integer":   ir.Integer{Width: 64},
		"decimal":   ir.Decimal{Precision: 10, Scale: 2},
		"char":      ir.Char{Length: 8},
		"varchar":   ir.Varchar{Length: 64},
		"text":      ir.Text{},
		"uuid":      ir.UUID{},
		"binary":    ir.Binary{Length: 16},
		"varbinary": ir.Varbinary{Length: 64},
		"blob":      ir.Blob{},
		"bit":       ir.Bit{Length: 1},
		"date":      ir.Date{},
		"time":      ir.Time{},
		"timestamp": ir.Timestamp{},
		"datetime":  ir.DateTime{},
	}
	for name, typ := range orderable {
		typ := typ
		t.Run("orderable/"+name, func(t *testing.T) {
			if !tableHasOrderablePK(mk(typ)) {
				t.Fatalf("PK type %s reported NOT orderable; the keyed-resume path would be skipped and the table would lossily restart-from-start on recovery", name)
			}
		})
	}

	nonOrderable := map[string]ir.Type{
		"json":     ir.JSON{},
		"enum":     ir.Enum{Values: []string{"a", "b"}},
		"set":      ir.Set{Values: []string{"a", "b"}},
		"geometry": ir.Geometry{},
	}
	for name, typ := range nonOrderable {
		typ := typ
		t.Run("non-orderable/"+name, func(t *testing.T) {
			if tableHasOrderablePK(mk(typ)) {
				t.Fatalf("PK type %s reported orderable; it cannot serve as a resume cursor (would build a malformed WHERE)", name)
			}
		})
	}

	t.Run("domain unwraps to base type", func(t *testing.T) {
		if !tableHasOrderablePK(mk(ir.Domain{Name: "d", BaseType: ir.Integer{Width: 32}})) {
			t.Fatal("domain over integer reported NOT orderable")
		}
		if tableHasOrderablePK(mk(ir.Domain{Name: "d", BaseType: ir.JSON{}})) {
			t.Fatal("domain over json reported orderable")
		}
	})

	t.Run("no primary key is keyless", func(t *testing.T) {
		if tableHasOrderablePK(&ir.Table{Name: "t", Columns: []*ir.Column{{Name: "x", Type: ir.Text{}}}}) {
			t.Fatal("a table with no PK reported orderable")
		}
	})

	t.Run("composite PK requires EVERY column orderable", func(t *testing.T) {
		good := &ir.Table{
			Name: "t",
			Columns: []*ir.Column{
				{Name: "a", Type: ir.Integer{Width: 32}},
				{Name: "b", Type: ir.Varchar{Length: 16}},
			},
			PrimaryKey: &ir.Index{Columns: []ir.IndexColumn{{Column: "a"}, {Column: "b"}}},
		}
		if !tableHasOrderablePK(good) {
			t.Fatal("composite (int, varchar) PK reported NOT orderable")
		}
		bad := &ir.Table{
			Name: "t",
			Columns: []*ir.Column{
				{Name: "a", Type: ir.Integer{Width: 32}},
				{Name: "b", Type: ir.JSON{}},
			},
			PrimaryKey: &ir.Index{Columns: []ir.IndexColumn{{Column: "a"}, {Column: "b"}}},
		}
		if tableHasOrderablePK(bad) {
			t.Fatal("composite PK with a non-orderable member reported orderable")
		}
	})

	t.Run("sluice-injected PK column is keyless", func(t *testing.T) {
		tbl := mk(ir.Varchar{Length: 64})
		tbl.Columns[0].SluiceInjected = true
		if tableHasOrderablePK(tbl) {
			t.Fatal("a sluice-injected PK column reported orderable (not present on source → cursor read would fail)")
		}
	})
}

// TestExtractPKTuple pins the per-row PK extraction the keyed cursor advance
// relies on (ordered by PK column order, missing column → nil).
func TestExtractPKTuple(t *testing.T) {
	row := ir.Row{"a": int64(1), "b": "x", "c": 3.5}
	got := extractPKTuple(row, []string{"b", "a"})
	if len(got) != 2 || got[0] != "x" || got[1].(int64) != 1 {
		t.Fatalf("extractPKTuple = %v; want [x 1] (PK column order)", got)
	}
	if extractPKTuple(row, nil) != nil {
		t.Fatal("extractPKTuple(nil names) should be nil")
	}
}

// TestPrimaryKeyColumnNames pins the PK-name extraction.
func TestPrimaryKeyColumnNames(t *testing.T) {
	tbl := &ir.Table{
		Name:       "t",
		PrimaryKey: &ir.Index{Columns: []ir.IndexColumn{{Column: "x"}, {Column: "y"}}},
	}
	got := primaryKeyColumnNames(tbl)
	if len(got) != 2 || got[0] != "x" || got[1] != "y" {
		t.Fatalf("primaryKeyColumnNames = %v; want [x y]", got)
	}
	if primaryKeyColumnNames(&ir.Table{Name: "t"}) != nil {
		t.Fatal("keyless table should yield nil PK names")
	}
}

// TestNativeResnapshotBackoff pins the bounded exponential backoff: doubles
// from base, never exceeds the cap (mirrors coldCopySourceReadBackoff).
func TestNativeResnapshotBackoff(t *testing.T) {
	if got := nativeResnapshotBackoff(1); got != nativeResnapshotBackoffBase {
		t.Fatalf("attempt 1 backoff = %v; want base %v", got, nativeResnapshotBackoffBase)
	}
	for attempt := 1; attempt <= 30; attempt++ {
		if got := nativeResnapshotBackoff(attempt); got > nativeResnapshotBackoffCap {
			t.Fatalf("attempt %d backoff %v exceeds cap %v", attempt, got, nativeResnapshotBackoffCap)
		}
	}
}

// TestRecoverFromDrop_TerminalWhenNoResnapshotWired proves a classified drop
// with NO re-snapshot fn wired is TERMINAL (loud), never a silent wrong-point
// re-read. (The happy in-process recovery is exercised by the integration
// test against a real DB.) We shrink the wall-clock + backoff so the bounded
// loop exits fast.
func TestRecoverFromDrop_TerminalWhenNoResnapshotWired(t *testing.T) {
	defer restoreResnapshotEnvelope(saveResnapshotEnvelope())
	nativeResnapshotMaxWall = 50 * time.Millisecond
	nativeResnapshotBackoffBase = time.Millisecond
	nativeResnapshotBackoffCap = time.Millisecond

	r := newConcurrentBinlogRows(nil, [][]string{{"a"}}, "db", nil, zeroDateInherit)
	r.anchor = binlogPos{Mode: positionModeGTID, GTIDSet: "uuid:1-1"}
	r.anchorSet = true
	r.resnapshot = nil // not wired

	cause := &retriableTestErr{}
	err := r.recoverFromDrop(context.Background(), cause, 0)
	if err == nil {
		t.Fatal("recoverFromDrop with no resnapshot fn returned nil; want a LOUD terminal error (never silent)")
	}
}

// TestRecoverFromDrop_BinlogPurgedFallsBack proves a re-snapshot that detects
// the original anchor's binlog file was purged surfaces
// errBinlogPurgedDuringResnapshot (the loud fallback to full
// restart-from-scratch, ADR-0111 §5) rather than resuming against a
// no-longer-replayable anchor.
func TestRecoverFromDrop_BinlogPurgedFallsBack(t *testing.T) {
	// Covered structurally here: the purge classification (binlogFileBefore)
	// needs a real DB and is exercised in the integration test. This unit pin
	// asserts the error sentinel wiring: a resnapshot fn that returns the
	// purge sentinel propagates it unchanged (not retried into the budget).
	defer restoreResnapshotEnvelope(saveResnapshotEnvelope())
	nativeResnapshotMaxWall = time.Second
	nativeResnapshotBackoffBase = time.Millisecond
	nativeResnapshotBackoffCap = time.Millisecond

	r := newConcurrentBinlogRows(nil, [][]string{{"a"}}, "db", nil, zeroDateInherit)
	r.anchor = binlogPos{Mode: positionModeGTID, GTIDSet: "uuid:1-1"}
	r.anchorSet = true
	r.resnapshot = func(_ context.Context) ([]*sql.Conn, *sql.DB, string, uint32, error) {
		return nil, nil, "", 0, errBinlogPurgedDuringResnapshot
	}
	err := r.recoverFromDrop(context.Background(), &retriableTestErr{}, 0)
	if !errors.Is(err, errBinlogPurgedDuringResnapshot) {
		t.Fatalf("recoverFromDrop = %v; want errBinlogPurgedDuringResnapshot (loud fallback to full restart-from-scratch)", err)
	}
}

// TestRecoverFromDrop_CoalesceByGeneration pins the peer-coalescing fix: a lane
// whose read began at a generation a PEER has already re-snapshotted past
// (observedGen < recoveryGen) must COALESCE — return nil WITHOUT a second FTWRL
// re-snapshot — while the FIRST lane of a generation (observedGen ==
// recoveryGen) DOES re-snapshot. Without this, every dropped lane re-snapshots
// (W× FTWRL stalls per grow window — the regression the value-fidelity review
// caught).
func TestRecoverFromDrop_CoalesceByGeneration(t *testing.T) {
	// (a) Peer already recovered: observedGen(0) < recoveryGen(1) → coalesce;
	// the resnapshot fn must NOT be invoked.
	rc := newConcurrentBinlogRows(nil, [][]string{{"a"}}, "db", nil, zeroDateInherit)
	rc.anchor = binlogPos{Mode: positionModeFilePos, File: "bin.000001", Pos: 4}
	rc.anchorSet = true
	rc.recoveryGen = 1
	rc.resnapshot = func(_ context.Context) ([]*sql.Conn, *sql.DB, string, uint32, error) {
		t.Fatal("coalesce path must NOT call the resnapshot fn (a peer already recovered this drop)")
		return nil, nil, "", 0, nil
	}
	if err := rc.recoverFromDrop(context.Background(), &retriableTestErr{}, 0); err != nil {
		t.Fatalf("coalesce path returned %v; want nil (resume on the peer-swapped conns)", err)
	}

	// (b) First lane of a generation: observedGen(0) == recoveryGen(0) →
	// proceeds to the actual re-snapshot (fn invoked). Return the purge sentinel
	// so the test stays DB-free while still proving the fn ran.
	defer restoreResnapshotEnvelope(saveResnapshotEnvelope())
	nativeResnapshotMaxWall = time.Second
	nativeResnapshotBackoffBase = time.Millisecond
	nativeResnapshotBackoffCap = time.Millisecond
	rf := newConcurrentBinlogRows(nil, [][]string{{"a"}}, "db", nil, zeroDateInherit)
	rf.anchor = binlogPos{Mode: positionModeFilePos, File: "bin.000001", Pos: 4}
	rf.anchorSet = true
	called := false
	rf.resnapshot = func(_ context.Context) ([]*sql.Conn, *sql.DB, string, uint32, error) {
		called = true
		return nil, nil, "", 0, errBinlogPurgedDuringResnapshot
	}
	_ = rf.recoverFromDrop(context.Background(), &retriableTestErr{}, 0)
	if !called {
		t.Fatal("first lane of a generation must re-snapshot (resnapshot fn was never called)")
	}
}

// --- test helpers ---

// retriableTestErr is a classified-transient stand-in (implements
// ir.RetriableError → Retriable() true), the source-read-drop class the
// recovery rides out.
type retriableTestErr struct{}

func (e *retriableTestErr) Error() string            { return "test: classified source-read drop" }
func (e *retriableTestErr) Retriable() bool          { return true }
func (e *retriableTestErr) RetryHint() time.Duration { return 0 }

var _ ir.RetriableError = (*retriableTestErr)(nil)

type resnapshotEnvelope struct {
	maxWall     time.Duration
	backoffBase time.Duration
	backoffCap  time.Duration
}

func saveResnapshotEnvelope() resnapshotEnvelope {
	return resnapshotEnvelope{
		maxWall:     nativeResnapshotMaxWall,
		backoffBase: nativeResnapshotBackoffBase,
		backoffCap:  nativeResnapshotBackoffCap,
	}
}

func restoreResnapshotEnvelope(e resnapshotEnvelope) {
	nativeResnapshotMaxWall = e.maxWall
	nativeResnapshotBackoffBase = e.backoffBase
	nativeResnapshotBackoffCap = e.backoffCap
}
