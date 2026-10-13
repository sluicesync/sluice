// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package backup

import (
	"errors"
	"fmt"
	"strconv"
	"strings"
	"testing"

	"sluicesync.dev/sluice/internal/ir"
)

// tokenOrderer orders positions whose tokens are decimal integers.
type tokenOrderer struct{}

func (tokenOrderer) PositionAtOrAfter(p, anchor ir.Position) (bool, error) {
	a, err := strconv.Atoi(p.Token)
	if err != nil {
		return false, err
	}
	b, err := strconv.Atoi(anchor.Token)
	if err != nil {
		return false, err
	}
	return a >= b, nil
}

func hpos(n int) ir.Position { return ir.Position{Engine: "test", Token: strconv.Itoa(n)} }

func keyTable(collation string) *ir.Table {
	return &ir.Table{
		Name: "kd",
		Columns: []*ir.Column{
			{Name: "id", Type: ir.Integer{Width: 32}},
			{Name: "k", Type: ir.Varchar{Length: 20, Charset: "utf8mb4", Collation: collation}},
		},
		PrimaryKey: &ir.Index{Columns: []ir.IndexColumn{{Column: "k"}}},
	}
}

func hsnap(at int, collation string) ir.SchemaSnapshot {
	return ir.SchemaSnapshot{Position: hpos(at), Table: "kd", IR: keyTable(collation)}
}

// replayOrder runs changes through h the way chain restore does and renders
// the stream the applier would see.
func replayOrder(t *testing.T, h *historyReplay, changes []ir.Change) string {
	t.Helper()
	var out []string
	push := func(c ir.Change) error {
		switch v := c.(type) {
		case ir.SchemaSnapshot:
			out = append(out, "S"+v.Position.Token+":"+v.IR.Columns[1].Type.(ir.Varchar).Collation)
		case ir.TxBegin:
			out = append(out, "B"+v.Position.Token)
		case ir.TxCommit:
			out = append(out, "C"+v.Position.Token)
		case ir.Insert:
			out = append(out, "I"+v.Position.Token)
		default:
			out = append(out, fmt.Sprintf("%T", c))
		}
		return nil
	}
	if err := h.start(push); err != nil {
		t.Fatal(err)
	}
	for _, c := range changes {
		if err := h.before(c, push); err != nil {
			t.Fatal(err)
		}
		if err := push(c); err != nil {
			t.Fatal(err)
		}
	}
	if err := h.releaseAll(push); err != nil {
		t.Fatal(err)
	}
	return strings.Join(out, " ")
}

func tx(begin, row, commit int) []ir.Change {
	return []ir.Change{
		ir.TxBegin{Position: hpos(begin)},
		ir.Insert{Position: hpos(row), Table: "kd"},
		ir.TxCommit{Position: hpos(commit)},
	}
}

// TestHistoryReplay_VersionsInterleaveByPosition pins
// KEY-COLLATION-REVERTED-WINDOW-RESTORE at the replay seam: the window's
// `_bin` version reaches the applier before the transaction written under it
// and its `_ci` revert after it — never both ahead of every row, which armed
// the key-collation guard from the revert.
func TestHistoryReplay_VersionsInterleaveByPosition(t *testing.T) {
	snaps := []ir.SchemaSnapshot{hsnap(10, "utf8mb4_bin"), hsnap(30, "utf8mb4_0900_ai_ci")}
	h, err := newHistoryReplay("b1", "test", tokenOrderer{}, nil, snaps)
	if err != nil {
		t.Fatal(err)
	}
	changes := append(tx(5, 6, 7), tx(15, 16, 17)...)
	changes = append(changes, ir.Insert{Position: hpos(35), Table: "kd"})
	got := replayOrder(t, h, changes)
	want := "B5 I6 C7 S10:utf8mb4_bin B15 I16 C17 S30:utf8mb4_0900_ai_ci I35"
	if got != want {
		t.Fatalf("replayed order\n got: %s\nwant: %s", got, want)
	}
}

// TestHistoryReplay_NeverInsideATransaction: a version anchored inside a
// transaction's span is released at the next boundary, not between the
// transaction's rows.
func TestHistoryReplay_NeverInsideATransaction(t *testing.T) {
	h, err := newHistoryReplay("b1", "test", tokenOrderer{}, nil, []ir.SchemaSnapshot{hsnap(12, "utf8mb4_bin")})
	if err != nil {
		t.Fatal(err)
	}
	got := replayOrder(t, h, append(tx(10, 13, 14), tx(20, 21, 22)...))
	if want := "B10 I13 C14 S12:utf8mb4_bin B20 I21 C22"; got != want {
		t.Fatalf("replayed order\n got: %s\nwant: %s", got, want)
	}
}

// TestHistoryReplay_TrailingVersionStillReleased: a version anchored after
// the window's last change still reaches the target's schema history.
func TestHistoryReplay_TrailingVersionStillReleased(t *testing.T) {
	h, err := newHistoryReplay("b1", "test", tokenOrderer{}, nil, []ir.SchemaSnapshot{hsnap(99, "utf8mb4_bin")})
	if err != nil {
		t.Fatal(err)
	}
	if got, want := replayOrder(t, h, tx(1, 2, 3)), "B1 I2 C3 S99:utf8mb4_bin"; got != want {
		t.Fatalf("replayed order\n got: %s\nwant: %s", got, want)
	}
}

// TestHistoryReplay_PositionlessVersionGoesFirst: a version recorded with no
// position has nothing to be ordered by and keeps its old place ahead of the
// window, rather than failing the orderer on an empty token.
func TestHistoryReplay_PositionlessVersionGoesFirst(t *testing.T) {
	positionless := hsnap(0, "utf8mb4_bin")
	positionless.Position = ir.Position{}
	h, err := newHistoryReplay("b1", "test", tokenOrderer{}, nil, []ir.SchemaSnapshot{positionless})
	if err != nil {
		t.Fatal(err)
	}
	if got, want := replayOrder(t, h, tx(1, 2, 3)), "S:utf8mb4_bin B1 I2 C3"; got != want {
		t.Fatalf("replayed order\n got: %s\nwant: %s", got, want)
	}
}

// TestRefuseKeyCollationHistory_UniqueKeyFromTheRecordedSchema: a MySQL
// binlog version carries the primary key only, so a UNIQUE key's collation
// change between two versions is seen through the segment's recorded schema
// (measured: without it the broker refused nothing and merged 'A' into 'a').
func TestRefuseKeyCollationHistory_UniqueKeyFromTheRecordedSchema(t *testing.T) {
	version := func(at int, collation string) ir.SchemaSnapshot {
		s := hsnap(at, collation)
		s.IR.PrimaryKey = &ir.Index{Columns: []ir.IndexColumn{{Column: "id"}}}
		return s
	}
	snaps := []ir.SchemaSnapshot{version(10, "utf8mb4_bin"), version(30, "utf8mb4_0900_ai_ci")}
	recorded := &ir.Schema{Tables: []*ir.Table{{
		Name:    "kd",
		Columns: keyTable("utf8mb4_0900_ai_ci").Columns,
		Indexes: []*ir.Index{{Name: "k", Unique: true, Columns: []ir.IndexColumn{{Column: "k"}}}},
	}}}
	if err := refuseKeyCollationHistory("b1", "test", recorded, snaps); !errors.Is(err, ir.ErrKeyCollationMerge) {
		t.Fatalf("err = %v; want %v (k is a UNIQUE key in the recorded schema)", err, ir.ErrKeyCollationMerge)
	}
	if err := refuseKeyCollationHistory("b1", "test", nil, snaps); err != nil {
		t.Fatalf("err = %v with no recorded schema; k is no key in either version", err)
	}
}

// orderingEngine is an ir.Engine (embedded nil, never called) that orders
// positions, under a chosen name.
type orderingEngine struct {
	ir.Engine
	tokenOrderer
	name string
}

func (e orderingEngine) Name() string { return e.name }

// TestPositionOrdererFor: the injected registry wins; without it a Target of
// the recorded source engine's name stands in, and any other Target does not
// (its decoder would refuse the source's positions).
func TestPositionOrdererFor(t *testing.T) {
	same := orderingEngine{name: "mysql"}
	other := orderingEngine{name: "postgres"}
	if (&ChainRestore{Target: same}).positionOrdererFor("mysql") == nil {
		t.Error("a Target named for the source engine did not stand in")
	}
	if (&ChainRestore{Target: other}).positionOrdererFor("mysql") != nil {
		t.Error("a Target of another engine ordered the source's positions")
	}
	lookup := func(name string) (ir.Engine, bool) { return orderingEngine{name: name}, name == "mysql" }
	if (&ChainRestore{Target: other, SourceEngine: lookup}).positionOrdererFor("mysql") == nil {
		t.Error("the injected source engine was not used")
	}
}

// TestHistoryReplay_Unordered: with no orderer the versions are released up
// front (the previous behaviour) — unless two of one table disagree on a key
// column's collation, which refuses before anything is replayed.
func TestHistoryReplay_Unordered(t *testing.T) {
	same := []ir.SchemaSnapshot{hsnap(10, "utf8mb4_bin"), hsnap(30, "utf8mb4_bin")}
	h, err := newHistoryReplay("b1", "test", nil, nil, same)
	if err != nil {
		t.Fatalf("same key collation refused: %v", err)
	}
	if got, want := replayOrder(t, h, tx(15, 16, 17)), "S10:utf8mb4_bin S30:utf8mb4_bin B15 I16 C17"; got != want {
		t.Fatalf("replayed order\n got: %s\nwant: %s", got, want)
	}
	_, err = newHistoryReplay("b1", "test", nil, nil, []ir.SchemaSnapshot{hsnap(10, "utf8mb4_bin"), hsnap(30, "utf8mb4_0900_ai_ci")})
	if !errors.Is(err, ir.ErrKeyCollationMerge) || !strings.Contains(err.Error(), keyCollationHistoryUnorderedMarker) {
		t.Fatalf("err = %v; want %v naming %s", err, ir.ErrKeyCollationMerge, keyCollationHistoryUnorderedMarker)
	}
}
