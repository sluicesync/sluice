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
	irbackup "sluicesync.dev/sluice/internal/ir/backup"
	"sluicesync.dev/sluice/internal/pipeline/lineage"
)

// varcharKey is a utf8mb4 VARCHAR(20) under collation.
func varcharKey(collation string) ir.Type {
	return ir.Varchar{Length: 20, Charset: "utf8mb4", Collation: collation}
}

// TestRefuseKeyCollationHistory_ComparesConsecutiveVersions is the
// reviewer's probe (review of df690b30, finding 2): a column added in V1
// with a `_bin` UNIQUE key and loosened in V2. Compared only with V0, which
// lacks the column, the change was invisible; each version is now compared
// with the one before it.
func TestRefuseKeyCollationHistory_ComparesConsecutiveVersions(t *testing.T) {
	v := func(at int, k2 ir.Type) ir.SchemaSnapshot {
		tbl := &ir.Table{
			Name: "kd", Columns: []*ir.Column{{Name: "id", Type: ir.Integer{Width: 32}}},
			PrimaryKey: &ir.Index{Columns: []ir.IndexColumn{{Column: "id"}}},
		}
		if k2 != nil {
			tbl.Columns = append(tbl.Columns, &ir.Column{Name: "k2", Type: k2})
			tbl.Indexes = []*ir.Index{{Name: "uk2", Unique: true, Columns: []ir.IndexColumn{{Column: "k2"}}}}
		}
		return ir.SchemaSnapshot{Position: hpos(at), Table: "kd", IR: tbl}
	}
	snaps := []ir.SchemaSnapshot{v(10, nil), v(20, varcharKey("utf8mb4_bin")), v(30, varcharKey("utf8mb4_0900_ai_ci"))}
	if err := refuseKeyCollationHistory("b1", "test", nil, snaps); !errors.Is(err, ir.ErrKeyCollationMerge) {
		t.Fatalf("err = %v; want %v (V1 and V2 disagree on k2)", err, ir.ErrKeyCollationMerge)
	}
}

// TestRefuseKeyCollationHistory_DeltaAndRecordedSchema: a window whose delta
// changes a key's collation (a loosened key), and a version that disagrees
// with the segment's recorded schema (a drain-commit, recorded with its
// parent's schema and no delta), both refuse for a replay that cannot place
// its versions; a window that changes nothing does not.
func TestRefuseKeyCollationHistory_DeltaAndRecordedSchema(t *testing.T) {
	binT, ciT := keyTable("utf8mb4_bin"), keyTable("utf8mb4_0900_ai_ci")
	loosened := &irbackup.Manifest{
		Schema:      &ir.Schema{Tables: []*ir.Table{ciT}},
		SchemaDelta: []*irbackup.SchemaDeltaEntry{{Kind: irbackup.SchemaDeltaAlterTable, Table: "kd", Before: binT, After: ciT}},
	}
	if err := refuseKeyCollationHistory("b1", "test", loosened, nil); !errors.Is(err, ir.ErrKeyCollationMerge) {
		t.Fatalf("loosening delta: err = %v; want %v", err, ir.ErrKeyCollationMerge)
	}
	drain := &irbackup.Manifest{Schema: &ir.Schema{Tables: []*ir.Table{ciT}}}
	if err := refuseKeyCollationHistory("b1", "test", drain, []ir.SchemaSnapshot{hsnap(10, "utf8mb4_bin")}); !errors.Is(err, ir.ErrKeyCollationMerge) {
		t.Fatalf("version against the recorded schema: err = %v; want %v", err, ir.ErrKeyCollationMerge)
	}
	if err := refuseKeyCollationHistory("b1", "test", drain, []ir.SchemaSnapshot{hsnap(10, "utf8mb4_0900_ai_ci")}); err != nil {
		t.Fatalf("no change: err = %v", err)
	}
}

// TestWindowStartVersions: the previous link's shape is seeded, at the
// window's start, only for a table whose key collation the window changes
// (KEY-COLLATION-LOOSENED-WINDOW).
func TestWindowStartVersions(t *testing.T) {
	binT, ciT := keyTable("utf8mb4_bin"), keyTable("utf8mb4_0900_ai_ci")
	start := ir.Position{Engine: "test", Token: "5"}
	parent := &ir.Schema{Tables: []*ir.Table{binT}}
	loosened := &irbackup.Manifest{
		StartPosition: start,
		SchemaDelta:   []*irbackup.SchemaDeltaEntry{{Kind: irbackup.SchemaDeltaAlterTable, Table: "kd", Before: binT, After: ciT}},
	}
	seeds := windowStartVersions(parent, loosened, []ir.SchemaSnapshot{hsnap(10, "utf8mb4_0900_ai_ci")})
	if len(seeds) != 1 || seeds[0].IR != binT || seeds[0].Position != start {
		t.Fatalf("seeds = %+v; want the parent's `_bin` shape at the window's start", seeds)
	}
	unchanged := &irbackup.Manifest{StartPosition: start}
	if seeds := windowStartVersions(parent, unchanged, []ir.SchemaSnapshot{hsnap(10, "utf8mb4_bin")}); len(seeds) != 0 {
		t.Fatalf("seeds = %+v; want none for a window that changes no key collation", seeds)
	}
	if seeds := windowStartVersions(nil, loosened, nil); len(seeds) != 0 {
		t.Fatalf("seeds = %+v; want none with no previous link", seeds)
	}
	// The seed names what the parent recorded (none, for MySQL) — not a name
	// borrowed from a version only some windows record (Bug 306); the
	// replay gives it the rows' namespace.
	inDB := hsnap(10, "utf8mb4_0900_ai_ci")
	inDB.Schema = "db"
	if seeds := windowStartVersions(parent, loosened, []ir.SchemaSnapshot{inDB}); len(seeds) != 1 || seeds[0].Schema != "" {
		t.Fatalf("seeds = %+v; want one naming the parent's (empty) namespace", seeds)
	}
	// A delta of a same-named table in another namespace changes nothing here.
	inA, inB := keyTable("utf8mb4_bin"), keyTable("utf8mb4_bin")
	inA.Schema, inB.Schema = "a", "b"
	otherDelta := &irbackup.Manifest{
		StartPosition: start,
		SchemaDelta:   []*irbackup.SchemaDeltaEntry{{Kind: irbackup.SchemaDeltaAlterTable, Schema: "b", Table: "kd", Before: binT, After: ciT}},
	}
	seeds = windowStartVersions(&ir.Schema{Tables: []*ir.Table{inA, inB}}, otherDelta, nil)
	if len(seeds) != 1 || seeds[0].Schema != "b" {
		t.Fatalf("seeds = %+v; want only b.kd", seeds)
	}
}

// TestKeyCollationHistory_AVersionRecordingNoCollationIsNoEvidence is the
// review-of-d6ecb939 finding 1 probe: a VStream version records no
// collation (empty Collation) while the schema reader recorded one for the
// same primary key. Read the wrong way round, that was a change, and the
// broker refused every VStream window of a character-keyed table; it must
// neither refuse nor seed.
func TestKeyCollationHistory_AVersionRecordingNoCollationIsNoEvidence(t *testing.T) {
	recorded := keyTable("utf8mb4_0900_ai_ci")
	vstream := hsnap(10, "")
	m := &irbackup.Manifest{Schema: &ir.Schema{Tables: []*ir.Table{recorded}}, StartPosition: hpos(1)}
	if err := refuseKeyCollationHistory("b1", "test", m, []ir.SchemaSnapshot{vstream}); err != nil {
		t.Fatalf("a version with no collation against the recorded schema: err = %v; want nil", err)
	}
	if err := refuseKeyCollationHistory("b1", "test", m, []ir.SchemaSnapshot{hsnap(10, ""), hsnap(20, "")}); err != nil {
		t.Fatalf("two versions with no collation: err = %v; want nil", err)
	}
	if err := refuseKeyCollationHistory("b1", "test", m, []ir.SchemaSnapshot{hsnap(10, "utf8mb4_0900_ai_ci"), hsnap(20, "")}); err != nil {
		t.Fatalf("a recorded version followed by one with no collation: err = %v; want nil", err)
	}
	if seeds := windowStartVersions(m.Schema, m, []ir.SchemaSnapshot{vstream}); len(seeds) != 0 {
		t.Fatalf("seeds = %+v; want none for a version that records no collation", seeds)
	}
	// The real change is still seen, with the version as the after side.
	if err := refuseKeyCollationHistory("b1", "test", m, []ir.SchemaSnapshot{hsnap(10, "utf8mb4_bin")}); !errors.Is(err, ir.ErrKeyCollationMerge) {
		t.Fatalf("a version that records another collation: err = %v; want %v", err, ir.ErrKeyCollationMerge)
	}
}

// TestHistoryReplay_StrandedAgreeingVersionIsDropped (review of d6ecb939,
// finding 6): a stranded version that agrees with a later version released
// past it is dropped, not released after it — which would leave the
// applier's active schema on the older shape for the rest of the window.
func TestHistoryReplay_StrandedAgreeingVersionIsDropped(t *testing.T) {
	snaps := []ir.SchemaSnapshot{hsnap(10, "utf8mb4_bin"), hsnap(30, "utf8mb4_bin")}
	h, err := newHistoryReplay("b1", "test", strandingOrderer{strand: "10"}, nil, snaps)
	if err != nil {
		t.Fatal(err)
	}
	if got, want := replayOrder(t, h, append(tx(15, 16, 17), tx(35, 36, 37)...)), "B15 I16 C17 S30:utf8mb4_bin B35 I36 C37"; got != want {
		t.Fatalf("replayed order\n got: %s\nwant: %s", got, want)
	}
}

// TestWindowKeyCollationRemedy (review of d6ecb939, finding 4): a guard
// refusal inside a window that changes a key's collation names the remedy
// that converges — a fresh full backup — because restarting the restore
// re-applies the change ahead of the same rows; any other error is left as
// it is.
//
// Bug 308: the restore's remedy REPLACES the applier's — whose "ALTER the
// key and restart, or re-copy with --restart-from-scratch" cannot run for
// `restore` — rather than following it.
func TestRestoreKeyCollationRemedy(t *testing.T) {
	binT, ciT := keyTable("utf8mb4_bin"), keyTable("utf8mb4_0900_ai_ci")
	// The applier's own refusal, worded for `sync start` (the MySQL engine's
	// text, abridged), wrapped the way the engine wraps it.
	engine := fmt.Errorf("mysql: applier: insert into db.kd: %w", &ir.KeyCollationMergeError{
		Schema: "db", Table: "kd", Cause: errors.New("1690"),
		Remedy: "Give the target's key column(s) `k` COLLATE utf8mb4_bin (`ALTER TABLE …`) and restart, or re-copy the " +
			"table: `sluice sync start --restart-from-scratch` recreates it with that collation",
		Needs: "`k` CHARACTER SET utf8mb4 COLLATE utf8mb4_bin",
	})
	loosened := &lineage.SegmentRecord{ManifestRecord: lineage.ManifestRecord{Manifest: &irbackup.Manifest{
		Schema:      &ir.Schema{Tables: []*ir.Table{ciT}},
		SchemaDelta: []*irbackup.SchemaDeltaEntry{{Kind: irbackup.SchemaDeltaAlterTable, Table: "kd", Before: binT, After: ciT}},
	}}}
	quiet := &lineage.SegmentRecord{ManifestRecord: lineage.ManifestRecord{Manifest: &irbackup.Manifest{Schema: &ir.Schema{Tables: []*ir.Table{ciT}}}}}
	restoreRunsNone := []string{"--restart-from-scratch", "and restart", "sync start"}

	got := restoreKeyCollationRemedy(loosened, engine)
	if !errors.Is(got, ir.ErrKeyCollationMerge) {
		t.Fatalf("loosening window: err = %v; want it still %v", got, ir.ErrKeyCollationMerge)
	}
	remedy := strings.SplitN(got.Error(), "Refusing before it is applied (the whole statement rolled back). ", 2)[1]
	if !strings.HasPrefix(remedy, "Take a fresh full backup after the key change") {
		t.Errorf("loosening window: the remedy does not lead with the fresh full backup:\n%s", remedy)
	}
	for _, f := range restoreRunsNone {
		if strings.Contains(got.Error(), f) {
			t.Errorf("loosening window: the restore's refusal says %q, which `restore` cannot run:\n%v", f, got)
		}
	}

	got = restoreKeyCollationRemedy(quiet, engine)
	if !errors.Is(got, ir.ErrKeyCollationMerge) || !strings.Contains(got.Error(), "Restore the chain into an empty target whose key column(s) carry `k` CHARACTER SET utf8mb4 COLLATE utf8mb4_bin") {
		t.Fatalf("a window that changes no key collation: err = %v; want the restore's own remedy naming what the key needs", got)
	}
	for _, f := range restoreRunsNone {
		if strings.Contains(got.Error(), f) {
			t.Errorf("no window change: the restore's refusal says %q, which `restore` cannot run:\n%v", f, got)
		}
	}

	// A KEY-COLLATION-MERGE that carries no engine remedy (the guard could not
	// be built) keeps its own text; inside such a window the fresh full follows it.
	bare := fmt.Errorf("apply: %w: KEY-COLLATION-GUARD-UNSUPPORTED", ir.ErrKeyCollationMerge)
	if got := restoreKeyCollationRemedy(loosened, bare); !strings.HasPrefix(got.Error(), bare.Error()) || !strings.Contains(got.Error(), "Take a fresh full backup") {
		t.Fatalf("bare refusal in a loosening window: err = %v", got)
	}
	if got := restoreKeyCollationRemedy(quiet, bare); got.Error() != bare.Error() {
		t.Fatalf("bare refusal, no window change: err = %v; want it unchanged", got)
	}
	other := errors.New("boom")
	if got := restoreKeyCollationRemedy(loosened, other); got.Error() != other.Error() {
		t.Fatalf("another error: err = %v; want it unchanged", got)
	}
}

// strandingOrderer places every version except the one anchored at strand.
type strandingOrderer struct{ strand string }

func (o strandingOrderer) PositionAtOrAfter(p, anchor ir.Position) (bool, error) {
	if anchor.Token == o.strand {
		return false, nil
	}
	return tokenOrderer{}.PositionAtOrAfter(p, anchor)
}

// TestHistoryReplay_StrandedVersionRefuses (finding 6): a version the orderer
// never places (file/pos with another ServerUUID, a VStream shard missing)
// while a LATER version of the same table is released means the window is
// unorderable there; when the two disagree on a key collation, it refuses
// instead of holding the earlier one to the window's end.
func TestHistoryReplay_StrandedVersionRefuses(t *testing.T) {
	snaps := []ir.SchemaSnapshot{hsnap(10, "utf8mb4_bin"), hsnap(30, "utf8mb4_0900_ai_ci")}
	h, err := newHistoryReplay("b1", "test", strandingOrderer{strand: "10"}, nil, snaps)
	if err != nil {
		t.Fatal(err)
	}
	push := func(ir.Change) error { return nil }
	var refused error
	for _, c := range append(tx(15, 16, 17), tx(35, 36, 37)...) {
		if refused = h.before(c, push); refused != nil {
			break
		}
	}
	if !errors.Is(refused, ir.ErrKeyCollationMerge) || !strings.Contains(refused.Error(), keyCollationHistoryUnorderedMarker) {
		t.Fatalf("err = %v; want %v naming %s", refused, ir.ErrKeyCollationMerge, keyCollationHistoryUnorderedMarker)
	}
	same := []ir.SchemaSnapshot{hsnap(10, "utf8mb4_bin"), hsnap(30, "utf8mb4_bin")}
	h, err = newHistoryReplay("b1", "test", strandingOrderer{strand: "10"}, nil, same)
	if err != nil {
		t.Fatal(err)
	}
	for _, c := range append(tx(15, 16, 17), tx(35, 36, 37)...) {
		if err := h.before(c, push); err != nil {
			t.Fatalf("versions that agree refused: %v", err)
		}
	}
}

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
// the stream the applier would see. A version or row naming a namespace
// renders it after "@".
func replayOrder(t *testing.T, h *historyReplay, changes []ir.Change) string {
	t.Helper()
	got, err := replay(h, changes)
	if err != nil {
		t.Fatal(err)
	}
	return got
}

// replay is [replayOrder] returning the error instead.
func replay(h *historyReplay, changes []ir.Change) (string, error) {
	var out []string
	at := func(namespace string) string {
		if namespace == "" {
			return ""
		}
		return "@" + namespace
	}
	push := func(c ir.Change) error {
		switch v := c.(type) {
		case ir.SchemaSnapshot:
			out = append(out, "S"+v.Position.Token+at(v.Schema)+":"+v.IR.Columns[1].Type.(ir.Varchar).Collation)
		case ir.TxBegin:
			out = append(out, "B"+v.Position.Token)
		case ir.TxCommit:
			out = append(out, "C"+v.Position.Token)
		case ir.Insert:
			out = append(out, "I"+v.Position.Token+at(v.Schema))
		default:
			out = append(out, fmt.Sprintf("%T", c))
		}
		return nil
	}
	if err := h.start(push); err != nil {
		return strings.Join(out, " "), err
	}
	for _, c := range changes {
		if err := h.admit(c, push, push); err != nil {
			return strings.Join(out, " "), err
		}
	}
	err := h.finish(push, push)
	return strings.Join(out, " "), err
}

func tx(begin, row, commit int) []ir.Change {
	return txIn("", begin, row, commit)
}

// txIn is [tx] with its row naming namespace.
func txIn(namespace string, begin, row, commit int) []ir.Change {
	return []ir.Change{
		ir.TxBegin{Position: hpos(begin)},
		ir.Insert{Position: hpos(row), Schema: namespace, Table: "kd"},
		ir.TxCommit{Position: hpos(commit)},
	}
}

// TestHistoryReplay_VersionTakesTheRowsNamespace pins Bug 306 at the replay
// seam. A MySQL full manifest records its tables with no namespace, so the
// window-start version names none, while every row the binlog recorded
// names the database — and the applier files a version, and looks a row's
// up, by namespace and table. The version must reach the applier naming
// the database, ahead of the first transaction, with or without a version of
// the window to borrow a name from.
func TestHistoryReplay_VersionTakesTheRowsNamespace(t *testing.T) {
	seed := hsnap(5, "utf8mb4_bin") // a window-start version: no namespace
	cases := []struct {
		name    string
		snaps   []ir.SchemaSnapshot
		orderer ir.PositionOrderer
		changes []ir.Change
		want    string
	}{
		{
			name: "no version of the table in the window (the loosening is its last event)", snaps: []ir.SchemaSnapshot{seed},
			orderer: tokenOrderer{}, changes: txIn("db", 10, 11, 12),
			want: "S5@db:utf8mb4_bin B10 I11@db C12",
		},
		{
			name: "an empty transaction first: it passes, the version waits for a row", snaps: []ir.SchemaSnapshot{seed},
			orderer: tokenOrderer{},
			changes: append([]ir.Change{ir.TxBegin{Position: hpos(8)}, ir.TxCommit{Position: hpos(9)}}, txIn("db", 10, 11, 12)...),
			want:    "B8 C9 S5@db:utf8mb4_bin B10 I11@db C12",
		},
		{
			name: "a row outside any transaction names it", snaps: []ir.SchemaSnapshot{seed},
			orderer: tokenOrderer{}, changes: []ir.Change{ir.Insert{Position: hpos(10), Schema: "db", Table: "kd"}},
			want: "S5@db:utf8mb4_bin I10@db",
		},
		{
			name: "unordered: released up front, once the namespace is known", snaps: []ir.SchemaSnapshot{seed},
			changes: txIn("db", 10, 11, 12),
			want:    "S5@db:utf8mb4_bin B10 I11@db C12",
		},
		{
			name: "a recorded version of the window keeps its own namespace",
			snaps: []ir.SchemaSnapshot{seed, func() ir.SchemaSnapshot {
				s := hsnap(20, "utf8mb4_0900_ai_ci")
				s.Schema = "db"
				return s
			}()},
			orderer: tokenOrderer{}, changes: append(txIn("db", 10, 11, 12), txIn("db", 21, 22, 23)...),
			want: "S5@db:utf8mb4_bin B10 I11@db C12 S20@db:utf8mb4_0900_ai_ci B21 I22@db C23",
		},
		{
			name: "no row at all: the version keeps no namespace (nothing for it to govern)", snaps: []ir.SchemaSnapshot{seed},
			orderer: tokenOrderer{}, changes: []ir.Change{ir.TxBegin{Position: hpos(8)}, ir.TxCommit{Position: hpos(9)}},
			want: "B8 C9 S5:utf8mb4_bin",
		},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			snaps := append([]ir.SchemaSnapshot(nil), c.snaps...)
			h, err := newHistoryReplay("b1", "test", c.orderer, nil, snaps)
			if err != nil {
				t.Fatal(err)
			}
			if got := replayOrder(t, h, c.changes); got != c.want {
				t.Fatalf("replayed order\n got: %s\nwant: %s", got, c.want)
			}
		})
	}
}

// TestHistoryReplay_VersionNamespaceIsChecked: the namespace a version
// naming none takes is the window's first change's, on the premise that a
// flat manifest is a single-namespace stream. A later change of that table
// naming another refuses (KEY-COLLATION-VERSION-NAMESPACE) — the version
// would be filed where that change's lookup does not look.
func TestHistoryReplay_VersionNamespaceIsChecked(t *testing.T) {
	h, err := newHistoryReplay("b1", "test", tokenOrderer{}, nil, []ir.SchemaSnapshot{hsnap(5, "utf8mb4_bin")})
	if err != nil {
		t.Fatal(err)
	}
	other := []ir.Change{ir.TxBegin{Position: hpos(10)}, ir.Insert{Position: hpos(11), Schema: "db", Table: "other"}, ir.TxCommit{Position: hpos(12)}}
	_, err = replay(h, append(other, txIn("db2", 20, 21, 22)...))
	if !errors.Is(err, ir.ErrKeyCollationMerge) || !strings.Contains(err.Error(), keyCollationVersionNamespaceMarker) {
		t.Fatalf("err = %v; want %v naming %s", err, ir.ErrKeyCollationMerge, keyCollationVersionNamespaceMarker)
	}
	h, err = newHistoryReplay("b1", "test", tokenOrderer{}, nil, []ir.SchemaSnapshot{hsnap(5, "utf8mb4_bin")})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := replay(h, append(other, txIn("db", 20, 21, 22)...)); err != nil {
		t.Fatalf("the same namespace refused: %v", err)
	}
}

// TestHistoryReplay_WindowEndVersionsGoLast: the window-end versions
// ([windowEndVersions]) follow every change and pending version, under the
// namespace the window's changes named.
func TestHistoryReplay_WindowEndVersionsGoLast(t *testing.T) {
	h, err := newHistoryReplay("b1", "test", tokenOrderer{}, nil, []ir.SchemaSnapshot{hsnap(5, "utf8mb4_bin")})
	if err != nil {
		t.Fatal(err)
	}
	h.windowEnd = []ir.SchemaSnapshot{hsnap(12, "utf8mb4_0900_ai_ci")}
	if got, want := replayOrder(t, h, txIn("db", 10, 11, 12)), "S5@db:utf8mb4_bin B10 I11@db C12 S12@db:utf8mb4_0900_ai_ci"; got != want {
		t.Fatalf("replayed order\n got: %s\nwant: %s", got, want)
	}
}

// TestWindowEndVersions: one per window-start version, the window's recorded
// shape of its table at the window's end; none without an end position.
func TestWindowEndVersions(t *testing.T) {
	binT, ciT := keyTable("utf8mb4_bin"), keyTable("utf8mb4_0900_ai_ci")
	m := &irbackup.Manifest{EndPosition: hpos(40), Schema: &ir.Schema{Tables: []*ir.Table{ciT}}}
	seeds := []ir.SchemaSnapshot{{Position: hpos(1), Table: "kd", IR: binT}}
	ends := windowEndVersions(m, seeds)
	if len(ends) != 1 || ends[0].IR != ciT || ends[0].Position != hpos(40) {
		t.Fatalf("ends = %+v; want the recorded `_ci` shape at the window's end", ends)
	}
	m.EndPosition = ir.Position{}
	if ends := windowEndVersions(m, seeds); len(ends) != 0 {
		t.Fatalf("ends = %+v; want none without an end position", ends)
	}
}

// TestKeyCollationHistory_ANamespacelessVersionIsTheSameTable is the sibling
// sweep of Bug 306 through the two comparisons keyed by a version's
// namespace: a window-start version naming none and the window's version
// naming the database are one table, so the unordered refusal compares
// them, and so does the stranded-version check.
func TestKeyCollationHistory_ANamespacelessVersionIsTheSameTable(t *testing.T) {
	inDB := func(at int, collation string) ir.SchemaSnapshot {
		s := hsnap(at, collation)
		s.Schema = "db"
		return s
	}
	snaps := []ir.SchemaSnapshot{hsnap(5, "utf8mb4_bin"), inDB(30, "utf8mb4_0900_ai_ci")}
	if err := refuseKeyCollationHistory("b1", "test", nil, snaps); !errors.Is(err, ir.ErrKeyCollationMerge) {
		t.Fatalf("unordered: err = %v; want %v (the two disagree on kd's key)", err, ir.ErrKeyCollationMerge)
	}
	h, err := newHistoryReplay("b1", "test", strandingOrderer{strand: "5"}, nil, append([]ir.SchemaSnapshot(nil), snaps...))
	if err != nil {
		t.Fatal(err)
	}
	var refused error
	for _, c := range append(txIn("db", 31, 32, 33), txIn("db", 35, 36, 37)...) {
		if refused = h.before(c, func(ir.Change) error { return nil }); refused != nil {
			break
		}
	}
	if !errors.Is(refused, ir.ErrKeyCollationMerge) || !strings.Contains(refused.Error(), keyCollationHistoryUnorderedMarker) {
		t.Fatalf("stranded: err = %v; want %v naming %s", refused, ir.ErrKeyCollationMerge, keyCollationHistoryUnorderedMarker)
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
// transaction's rows. UNVERIFIED PREMISE behind the rule: a DDL is never
// inside a source transaction — true of MySQL (implicit commit), false of
// Postgres, whose versions are anchored at LSN 0/0 and released ahead of the
// window instead (GC-44 F23).
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
	if err := refuseKeyCollationHistory("b1", "test", &irbackup.Manifest{Schema: recorded}, snaps); !errors.Is(err, ir.ErrKeyCollationMerge) {
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
