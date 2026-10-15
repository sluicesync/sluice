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
	// A byte-armed key (a Postgres source collation a MySQL target cannot
	// declare): sluice recreates it under the target's default (review of
	// 7ff76a4c, L5). And the Postgres applier's refusal, which names nothing.
	diverges := fmt.Errorf("mysql: applier: insert into db.kd: %w", &ir.KeyCollationMergeError{
		Schema: "db", Table: "kd", Cause: errors.New("1690"), Remedy: "engine text",
		Needs: "`k` a binary collation (for example COLLATE utf8mb4_bin)", RecopyDiverges: true,
	})
	unknown := fmt.Errorf("postgres: applier: %w", &ir.KeyCollationMergeError{Schema: "public", Table: "kd", Cause: errors.New("22P02")})

	// What each command cannot run: `restore` has no --restart-from-scratch
	// and does not resume; the broker has no --exclude-table and is not
	// `sluice restore`.
	runsNone := map[RestoreCommand][]string{
		RestoreCommandRestore:         {"--restart-from-scratch", "and restart", "sync start", "sync from-backup"},
		RestoreCommandBrokerColdStart: {"--restart-from-scratch", "and restart", "sync start", "sluice restore", "--exclude-table"},
	}
	cases := []struct {
		name string
		cmd  RestoreCommand
		link *lineage.SegmentRecord
		err  error
		lead string // the remedy's first words
		has  []string
	}{
		{"restore, window", RestoreCommandRestore, loosened, engine, "Take a fresh full backup after the key change and restore it into an empty target", nil},
		{"broker cold start, window", RestoreCommandBrokerColdStart, loosened, engine, "Take a fresh full backup after the key change, then replay the new chain from its full: `sluice sync from-backup run --reset-target-data --yes`", nil},
		{"restore, recopy aligns", RestoreCommandRestore, quiet, engine, "sluice creates a missing table's key with `k` CHARACTER SET utf8mb4 COLLATE utf8mb4_bin", []string{"`sluice restore --exclude-table kd`", "restore again into the empty target"}},
		{"restore, recopy diverges", RestoreCommandRestore, quiet, diverges, "This table cannot be restored onto this target as it is", []string{"`sluice restore --exclude-table kd`", "a binary collation", "Postgres target"}},
		{"restore, engine cannot tell", RestoreCommandRestore, quiet, unknown, "If this table existed before the restore", []string{"`sluice restore --exclude-table kd`", "If the restore created it, a re-run creates the same key and refuses here again"}},
		{"broker cold start, recopy aligns", RestoreCommandBrokerColdStart, quiet, engine, "This replay drops the chain's tables and creates them itself", []string{"`k` CHARACTER SET utf8mb4 COLLATE utf8mb4_bin"}},
		{"broker cold start, recopy diverges", RestoreCommandBrokerColdStart, quiet, diverges, "This replay drops the chain's tables and creates them itself", []string{"a binary collation", "Postgres target"}},
	}
	for _, c := range cases {
		got := restoreKeyCollationRemedy(c.link, c.cmd, c.err)
		if !errors.Is(got, ir.ErrKeyCollationMerge) {
			t.Fatalf("%s: err = %v; want it still %v", c.name, got, ir.ErrKeyCollationMerge)
		}
		_, remedy, ok := strings.Cut(got.Error(), "(the whole statement rolled back). ")
		if !ok || !strings.HasPrefix(remedy, c.lead) {
			t.Errorf("%s: the remedy does not lead with %q:\n%s", c.name, c.lead, got)
		}
		for _, h := range c.has {
			if !strings.Contains(remedy, h) {
				t.Errorf("%s: the remedy lacks %q:\n%s", c.name, h, remedy)
			}
		}
		for _, f := range runsNone[c.cmd] {
			if strings.Contains(got.Error(), f) {
				t.Errorf("%s: the refusal says %q, which this command cannot run:\n%v", c.name, f, got)
			}
		}
	}

	// A KEY-COLLATION-MERGE that carries no engine remedy (the guard could not
	// be built) keeps its own text; inside such a window the fresh full follows it.
	bare := fmt.Errorf("apply: %w: KEY-COLLATION-GUARD-UNSUPPORTED", ir.ErrKeyCollationMerge)
	if got := restoreKeyCollationRemedy(loosened, RestoreCommandRestore, bare); !strings.HasPrefix(got.Error(), bare.Error()) || !strings.Contains(got.Error(), "Take a fresh full backup") {
		t.Fatalf("bare refusal in a loosening window: err = %v", got)
	}
	if got := restoreKeyCollationRemedy(quiet, RestoreCommandRestore, bare); got.Error() != bare.Error() {
		t.Fatalf("bare refusal, no window change: err = %v; want it unchanged", got)
	}
	other := errors.New("boom")
	if got := restoreKeyCollationRemedy(loosened, RestoreCommandRestore, other); got.Error() != other.Error() {
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

	// Review of 7ff76a4c: the ADD COLUMN fill both capture lanes append at the
	// window's end names no namespace (the flat manifest's). It neither binds
	// nor contradicts: after a named row it passes and the version keeps the
	// row's namespace.
	fresh := func() *historyReplay {
		t.Helper()
		h, err := newHistoryReplay("b1", "test", tokenOrderer{}, nil, []ir.SchemaSnapshot{hsnap(5, "utf8mb4_bin")})
		if err != nil {
			t.Fatal(err)
		}
		return h
	}
	got, err := replay(fresh(), append(txIn("db", 10, 11, 12), txIn("", 20, 21, 22)...))
	if err != nil || got != "S5@db:utf8mb4_bin B10 I11@db C12 B20 I21 C22" {
		t.Fatalf("a fill row after a named row: %q, err = %v; want it to pass with the version under db", got, err)
	}
	// A fill-only window: the version goes out under none, ahead of the fill.
	got, err = replay(fresh(), txIn("", 20, 21, 22))
	if err != nil || got != "S5:utf8mb4_bin B20 I21 C22" {
		t.Fatalf("a fill-only window: %q, err = %v", got, err)
	}
	// A named row of the table AFTER the version went out under none would
	// miss it: that refuses.
	_, err = replay(fresh(), append(txIn("", 20, 21, 22), txIn("db", 30, 31, 32)...))
	if !errors.Is(err, ir.ErrKeyCollationMerge) || !strings.Contains(err.Error(), keyCollationVersionNamespaceMarker) {
		t.Fatalf("a named row after the version went out under none: err = %v; want %s", err, keyCollationVersionNamespaceMarker)
	}
	// Review of 5f1f19a6, LOW 1: a NAMED version of the table released after
	// the unnamed one supersedes it — the named row's lookup finds that one —
	// so the named row passes.
	named := hsnap(25, "utf8mb4_0900_ai_ci")
	named.Schema = "db"
	h, err = newHistoryReplay("b1", "test", tokenOrderer{}, nil, []ir.SchemaSnapshot{hsnap(5, "utf8mb4_bin"), named})
	if err != nil {
		t.Fatal(err)
	}
	got, err = replay(h, append(txIn("", 20, 21, 22), txIn("db", 30, 31, 32)...))
	if err != nil || got != "S5:utf8mb4_bin B20 I21 C22 S25@db:utf8mb4_0900_ai_ci B30 I31@db C32" {
		t.Fatalf("a named version superseding the unnamed one: %q, err = %v; want the named row to pass", got, err)
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

// TestWindowEndVersions: the window's recorded shape of a seeded table at
// the window's end, unless the window's own versions already end on it.
//   - no recorded version: one (Bug 306);
//   - a recorded version that compares the key differently from the end
//     shape — a row, then the key's collation changed as the table's last
//     event, recorded lazily so no version follows: one (review of
//     5f1f19a6; the first cut of L3 skipped it and left the earlier shape
//     active);
//   - a recorded version that already compares as the end shape: none;
//   - no schema delta (a drain-commit, which records its PARENT's schema):
//     none, whatever was recorded (review of 7ff76a4c, L3);
//   - no end position: none.
func TestWindowEndVersions(t *testing.T) {
	binT, ciT := keyTable("utf8mb4_bin"), keyTable("utf8mb4_0900_ai_ci")
	delta := []*irbackup.SchemaDeltaEntry{{Kind: irbackup.SchemaDeltaAlterTable, Table: "kd", Before: binT, After: ciT}}
	m := &irbackup.Manifest{EndPosition: hpos(40), Schema: &ir.Schema{Tables: []*ir.Table{ciT}}, SchemaDelta: delta}
	seeds := []ir.SchemaSnapshot{{Position: hpos(1), Table: "kd", IR: binT}}
	inDB := func(collation string) ir.SchemaSnapshot {
		s := hsnap(20, collation)
		s.Schema = "db"
		return s
	}
	ends := windowEndVersions(m, seeds, nil)
	if len(ends) != 1 || ends[0].IR != ciT || ends[0].Position != hpos(40) {
		t.Fatalf("no recorded version: ends = %+v; want the recorded `_ci` shape at the window's end", ends)
	}
	if ends := windowEndVersions(m, seeds, []ir.SchemaSnapshot{inDB("utf8mb4_bin")}); len(ends) != 1 || ends[0].IR != ciT {
		t.Fatalf("a recorded `_bin` version before the change: ends = %+v; want the `_ci` end shape", ends)
	}
	if ends := windowEndVersions(m, seeds, []ir.SchemaSnapshot{inDB("utf8mb4_0900_ai_ci")}); len(ends) != 0 {
		t.Fatalf("a recorded version already on the end shape: ends = %+v; want none", ends)
	}
	drain := &irbackup.Manifest{EndPosition: hpos(40), Schema: &ir.Schema{Tables: []*ir.Table{binT}}}
	if ends := windowEndVersions(drain, seeds, []ir.SchemaSnapshot{inDB("utf8mb4_0900_ai_ci")}); len(ends) != 0 {
		t.Fatalf("no schema delta (the parent's schema recorded): ends = %+v; want none", ends)
	}
	m.EndPosition = ir.Position{}
	if ends := windowEndVersions(m, seeds, nil); len(ends) != 0 {
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

// TestRefuseUnreplayableKeyCollationHistory_NotEstablishedKey pins the broker
// half of KEY-COLLATION-PG-LOOSENED-REPLAY (review of the first cut, finding
// 2). A `backup incremental` taken after a Postgres key was tightened, written
// and loosened back records no delta and only versions whose key collation is
// not established (named, no determinism). A guarded replay compares that key
// as bytes; the broker arms nothing, so it must refuse. A version whose key is
// established, and a not-established column outside every key, do not.
func TestRefuseUnreplayableKeyCollationHistory_NotEstablishedKey(t *testing.T) {
	link := func(k, v ir.Type) *lineage.SegmentRecord {
		tbl := &ir.Table{
			Schema: "public", Name: "kd",
			Columns:    []*ir.Column{{Name: "k", Type: k}, {Name: "v", Type: v}},
			PrimaryKey: &ir.Index{Columns: []ir.IndexColumn{{Column: "k"}}},
		}
		payload, err := ir.MarshalTable(tbl)
		if err != nil {
			t.Fatal(err)
		}
		return &lineage.SegmentRecord{ManifestRecord: lineage.ManifestRecord{Manifest: &irbackup.Manifest{
			SchemaHistory: []*irbackup.SchemaHistoryEntry{{Schema: "public", Table: "kd", AnchorPosition: ir.Position{Token: "0/0"}, TableJSON: payload}},
		}}}
	}
	ci := ir.Text{Collation: "ci", Determinism: ir.CollationNonDeterministic}
	unest := ir.Text{Collation: "ci"}
	err := RefuseUnreplayableKeyCollationHistory(link(unest, ci), "test")
	if !errors.Is(err, ir.ErrKeyCollationMerge) || !strings.Contains(err.Error(), keyCollationHistoryUnorderedMarker) {
		t.Fatalf("not-established key: err = %v; want %s", err, keyCollationHistoryUnorderedMarker)
	}
	if err := RefuseUnreplayableKeyCollationHistory(link(ci, ci), "test"); err != nil {
		t.Fatalf("established key: err = %v; want nil", err)
	}
	if err := RefuseUnreplayableKeyCollationHistory(link(ci, unest), "test"); err != nil {
		t.Fatalf("not-established non-key column: err = %v; want nil", err)
	}
}

// TestHistoryReplay_VersionsOutOfPositionOrder pins
// KEY-COLLATION-STRANDED-VERSION-ORDER (and the reachable shape of
// SEEDED-FLAG-SYNTHESIZED-CARRY): a window whose versions of one table are
// listed against position order — the later one first — refuses
// KEY-COLLATION-HISTORY-UNORDERED before anything is replayed when the
// window changes a key collation, because released in list order the
// applier would end on the older shape and the stranded drop could discard
// the version a later one contradicts. Controls: the same versions in order,
// an out-of-order pair that agrees (no key collation change: replayed as
// before), another table's version in between (consecutive is per table),
// a pair the orderer cannot place either way (left to the stranded check),
// and a positionless version.
func TestHistoryReplay_VersionsOutOfPositionOrder(t *testing.T) {
	other := func(at int) ir.SchemaSnapshot {
		s := hsnap(at, "utf8mb4_bin")
		s.Table = "other"
		s.IR = &ir.Table{Name: "other", Columns: []*ir.Column{{Name: "id", Type: ir.Integer{Width: 32}}}}
		return s
	}
	refused := func(snaps []ir.SchemaSnapshot, orderer ir.PositionOrderer) error {
		_, err := newHistoryReplay("b1", "test", orderer, nil, snaps)
		return err
	}

	for name, snaps := range map[string][]ir.SchemaSnapshot{
		"later version listed first":               {hsnap(30, "utf8mb4_0900_ai_ci"), hsnap(10, "utf8mb4_bin")},
		"out of order behind another table's":      {hsnap(30, "utf8mb4_0900_ai_ci"), other(20), hsnap(10, "utf8mb4_bin")},
		"a stream-start version after a later one": {hsnap(10, "utf8mb4_bin"), hsnap(30, "utf8mb4_0900_ai_ci"), hsnap(5, "utf8mb4_bin")},
	} {
		err := refused(snaps, tokenOrderer{})
		if !errors.Is(err, ir.ErrKeyCollationMerge) || !strings.Contains(err.Error(), keyCollationHistoryUnorderedMarker) ||
			!strings.Contains(err.Error(), "not in position order") {
			t.Errorf("%s: err = %v; want %v naming %s and the order", name, err, ir.ErrKeyCollationMerge, keyCollationHistoryUnorderedMarker)
		}
	}

	for name, tc := range map[string]struct {
		snaps   []ir.SchemaSnapshot
		orderer ir.PositionOrderer
	}{
		"in order":                        {[]ir.SchemaSnapshot{hsnap(10, "utf8mb4_bin"), hsnap(30, "utf8mb4_0900_ai_ci")}, tokenOrderer{}},
		"equal anchors (Postgres 0/0)":    {[]ir.SchemaSnapshot{hsnap(10, "utf8mb4_bin"), hsnap(10, "utf8mb4_0900_ai_ci")}, tokenOrderer{}},
		"out of order but agreeing":       {[]ir.SchemaSnapshot{hsnap(30, "utf8mb4_bin"), hsnap(10, "utf8mb4_bin")}, tokenOrderer{}},
		"another table out of order":      {[]ir.SchemaSnapshot{other(30), hsnap(10, "utf8mb4_bin"), other(5)}, tokenOrderer{}},
		"unplaceable either way":          {[]ir.SchemaSnapshot{hsnap(30, "utf8mb4_0900_ai_ci"), hsnap(10, "utf8mb4_bin")}, strandingOrderer{strand: "10"}},
		"a positionless version in front": {[]ir.SchemaSnapshot{{Table: "kd", IR: keyTable("utf8mb4_0900_ai_ci")}, hsnap(10, "utf8mb4_bin")}, tokenOrderer{}},
	} {
		if err := refused(tc.snaps, tc.orderer); err != nil {
			t.Errorf("%s: refused: %v", name, err)
		}
	}
}
