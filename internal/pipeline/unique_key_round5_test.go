// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package pipeline

import (
	"context"
	"database/sql"
	"errors"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"

	"sluicesync.dev/sluice/internal/engines"
	"sluicesync.dev/sluice/internal/ir"
	"sluicesync.dev/sluice/internal/pipeline/migcore"
)

// TestUniqueKeyDoor_AcceptanceFingerprint pins round-6 MEDIUM-A: an
// acceptance carries the key's shape as a fingerprint in the flag value, so
// it is honoured, at open and mid-stream, only while the key hashes to it. A
// key narrowed under its name refuses mid-stream AND at the next open (where
// the round-5 cut re-recorded the narrowed shape and passed), and the
// refusal prints the value accepting the key as it is now. An accepted key
// on a table the open door never saw is honoured on its fingerprint alone.
func TestUniqueKeyDoor_AcceptanceFingerprint(t *testing.T) {
	pk := ukDoorKey("PRIMARY", "id")
	wide := ukDoorTable("kd", pk, ukDoorKey("uk", "k", "v"))
	narrow := ukDoorTable("kd", pk, ukDoorKey("uk", "k"))
	kn := ukDoorTable("kn", pk, ukDoorKey("uk", "k"))
	s, _, target := ukDoorStreamer(t, ir.CDCLogicalReplication,
		[]*ir.Table{ukDoorTable("kd", pk), ukDoorTable("kn", pk)}, []*ir.Table{wide})
	s.AcceptTargetOnlyUniqueKeys = []string{
		"kd.uk=" + keyFingerprint(wide.Indexes[0], wide), "kn.uk=" + keyFingerprint(kn.Indexes[0], kn),
	}
	ctx := context.Background()
	if err := s.phaseRefuseTargetOnlyUniqueKeys(ctx, nil, "s1", true); err != nil {
		t.Fatalf("open with the key accepted: %v", err)
	}
	boundary := s.uniqueKeyChecksFor().boundary
	if refusal, err := boundary(ctx, ir.SchemaSnapshot{Table: "kd", IR: ukDoorTable("kd", pk)}); err != nil || refusal != nil {
		t.Fatalf("unchanged accepted key: refusal %v, err %v", refusal, err)
	}

	// Narrowed under its name: refused mid-stream, printing the new value.
	target.schemas["tgt-dsn"].Tables[0] = narrow
	newValue := "kd.uk=" + keyFingerprint(narrow.Indexes[0], narrow)
	refusal, err := boundary(ctx, ir.SchemaSnapshot{Table: "kd", IR: ukDoorTable("kd", pk)})
	if err != nil || refusal == nil || refusal.Keys[0].AcceptedFingerprint != keyFingerprint(wide.Indexes[0], wide) ||
		!strings.Contains(refusal.Error(), "--accept-target-only-unique-key "+newValue) {
		t.Fatalf("narrowed accepted key: refusal %v, err %v", refusal, err)
	}
	// And at the next open: the acceptance does not re-bind to the new shape.
	if err := s.phaseRefuseTargetOnlyUniqueKeys(ctx, nil, "s1", true); !errors.Is(err, ir.ErrSourceUniqueKeyAbsent) {
		t.Fatalf("the next open re-accepted the narrowed key: %v", err)
	}
	// The operator re-accepts the new shape knowingly: it passes.
	s.AcceptTargetOnlyUniqueKeys = []string{newValue, s.AcceptTargetOnlyUniqueKeys[1]}
	if err := s.phaseRefuseTargetOnlyUniqueKeys(ctx, nil, "s1", true); err != nil {
		t.Fatalf("the re-accepted shape refused: %v", err)
	}
	// A table the open door never saw, accepted on its fingerprint.
	target.schemas["tgt-dsn"].Tables = append(target.schemas["tgt-dsn"].Tables, kn)
	boundary = s.uniqueKeyChecksFor().boundary
	if refusal, err := boundary(ctx, ir.SchemaSnapshot{Table: "kn", IR: ukDoorTable("kn", pk)}); err != nil || refusal != nil {
		t.Fatalf("an accepted key on a table the open never saw: refusal %v, err %v", refusal, err)
	}
}

// TestDefinitionFingerprint pins the fingerprint as a codec over the
// engine's definition (round-7 review MEDIUM-1): a fixed value, so a
// restart or a new release computes the same one (a change here invalidates
// every operator's flag), and NO normalisation — the round-6 shape folded
// case and whitespace inside a predicate's literals, so `status='Active'`
// and `'active '` were one key. Identical definitions hash the same, and a
// change of one byte anywhere, a literal's case or inner space included,
// changes it. That each engine's definition carries every dimension (an
// array element's collation, an expression's, a literal, an operator class)
// is pinned on the real servers: TestUniqueKeyDefinition_* in the postgres
// and mysql packages.
func TestDefinitionFingerprint(t *testing.T) {
	def := "index\x1fCREATE UNIQUE INDEX uk ON public.kd USING btree (k)\x1ftrue"
	// sha256 of def, computed independently (printf … | sha256sum).
	if got := definitionFingerprint(def); got != "dad423a5e223" || len(got) != acceptedFingerprintLen {
		t.Fatalf("definitionFingerprint = %q, want the golden dad423a5e223", got)
	}
	if definitionFingerprint(def) != definitionFingerprint(strings.Clone(def)) {
		t.Fatal("identical definitions hashed differently")
	}
	pred := "index\x1fCREATE UNIQUE INDEX uk ON public.kd USING btree (k) WHERE (status = 'Active'::text)\x1ftrue"
	for name, other := range map[string]string{
		"a literal's case":         strings.Replace(pred, "'Active'", "'active'", 1),
		"a literal's inner space":  strings.Replace(pred, "'Active'", "'Active '", 1),
		"identifier case":          strings.Replace(pred, "uk ON", "UK ON", 1),
		"an operator class":        strings.Replace(pred, "(k)", "(k text_pattern_ops)", 1),
		"an expression collation":  strings.Replace(pred, "(k)", `((lower(k)) COLLATE "C")`, 1),
		"a dependent column's row": pred + "\ncolumn\x1f2\x1fk\x1ftext[]\x1f\x1fpublic.nd\x1ffalse\x1ffalse\x1f\x1f",
	} {
		if definitionFingerprint(other) == definitionFingerprint(pred) {
			t.Errorf("%s did not change the fingerprint", name)
		}
	}
	// The refusal shows the first line, separators readable, truncated.
	if got := definitionDisplay(def + "\ncolumn\x1f1"); got != "index | CREATE UNIQUE INDEX uk ON public.kd USING btree (k) | true" {
		t.Errorf("definitionDisplay = %q", got)
	}
	if got := definitionDisplay(strings.Repeat("x", definitionDisplayMax+5)); len([]rune(got)) != definitionDisplayMax+1 {
		t.Errorf("a long definition was not truncated: %d runes", len([]rune(got)))
	}
}

// TestUniqueKeyDoor_DefinitionReadFailsClosed pins the fail-closed branch
// (round-7 review MEDIUM-1): an accepted key whose definition cannot be read
// is never passed on a fallback. A read error fails the check, at open and
// at a boundary; a target engine with no definition surface refuses the key
// as one that cannot be accepted.
func TestUniqueKeyDoor_DefinitionReadFailsClosed(t *testing.T) {
	pk := ukDoorKey("PRIMARY", "id")
	keyed := ukDoorTable("kd", pk, ukDoorKey("uk", "k"))
	s, _, target := ukDoorStreamer(t, ir.CDCLogicalReplication, []*ir.Table{ukDoorTable("kd", pk)}, []*ir.Table{keyed})
	s.AcceptTargetOnlyUniqueKeys = []string{"kd.uk=" + keyFingerprint(keyed.Indexes[0], keyed)}
	ctx := context.Background()
	if err := s.phaseRefuseTargetOnlyUniqueKeys(ctx, nil, "s1", true); err != nil {
		t.Fatalf("open with the key accepted: %v", err)
	}
	boundary := s.uniqueKeyChecksFor().boundary

	target.defErr = errors.New("catalog read failed")
	if err := s.phaseRefuseTargetOnlyUniqueKeys(ctx, nil, "s1", true); err == nil || !strings.Contains(err.Error(), "catalog read failed") {
		t.Fatalf("open with the definition unreadable: %v, want the read error", err)
	}
	if refusal, err := boundary(ctx, ir.SchemaSnapshot{Table: "kd", IR: ukDoorTable("kd", pk)}); err == nil || refusal != nil {
		t.Fatalf("boundary with the definition unreadable: refusal %v, err %v, want the read error", refusal, err)
	}

	target.defErr, target.noDefinitions = nil, true
	err := s.phaseRefuseTargetOnlyUniqueKeys(ctx, nil, "s1", true)
	var refusal *ir.SourceUniqueKeyAbsentError
	if !errors.As(err, &refusal) || refusal.Keys[0].DefinitionUnreadable == "" || refusal.Keys[0].Fingerprint != "" ||
		!strings.Contains(err.Error(), "cannot be accepted") || !strings.Contains(err.Error(), "drop the key on the target") {
		t.Fatalf("a target with no definition surface: %v, want the key refused as unacceptable, naming the remedy", err)
	}

	// An empty definition with no error (round-8 review LOW-2) hashes to one
	// fingerprint for every key: it is refused, never honoured — even for an
	// acceptance carrying exactly that fingerprint.
	empty := func(context.Context, string, string) (string, error) { return "", nil }
	accepted := acceptedUniqueKey{table: "kd", key: "uk", fingerprint: definitionFingerprint("")}
	k, err := judgeUncoveredKey(ctx, empty, "kd", keyed, keyed.Indexes[0], nil, &accepted)
	if err != nil || k == nil || k.DefinitionUnreadable == "" || k.Fingerprint != "" {
		t.Fatalf("an empty definition: key %+v, err %v; want it refused as unreadable", k, err)
	}
}

// TestUniqueKeyDoor_AmbiguousAcceptance pins round-5 LOW-3: on a
// multi-database stream a 2-part acceptance that accepts the key in two
// databases refuses at the open door, naming the 3-part form; the 3-part
// form passes.
func TestUniqueKeyDoor_AmbiguousAcceptance(t *testing.T) {
	pk := ukDoorKey("PRIMARY", "id")
	keyed := ukDoorTable("kd", pk, ukDoorKey("uk", "k"))
	fp := keyFingerprint(keyed.Indexes[0], keyed)
	source := &ukDoorEngine{
		resumeDispatchEngine: &resumeDispatchEngine{name: "src", caps: ir.Capabilities{CDC: ir.CDCBinlog}},
		schemas: map[string]*ir.Schema{
			"src-a": {Tables: []*ir.Table{ukDoorTable("kd", pk)}}, "src-b": {Tables: []*ir.Table{ukDoorTable("kd", pk)}},
		},
	}
	tgt := func() *ukDoorEngine {
		return &ukDoorEngine{
			resumeDispatchEngine: &resumeDispatchEngine{name: "tgt"},
			schemas: map[string]*ir.Schema{
				"tgt-a": {Tables: []*ir.Table{ukDoorTable("kd", pk, ukDoorKey("uk", "k"))}},
				"tgt-b": {Tables: []*ir.Table{ukDoorTable("kd", pk, ukDoorKey("uk", "k"))}},
			},
		}
	}
	scopes := []uniqueKeyDoorScope{
		{namespace: "a", targetNamespace: "a", sourceDSN: "src-a", targetDSN: "tgt-a"},
		{namespace: "b", targetNamespace: "b", sourceDSN: "src-b", targetDSN: "tgt-b"},
	}
	for _, c := range []struct {
		accept []string
		want   string // "" passes
	}{
		{[]string{"kd.uk=" + fp}, "<database>.kd.uk"},
		{[]string{"a.kd.uk=" + fp, "b.kd.uk=" + fp}, ""},
	} {
		s := &Streamer{Source: source, Target: tgt(), AcceptTargetOnlyUniqueKeys: c.accept}
		_, _, err := s.compareUniqueKeys(context.Background(), scopes, uniqueKeyCompare{})
		switch {
		case c.want == "" && err != nil:
			t.Errorf("%v: %v", c.accept, err)
		case c.want != "" && (err == nil || !strings.Contains(err.Error(), c.want)):
			t.Errorf("%v: err %v, want it to name %s", c.accept, err, c.want)
		}
	}
}

// TestAddTableUniqueKeyDoor_CollationAsRead pins round-5 LOW-6: add-table
// judges collation direction on the source table as READ, so an override
// toward a looser collation (`--type-override` to a `_ci` column onto a `_ci`
// target key) cannot hide that the source key is case-sensitive.
func TestAddTableUniqueKeyDoor_CollationAsRead(t *testing.T) {
	loose := map[string]ir.CollationLoosening{"bin": 0, "ci": ir.LooseCase | ir.LooseAccent}
	eng := ukLooseEngine{name: "mysql", loose: loose}
	table := func(coll string) *ir.Table {
		return &ir.Table{
			Name: "kd", Columns: []*ir.Column{{Name: "k", Type: ir.Varchar{Length: 9, Collation: coll}}},
			Indexes: []*ir.Index{ukDoorKey("uk", "k")},
		}
	}
	overridden, asRead, target := table("ci"), table("bin"), map[string]*ir.Table{"kd": table("ci")}
	if err := refuseAddTableTargetOnlyUniqueKeys(overridden, target, eng, eng, nil); err != nil {
		t.Fatalf("the overridden table alone (the old behaviour) refused: %v", err)
	}
	err := refuseAddTableTargetOnlyUniqueKeysAsRead(context.Background(), addTableKeyDoor{
		source: overridden, asRead: asRead, target: target, sourceEngine: eng, targetEngine: eng, define: ukDoorDefiner(target),
	})
	if err == nil || !strings.Contains(err.Error(), "the target's k compares case-") {
		t.Fatalf("the source as read is case-sensitive, the target key folds: err %v", err)
	}
}

// TestUniqueKeyDoorScopeFor pins round-5 LOW-5: a namespace no scope of a
// multi-database stream names is not scope 0 (whose filter would judge
// nothing for the boundary) but no scope, which the boundary check refuses.
func TestUniqueKeyDoorScopeFor(t *testing.T) {
	single := &uniqueKeyDoorState{scopes: []uniqueKeyDoorScope{{}}}
	if i, ok := single.scopeFor("anything"); i != 0 || !ok {
		t.Errorf("single-namespace: %d, %v", i, ok)
	}
	multi := &uniqueKeyDoorState{scopes: []uniqueKeyDoorScope{{namespace: "a"}, {namespace: "b"}}}
	if i, ok := multi.scopeFor("B"); i != 1 || !ok {
		t.Errorf("named namespace: %d, %v", i, ok)
	}
	if _, ok := multi.scopeFor("c"); ok {
		t.Error("an unnamed namespace found a scope")
	}
}

// r5Gate builds a boundary gate over table kd whose check records what it
// judged.
func r5Gate(seedCols map[string]string, seedTypes map[string]map[string]ir.Type) (gate *uniqueKeyBoundaryGate, judgedTables *[]string) {
	var judged []string
	g := newUniqueKeyBoundaryGate(uniqueKeyChecks{
		boundary: func(_ context.Context, s ir.SchemaSnapshot) (*ir.SourceUniqueKeyAbsentError, error) {
			judged = append(judged, s.Table)
			return nil, nil
		},
		colsKey:   func(_, table string) string { return table },
		seedCols:  seedCols,
		seedTypes: seedTypes,
	}, &atomic.Pointer[error]{})
	return g, &judged
}

func r5Snap(table string, cols map[string]ir.Type, order ...string) ir.SchemaSnapshot {
	t := &ir.Table{Name: table}
	for _, n := range order {
		t.Columns = append(t.Columns, &ir.Column{Name: n, Type: cols[n]})
	}
	return ir.SchemaSnapshot{Table: table, IR: t, Position: ir.Position{Token: "0/0"}}
}

// TestUniqueKeyBoundaryGate_EarlyNeedsUnchangedTypes pins round-5 LOW-1: a
// boundary that adds a column AND changes a kept column's collation is not
// judged before its DDL (it could change a target key), but at the late
// call; a purely additive one is judged early.
func TestUniqueKeyBoundaryGate_EarlyNeedsUnchangedTypes(t *testing.T) {
	ci := ir.Varchar{Length: 20, Collation: "utf8mb4_0900_ai_ci"}
	bin := ir.Varchar{Length: 20, Collation: "utf8mb4_bin"}
	seed := r5Snap("kd", map[string]ir.Type{"id": ir.Integer{Width: 32}, "k": ci}, "id", "k")
	g, judged := r5Gate(map[string]string{"kd": columnList(seed.IR)}, map[string]map[string]ir.Type{"kd": columnTypes(seed.IR)})
	early, late := g.beforeDDLHook(), g.hook()
	ctx := context.Background()

	modified := r5Snap("kd", map[string]ir.Type{"id": ir.Integer{Width: 32}, "k": bin, "n": ir.Integer{Width: 32}}, "id", "k", "n")
	if !early(ctx, modified) || len(*judged) != 0 {
		t.Fatalf("an ADD plus a kept column's collation change was judged before its DDL: %v", *judged)
	}
	if !late(ctx, modified) || len(*judged) != 1 {
		t.Fatalf("it was not judged after its DDL: %v", *judged)
	}
	added := r5Snap("kd", map[string]ir.Type{"id": ir.Integer{Width: 32}, "k": bin, "n": ir.Integer{Width: 32}, "m": ir.Integer{Width: 32}}, "id", "k", "n", "m")
	if !early(ctx, added) || len(*judged) != 2 {
		t.Fatalf("a purely additive boundary was not judged early: %v", *judged)
	}
}

// TestFoldingTargetColumn_RealSQLiteNOCASE pins round-6 LOW-A on the SQLite
// reader's real output: a `k TEXT COLLATE NOCASE` key column reads back with
// no collation stamped, and against a Postgres citext target column it is
// weakened (refuses) — NOCASE folds ASCII case only, citext folds Unicode
// case, so no exemption applies.
func TestFoldingTargetColumn_RealSQLiteNOCASE(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "src.db")
	db, err := sql.Open("sqlite", path)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := db.ExecContext(ctx, `CREATE TABLE kd (id INTEGER NOT NULL, k TEXT COLLATE NOCASE PRIMARY KEY, v TEXT)`); err != nil {
		t.Fatal(err)
	}
	_ = db.Close()
	sqliteEng, ok := engines.Get("sqlite")
	if !ok {
		t.Fatal("sqlite engine not registered")
	}
	sr, err := sqliteEng.OpenSchemaReader(ctx, path)
	if err != nil {
		t.Fatal(err)
	}
	schema, err := sr.ReadSchema(ctx)
	migcore.CloseIf(sr)
	if err != nil {
		t.Fatal(err)
	}
	var st *ir.Table
	for _, tbl := range schema.Tables {
		if tbl.Name == "kd" {
			st = tbl
		}
	}
	if st == nil {
		t.Fatal("the SQLite reader did not return kd")
	}
	pg := ukLooseEngine{name: "postgres", loose: map[string]ir.CollationLoosening{"citext": ir.LooseCase}}
	tt := &ir.Table{Name: "kd", Columns: []*ir.Column{{Name: "k", Type: ir.Varchar{Length: 9, Collation: "citext"}}}}
	w := keyColumnWeakening(sqliteEng, pg, st, tt)
	if w == nil || !w("k", "k") {
		t.Fatalf("a real SQLite NOCASE key column against a citext target column was not weakened (source type %#v)",
			columnNamed(st, "k").Type)
	}
}

// TestStampArrayElementDeterminism pins round-6 LOW-B's fold: an array key
// column whose element the schema read left unclassified takes the
// determinism of the collation its unique index compares it under; a
// classified element and a non-key array column are left alone.
func TestStampArrayElementDeterminism(t *testing.T) {
	arr := func(det ir.CollationDeterminism) ir.Type {
		return ir.Array{Element: ir.Text{Size: ir.TextLong, Determinism: det}}
	}
	tbl := &ir.Table{Name: "kd", Columns: []*ir.Column{
		{Name: "tags", Type: arr(ir.CollationDeterminismUnknown)},
		{Name: "set", Type: arr(ir.CollationDeterministic)},
		{Name: "other", Type: arr(ir.CollationDeterminismUnknown)},
	}}
	stampArrayElementDeterminism(tbl, map[string]ir.UniqueKeyFact{
		"kd_tags": {Parts: []ir.IndexColumn{{Column: "tags"}}, PartDeterministic: []bool{false}},
		"kd_set":  {Parts: []ir.IndexColumn{{Column: "set"}}, PartDeterministic: []bool{false}},
	})
	want := map[string]ir.CollationDeterminism{
		"tags": ir.CollationNonDeterministic, "set": ir.CollationDeterministic, "other": ir.CollationDeterminismUnknown,
	}
	for _, c := range tbl.Columns {
		if got := c.Type.(ir.Array).Element.(ir.Text).Determinism; got != want[c.Name] {
			t.Errorf("%s: determinism %v, want %v", c.Name, got, want[c.Name])
		}
	}
}

// TestUniqueKeyBoundaryGate_TypeOnlyOnAcceptedTable pins round-6 MEDIUM-B:
// a boundary that changes only a column's type or collation is judged on a
// table carrying an accepted key (the acceptance's fingerprint holds each
// part's type and collation), and not on any other table.
func TestUniqueKeyBoundaryGate_TypeOnlyOnAcceptedTable(t *testing.T) {
	bin := map[string]ir.Type{"id": ir.Integer{Width: 32}, "k": ir.Varchar{Length: 20, Collation: "utf8mb4_bin"}}
	ci := map[string]ir.Type{"id": ir.Integer{Width: 32}, "k": ir.Varchar{Length: 20, Collation: "utf8mb4_0900_ai_ci"}}
	for _, accepted := range []bool{true, false} {
		g, judged := r5Gate(nil, nil)
		g.acceptedTable = func(_, table string) bool { return accepted && table == "kd" }
		ctx := context.Background()
		if !g.hook()(ctx, r5Snap("kd", bin, "id", "k")) || len(*judged) != 0 {
			t.Fatalf("accepted=%v: the first snapshot was judged: %v", accepted, *judged)
		}
		if !g.hook()(ctx, r5Snap("kd", ci, "id", "k")) {
			t.Fatal("the gate stopped")
		}
		if got := len(*judged) == 1; got != accepted {
			t.Errorf("accepted=%v: the collation-only boundary judged=%v", accepted, got)
		}
	}
}

// TestUniqueKeyBoundaryGate_LateSkipOnlyNext pins round-5 LOW-2: the late
// call skips only the boundary the IMMEDIATELY preceding early call judged.
// A later snapshot with the same columns and the same (constant, pgoutput
// 0/0) position token, flagged UniqueKeysMoved, is judged.
func TestUniqueKeyBoundaryGate_LateSkipOnlyNext(t *testing.T) {
	types := map[string]ir.Type{"id": ir.Integer{Width: 32}, "k": ir.Text{}, "n": ir.Integer{Width: 32}}
	seed := r5Snap("kd", types, "id", "k")
	g, judged := r5Gate(map[string]string{"kd": columnList(seed.IR)}, nil)
	early, late := g.beforeDDLHook(), g.hook()
	ctx := context.Background()

	added := r5Snap("kd", types, "id", "k", "n")
	if !early(ctx, added) || !late(ctx, added) || len(*judged) != 1 {
		t.Fatalf("early then late: judged %v, want once", *judged)
	}
	again := r5Snap("kd", types, "id", "k", "n")
	again.UniqueKeysMoved = true
	if !late(ctx, again) || len(*judged) != 2 {
		t.Fatalf("a flagged snapshot with the same identity was skipped: judged %v", *judged)
	}
	// An early call whose late call never came (the intercept stopped) does
	// not let a later late call skip.
	g2, judged2 := r5Gate(map[string]string{"kd": columnList(seed.IR)}, nil)
	_ = g2.beforeDDLHook()(ctx, added)
	other := r5Snap("kd", types, "id", "k")
	_ = g2.hook()(ctx, other)
	moved := r5Snap("kd", types, "id", "k", "n")
	moved.UniqueKeysMoved = true
	if !g2.hook()(ctx, moved) || len(*judged2) != 3 {
		t.Fatalf("judged %v, want every call after the skipped one judged", *judged2)
	}
}

// TestUniqueKeyBoundaryGate_FoldedSeedIsReadOnly pins round-5 LOW-4: a
// table the change stream spells differently from the open read finds the
// open door's baseline, but a table that only folds onto a seeded one (one
// added mid-run) never overwrites that baseline; folded spellings that
// collide are no baseline at all.
func TestUniqueKeyBoundaryGate_FoldedSeedIsReadOnly(t *testing.T) {
	types := map[string]ir.Type{"id": ir.Integer{Width: 32}, "k": ir.Text{}, "n": ir.Integer{Width: 32}}
	base := columnList(r5Snap("t", types, "id", "k").IR)
	g, judged := r5Gate(map[string]string{"Orders": base, "Users": base, "users": base}, nil)
	ctx := context.Background()

	// "orders" folds onto the seeded "Orders": the moved list is judged.
	if !g.hook()(ctx, r5Snap("orders", types, "id", "k", "n")) || strings.Join(*judged, ",") != "orders" {
		t.Fatalf("a respelled seeded table was not judged against its seed: %v", *judged)
	}
	if g.prevCols["Orders"] != base {
		t.Fatalf("the seeded baseline was overwritten by another spelling: %q", g.prevCols["Orders"])
	}
	// "USERS" folds onto two seeded tables: no baseline, not judged.
	if !g.hook()(ctx, r5Snap("USERS", types, "id", "k", "n")) || len(*judged) != 1 {
		t.Fatalf("an ambiguous folded spelling found a baseline: %v", *judged)
	}
}

// TestUniqueKeyDoor_TargetQualifiedAcceptance pins round-7 MEDIUM-2 and its
// add-table LOW: the refusal of a `--target-schema tgt` stream prints
// `tgt.kd.uk=…`, and every check honours that value as pasted — the open
// door, the boundary gate's type-only arming (which compared the source
// namespace alone, so a forwarded `ALTER COLUMN k TYPE char(n)` under the
// accepted key went unjudged), and add-table (which matched the empty
// namespace only, refusing the value sync start had printed). A namespace
// that is neither the source's nor the target's matches nothing.
func TestUniqueKeyDoor_TargetQualifiedAcceptance(t *testing.T) {
	pk := ukDoorKey("PRIMARY", "id")
	keyed := ukDoorTable("kd", pk, ukDoorKey("uk", "k"))
	src := ukDoorTable("kd", pk)
	s, _, _ := ukDoorStreamer(t, ir.CDCLogicalReplication, []*ir.Table{src}, []*ir.Table{keyed})
	s.TargetSchema = "tgt"
	ctx := context.Background()
	err := s.phaseRefuseTargetOnlyUniqueKeys(ctx, nil, "s1", true)
	var refusal *ir.SourceUniqueKeyAbsentError
	if !errors.As(err, &refusal) {
		t.Fatalf("an unaccepted key: %v", err)
	}
	value := "tgt.kd.uk=" + keyFingerprint(keyed.Indexes[0], keyed)
	if !strings.Contains(err.Error(), "--accept-target-only-unique-key "+value) {
		t.Fatalf("the refusal does not print %s: %v", value, err)
	}
	s.AcceptTargetOnlyUniqueKeys = []string{value}
	if err := s.phaseRefuseTargetOnlyUniqueKeys(ctx, nil, "s1", true); err != nil {
		t.Fatalf("the pasted target-qualified value refused at open: %v", err)
	}
	if !s.uniqueKeyChecksFor().acceptedTable("", "kd") {
		t.Error("the boundary gate does not see the target-qualified acceptance: type-only boundaries go unjudged")
	}
	target := map[string]*ir.Table{"kd": keyed}
	if err := refuseAddTableTargetOnlyUniqueKeys(src, target, nil, nil, []string{value}, "", "tgt"); err != nil {
		t.Errorf("add-table refused the value sync start printed: %v", err)
	}
	// The namespace compares exactly (round-8 review LOW-4b): under MySQL's
	// lower_case_table_names=0 `TGT` and `tgt` are two databases.
	folded := "TGT.kd.uk=" + keyFingerprint(keyed.Indexes[0], keyed)
	if err := refuseAddTableTargetOnlyUniqueKeys(src, target, nil, nil, []string{folded}, "", "tgt"); err == nil {
		t.Error("add-table accepted a value naming the namespace in another case")
	}
	other := "other.kd.uk=" + keyFingerprint(keyed.Indexes[0], keyed)
	if err := refuseAddTableTargetOnlyUniqueKeys(src, target, nil, nil, []string{other}, "", "tgt"); err == nil {
		t.Error("add-table accepted a value naming another namespace")
	}
	s.AcceptTargetOnlyUniqueKeys = []string{other}
	if s.uniqueKeyChecksFor().acceptedTable("", "kd") {
		t.Error("the boundary gate honoured a value naming another namespace")
	}
	s.AcceptTargetOnlyUniqueKeys = []string{folded}
	if s.uniqueKeyChecksFor().acceptedTable("", "kd") {
		t.Error("the boundary gate honoured a value naming the namespace in another case")
	}
	if err := s.phaseRefuseTargetOnlyUniqueKeys(ctx, nil, "s1", true); err == nil {
		t.Error("the open door accepted a value naming the namespace in another case")
	}
}
