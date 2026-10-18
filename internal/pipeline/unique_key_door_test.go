// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package pipeline

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"

	"sluicesync.dev/sluice/internal/ir"
	"sluicesync.dev/sluice/internal/pipeline/migcore"
)

// ukDoorEngine serves a fixed schema per DSN, and records the table scope
// the door pushed down.
type ukDoorEngine struct {
	*resumeDispatchEngine
	schemas map[string]*ir.Schema
	scoped  []string
	// noDefinitions serves a reader with no [ir.UniqueKeyDefinitionReader];
	// defErr makes its definition read fail.
	noDefinitions bool
	defErr        error
}

func (e *ukDoorEngine) OpenSchemaReader(_ context.Context, dsn string) (ir.SchemaReader, error) {
	s, ok := e.schemas[dsn]
	if !ok {
		return nil, errors.New("ukDoorEngine: no schema for " + dsn)
	}
	r := &ukDoorReader{engine: e, schema: s}
	if e.noDefinitions {
		return r, nil
	}
	return &ukDoorDefReader{r}, nil
}

type ukDoorReader struct {
	engine *ukDoorEngine
	schema *ir.Schema
	scope  func(string) bool
}

// ukDoorDefReader adds the definition surface: the key and the columns it
// reads, rendered with %#v — a stand-in for an engine's own text that
// changes with every field of either.
type ukDoorDefReader struct{ *ukDoorReader }

func (r *ukDoorDefReader) UniqueKeyDefinition(_ context.Context, table, key string) (string, error) {
	if r.engine.defErr != nil {
		return "", r.engine.defErr
	}
	for _, t := range r.schema.Tables {
		if !strings.EqualFold(t.Name, table) {
			continue
		}
		for _, idx := range append([]*ir.Index{t.PrimaryKey}, t.Indexes...) {
			if idx != nil && idx.Name == key {
				return ukDoorDefinition(idx, t), nil
			}
		}
	}
	return "", errors.New("ukDoorDefReader: no key " + table + "." + key)
}

func ukDoorDefinition(key *ir.Index, t *ir.Table) string {
	def := fmt.Sprintf("%#v", *key)
	for _, c := range t.Columns {
		for _, p := range key.Columns {
			if p.Expression != "" || strings.EqualFold(p.Column, c.Name) {
				def += fmt.Sprintf("\n%#v", *c)
				break
			}
		}
	}
	return def
}

// ukDoorDefiner defines the keys of tables as [ukDoorDefReader] does.
func ukDoorDefiner(tables map[string]*ir.Table) keyDefiner {
	s := &ir.Schema{}
	for _, t := range tables {
		s.Tables = append(s.Tables, t)
	}
	r := &ukDoorDefReader{&ukDoorReader{engine: &ukDoorEngine{}, schema: s}}
	return r.UniqueKeyDefinition
}

// refuseAddTableTargetOnlyUniqueKeys runs the add-table door on source as
// read, its target keys defined by [ukDoorDefiner].
func refuseAddTableTargetOnlyUniqueKeys(source *ir.Table, target map[string]*ir.Table, se, te ir.Engine, accept []string, namespaces ...string) error {
	return refuseAddTableTargetOnlyUniqueKeysAsRead(context.Background(), addTableKeyDoor{
		source: source, target: target, sourceEngine: se, targetEngine: te, accept: accept,
		define: ukDoorDefiner(target), namespaces: namespaces,
	})
}

// keyFingerprint is the fingerprint the test target reports for key on t.
func keyFingerprint(key *ir.Index, t *ir.Table) string {
	return definitionFingerprint(ukDoorDefinition(key, t))
}

func (r *ukDoorReader) SetTableScope(allow func(string) bool) { r.scope = allow }

func (r *ukDoorReader) ReadSchema(context.Context) (*ir.Schema, error) {
	out := &ir.Schema{}
	for _, t := range r.schema.Tables {
		if r.scope != nil && !r.scope(t.Name) {
			continue
		}
		r.engine.scoped = append(r.engine.scoped, t.Name)
		out.Tables = append(out.Tables, t)
	}
	return out, nil
}

func ukDoorTable(name string, pk *ir.Index, keys ...*ir.Index) *ir.Table {
	t := &ir.Table{Name: name, PrimaryKey: pk, Indexes: keys}
	for _, c := range []string{"id", "k", "v"} {
		t.Columns = append(t.Columns, &ir.Column{Name: c, Type: ir.Text{}})
	}
	return t
}

func ukDoorKey(name string, cols ...string) *ir.Index {
	idx := &ir.Index{Name: name, Unique: true}
	for _, c := range cols {
		idx.Columns = append(idx.Columns, ir.IndexColumn{Column: c})
	}
	return idx
}

func ukDoorStreamer(t *testing.T, cdc ir.CDCMethod, src, tgt []*ir.Table) (s *Streamer, source, target *ukDoorEngine) {
	t.Helper()
	source = &ukDoorEngine{
		resumeDispatchEngine: &resumeDispatchEngine{name: "src", caps: ir.Capabilities{CDC: cdc}},
		schemas:              map[string]*ir.Schema{"src-dsn": {Tables: src}},
	}
	target = &ukDoorEngine{
		resumeDispatchEngine: &resumeDispatchEngine{name: "tgt"},
		schemas:              map[string]*ir.Schema{"tgt-dsn": {Tables: tgt}},
	}
	s = &Streamer{Source: source, Target: target, SourceDSN: "src-dsn", TargetDSN: "tgt-dsn"}
	return s, source, target
}

// TestUniqueKeyDoor_AtOpen pins the stream-open door's verdicts and its
// remedy per source class.
func TestUniqueKeyDoor_AtOpen(t *testing.T) {
	pk := ukDoorKey("PRIMARY", "id")
	t.Run("a key the source dropped refuses, naming it", func(t *testing.T) {
		s, _, _ := ukDoorStreamer(t, ir.CDCBinlog,
			[]*ir.Table{ukDoorTable("kd", pk)},
			[]*ir.Table{ukDoorTable("kd", pk, ukDoorKey("kd_k", "k"))})
		err := s.phaseRefuseTargetOnlyUniqueKeys(context.Background(), nil, "s1", true)
		var refusal *ir.SourceUniqueKeyAbsentError
		if !errors.As(err, &refusal) || !errors.Is(err, ir.ErrSourceUniqueKeyAbsent) {
			t.Fatalf("got %v, want SOURCE-UNIQUE-KEY-ABSENT", err)
		}
		if !strings.Contains(err.Error(), "kd.kd_k (k)") || !strings.Contains(err.Error(), "nothing written after it has been applied") {
			t.Errorf("the refusal does not name the key or the live-source remedy: %v", err)
		}
		if !strings.Contains(err.Error(), "--accept-target-only-unique-key") {
			t.Errorf("the refusal does not name the acceptance: %v", err)
		}
	})
	t.Run("a source that carries no DDL is told rows may have merged", func(t *testing.T) {
		s, _, _ := ukDoorStreamer(t, ir.CDCTriggers,
			[]*ir.Table{ukDoorTable("kd", nil)},
			[]*ir.Table{ukDoorTable("kd", nil, ukDoorKey("kd_k", "k"))})
		err := s.phaseRefuseTargetOnlyUniqueKeys(context.Background(), nil, "s1", true)
		if !errors.Is(err, ir.ErrSourceUniqueKeyAbsent) || !strings.Contains(err.Error(), "may already have merged") ||
			!strings.Contains(err.Error(), "--reset-target-data --yes") {
			t.Fatalf("got %v, want the re-copy remedy first", err)
		}
	})
	t.Run("the same keys pass", func(t *testing.T) {
		s, _, _ := ukDoorStreamer(t, ir.CDCBinlog,
			[]*ir.Table{ukDoorTable("kd", pk, ukDoorKey("kd_k", "k"))},
			[]*ir.Table{ukDoorTable("kd", pk, ukDoorKey("kd_k", "k"))})
		if err := s.phaseRefuseTargetOnlyUniqueKeys(context.Background(), nil, "s1", true); err != nil {
			t.Fatal(err)
		}
		if s.uniqueKeyDoor == nil || s.uniqueKeyDoor.periodic {
			t.Error("a source that sees unique-key DDL live armed the mid-stream re-check")
		}
	})
	t.Run("an accepted key passes, case-insensitively", func(t *testing.T) {
		s, _, _ := ukDoorStreamer(t, ir.CDCBinlog,
			[]*ir.Table{ukDoorTable("kd", pk)},
			[]*ir.Table{ukDoorTable("KD", pk, ukDoorKey("kd_k", "k"))})
		fp := keyFingerprint(ukDoorKey("kd_k", "k"), ukDoorTable("KD", pk))
		s.AcceptTargetOnlyUniqueKeys = []string{"kd.KD_K=" + fp}
		if err := s.phaseRefuseTargetOnlyUniqueKeys(context.Background(), nil, "s1", true); err != nil {
			t.Fatal(err)
		}
		s.AcceptTargetOnlyUniqueKeys = []string{"kd.other=" + fp}
		if err := s.phaseRefuseTargetOnlyUniqueKeys(context.Background(), nil, "s1", true); !errors.Is(err, ir.ErrSourceUniqueKeyAbsent) {
			t.Fatalf("accepting another key let kd_k through: %v", err)
		}
	})
	t.Run("an out-of-scope table is not judged", func(t *testing.T) {
		s, _, tgt := ukDoorStreamer(t, ir.CDCBinlog,
			[]*ir.Table{ukDoorTable("kd", pk), ukDoorTable("other", pk)},
			[]*ir.Table{ukDoorTable("kd", pk), ukDoorTable("other", pk, ukDoorKey("o_k", "k"))})
		f, err := migcore.NewTableFilter([]string{"kd"}, nil)
		if err != nil {
			t.Fatal(err)
		}
		s.Filter = f
		if err := s.phaseRefuseTargetOnlyUniqueKeys(context.Background(), nil, "s1", true); err != nil {
			t.Fatal(err)
		}
		for _, n := range tgt.scoped {
			if n == "other" {
				t.Error("the target read was not scoped to the stream's tables")
			}
		}
	})
	t.Run("a table the target does not hold is not judged", func(t *testing.T) {
		s, _, _ := ukDoorStreamer(t, ir.CDCBinlog,
			[]*ir.Table{ukDoorTable("kd", pk)}, nil)
		if err := s.phaseRefuseTargetOnlyUniqueKeys(context.Background(), nil, "s1", true); err != nil {
			t.Fatal(err)
		}
	})
	t.Run("a catalog read failure is not the marker", func(t *testing.T) {
		s, _, _ := ukDoorStreamer(t, ir.CDCBinlog, nil, nil)
		s.TargetDSN = "missing"
		s.Source.(*ukDoorEngine).schemas["src-dsn"] = &ir.Schema{Tables: []*ir.Table{ukDoorTable("kd", pk)}}
		err := s.phaseRefuseTargetOnlyUniqueKeys(context.Background(), nil, "s1", true)
		if err == nil || errors.Is(err, ir.ErrSourceUniqueKeyAbsent) {
			t.Fatalf("got %v, want a restartable read failure", err)
		}
	})
	t.Run("a source with no live DDL arms the re-check on tables with a secondary unique key", func(t *testing.T) {
		s, _, _ := ukDoorStreamer(t, ir.CDCVStream,
			[]*ir.Table{ukDoorTable("kd", pk, ukDoorKey("kd_k", "k")), ukDoorTable("plain", pk)},
			[]*ir.Table{ukDoorTable("kd", pk, ukDoorKey("kd_k", "k")), ukDoorTable("plain", pk)})
		if err := s.phaseRefuseTargetOnlyUniqueKeys(context.Background(), nil, "s1", true); err != nil {
			t.Fatal(err)
		}
		if s.uniqueKeyDoor == nil || strings.Join(s.uniqueKeyDoor.watched[0], ",") != "kd" {
			t.Fatalf("watched = %+v, want [kd]", s.uniqueKeyDoor)
		}
		// The source drops kd_k mid-stream: the re-check refuses, mid-stream.
		s.Source.(*ukDoorEngine).schemas["src-dsn"].Tables[0] = ukDoorTable("kd", pk)
		refusal, err := s.uniqueKeyChecksFor().periodic(context.Background())
		if err != nil || refusal == nil || refusal.When != ir.UniqueKeyDoorMidStream {
			t.Fatalf("re-check = %v, %v; want a mid-stream refusal", refusal, err)
		}
		if !strings.Contains(refusal.Error(), "up to 15s of them") {
			t.Errorf("the mid-stream refusal does not state its window: %v", refusal)
		}
	})
}

func TestValidateAcceptedUniqueKeys(t *testing.T) {
	for _, ok := range []string{"t.k=0123456789ab", "ns.t.k=0123456789ab", " t.k=abcdef012345 "} {
		if err := validateAcceptedUniqueKeys([]string{ok}); err != nil {
			t.Errorf("%q refused: %v", ok, err)
		}
	}
	for _, bad := range []string{"t.k", "ns.t.k", "t.k=", "t.k=0123", "t.k=0123456789AB", "t.k=0123456789abc", "t.k=zzzzzzzzzzzz", "t=0123456789ab", ".k=0123456789ab", "a.b.c.d=0123456789ab", ""} {
		if err := validateAcceptedUniqueKeys([]string{bad}); err == nil {
			t.Errorf("%q accepted", bad)
		}
	}
	if _, ok := parseAcceptedUniqueKeys([]string{"db1.t.k=0123456789ab"}).match("t", "k", "db1"); !ok {
		t.Error("a namespace-qualified acceptance does not match its namespace")
	}
	if _, ok := parseAcceptedUniqueKeys([]string{"db1.t.k=0123456789ab"}).match("t", "k", "DB1"); ok {
		t.Error("a namespace-qualified acceptance matched the namespace in another case (round-8 LOW-4b: exact)")
	}
	if _, ok := parseAcceptedUniqueKeys([]string{"db1.t.k=0123456789ab"}).match("t", "k", "db2"); ok {
		t.Error("a namespace-qualified acceptance does not scope to its namespace")
	}
}

// TestSourceUniqueKeyAbsentError_Remedies pins each check's remedy text.
func TestSourceUniqueKeyAbsentError_Remedies(t *testing.T) {
	keys := []ir.AbsentUniqueKey{{Table: "kd", Key: "kd_k", Columns: "(k)"}}
	cells := []struct {
		name string
		err  *ir.SourceUniqueKeyAbsentError
		want []string
	}{
		{
			"open, live source", &ir.SourceUniqueKeyAbsentError{Keys: keys, SourceSeesDDLLive: true},
			[]string{"SOURCE-UNIQUE-KEY-ABSENT", "kd.kd_k (k)", "nothing written after it has been applied", "DROP INDEX", "--stream-id", "--accept-target-only-unique-key"},
		},
		{
			"open, no live DDL", &ir.SourceUniqueKeyAbsentError{Keys: keys},
			[]string{"may already have merged", "--reset-target-data --yes", "DROP INDEX"},
		},
		{
			"mid-stream", &ir.SourceUniqueKeyAbsentError{Keys: keys, When: ir.UniqueKeyDoorMidStream, RecheckInterval: "15s"},
			[]string{"periodic re-check", "up to 15s of them", "Re-copy from a clean target", "--reset-target-data --yes"},
		},
		{
			"add-table", &ir.SourceUniqueKeyAbsentError{Keys: keys, When: ir.UniqueKeyDoorAddTable},
			[]string{"before the table is copied", "run add-table again"},
		},
		{
			"at a schema change", &ir.SourceUniqueKeyAbsentError{Keys: keys, When: ir.UniqueKeyDoorAtSchemaChange},
			[]string{"not forwarded at all", "stopped before anything after the change was applied", "DROP INDEX"},
		},
		{
			"a looser key column", &ir.SourceUniqueKeyAbsentError{
				Keys:              []ir.AbsentUniqueKey{{Table: "kd", Key: "kd_k", Columns: "(k)", LooserColumns: []string{"k"}}},
				SourceSeesDDLLive: true,
			},
			[]string{"[the target's k compares case-, accent- or trailing-space-insensitively", "ALTERing the target column(s)"},
		},
	}
	for _, c := range cells {
		t.Run(c.name, func(t *testing.T) {
			msg := c.err.Error()
			for _, w := range c.want {
				if !strings.Contains(msg, w) {
					t.Errorf("missing %q in: %s", w, msg)
				}
			}
			if !c.err.Terminal() || !errors.Is(c.err, ir.ErrSourceUniqueKeyAbsent) {
				t.Error("not terminal or not the sentinel")
			}
		})
	}
}

// TestRefuseAddTableTargetOnlyUniqueKeys pins the add-table entry.
func TestRefuseAddTableTargetOnlyUniqueKeys(t *testing.T) {
	pk := ukDoorKey("PRIMARY", "id")
	src := ukDoorTable("kd", pk)
	precreated := map[string]*ir.Table{"kd": ukDoorTable("kd", pk, ukDoorKey("kd_k", "k"))}
	err := refuseAddTableTargetOnlyUniqueKeys(src, precreated, nil, nil, nil)
	var refusal *ir.SourceUniqueKeyAbsentError
	if !errors.As(err, &refusal) || refusal.When != ir.UniqueKeyDoorAddTable {
		t.Fatalf("got %v, want the add-table refusal", err)
	}
	accept := "kd.kd_k=" + keyFingerprint(precreated["kd"].Indexes[0], precreated["kd"])
	if err := refuseAddTableTargetOnlyUniqueKeys(src, precreated, nil, nil, []string{accept}); err != nil {
		t.Fatalf("an accepted key refused: %v", err)
	}
	// The same name accepted at another shape (round-6 MEDIUM-A): the
	// fingerprint binds the acceptance, so the key as it is now refuses and
	// the refusal names the stale fingerprint.
	stale := "kd.kd_k=" + keyFingerprint(ukDoorKey("kd_k", "id"), precreated["kd"])
	err = refuseAddTableTargetOnlyUniqueKeys(src, precreated, nil, nil, []string{stale})
	if !errors.As(err, &refusal) || refusal.Keys[0].AcceptedFingerprint != strings.TrimPrefix(stale, "kd.kd_k=") {
		t.Fatalf("a key accepted at another shape: got %v, want the add-table refusal naming the stale fingerprint", err)
	}
	if err := refuseAddTableTargetOnlyUniqueKeys(src, map[string]*ir.Table{}, nil, nil, nil); err != nil {
		t.Fatalf("a table sluice will create refused: %v", err)
	}
}

// TestUncoveredTargetKeys_TargetIndexNames pins H1 of the round-1 review: a
// cross-dialect expression or partial key is matched by name, and a
// Postgres target table-prefixes a secondary index name, so the source's
// names are mapped to the target engine's spelling first. MySQL `users
// UNIQUE KEY uniq_lower_email ((lower(email)))` lands on Postgres as
// `users_uniq_lower_email`; a MySQL or SQLite target keeps the name.
func TestUncoveredTargetKeys_TargetIndexNames(t *testing.T) {
	expr := func(name, text, dialect string) *ir.Index {
		return &ir.Index{Name: name, Unique: true, Columns: []ir.IndexColumn{{Expression: text, ExpressionDialect: dialect}}}
	}
	partial := func(name, pred, dialect string) *ir.Index {
		return &ir.Index{Name: name, Unique: true, Columns: []ir.IndexColumn{{Column: "k"}}, Predicate: pred, PredicateDialect: dialect}
	}
	pk := ukDoorKey("PRIMARY", "id")
	cells := []struct {
		name, engine string
		src, tgt     *ir.Index
		wantRefused  bool
	}{
		{"mysql to postgres, expression", "postgres", expr("uniq_lower_email", "lower(`email`)", "mysql"), expr("users_uniq_lower_email", "lower((email)::text)", "postgres"), false},
		{"sqlite to postgres, partial", "postgres", partial("uq_live", "deleted IS NULL", "sqlite"), partial("users_uq_live", "(deleted IS NULL)", "postgres"), false},
		{"postgres to mysql keeps the name", "mysql", expr("uniq_lower_email", "lower((email)::text)", "postgres"), expr("uniq_lower_email", "lower(`email`)", "mysql"), false},
		{"postgres target, unprefixed name on the target (renamed by hand)", "postgres", expr("uniq_lower_email", "lower(`email`)", "mysql"), expr("uniq_lower_email", "lower((email)::text)", "postgres"), true},
		{"postgres target, another key entirely", "postgres", expr("uniq_lower_email", "lower(`email`)", "mysql"), expr("users_uniq_upper", "upper((email)::text)", "postgres"), true},
	}
	t.Run("two source keys mapping to one target name cover nothing by name (round-2 L-c)", func(t *testing.T) {
		src := ukDoorTable("users", pk,
			expr("users_e", "lower(`email`)", "mysql"), expr("e", "upper(`email`)", "mysql"))
		tgt := ukDoorTable("users", pk, expr("users_e", "lower((email)::text)", "postgres"))
		if got := uncoveredTargetKeys(src, tgt, "postgres", nil); len(got) != 1 {
			t.Errorf("uncovered = %v; an ambiguous name must not vouch for the target key", got)
		}
	})
	for _, c := range cells {
		t.Run(c.name, func(t *testing.T) {
			got := uncoveredTargetKeys(ukDoorTable("users", pk, c.src), ukDoorTable("users", pk, c.tgt), c.engine, nil)
			if (len(got) > 0) != c.wantRefused {
				t.Errorf("uncovered = %v, want refused=%v", got, c.wantRefused)
			}
		})
	}
}

// ukLooseEngine is an engine with a name and a collation-loosening table
// keyed by collation name, for the door's collation-direction eligibility.
type ukLooseEngine struct {
	stubEngine
	name  string
	loose map[string]ir.CollationLoosening
}

func (e ukLooseEngine) Name() string { return e.name }

func (e ukLooseEngine) CollationLoosening(t ir.Type) (ir.CollationLoosening, bool) {
	v, ok := t.(ir.Varchar)
	if !ok {
		return 0, false
	}
	l, ok := e.loose[v.Collation]
	return l, ok
}

// TestKeyColumnWeakening_Eligibility pins which engine pairs arm the door's
// collation direction (round-2 review MEDIUM-2) and what it reports: armed
// for a same-family pair and a Postgres-family target, withheld for a
// non-MySQL source into a MySQL-family target (a Postgres byte-exact key
// lands on the target's case-insensitive default) and for an engine
// without a resolver; a column either side cannot classify is never
// weakened.
func TestKeyColumnWeakening_Eligibility(t *testing.T) {
	loose := map[string]ir.CollationLoosening{"bin": 0, "ci": ir.LooseCase | ir.LooseAccent, "citext": ir.LooseCase, "pad": ir.LoosePad}
	eng := func(name string) ir.Engine { return ukLooseEngine{name: name, loose: loose} }
	table := func(coll string) *ir.Table {
		return &ir.Table{Name: "kd", Columns: []*ir.Column{{Name: "k", Type: ir.Varchar{Length: 9, Collation: coll}}}}
	}
	cases := []struct {
		name           string
		source, target ir.Engine
		srcColl        string
		tgtColl        string
		want           bool
	}{
		{"mysql bin -> mysql ci", eng("mysql"), eng("mysql"), "bin", "ci", true},
		{"mysql ci -> mysql bin", eng("mysql"), eng("mysql"), "ci", "bin", false},
		{"mysql bin -> mariadb ci", eng("mysql"), eng("mariadb"), "bin", "ci", true},
		{"postgres bin -> postgres ci", eng("postgres"), eng("postgres"), "bin", "ci", true},
		{"mysql bin -> postgres ci", eng("mysql"), eng("postgres"), "bin", "ci", true},
		{"mysql bin -> postgres-trigger ci", eng("mysql"), eng("postgres-trigger"), "bin", "ci", true},
		{"postgres bin -> mysql ci: withheld", eng("postgres"), eng("mysql"), "bin", "ci", false},
		// An unclassified collation (round-3 review LOW): looser unless the
		// names match or the target is known to fold nothing; not judged
		// across families, whose names are not comparable.
		{"unclassified target column, another name", eng("mysql"), eng("mysql"), "bin", "unknown", true},
		{"unclassified source column, another name", eng("mysql"), eng("mysql"), "unknown", "ci", true},
		{"unclassified on both sides, the same name", eng("mysql"), eng("mysql"), "unknown", "unknown", false},
		{"unclassified source, target folds nothing", eng("mysql"), eng("mysql"), "unknown", "bin", false},
		{"unclassified across families", eng("mysql"), eng("postgres"), "bin", "unknown", false},
		{"no resolver into mysql: withheld", stubEngine{}, eng("mysql"), "bin", "ci", false},
		// A source with no resolver into a Postgres-family target (round-5
		// MEDIUM-2): a target column known to fold refuses; none, or a
		// bpchar pad rule, passes.
		{"no resolver into a folding postgres column", stubEngine{}, eng("postgres"), "", "ci", true},
		{"no resolver into citext", stubEngine{}, eng("postgres"), "", "citext", true},
		{"no resolver into a deterministic column", stubEngine{}, eng("postgres"), "", "bin", false},
		{"no resolver into bpchar", stubEngine{}, eng("postgres"), "", "pad", false},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			w := keyColumnWeakening(c.source, c.target, table(c.srcColl), table(c.tgtColl))
			got := w != nil && w("k", "k")
			if got != c.want {
				t.Fatalf("weakened = %v, want %v", got, c.want)
			}
		})
	}
}

// ukCDCEngine is an engine with a CDC method, for which checks judge
// collation direction.
type ukCDCEngine struct {
	stubEngine
	cdc ir.CDCMethod
}

func (e ukCDCEngine) Capabilities() ir.Capabilities { return ir.Capabilities{CDC: e.cdc} }

// TestUniqueKeyCollationsArmed pins when the door judges collation
// direction: on a DDL-carrying source (binlog, pgoutput) only at a cold
// start's open, since a warm open would refuse a collation change the
// key-collation door forwards; on a source whose stream carries no DDL
// (VStream, triggers) at every check.
func TestUniqueKeyCollationsArmed(t *testing.T) {
	cases := []struct {
		cdc      ir.CDCMethod
		coldOpen bool
		want     bool
	}{
		{ir.CDCBinlog, true, true},
		{ir.CDCBinlog, false, false},
		{ir.CDCLogicalReplication, true, true},
		{ir.CDCLogicalReplication, false, false},
		{ir.CDCVStream, false, true},
		{ir.CDCTriggers, false, true},
	}
	for _, c := range cases {
		s := &Streamer{Source: ukCDCEngine{cdc: c.cdc}}
		if got := s.uniqueKeyCollationsArmed(c.coldOpen); got != c.want {
			t.Errorf("%v coldOpen=%v: armed=%v, want %v", c.cdc, c.coldOpen, got, c.want)
		}
	}
}
