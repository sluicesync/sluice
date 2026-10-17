// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package pipeline

import (
	"context"
	"errors"
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
}

func (e *ukDoorEngine) OpenSchemaReader(_ context.Context, dsn string) (ir.SchemaReader, error) {
	s, ok := e.schemas[dsn]
	if !ok {
		return nil, errors.New("ukDoorEngine: no schema for " + dsn)
	}
	return &ukDoorReader{engine: e, schema: s}, nil
}

type ukDoorReader struct {
	engine *ukDoorEngine
	schema *ir.Schema
	scope  func(string) bool
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
		err := s.phaseRefuseTargetOnlyUniqueKeys(context.Background(), nil, "s1")
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
		err := s.phaseRefuseTargetOnlyUniqueKeys(context.Background(), nil, "s1")
		if !errors.Is(err, ir.ErrSourceUniqueKeyAbsent) || !strings.Contains(err.Error(), "may already have merged") ||
			!strings.Contains(err.Error(), "--reset-target-data --yes") {
			t.Fatalf("got %v, want the re-copy remedy first", err)
		}
	})
	t.Run("the same keys pass", func(t *testing.T) {
		s, _, _ := ukDoorStreamer(t, ir.CDCBinlog,
			[]*ir.Table{ukDoorTable("kd", pk, ukDoorKey("kd_k", "k"))},
			[]*ir.Table{ukDoorTable("kd", pk, ukDoorKey("kd_k", "k"))})
		if err := s.phaseRefuseTargetOnlyUniqueKeys(context.Background(), nil, "s1"); err != nil {
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
		s.AcceptTargetOnlyUniqueKeys = []string{"kd.KD_K"}
		if err := s.phaseRefuseTargetOnlyUniqueKeys(context.Background(), nil, "s1"); err != nil {
			t.Fatal(err)
		}
		s.AcceptTargetOnlyUniqueKeys = []string{"kd.other"}
		if err := s.phaseRefuseTargetOnlyUniqueKeys(context.Background(), nil, "s1"); !errors.Is(err, ir.ErrSourceUniqueKeyAbsent) {
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
		if err := s.phaseRefuseTargetOnlyUniqueKeys(context.Background(), nil, "s1"); err != nil {
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
		if err := s.phaseRefuseTargetOnlyUniqueKeys(context.Background(), nil, "s1"); err != nil {
			t.Fatal(err)
		}
	})
	t.Run("a catalog read failure is not the marker", func(t *testing.T) {
		s, _, _ := ukDoorStreamer(t, ir.CDCBinlog, nil, nil)
		s.TargetDSN = "missing"
		s.Source.(*ukDoorEngine).schemas["src-dsn"] = &ir.Schema{Tables: []*ir.Table{ukDoorTable("kd", pk)}}
		err := s.phaseRefuseTargetOnlyUniqueKeys(context.Background(), nil, "s1")
		if err == nil || errors.Is(err, ir.ErrSourceUniqueKeyAbsent) {
			t.Fatalf("got %v, want a restartable read failure", err)
		}
	})
	t.Run("a source with no live DDL arms the re-check on tables with a secondary unique key", func(t *testing.T) {
		s, _, _ := ukDoorStreamer(t, ir.CDCVStream,
			[]*ir.Table{ukDoorTable("kd", pk, ukDoorKey("kd_k", "k")), ukDoorTable("plain", pk)},
			[]*ir.Table{ukDoorTable("kd", pk, ukDoorKey("kd_k", "k")), ukDoorTable("plain", pk)})
		if err := s.phaseRefuseTargetOnlyUniqueKeys(context.Background(), nil, "s1"); err != nil {
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
	for _, ok := range []string{"t.k", "ns.t.k", " t.k "} {
		if err := validateAcceptedUniqueKeys([]string{ok}); err != nil {
			t.Errorf("%q refused: %v", ok, err)
		}
	}
	for _, bad := range []string{"t", "t.", ".k", "a.b.c.d", ""} {
		if err := validateAcceptedUniqueKeys([]string{bad}); err == nil {
			t.Errorf("%q accepted", bad)
		}
	}
	if !parseAcceptedUniqueKeys([]string{"db1.t.k"}).allows("DB1", "t", "k") ||
		parseAcceptedUniqueKeys([]string{"db1.t.k"}).allows("db2", "t", "k") {
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
	err := refuseAddTableTargetOnlyUniqueKeys(src, precreated, "mysql", nil)
	var refusal *ir.SourceUniqueKeyAbsentError
	if !errors.As(err, &refusal) || refusal.When != ir.UniqueKeyDoorAddTable {
		t.Fatalf("got %v, want the add-table refusal", err)
	}
	if err := refuseAddTableTargetOnlyUniqueKeys(src, precreated, "mysql", []string{"kd.kd_k"}); err != nil {
		t.Fatalf("an accepted key refused: %v", err)
	}
	if err := refuseAddTableTargetOnlyUniqueKeys(src, map[string]*ir.Table{}, "mysql", nil); err != nil {
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
	for _, c := range cells {
		t.Run(c.name, func(t *testing.T) {
			got := uncoveredTargetKeys(ukDoorTable("users", pk, c.src), ukDoorTable("users", pk, c.tgt), c.engine)
			if (len(got) > 0) != c.wantRefused {
				t.Errorf("uncovered = %v, want refused=%v", got, c.wantRefused)
			}
		})
	}
}
