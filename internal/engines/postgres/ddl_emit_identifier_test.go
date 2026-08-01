// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package postgres

import (
	"strings"
	"testing"

	"sluicesync.dev/sluice/internal/ir"
	"sluicesync.dev/sluice/internal/sluicecode"
)

// mkNamedIdx builds a single-column index carrying name.
func mkNamedIdx(name string, unique bool) *ir.Index {
	return &ir.Index{Name: name, Unique: unique, Columns: []ir.IndexColumn{{Column: "a"}}}
}

// tableWithIndexes builds a minimal two-column table carrying idxs.
func tableWithIndexes(name string, idxs ...*ir.Index) *ir.Table {
	return &ir.Table{
		Name: name,
		Columns: []*ir.Column{
			{Name: "id", Type: ir.Integer{Width: 64, AutoIncrement: true}},
			{Name: "a", Type: ir.Integer{Width: 64}},
			{Name: "b", Type: ir.Integer{Width: 64}},
		},
		Indexes: idxs,
	}
}

// TestPGIndexNamespace_RefusesTheNonInjectiveArms is the load-bearing pin
// for the collision class: [pgIndexName] is a TRANSFORMATION, not an
// escape, and it is NOT injective — several distinct source names map onto
// one Postgres identifier. Because the index build emits
// `CREATE INDEX IF NOT EXISTS`, the second build of a colliding name is a
// SILENT no-op, so every non-injective pair must be refused.
//
// The matrix walks each arm of pgIndexName's dispatch rather than one
// representative (the Bug 74 discipline applied to a name transform):
//
//   - the explicit `<table>_` verbatim arm vs the prepend arm — the
//     reachable pair, since MySQL auto-names a single-column index after
//     its column;
//   - the convention-prefix arms (`ix_`/`idx_`/… per
//     [indexNamingConventionPrefixes]), every prefix, both the
//     `<conv><table>_<rest>` and the exact `<conv><table>` rule;
//   - the >63-byte overflow arm, where pgIndexName gives up on prefixing;
//   - the cross-TABLE shape, which the same transform reaches from a
//     Postgres source (index names are schema-scoped there, so `x` on
//     table `a_b` and `a_b_x` on table `a` are both legal at the source
//     and collide on the target).
func TestPGIndexNamespace_RefusesTheNonInjectiveArms(t *testing.T) {
	longTable := strings.Repeat("q", 50)

	cases := []struct {
		name      string
		tables    []*ir.Table
		wantIdent string // non-empty ⇒ expect a refusal naming this ident
		rationale string
	}{
		{
			name: "explicit table-prefix arm: user_id vs posts_user_id",
			tables: []*ir.Table{tableWithIndexes(
				"posts",
				mkNamedIdx("user_id", false),
				mkNamedIdx("posts_user_id", true),
			)},
			wantIdent: "posts_user_id",
			rationale: "`user_id` prepends to `posts_user_id`; `posts_user_id` is already table-scoped and emits verbatim",
		},
		{
			name: "the confirmed silent-loss input: a_idx (non-unique) vs t_a_idx (UNIQUE)",
			tables: []*ir.Table{tableWithIndexes(
				"t",
				mkNamedIdx("a_idx", false),
				mkNamedIdx("t_a_idx", true),
			)},
			wantIdent: "t_a_idx",
			rationale: "jobs sort alphabetically, so the non-unique build wins and the UNIQUE one silently no-ops — the target then admits duplicates the source rejects",
		},
		{
			name: "primary-key constraint name vs an index that prepends onto it",
			tables: []*ir.Table{func() *ir.Table {
				tbl := tableWithIndexes("t", mkNamedIdx("pkey", false))
				tbl.PrimaryKey = &ir.Index{
					Name:            "t_pkey",
					Unique:          true,
					ConstraintNamed: true,
					Columns:         []ir.IndexColumn{{Column: "id"}},
				}
				return tbl
			}()},
			wantIdent: "t_pkey",
			rationale: "a source-named PK's backing index takes its name in the same per-schema relation namespace",
		},
		{
			name: "constraint-backed unique index (emitted verbatim) vs a prepending index",
			tables: []*ir.Table{tableWithIndexes(
				"t",
				mkNamedIdx("a_uq", false),
				&ir.Index{
					Name: "t_a_uq", Unique: true, ConstraintBacked: true,
					Columns: []ir.IndexColumn{{Column: "a"}},
				},
			)},
			wantIdent: "t_a_uq",
			rationale: "a constraint-backed index is ADDed verbatim by the constraints phase, so it lands in the same namespace as the prepended one",
		},
		{
			name: "cross-table: index b_c on table a vs index c on table a_b",
			tables: []*ir.Table{
				tableWithIndexes("a", mkNamedIdx("a_b_c", false)),
				tableWithIndexes("a_b", mkNamedIdx("c", true)),
			},
			wantIdent: "a_b_c",
			rationale: "Postgres index names are schema-scoped, so two tables' indexes share one namespace",
		},
		{
			name: "control: two genuinely distinct source names do NOT collide",
			tables: []*ir.Table{tableWithIndexes(
				"posts",
				mkNamedIdx("user_id", false),
				mkNamedIdx("created_at", false),
			)},
			rationale: "both prepend to distinct names; refusing here would be a false positive",
		},
		{
			name: "control: the convention arm cannot collide WITHIN one table",
			tables: []*ir.Table{tableWithIndexes(
				"a",
				mkNamedIdx("ix_a_one", false),
				mkNamedIdx("ix_a_two", false),
				mkNamedIdx("one", false),
			)},
			rationale: "a convention-verbatim name never starts with `<table>_`, so nothing prepends onto it on the same table",
		},
		{
			name: "control: the >63-byte overflow arm emits verbatim without colliding",
			tables: []*ir.Table{tableWithIndexes(
				longTable,
				mkNamedIdx(strings.Repeat("x", 20), false),
				mkNamedIdx(strings.Repeat("y", 20), false),
			)},
			rationale: "prepending would overflow so both emit verbatim — distinct names stay distinct",
		},
	}

	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			err := validatePGIndexNamespace(c.tables)
			if c.wantIdent == "" {
				if err != nil {
					t.Fatalf("expected NO refusal (%s), got: %v", c.rationale, err)
				}
				return
			}
			if err == nil {
				t.Fatalf("expected a refusal naming %q (%s), got nil", c.wantIdent, c.rationale)
			}
			if !strings.Contains(err.Error(), c.wantIdent) {
				t.Errorf("refusal must name the colliding effective identifier %q; got %q", c.wantIdent, err.Error())
			}
			// Both source indexes must be named — the operator cannot act on
			// "something collided".
			for _, tbl := range c.tables {
				for _, idx := range tbl.Indexes {
					if !strings.Contains(err.Error(), idx.Name) {
						t.Errorf("refusal must name source index %q; got %q", idx.Name, err.Error())
					}
				}
			}
			ce, ok := sluicecode.FromError(err)
			if !ok || ce.Code != sluicecode.CodeSchemaIndexNameCollision {
				t.Errorf("refusal must carry %s; got %+v (ok=%v)", sluicecode.CodeSchemaIndexNameCollision, ce, ok)
			}
		})
	}
}

// TestPGIndexNamespace_EveryConventionPrefixArm walks EVERY entry of
// [indexNamingConventionPrefixes] rather than one representative: a table
// named exactly like the prefix (`idx`, `uq`, `fk`, …) makes
// `<conv><table>` and the bare rest collapse onto one identifier. A new
// prefix added to the list is covered automatically.
func TestPGIndexNamespace_EveryConventionPrefixArm(t *testing.T) {
	for _, conv := range indexNamingConventionPrefixes {
		table := strings.TrimSuffix(conv, "_")
		t.Run(conv, func(t *testing.T) {
			// `<table>` prepends to `<table>_<table>`; `<conv><table>` is
			// literally that same string.
			bare, prefixed := table, conv+table
			if got, want := pgIndexName(table, bare), prefixed; got != want {
				t.Fatalf("setup: pgIndexName(%q, %q) = %q; want %q", table, bare, got, want)
			}
			if got := pgIndexName(table, prefixed); got != prefixed {
				t.Fatalf("setup: pgIndexName(%q, %q) = %q; want it verbatim", table, prefixed, got)
			}
			err := validatePGIndexNamespace([]*ir.Table{
				tableWithIndexes(table, mkNamedIdx(bare, false), mkNamedIdx(prefixed, true)),
			})
			if err == nil {
				t.Fatalf("source indexes %q and %q on table %q both render %q — must be refused", bare, prefixed, table, prefixed)
			}
			if !strings.Contains(err.Error(), prefixed) {
				t.Errorf("refusal must name %q; got %q", prefixed, err.Error())
			}
		})
	}
}

// TestPGIndexNamespace_AgreesWithTheEmitterOverAGeneratedCorpus is the
// property half: over every PAIR of distinct source names drawn from a
// corpus that exercises all of pgIndexName's arms, the namespace check
// must refuse EXACTLY when the two names emit the same identifier —
// never more (a false refusal breaks working migrations) and never less
// (a missed one is the silent no-op). The expectation is derived from the
// emitter itself because the emitter is ground truth for what actually
// gets created; the hard-coded arms above are what keep that honest.
func TestPGIndexNamespace_AgreesWithTheEmitterOverAGeneratedCorpus(t *testing.T) {
	tables := []string{"t", "posts", "idx", "ix", "a_b", strings.Repeat("q", 50)}

	corpusFor := func(table string) []string {
		names := make([]string, 0, 7+3*len(indexNamingConventionPrefixes))
		names = append(names, "a", "user_id", "a_idx", table, table+"_a", table+"_user_id")
		for _, conv := range indexNamingConventionPrefixes {
			names = append(names, conv+table, conv+table+"_a", conv+"a")
		}
		names = append(names, strings.Repeat("z", 40))
		return names
	}

	collisions := 0
	for _, table := range tables {
		names := corpusFor(table)
		for i := 0; i < len(names); i++ {
			for j := i + 1; j < len(names); j++ {
				if names[i] == names[j] {
					continue // the IR cannot carry one name twice on a table
				}
				wantCollide := pgIndexName(table, names[i]) == pgIndexName(table, names[j])
				if wantCollide {
					collisions++
				}
				err := validatePGIndexNamespace([]*ir.Table{
					tableWithIndexes(table, mkNamedIdx(names[i], false), mkNamedIdx(names[j], true)),
				})
				if wantCollide && err == nil {
					t.Errorf("table %q: %q and %q both emit %q but were ACCEPTED (silent no-op)",
						table, names[i], names[j], pgIndexName(table, names[i]))
				}
				if !wantCollide && err != nil {
					t.Errorf("table %q: %q → %q and %q → %q are distinct but were REFUSED: %v",
						table, names[i], pgIndexName(table, names[i]), names[j], pgIndexName(table, names[j]), err)
				}
			}
		}
	}
	// Vacuity guard: a corpus that generates no colliding pair would make
	// the whole sweep pass without testing the refusal at all.
	if collisions < 10 {
		t.Fatalf("the generated corpus produced only %d colliding pairs — the sweep is not exercising the refusal", collisions)
	}
}

// TestEmitters_IdentifierLengthRefusal extends the roadmap item 43 index
// refusal to EVERY identifier position the writer emits. Each emitter gets
// the same three shapes: exactly 63 bytes accepted, 64 refused, and a
// multibyte name whose RUNE count fits 63 but whose BYTE count does not
// (PG counts bytes against NAMEDATALEN, so a rune-based check would let it
// through and PG would truncate it silently).
//
// This is not decoration on the non-index positions: `CREATE TABLE IF NOT
// EXISTS` makes an over-length TABLE name's truncation collision silent,
// and the COPY that follows truncates identically — landing the second
// table's rows inside the first with exit 0. SQLite/D1 sources impose no
// identifier-length limit of their own.
func TestEmitters_IdentifierLengthRefusal(t *testing.T) {
	emitters := []struct {
		kind string
		// prefix keeps the EFFECTIVE name equal to the source name where
		// the emitter would otherwise transform it (pgIndexName prepends
		// `t_` unless the name already carries it).
		prefix string
		emit   func(name string) error
	}{
		{
			kind: "index",
			// `t_…` so pgIndexName emits verbatim and the boundary under
			// test is the effective name's own length.
			prefix: "t_",
			emit: func(name string) error {
				_, err := emitCreateIndex("public", "t", mkNamedIdx(name, false), emitOpts{})
				return err
			},
		},
		{
			kind: "table",
			emit: func(name string) error {
				_, err := emitTableDef("public", &ir.Table{
					Name:    name,
					Columns: []*ir.Column{{Name: "id", Type: ir.Integer{Width: 64}}},
				}, emitOpts{})
				return err
			},
		},
		{
			kind: "column",
			emit: func(name string) error {
				tbl := &ir.Table{Name: "t"}
				_, err := emitColumnDef(tbl, &ir.Column{Name: name, Type: ir.Integer{Width: 64}}, emitOpts{})
				return err
			},
		},
		{
			kind: "foreign key constraint",
			emit: func(name string) error {
				_, err := emitAddForeignKey("public", "child", &ir.ForeignKey{
					Name:              name,
					Columns:           []string{"parent_id"},
					ReferencedTable:   "parent",
					ReferencedColumns: []string{"id"},
				})
				return err
			},
		},
		{
			kind: "check constraint",
			emit: func(name string) error {
				_, err := emitCheckConstraint(&ir.CheckConstraint{Name: name, Expr: "a > 0"}, nil, emitOpts{})
				return err
			},
		},
		{
			kind: "enum type",
			emit: func(name string) error {
				// TypeName set ⇒ resolveEnumTypeName carries it verbatim,
				// which is the PG-source shape; the MySQL-source shape
				// (synthesized `<table>_<column>_enum`) is covered by the
				// synthesized-name case below.
				_, err := emitCreateEnumType(ir.Enum{Values: []string{"a", "b"}, TypeName: name}, "public", "t", "c")
				return err
			},
		},
	}

	for _, e := range emitters {
		t.Run(e.kind, func(t *testing.T) {
			pad := func(total int) string {
				return e.prefix + strings.Repeat("a", total-len(e.prefix))
			}
			at63, at64 := pad(63), pad(64)
			// 'é' is 2 UTF-8 bytes: prefix + 32×'é' is >63 bytes and <=63 runes.
			multibyte := e.prefix + strings.Repeat("é", 32)

			if len(at63) != 63 || len(at64) != 64 {
				t.Fatalf("setup: lengths are %d/%d, want 63/64", len(at63), len(at64))
			}
			if len(multibyte) <= maxPGIdentifierLen || len([]rune(multibyte)) > maxPGIdentifierLen {
				t.Fatalf("setup: multibyte is %d bytes / %d runes; want >63 bytes and <=63 runes",
					len(multibyte), len([]rune(multibyte)))
			}

			if err := e.emit(at63); err != nil {
				t.Errorf("63 bytes must be accepted; got %v", err)
			}
			for _, tc := range []struct {
				label string
				name  string
			}{{"64 bytes", at64}, {"multibyte >63 bytes but <=63 runes", multibyte}} {
				err := e.emit(tc.name)
				if err == nil {
					t.Errorf("%s: must be refused, got nil", tc.label)
					continue
				}
				if !strings.Contains(err.Error(), tc.name) {
					t.Errorf("%s: refusal must name the offending identifier; got %q", tc.label, err.Error())
				}
				ce, ok := sluicecode.FromError(err)
				if !ok || ce.Code != sluicecode.CodeSchemaIdentifierTooLong {
					t.Errorf("%s: refusal must carry %s; got %+v (ok=%v)",
						tc.label, sluicecode.CodeSchemaIdentifierTooLong, ce, ok)
				}
			}
		})
	}
}

// TestEmitCreateEnumType_SynthesizedNameLengthRefusal covers the enum
// position's REACHABLE shape: a MySQL source has no enum type identity, so
// the name is SYNTHESIZED as `<table>_<column>_enum` from two names that
// each fit 63 bytes and together need not. guardedCreateEnumType swallows
// `duplicate_object`, so a truncated synthesized name silently binds the
// second column to the FIRST column's enum type.
func TestEmitCreateEnumType_SynthesizedNameLengthRefusal(t *testing.T) {
	table := strings.Repeat("t", 40)
	column := strings.Repeat("c", 30)
	synthesized := enumTypeName(table, column)
	if len(synthesized) <= maxPGIdentifierLen {
		t.Fatalf("setup: synthesized name is %d bytes; want >%d", len(synthesized), maxPGIdentifierLen)
	}
	_, err := emitCreateEnumType(ir.Enum{Values: []string{"a"}}, "public", table, column)
	if err == nil {
		t.Fatal("a synthesized enum type name over 63 bytes must be refused, got nil")
	}
	if !strings.Contains(err.Error(), synthesized) {
		t.Errorf("refusal must name the synthesized type %q; got %q", synthesized, err.Error())
	}
}
