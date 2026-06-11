// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package translate

import (
	"strings"
	"testing"

	"sluicesync.dev/sluice/internal/ir"
)

// textIndexSchema builds the canonical single-table scan fixture: an
// indexable id column plus column "c" of the given type, carried by
// one index shaped by mutate (default: a plain secondary index on c).
func textIndexSchema(colType ir.Type, mutate func(*ir.Table)) *ir.Schema {
	tbl := &ir.Table{
		Name: "t",
		Columns: []*ir.Column{
			{Name: "id", Type: ir.Integer{Width: 64}},
			{Name: "c", Type: colType},
		},
		Indexes: []*ir.Index{
			{Name: "idx_c", Columns: []ir.IndexColumn{{Column: "c"}}},
		},
	}
	if mutate != nil {
		mutate(tbl)
	}
	return &ir.Schema{Tables: []*ir.Table{tbl}}
}

// TestScanTextIndexNotices_TypeFamilyMatrix pins the full dispatch
// matrix of mysqlKeylessIndexTarget per Bug 74 doctrine: EVERY type
// family that maps to a no-key-length MySQL type (each TEXT tier, each
// BLOB tier, the Bug 72 wide-varchar down-map tiers, JSON, Array,
// hstore, Domain-wrapped) must flag — and every indexable family
// (narrow varchar, char, uuid, citext, integers, binaries, …) must
// stay clear. One representative per family is NOT enough: the MySQL
// emitter dispatches per IR type, so each arm is pinned.
func TestScanTextIndexNotices_TypeFamilyMatrix(t *testing.T) {
	cases := []struct {
		name       string
		typ        ir.Type
		wantTarget string // "" = must NOT flag
	}{
		// TEXT family — every tier.
		{"text tiny", ir.Text{Size: ir.TextTiny}, "TINYTEXT"},
		{"text regular", ir.Text{Size: ir.TextRegular}, "TEXT"},
		{"text zero-value size (TextTiny)", ir.Text{}, "TINYTEXT"},
		{"text medium", ir.Text{Size: ir.TextMedium}, "MEDIUMTEXT"},
		{"text long (PG text)", ir.Text{Size: ir.TextLong}, "LONGTEXT"},

		// BLOB family — every tier (PG bytea arrives as BlobLong).
		{"blob tiny", ir.Blob{Size: ir.BlobTiny}, "TINYBLOB"},
		{"blob regular", ir.Blob{Size: ir.BlobRegular}, "BLOB"},
		{"blob medium", ir.Blob{Size: ir.BlobMedium}, "MEDIUMBLOB"},
		{"blob long (PG bytea)", ir.Blob{Size: ir.BlobLong}, "LONGBLOB"},

		// Wide varchar — the Bug 72 down-map tiers; the boundary stays
		// VARCHAR and must NOT flag.
		{"varchar at threshold", ir.Varchar{Length: 16000}, ""},
		{"varchar just over threshold", ir.Varchar{Length: 16001}, "TEXT"},
		{"varchar mediumtext tier", ir.Varchar{Length: 70000}, "MEDIUMTEXT"},
		{"varchar longtext tier", ir.Varchar{Length: 5000000}, "LONGTEXT"},

		// JSON-mapped families — MySQL JSON cannot be a key part at all
		// (Error 3152), same early-refusal class.
		{"json", ir.JSON{}, "JSON"},
		{"jsonb", ir.JSON{Binary: true}, "JSON"},
		{"array", ir.Array{Element: ir.Text{Size: ir.TextLong}}, "JSON"},
		{"hstore extension", ir.ExtensionType{Extension: "hstore", Name: "hstore"}, "JSON"},

		// Domain wrapper recurses into the base type.
		{"domain over text", ir.Domain{Name: "d", BaseType: ir.Text{Size: ir.TextLong}}, "LONGTEXT"},
		{"domain over wide varchar", ir.Domain{Name: "d", BaseType: ir.Varchar{Length: 70000}}, "MEDIUMTEXT"},
		{"domain over narrow varchar", ir.Domain{Name: "d", BaseType: ir.Varchar{Length: 100}}, ""},
		{"domain with nil base", ir.Domain{Name: "d"}, ""},

		// Indexable shapes — must stay clear.
		{"narrow varchar", ir.Varchar{Length: 255}, ""},
		{"char", ir.Char{Length: 36}, ""},
		{"uuid (CHAR(36))", ir.UUID{}, ""},
		{"citext (VARCHAR(255))", ir.ExtensionType{Extension: "citext", Name: "citext"}, ""},
		{"integer", ir.Integer{Width: 64}, ""},
		{"decimal", ir.Decimal{Precision: 10, Scale: 2}, ""},
		{"binary", ir.Binary{Length: 16}, ""},
		{"varbinary", ir.Varbinary{Length: 255}, ""},
		{"inet (VARCHAR(45))", ir.Inet{}, ""},
		{"timestamp", ir.Timestamp{Precision: 6}, ""},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := ScanTextIndexNotices(textIndexSchema(tc.typ, nil), "postgres", "mysql")
			if tc.wantTarget == "" {
				if len(got) != 0 {
					t.Fatalf("notices = %+v; want none for indexable type %T", got, tc.typ)
				}
				return
			}
			if len(got) != 1 {
				t.Fatalf("notices = %+v; want exactly 1 for %T", got, tc.typ)
			}
			n := got[0]
			if n.Table != "t" || n.Column != "c" || n.Index != "idx_c" {
				t.Errorf("notice location = %s.%s index %q; want t.c index \"idx_c\"", n.Table, n.Column, n.Index)
			}
			if n.TargetType != tc.wantTarget {
				t.Errorf("TargetType = %q; want %q", n.TargetType, tc.wantTarget)
			}
		})
	}
}

// TestScanTextIndexNotices_IndexShapeMatrix pins the index-shape half
// of the Bug 74 matrix: UNIQUE, plain secondary, composite-member, and
// PRIMARY KEY parts flag; prefix-bearing parts, expression entries,
// and FULLTEXT/SPATIAL kinds are deliberately skipped.
func TestScanTextIndexNotices_IndexShapeMatrix(t *testing.T) {
	text := ir.Text{Size: ir.TextLong}

	t.Run("plain secondary index", func(t *testing.T) {
		got := ScanTextIndexNotices(textIndexSchema(text, nil), "postgres", "mysql")
		if len(got) != 1 || got[0].Unique || got[0].PrimaryKey {
			t.Fatalf("notices = %+v; want one non-unique non-PK notice", got)
		}
	})

	t.Run("unique index", func(t *testing.T) {
		s := textIndexSchema(text, func(tbl *ir.Table) {
			tbl.Indexes = []*ir.Index{{
				Name:    "uq_c",
				Unique:  true,
				Columns: []ir.IndexColumn{{Column: "c"}},
			}}
		})
		got := ScanTextIndexNotices(s, "postgres", "mysql")
		if len(got) != 1 || !got[0].Unique || got[0].Index != "uq_c" {
			t.Fatalf("notices = %+v; want one UNIQUE notice on uq_c", got)
		}
	})

	t.Run("composite index flags only the text member", func(t *testing.T) {
		s := textIndexSchema(text, func(tbl *ir.Table) {
			tbl.Indexes = []*ir.Index{{
				Name:    "idx_id_c",
				Columns: []ir.IndexColumn{{Column: "id"}, {Column: "c"}},
			}}
		})
		got := ScanTextIndexNotices(s, "postgres", "mysql")
		if len(got) != 1 || got[0].Column != "c" {
			t.Fatalf("notices = %+v; want exactly the text member c", got)
		}
	})

	t.Run("primary key on text", func(t *testing.T) {
		s := textIndexSchema(text, func(tbl *ir.Table) {
			tbl.Indexes = nil
			tbl.PrimaryKey = &ir.Index{Columns: []ir.IndexColumn{{Column: "c"}}}
		})
		got := ScanTextIndexNotices(s, "postgres", "mysql")
		if len(got) != 1 || !got[0].PrimaryKey || got[0].Index != "PRIMARY KEY" {
			t.Fatalf("notices = %+v; want one PRIMARY KEY notice", got)
		}
	})

	t.Run("prefix-bearing part is valid MySQL syntax and skipped", func(t *testing.T) {
		s := textIndexSchema(text, func(tbl *ir.Table) {
			tbl.Indexes[0].Columns = []ir.IndexColumn{{Column: "c", Length: 64}}
		})
		if got := ScanTextIndexNotices(s, "postgres", "mysql"); len(got) != 0 {
			t.Fatalf("notices = %+v; want none for a prefix-bearing key part", got)
		}
	})

	t.Run("expression entry skipped", func(t *testing.T) {
		s := textIndexSchema(text, func(tbl *ir.Table) {
			tbl.Indexes[0].Columns = []ir.IndexColumn{{Expression: "lower(c)", ExpressionDialect: "postgres"}}
		})
		if got := ScanTextIndexNotices(s, "postgres", "mysql"); len(got) != 0 {
			t.Fatalf("notices = %+v; want none for an expression entry", got)
		}
	})

	t.Run("fulltext kind skipped", func(t *testing.T) {
		s := textIndexSchema(text, func(tbl *ir.Table) {
			tbl.Indexes[0].Kind = ir.IndexKindFullText
		})
		if got := ScanTextIndexNotices(s, "postgres", "mysql"); len(got) != 0 {
			t.Fatalf("notices = %+v; want none for FULLTEXT (no prefix needed)", got)
		}
	})

	t.Run("spatial kind skipped", func(t *testing.T) {
		s := textIndexSchema(text, func(tbl *ir.Table) {
			tbl.Indexes[0].Kind = ir.IndexKindSpatial
		})
		if got := ScanTextIndexNotices(s, "postgres", "mysql"); len(got) != 0 {
			t.Fatalf("notices = %+v; want none for SPATIAL (no prefix allowed)", got)
		}
	})
}

// TestScanTextIndexNotices_EnginePairGates pins the engine-pair gate:
// every PG-family source × MySQL-family target fires (incl. the
// postgres-trigger source and the planetscale / vitess flavors); every
// other pair — same-engine MySQL with its native prefix indexes above
// all — short-circuits to nil.
func TestScanTextIndexNotices_EnginePairGates(t *testing.T) {
	s := textIndexSchema(ir.Text{Size: ir.TextLong}, nil)
	cases := []struct {
		src, tgt string
		want     bool
	}{
		{"postgres", "mysql", true},
		{"postgres", "planetscale", true},
		{"postgres", "vitess", true},
		{"postgres-trigger", "mysql", true},
		{"mysql", "mysql", false},
		{"planetscale", "planetscale", false},
		{"postgres", "postgres", false},
		{"mysql", "postgres", false},
		{"", "mysql", false},
	}
	for _, tc := range cases {
		got := ScanTextIndexNotices(s, tc.src, tc.tgt)
		if (len(got) > 0) != tc.want {
			t.Errorf("%s -> %s: notices = %+v; want fire=%v", tc.src, tc.tgt, got, tc.want)
		}
	}
}

// TestTextIndexRefusalError_Message pins the operator-facing refusal:
// it names the row (table.column), the index, the MySQL error class,
// and the `--type-override TABLE.COL=varchar(N)` recovery — and stays
// nil for clean schemas (incl. the override-applied shape, where the
// column's IR type is already a narrow varchar).
func TestTextIndexRefusalError_Message(t *testing.T) {
	s := textIndexSchema(ir.Text{Size: ir.TextLong}, func(tbl *ir.Table) {
		tbl.Indexes[0].Unique = true
		tbl.Indexes[0].Name = "users_email_key"
	})
	err := TextIndexRefusalError(s, "postgres", "mysql", "migrate")
	if err == nil {
		t.Fatal("err = nil; want Bug 136 refusal")
	}
	for _, want := range []string{
		"migrate",         // contextID
		"t.c",             // the offending row named
		"users_email_key", // the offending index named
		"UNIQUE",          // the uniqueness-semantics stake named
		"Error 1170",      // the MySQL failure it pre-empts
		"--type-override", // the escape hatch
		"varchar(N)",      // ... with the concrete shape
		"before any data", // the early-refusal promise
		"LONGTEXT",        // the MySQL landing type
	} {
		if !strings.Contains(err.Error(), want) {
			t.Errorf("refusal missing %q\n--- got ---\n%v", want, err)
		}
	}

	if err := TextIndexRefusalError(
		textIndexSchema(ir.Varchar{Length: 255}, nil), "postgres", "mysql", "migrate",
	); err != nil {
		t.Errorf("override-applied schema: err = %v; want nil", err)
	}
	if err := TextIndexRefusalError(nil, "postgres", "mysql", "migrate"); err != nil {
		t.Errorf("nil schema: err = %v; want nil", err)
	}
}
