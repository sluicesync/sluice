// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package postgres

import (
	"strings"
	"testing"

	"sluicesync.dev/sluice/internal/ir"
	"sluicesync.dev/sluice/internal/sluicecode"
)

// TestPgIndexName_GitHub26 covers the v0.49.0 pgIndexName fix:
// the original "already-prefixed → verbatim" behavior is preserved,
// plus three new shapes — convention-prefix detection (ix_/idx_/fk_/
// etc. + table name), length-overflow fallback to verbatim, and the
// edge case of source name exactly matching the convention+table
// form. Pre-v0.49.0 names like
// `ix_entity_field_operation_relation_workflow_block_id` got the
// table prefix prepended unconditionally, producing 84-char names
// that PG silently truncated to 63 and collided on the second
// CREATE INDEX.
func TestPgIndexName_GitHub26(t *testing.T) {
	cases := []struct {
		name      string
		table     string
		source    string
		want      string
		rationale string
	}{
		{
			name:      "existing: already explicitly prefixed",
			table:     "users",
			source:    "users_idx_email",
			want:      "users_idx_email",
			rationale: "preserved — source already starts with `<table>_`",
		},
		{
			name:      "existing: short name fits with prepend",
			table:     "users",
			source:    "idx_email",
			want:      "users_idx_email",
			rationale: "preserved — short prepend, no overflow risk",
		},
		{
			name:      "new (#26 cause): long convention-prefixed name → verbatim",
			table:     "entity_field_operation_relation",
			source:    "ix_entity_field_operation_relation_workflow_block_id",
			want:      "ix_entity_field_operation_relation_workflow_block_id",
			rationale: "convention-prefix `ix_<table>_` detected → verbatim; avoids 84-char prepend collision",
		},
		{
			name:      "new (#26 cause): long fk_ convention-prefixed → verbatim",
			table:     "entity_field_operation_relation",
			source:    "fk_entity_field_operation_relation_workflow_block_id",
			want:      "fk_entity_field_operation_relation_workflow_block_id",
			rationale: "convention-prefix `fk_<table>_` detected → verbatim",
		},
		{
			name:      "new (#26 fallback): non-convention long name → length-check verbatim",
			table:     "entity_field_operation_relation",
			source:    "ix_workflow_block_id_for_op_rel_alpha",
			want:      "ix_workflow_block_id_for_op_rel_alpha",
			rationale: "prepend (32+37=69) would exceed 63 → emit verbatim; sacrifices sibling-table disambig for collision-freedom",
		},
		{
			name:      "new (#26): convention prefix with no extra suffix (exact match)",
			table:     "users",
			source:    "ix_users",
			want:      "ix_users",
			rationale: "convention `ix_<table>` exact match → verbatim",
		},
		{
			name:      "regression: idx_x on users still prepends (fits)",
			table:     "users",
			source:    "idx_x",
			want:      "users_idx_x",
			rationale: "convention prefix `idx_` is present but `idx_users_` is not the source's start, so general prepend applies and fits",
		},
		{
			name:      "regression: uq_ convention detected",
			table:     "users",
			source:    "uq_users_email",
			want:      "uq_users_email",
			rationale: "convention prefix `uq_<table>_` detected → verbatim",
		},
		{
			name:      "empty source → empty result",
			table:     "users",
			source:    "",
			want:      "",
			rationale: "preserved — empty-in, empty-out",
		},
	}
	for _, c := range cases {
		c := c
		t.Run(c.name, func(t *testing.T) {
			got := pgIndexName(c.table, c.source)
			if got != c.want {
				t.Errorf("pgIndexName(%q, %q) = %q; want %q\nrationale: %s",
					c.table, c.source, got, c.want, c.rationale)
			}
		})
	}
}

// TestPgIndexName_NoCollisionAcrossLongSiblingNames is the
// load-bearing pin for GitHub #26: two sibling indexes that
// triggered the bug pre-v0.49.0 must now emit to distinct PG
// identifiers (no truncation collision).
func TestPgIndexName_NoCollisionAcrossLongSiblingNames(t *testing.T) {
	table := "entity_field_operation_relation"
	a := pgIndexName(table, "ix_workflow_block_id_for_op_rel_alpha")
	b := pgIndexName(table, "ix_workflow_block_id_for_op_rel_beta")
	if a == b {
		t.Errorf("sibling long index names should not collide post-fix; both → %q", a)
	}
	if len(a) > maxPGIdentifierLen || len(b) > maxPGIdentifierLen {
		t.Errorf("emitted names should fit PG's %d-char limit; got len(a)=%d len(b)=%d",
			maxPGIdentifierLen, len(a), len(b))
	}
}

// TestEmitCreateIndex_NameLengthRefusal pins roadmap item 43: an
// effective PG index name >63 BYTES is refused loudly (PG would silently
// truncate it into a collision against another index sharing the same
// 63-byte prefix; with CREATE INDEX IF NOT EXISTS the second create
// silently no-ops → missing index = silent schema-fidelity loss). The
// matrix proves: exactly-63 accepted; 64 refused with the name in the
// message; a multibyte/UTF-8 name whose RUNE count <=63 but BYTE count
// >63 refused (proves BYTE-based, not rune-based); and two names
// differing only after byte 63 both refused.
func TestEmitCreateIndex_NameLengthRefusal(t *testing.T) {
	// A short table name so pgIndexName's prepend doesn't itself push a
	// boundary case over the limit — these cases exercise the EFFECTIVE
	// name length, and the source names here already start with the
	// convention/table form so they emit verbatim (no prepend). We
	// construct verbatim-emitted names by using the `ix_<table>_` shape
	// so pgIndexName returns the source name unchanged and the assertion
	// is about the source's own byte length.
	const tbl = "t"

	// helper: build a single-column index with the given name.
	mkIdx := func(name string) *ir.Index {
		return &ir.Index{Name: name, Columns: []ir.IndexColumn{{Column: "c"}}}
	}

	// 63-byte ASCII name, emitted verbatim (starts with `ix_t_`).
	name63 := "ix_t_" + strings.Repeat("a", 63-len("ix_t_"))
	if len(name63) != 63 {
		t.Fatalf("test setup: name63 is %d bytes, want 63", len(name63))
	}
	// 64-byte ASCII name, emitted verbatim.
	name64 := "ix_t_" + strings.Repeat("a", 64-len("ix_t_"))
	if len(name64) != 64 {
		t.Fatalf("test setup: name64 is %d bytes, want 64", len(name64))
	}

	// Multibyte: rune count <=63 but byte count >63. 'é' is 2 bytes in
	// UTF-8. `ix_t_` (5 bytes) + 30×'é' (60 bytes) = 65 bytes, 35 runes.
	multibyte := "ix_t_" + strings.Repeat("é", 30)
	if rc := len([]rune(multibyte)); rc > maxPGIdentifierLen {
		t.Fatalf("test setup: multibyte rune count %d should be <=%d to prove byte-vs-rune", rc, maxPGIdentifierLen)
	}
	if len(multibyte) <= maxPGIdentifierLen {
		t.Fatalf("test setup: multibyte byte count %d should be >%d", len(multibyte), maxPGIdentifierLen)
	}

	t.Run("exactly 63 bytes accepted", func(t *testing.T) {
		stmt, err := emitCreateIndex("public", tbl, mkIdx(name63), emitOpts{})
		if err != nil {
			t.Fatalf("63-byte name should be accepted, got error: %v", err)
		}
		if !strings.Contains(stmt, name63) {
			t.Errorf("emitted statement should contain the 63-byte name; got %q", stmt)
		}
	})

	t.Run("64 bytes refused loudly with name in message", func(t *testing.T) {
		_, err := emitCreateIndex("public", tbl, mkIdx(name64), emitOpts{})
		if err == nil {
			t.Fatal("64-byte name must be refused, got nil error")
		}
		if !strings.Contains(err.Error(), name64) {
			t.Errorf("error message must name the offending index %q; got %q", name64, err.Error())
		}
		if !strings.Contains(err.Error(), tbl) {
			t.Errorf("error message must name the offending table %q; got %q", tbl, err.Error())
		}
	})

	t.Run("multibyte name >63 bytes but <=63 runes refused (byte-based)", func(t *testing.T) {
		_, err := emitCreateIndex("public", tbl, mkIdx(multibyte), emitOpts{})
		if err == nil {
			t.Fatalf("multibyte name of %d bytes (%d runes) must be refused (byte-based check), got nil error",
				len(multibyte), len([]rune(multibyte)))
		}
		if !strings.Contains(err.Error(), multibyte) {
			t.Errorf("error message must name the offending index; got %q", err.Error())
		}
	})

	t.Run("two names differing only after byte 63 both refused", func(t *testing.T) {
		base := "ix_t_" + strings.Repeat("a", 63-len("ix_t_")) // 63 bytes shared prefix
		nameA := base + "_alpha"
		nameB := base + "_beta"
		if _, err := emitCreateIndex("public", tbl, mkIdx(nameA), emitOpts{}); err == nil {
			t.Errorf("name A (%d bytes) sharing a 63-byte prefix must be refused", len(nameA))
		}
		if _, err := emitCreateIndex("public", tbl, mkIdx(nameB), emitOpts{}); err == nil {
			t.Errorf("name B (%d bytes) sharing a 63-byte prefix must be refused", len(nameB))
		}
	})
}

// TestValidatePGIndexName_Boundary is a direct unit pin on the helper at
// the exact 63/64-byte boundary (separate from the emitter wiring so a
// regression in either layer is localizable).
func TestValidatePGIndexName_Boundary(t *testing.T) {
	at63 := strings.Repeat("x", 63)
	if err := validatePGIndexName(at63, at63, "tbl"); err != nil {
		t.Errorf("63 bytes must pass; got %v", err)
	}
	at64 := strings.Repeat("x", 64)
	if err := validatePGIndexName(at64, at64, "tbl"); err == nil {
		t.Error("64 bytes must be refused")
	}
}

func TestEmitColumnType(t *testing.T) {
	cases := []struct {
		name string
		in   ir.Type
		want string
	}{
		// ---- Numeric / boolean ----
		{"boolean", ir.Boolean{}, "BOOLEAN"},
		{"smallint", ir.Integer{Width: 16}, "SMALLINT"},
		{"integer", ir.Integer{Width: 32}, "INTEGER"},
		{"bigint", ir.Integer{Width: 64}, "BIGINT"},
		{"int auto", ir.Integer{Width: 32, AutoIncrement: true}, "INTEGER GENERATED BY DEFAULT AS IDENTITY"},
		{"bigint auto", ir.Integer{Width: 64, AutoIncrement: true}, "BIGINT GENERATED BY DEFAULT AS IDENTITY"},
		{"unsigned int → bigint", ir.Integer{Width: 32, Unsigned: true}, "BIGINT"},
		{"unsigned tinyint → smallint", ir.Integer{Width: 8, Unsigned: true}, "SMALLINT"},
		{"unsigned smallint → integer", ir.Integer{Width: 16, Unsigned: true}, "INTEGER"},
		// Bug 11: `bigint unsigned` maps UNIFORMLY to PG BIGINT (PK,
		// FK-child, standalone alike) so an FK column's type matches
		// the AUTO_INCREMENT PK's type by construction. Pre-Bug-11 a
		// plain unsigned bigint became NUMERIC(20,0) while an
		// AUTO_INCREMENT one stayed BIGINT IDENTITY — the divergence
		// that broke FK creation for every default ORM schema.
		{"unsigned bigint → bigint (Bug 11 uniform)", ir.Integer{Width: 64, Unsigned: true}, "BIGINT"},
		{"unsigned bigint auto → bigint identity", ir.Integer{Width: 64, Unsigned: true, AutoIncrement: true}, "BIGINT GENERATED BY DEFAULT AS IDENTITY"},
		{"decimal", ir.Decimal{Precision: 10, Scale: 2}, "NUMERIC(10,2)"},
		{"decimal unconstrained (Bug 69)", ir.Decimal{Unconstrained: true}, "NUMERIC"},
		{"numeric[] unconstrained element (Bug 69)", ir.Array{Element: ir.Decimal{Unconstrained: true}}, "NUMERIC[]"},
		{"real", ir.Float{Precision: ir.FloatSingle}, "REAL"},
		{"double", ir.Float{Precision: ir.FloatDouble}, "DOUBLE PRECISION"},

		// ---- Character / binary ----
		{"char", ir.Char{Length: 10}, "CHAR(10)"},
		{"varchar", ir.Varchar{Length: 255}, "VARCHAR(255)"},
		{"text any size", ir.Text{Size: ir.TextRegular}, "TEXT"},
		{"text long", ir.Text{Size: ir.TextLong}, "TEXT"},
		{"binary → bytea", ir.Binary{Length: 16}, "BYTEA"},
		{"varbinary → bytea", ir.Varbinary{Length: 64}, "BYTEA"},
		{"blob → bytea", ir.Blob{Size: ir.BlobLong}, "BYTEA"},

		// ---- Bit (catalog Bug 62) ----
		{"bit(8) → BIT(8)", ir.Bit{Length: 8}, "BIT(8)"},
		{"bit(16) → BIT(16)", ir.Bit{Length: 16}, "BIT(16)"},
		{"bit(9) → BIT(9)", ir.Bit{Length: 9}, "BIT(9)"},

		// ---- Temporal ----
		// TRIAGE #3 emit matrix, pinned per family × shape (the Bug 74
		// discipline): every family member — Time / TimeTZ / DateTime /
		// Timestamp / TimestampTZ — × {unspecified → bare, explicit 0 →
		// (0), mid explicit → (p), explicit 6 → (6)}. A KNOWN precision
		// always emits, INCLUDING (0): the pre-fix 0-renders-bare rule
		// silently widened an explicit (0) to the 6-behaving bare form.
		{"date", ir.Date{}, "DATE"},
		{"time unspecified", ir.Time{PrecisionUnspecified: true}, "TIME"},
		{"time precision 0", ir.Time{Precision: 0}, "TIME(0)"},
		{"time precision 4", ir.Time{Precision: 4}, "TIME(4)"},
		{"time precision 6", ir.Time{Precision: 6}, "TIME(6)"},
		// Bug 71: timetz round-trips as TIME WITH TIME ZONE on a PG
		// target (not collapsed to plain TIME).
		{"timetz unspecified", ir.Time{WithTimeZone: true, PrecisionUnspecified: true}, "TIME WITH TIME ZONE"},
		{"timetz precision 0", ir.Time{Precision: 0, WithTimeZone: true}, "TIME(0) WITH TIME ZONE"},
		{"timetz precision 2", ir.Time{Precision: 2, WithTimeZone: true}, "TIME(2) WITH TIME ZONE"},
		{"timetz precision 6", ir.Time{Precision: 6, WithTimeZone: true}, "TIME(6) WITH TIME ZONE"},
		{"datetime unspecified", ir.DateTime{PrecisionUnspecified: true}, "TIMESTAMP"},
		{"datetime precision 0", ir.DateTime{Precision: 0}, "TIMESTAMP(0)"},
		{"datetime precision 3", ir.DateTime{Precision: 3}, "TIMESTAMP(3)"},
		{"datetime precision 6", ir.DateTime{Precision: 6}, "TIMESTAMP(6)"},
		{"timestamp unspecified", ir.Timestamp{PrecisionUnspecified: true}, "TIMESTAMP"},
		{"timestamp precision 0", ir.Timestamp{Precision: 0, WithTimeZone: false}, "TIMESTAMP(0)"},
		{"timestamp precision 3", ir.Timestamp{Precision: 3}, "TIMESTAMP(3)"},
		{"timestamptz unspecified", ir.Timestamp{WithTimeZone: true, PrecisionUnspecified: true}, "TIMESTAMP WITH TIME ZONE"},
		{"timestamptz precision 0", ir.Timestamp{Precision: 0, WithTimeZone: true}, "TIMESTAMP(0) WITH TIME ZONE"},
		{"timestamptz precision 6", ir.Timestamp{Precision: 6, WithTimeZone: true}, "TIMESTAMP(6) WITH TIME ZONE"},
		// interval: MySQL TIME duration → PG INTERVAL override (Vector C).
		{"interval", ir.Interval{}, "INTERVAL"},

		// ---- Structured ----
		{"json", ir.JSON{Binary: false}, "JSON"},
		{"jsonb", ir.JSON{Binary: true}, "JSONB"},

		// ---- Identity / network ----
		{"uuid", ir.UUID{}, "UUID"},
		{"inet", ir.Inet{}, "INET"},
		{"cidr", ir.Cidr{}, "CIDR"},
		{"macaddr", ir.Macaddr{}, "MACADDR"},

		// ---- Arrays ----
		{"int array", ir.Array{Element: ir.Integer{Width: 32}}, "INTEGER[]"},
		{"text array", ir.Array{Element: ir.Text{Size: ir.TextLong}}, "TEXT[]"},
		{"uuid array", ir.Array{Element: ir.UUID{}}, "UUID[]"},

		// ---- Set → TEXT[] (membership CHECK emitted by emitTableDef) ----
		{"set", ir.Set{Values: []string{"a", "b", "c"}}, "TEXT[]"},
		{"empty set", ir.Set{Values: nil}, "TEXT[]"},
	}

	for _, c := range cases {
		c := c
		t.Run(c.name, func(t *testing.T) {
			got, err := emitColumnType(c.in, emitOpts{})
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if got != c.want {
				t.Errorf("emitColumnType(%T) = %q; want %q", c.in, got, c.want)
			}
		})
	}
}

func TestEmitColumnTypeUnsupported(t *testing.T) {
	cases := []ir.Type{
		ir.Geometry{Subtype: ir.GeometryPoint},
		ir.Enum{Values: []string{"x"}}, // requires column context
	}
	for _, c := range cases {
		c := c
		t.Run("unsupported", func(t *testing.T) {
			if _, err := emitColumnType(c, emitOpts{}); err == nil {
				t.Errorf("expected error for %T", c)
			}
		})
	}
}

// TestEmitColumnType_PgvectorEnabled emits the canonical
// `vector(384)` form when the operator opted into the pgvector
// extension via emitOpts.EnabledExtensions. This is the
// load-bearing same-engine PG → PG passthrough path (ADR-0032).
func TestEmitColumnType_PgvectorEnabled(t *testing.T) {
	got, err := emitColumnType(
		ir.ExtensionType{Extension: "vector", Name: "vector", Modifiers: []int{384}},
		emitOpts{EnabledExtensions: map[string]bool{"vector": true}},
	)
	if err != nil {
		t.Fatalf("emitColumnType: %v", err)
	}
	if got != "vector(384)" {
		t.Errorf("emitColumnType = %q; want %q", got, "vector(384)")
	}
}

// TestEmitColumnType_PgvectorDisabled refuses pgvector columns
// when the operator didn't opt in via --enable-pg-extension.
// Defends against the "ExtensionType reached the writer through a
// hand-constructed IR" path; the operator-actionable error names
// the missing flag.
func TestEmitColumnType_PgvectorDisabled(t *testing.T) {
	_, err := emitColumnType(
		ir.ExtensionType{Extension: "vector", Name: "vector", Modifiers: []int{384}},
		emitOpts{},
	)
	if err == nil {
		t.Fatal("expected error when extension is not enabled; got nil")
	}
	if !strings.Contains(err.Error(), "--enable-pg-extension") {
		t.Errorf("err = %v; want contains \"--enable-pg-extension\"", err)
	}
	// The refusal carries the stable code + concise remedy as metadata
	// (docs/operator/error-codes.md); prose above is unchanged.
	ce, ok := sluicecode.FromError(err)
	if !ok {
		t.Fatal("extension refusal does not carry a CodedError")
	}
	if ce.Code != sluicecode.CodeSchemaExtensionNotEnabled {
		t.Errorf("Code = %q; want %q", ce.Code, sluicecode.CodeSchemaExtensionNotEnabled)
	}
	if !strings.Contains(ce.Hint, "--enable-pg-extension vector") {
		t.Errorf("Hint = %q; want the flag naming the extension", ce.Hint)
	}
}

// TestEmitColumnType_VerbatimType pins the ADR-0047 writer path: the
// captured pg_catalog.format_type spelling is emitted literally in the
// column-type position, no catalog dispatch, no flag gate.
func TestEmitColumnType_VerbatimType(t *testing.T) {
	cases := []struct {
		name string
		in   ir.VerbatimType
		want string
	}{
		{"ltree", ir.VerbatimType{Definition: "ltree"}, "ltree"},
		{"cube", ir.VerbatimType{Definition: "cube"}, "cube"},
		{"schema-qualified", ir.VerbatimType{Definition: "public.mytype"}, "public.mytype"},
		{"with modifiers", ir.VerbatimType{Definition: "geometry(Point,4326)"}, "geometry(Point,4326)"},
	}
	for _, c := range cases {
		c := c
		t.Run(c.name, func(t *testing.T) {
			// No EnabledExtensions, no flag — verbatim emits unconditionally.
			got, err := emitColumnType(c.in, emitOpts{})
			if err != nil {
				t.Fatalf("emitColumnType: %v", err)
			}
			if got != c.want {
				t.Errorf("emitColumnType = %q; want %q (must be literal)", got, c.want)
			}
		})
	}
}

// TestEmitColumnType_VerbatimTypeEmptyDefinition guards the corrupt-IR
// case: an empty Definition is a loud error, never a silent "" column.
func TestEmitColumnType_VerbatimTypeEmptyDefinition(t *testing.T) {
	_, err := emitColumnType(ir.VerbatimType{Definition: ""}, emitOpts{})
	if err == nil {
		t.Fatal("expected loud error on empty VerbatimType.Definition; got nil")
	}
}

// TestResolveIndexMethod_PrefersVerbatimMethod ensures the writer
// emits the IR's verbatim Method (pgvector's ivfflat / hnsw) ahead
// of the canonical Kind dispatch — the load-bearing path for
// extension-introduced index methods round-tripping through PG → PG.
func TestResolveIndexMethod_PrefersVerbatimMethod(t *testing.T) {
	cases := []struct {
		name string
		idx  *ir.Index
		want string
	}{
		{
			"ivfflat verbatim",
			&ir.Index{Method: "ivfflat", Kind: ir.IndexKindUnspecified},
			"ivfflat",
		},
		{
			"hnsw verbatim",
			&ir.Index{Method: "hnsw", Kind: ir.IndexKindUnspecified},
			"hnsw",
		},
		{
			"falls back to Kind when Method empty",
			&ir.Index{Kind: ir.IndexKindGIN},
			"gin",
		},
		{
			"empty Kind + empty Method returns empty",
			&ir.Index{},
			"",
		},
		{
			"nil index returns empty",
			nil,
			"",
		},
	}
	for _, c := range cases {
		c := c
		t.Run(c.name, func(t *testing.T) {
			got := resolveIndexMethod(c.idx)
			if got != c.want {
				t.Errorf("resolveIndexMethod = %q; want %q", got, c.want)
			}
		})
	}
}

// TestEmitCreateIndex_PgvectorIVFFlat exercises the full CREATE
// INDEX rendering for a pgvector ivfflat index — the shape the PG →
// PG passthrough must round-trip verbatim.
func TestEmitCreateIndex_PgvectorIVFFlat(t *testing.T) {
	idx := &ir.Index{
		Name:    "idx_items_embedding_ivfflat",
		Method:  "ivfflat",
		Kind:    ir.IndexKindUnspecified,
		Columns: []ir.IndexColumn{{Column: "embedding"}},
	}
	got, err := emitCreateIndex("public", "items", idx, emitOpts{})
	if err != nil {
		t.Fatalf("emitCreateIndex: %v", err)
	}
	if !strings.Contains(got, "USING ivfflat") {
		t.Errorf("emitCreateIndex output missing USING ivfflat: %q", got)
	}
}

// TestEmitSetCheckConstraint covers the table-level CHECK fragment
// the SET → TEXT[] policy emits. The constraint is what enforces the
// "members must be in the source's value list" guarantee on the PG
// target after the type translation.
func TestEmitSetCheckConstraint(t *testing.T) {
	cases := []struct {
		name   string
		table  string
		column string
		values []string
		want   string
	}{
		{
			"basic three-member SET",
			"events", "flags",
			[]string{"a", "b", "c"},
			`CONSTRAINT "events_flags_set" CHECK ("flags" <@ ARRAY['a','b','c']::TEXT[])`,
		},
		{
			"empty value list",
			"events", "flags",
			nil,
			`CONSTRAINT "events_flags_set" CHECK ("flags" <@ '{}'::TEXT[])`,
		},
		{
			"member containing apostrophe",
			"t", "c",
			[]string{"a'b"},
			`CONSTRAINT "t_c_set" CHECK ("c" <@ ARRAY['a''b']::TEXT[])`,
		},
	}
	for _, c := range cases {
		c := c
		t.Run(c.name, func(t *testing.T) {
			got := emitSetCheckConstraint(c.table, c.column, c.values)
			if got != c.want {
				t.Errorf("\n got:  %s\nwant:  %s", got, c.want)
			}
		})
	}
}

// TestSetDefaultToArrayLiteral covers the comma-separated → PG
// array-literal translation used when a SET column's source DEFAULT
// crosses into a TEXT[] target.
func TestSetDefaultToArrayLiteral(t *testing.T) {
	cases := []struct {
		in   string
		want string
	}{
		{"", "'{}'::TEXT[]"},
		{"a", "ARRAY['a']::TEXT[]"},
		{"a,b", "ARRAY['a','b']::TEXT[]"},
		{"a,b,c", "ARRAY['a','b','c']::TEXT[]"},
		{"a'b", "ARRAY['a''b']::TEXT[]"},
	}
	for _, c := range cases {
		c := c
		t.Run(c.in, func(t *testing.T) {
			got := setDefaultToArrayLiteral(c.in)
			if got != c.want {
				t.Errorf("\n got:  %s\nwant:  %s", got, c.want)
			}
		})
	}
}

// TestEmitTableDef_SETColumn covers the integration between
// emitColumnType (TEXT[]), the SET CHECK constraint emission, and
// the SET-default translation in emitColumnDef.
// TestEmitTableDef_GeneratedEnum_AsTextWithCheck pins Bug 25's
// fix: an enum-typed STORED generated column emits as TEXT (no
// enum type reference, since `(body)::enum_type` would trip PG's
// IMMUTABLE check) and gets a table-level CHECK enforcing the
// value-list. Mirrors the SET → TEXT[] + CHECK fallback shape.
func TestEmitTableDef_GeneratedEnum_AsTextWithCheck(t *testing.T) {
	tbl := &ir.Table{
		Name: "shipments",
		Columns: []*ir.Column{
			{Name: "id", Type: ir.Integer{Width: 64}, Nullable: false},
			{Name: "picked_at", Type: ir.Timestamp{Precision: 6}, Nullable: true},
			{
				Name:            "pickup_status",
				Type:            ir.Enum{Values: []string{"pending", "picked", "cancelled"}},
				Nullable:        true,
				GeneratedExpr:   "CASE WHEN picked_at IS NULL THEN 'pending' ELSE 'picked' END",
				GeneratedStored: true,
			},
		},
		PrimaryKey: &ir.Index{Columns: []ir.IndexColumn{{Column: "id"}}},
	}
	got, err := emitTableDef("public", tbl, emitOpts{})
	if err != nil {
		t.Fatalf("emitTableDef: %v", err)
	}
	wantContains := []string{
		// Column emits as TEXT, not as the enum type, and the body
		// is unwrapped (no ::enum_type cast).
		`"pickup_status" TEXT GENERATED ALWAYS AS (CASE WHEN picked_at IS NULL THEN 'pending' ELSE 'picked' END) STORED`,
		// Table-level CHECK enforces the value-list. Constraint name
		// uses the `_enum_chk` suffix to disambiguate from `_set` and
		// `_enum` (the type name).
		`CONSTRAINT "shipments_pickup_status_enum_chk" CHECK ("pickup_status" IN ('pending','picked','cancelled'))`,
	}
	for _, sub := range wantContains {
		if !strings.Contains(got, sub) {
			t.Errorf("CREATE TABLE missing %q\n--- got ---\n%s", sub, got)
		}
	}
	// Sanity: the column DOES NOT reference the enum type by name.
	// If a future refactor reintroduces the `::enum_type` cast or
	// the type-named column form, this catches it.
	if strings.Contains(got, `"shipments_pickup_status_enum"`) {
		t.Errorf("emitted DDL references the enum type name; should be TEXT-only for generated enums:\n%s", got)
	}
}

func TestEmitTableDef_SETColumn(t *testing.T) {
	tbl := &ir.Table{
		Name: "events",
		Columns: []*ir.Column{
			{Name: "id", Type: ir.Integer{Width: 64}, Nullable: false},
			{
				Name:     "flags",
				Type:     ir.Set{Values: []string{"a", "b", "c"}},
				Nullable: false,
				Default:  ir.DefaultLiteral{Value: "a,b"},
			},
		},
		PrimaryKey: &ir.Index{
			Columns: []ir.IndexColumn{{Column: "id"}},
		},
	}
	got, err := emitTableDef("public", tbl, emitOpts{})
	if err != nil {
		t.Fatalf("emitTableDef: %v", err)
	}
	wantContains := []string{
		`"flags" TEXT[] NOT NULL DEFAULT ARRAY['a','b']::TEXT[]`,
		`CONSTRAINT "events_flags_set" CHECK ("flags" <@ ARRAY['a','b','c']::TEXT[])`,
		`PRIMARY KEY ("id")`,
	}
	for _, sub := range wantContains {
		if !strings.Contains(got, sub) {
			t.Errorf("CREATE TABLE missing %q\n--- got ---\n%s", sub, got)
		}
	}
}

func TestEmitDefault(t *testing.T) {
	cases := []struct {
		name     string
		in       ir.DefaultValue
		want     string
		wantEmit bool
	}{
		{"none", ir.DefaultNone{}, "", false},
		{"nil", nil, "", false},
		{"literal zero", ir.DefaultLiteral{Value: "0"}, "'0'", true},
		{"literal text", ir.DefaultLiteral{Value: "hello"}, "'hello'", true},
		{"literal with quote", ir.DefaultLiteral{Value: "it's"}, "'it''s'", true},
		{"expression no dialect", ir.DefaultExpression{Expr: "now()"}, "now()", true},
		// v0.11.3 — DEFAULT-expression dialect-gated translation.
		// Bugs 28/29/30: pre-fix the DEFAULT path bypassed the
		// translator entirely. Post-fix, MySQL-tagged defaults route
		// through the same MySQL→PG rewrites as generated columns and
		// CHECK constraints. PG-tagged or untagged defaults still
		// emit verbatim.
		{
			"expression mysql dialect: UUID() translates",
			ir.DefaultExpression{Expr: "UUID()", Dialect: "mysql"},
			"gen_random_uuid()", true,
		},
		{
			"expression mysql dialect: RAND() translates",
			ir.DefaultExpression{Expr: "RAND()", Dialect: "mysql"},
			"RANDOM()", true,
		},
		{
			"expression mysql dialect: NOW() translates to keyword",
			ir.DefaultExpression{Expr: "NOW()", Dialect: "mysql"},
			"CURRENT_TIMESTAMP", true,
		},
		{
			"expression postgres dialect emits verbatim",
			ir.DefaultExpression{Expr: "now()", Dialect: "postgres"},
			"now()", true,
		},
		{
			"expression mysql dialect with unrecognised function passes through verbatim",
			ir.DefaultExpression{Expr: "WEIRD_FN()", Dialect: "mysql"},
			"WEIRD_FN()", true,
		},
		// Validation-rig catalog #6: a MySQL TEXT-family column default
		// `DEFAULT (_utf8mb4'vazio')` reaches the PG writer. The MySQL
		// reader now strips the charset introducer + C-style apostrophe
		// escapes (the same normalization generated/CHECK exprs get) so
		// the IR carries the portable `'vazio'`. Pre-fix the PG target
		// emitted `_utf8mb4\'vazio\'` → SQLSTATE 42601. Post-fix the
		// string-literal default passes through the PG translator
		// verbatim and is valid PG.
		{
			"catalog #6: introducer-stripped string-literal default emits verbatim",
			ir.DefaultExpression{Expr: "'vazio'", Dialect: "mysql"},
			"'vazio'", true,
		},
		// catalog Bug 62: a BIT(N>1) bit-literal default. The MySQL
		// reader tags it "bit" with the MySQL spelling `b'…'`; the PG
		// writer rewrites the prefix to PG's `B'…'` bit-string literal
		// (value identical, only the surface prefix differs). Pre-fix
		// the column was BYTEA and the literal was the decimal string
		// '165' → silently corrupted default (\x313635 not \xa5).
		{
			"Bug 62: bit literal b'10100101' → B'10100101'",
			ir.DefaultExpression{Expr: "b'10100101'", Dialect: "bit"},
			"B'10100101'", true,
		},
		{
			"Bug 62: wide bit literal b'1111000011110000' → B'…'",
			ir.DefaultExpression{Expr: "b'1111000011110000'", Dialect: "bit"},
			"B'1111000011110000'", true,
		},
		// D1/SQLite robustness Chunk A: a portable SQLite default
		// (datetime('now')) translates to the PG keyword instead of
		// emitting verbatim (which aborted CREATE TABLE).
		{
			"sqlite portable: datetime('now') → CURRENT_TIMESTAMP",
			ir.DefaultExpression{Expr: "datetime('now')", Dialect: "sqlite"},
			"CURRENT_TIMESTAMP", true,
		},
		// A non-portable SQLite default (julianday) is DROPPED — no DEFAULT
		// clause, ok=false — rather than emitted verbatim. (The loud warn
		// fires; TestEmitDefault_SQLiteNonPortableWarns asserts it.)
		{
			"sqlite non-portable: julianday('now') drops (no DEFAULT)",
			ir.DefaultExpression{Expr: "julianday('now')", Dialect: "sqlite"},
			"", false,
		},
		// SQLite's double-quoted-string misfeature is non-portable → drop.
		{
			"sqlite non-portable: double-quoted \"draft\" drops",
			ir.DefaultExpression{Expr: `"draft"`, Dialect: "sqlite"},
			"", false,
		},
	}
	for _, c := range cases {
		c := c
		t.Run(c.name, func(t *testing.T) {
			got, ok := emitDefault(nil, &ir.Column{Name: "col", Default: c.in}, emitOpts{})
			if ok != c.wantEmit {
				t.Errorf("emit flag = %v; want %v", ok, c.wantEmit)
			}
			if got != c.want {
				t.Errorf("emitDefault = %q; want %q", got, c.want)
			}
		})
	}
}

func TestEmitColumnDef(t *testing.T) {
	usersTable := &ir.Table{Name: "users"}

	cases := []struct {
		name string
		in   *ir.Column
		want string
	}{
		{
			name: "id bigint identity not null",
			in: &ir.Column{
				Name: "id",
				Type: ir.Integer{Width: 64, AutoIncrement: true},
			},
			want: `"id" BIGINT GENERATED BY DEFAULT AS IDENTITY NOT NULL`,
		},
		{
			name: "active boolean default true",
			in: &ir.Column{
				Name:    "active",
				Type:    ir.Boolean{},
				Default: ir.DefaultLiteral{Value: "true"},
			},
			want: `"active" BOOLEAN NOT NULL DEFAULT 'true'`,
		},
		{
			name: "created_at default now()",
			in: &ir.Column{
				Name:    "created_at",
				Type:    ir.Timestamp{Precision: 6, WithTimeZone: true},
				Default: ir.DefaultExpression{Expr: "now()"},
			},
			want: `"created_at" TIMESTAMP(6) WITH TIME ZONE NOT NULL DEFAULT now()`,
		},
		{
			name: "nullable text",
			in: &ir.Column{
				Name:     "notes",
				Type:     ir.Text{Size: ir.TextLong},
				Nullable: true,
			},
			want: `"notes" TEXT`,
		},
		{
			name: "enum with type-name reference",
			in: &ir.Column{
				Name: "role",
				Type: ir.Enum{Values: []string{"admin", "user"}},
			},
			want: `"role" "users_role_enum" NOT NULL`,
		},
		{
			// Cross-engine MySQL → PG with `rating ENUM(...) DEFAULT 'G'`
			// must emit the explicit type cast on the default; without
			// the cast Postgres rejects the literal as "invalid input
			// value for enum ...".
			name: "enum with default literal needs explicit type cast",
			in: &ir.Column{
				Name:    "rating",
				Type:    ir.Enum{Values: []string{"G", "PG", "PG-13", "R", "NC-17"}},
				Default: ir.DefaultLiteral{Value: "G"},
			},
			want: `"rating" "users_rating_enum" NOT NULL DEFAULT 'G'::"users_rating_enum"`,
		},
		{
			// Bug 23: MySQL's `DEFAULT ('pending')` parenthesised form
			// arrives as DefaultExpression{Expr: "'pending'"} via
			// information_schema's EXTRA=DEFAULT_GENERATED. The
			// expression body is a string literal so the cast must
			// still fire — without it PG rejects with "column X is
			// of type Y_enum but default expression is of type text".
			name: "enum with paren-literal expression default also gets cast",
			in: &ir.Column{
				Name:     "rating",
				Type:     ir.Enum{Values: []string{"G", "PG", "R"}},
				Nullable: true,
				Default:  ir.DefaultExpression{Expr: "'PG'"},
			},
			want: `"rating" "users_rating_enum" DEFAULT 'PG'::"users_rating_enum"`,
		},
		{
			// Bug 23 negative: a true expression default (not a
			// string-literal-shaped one) does NOT get the cast — the
			// cast wouldn't be safe and the resulting CREATE TABLE
			// would fail loudly on the target if the operator's
			// expression isn't enum-compatible.
			name: "enum with non-literal expression default is not cast",
			in: &ir.Column{
				Name:     "rating",
				Type:     ir.Enum{Values: []string{"G", "PG", "R"}},
				Nullable: true,
				Default:  ir.DefaultExpression{Expr: "current_setting('app.default_rating')"},
			},
			want: `"rating" "users_rating_enum" DEFAULT current_setting('app.default_rating')`,
		},
	}

	for _, c := range cases {
		c := c
		t.Run(c.name, func(t *testing.T) {
			got, err := emitColumnDef(usersTable, c.in, emitOpts{})
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if got != c.want {
				t.Errorf("\n got  %q\n want %q", got, c.want)
			}
		})
	}
}

// TestEmitColumnDef_Generated covers GENERATED ALWAYS AS (...) STORED
// emission. Postgres only supports STORED; a VIRTUAL source column is
// silently promoted with a slog warning (verified separately by the
// integration test that exercises a MySQL VIRTUAL → PG path).
func TestEmitColumnDef_Generated(t *testing.T) {
	tbl := &ir.Table{Name: "invoices"}

	cases := []struct {
		name string
		in   *ir.Column
		want string
	}{
		{
			name: "stored generated bigint",
			in: &ir.Column{
				Name:            "total",
				Type:            ir.Integer{Width: 64},
				GeneratedExpr:   "qty * price",
				GeneratedStored: true,
			},
			want: `"total" BIGINT GENERATED ALWAYS AS (qty * price) STORED NOT NULL`,
		},
		{
			name: "virtual source column promoted to stored",
			in: &ir.Column{
				Name:            "tax",
				Type:            ir.Decimal{Precision: 10, Scale: 2},
				Nullable:        true,
				GeneratedExpr:   "subtotal * 0.07",
				GeneratedStored: false,
			},
			want: `"tax" NUMERIC(10,2) GENERATED ALWAYS AS (subtotal * 0.07) STORED`,
		},
		{
			// Bug 25 (v0.10.1): enum-typed STORED generated columns
			// can't reference the enum type — `(body)::enum_type`
			// triggers PG's "generation expression is not immutable"
			// error because `enum_in()` is STABLE not IMMUTABLE.
			// Sluice sidesteps by emitting the column as TEXT (no
			// enum type, no cast); a table-level CHECK constraint
			// (added by emitTableDef) enforces the value-list. The
			// previous v0.9.2/v0.10.0 (body)::enum_type wrapper is
			// gone — see emitTableDef test for the CHECK side.
			name: "enum-typed generated column emits as TEXT (Bug 25)",
			in: &ir.Column{
				Name:            "pickup_status",
				Type:            ir.Enum{Values: []string{"pending", "picked", "cancelled"}},
				Nullable:        true,
				GeneratedExpr:   "CASE WHEN picked_at IS NULL THEN 'pending' ELSE 'picked' END",
				GeneratedStored: true,
			},
			want: `"pickup_status" TEXT GENERATED ALWAYS AS (CASE WHEN picked_at IS NULL THEN 'pending' ELSE 'picked' END) STORED`,
		},
	}
	for _, c := range cases {
		c := c
		t.Run(c.name, func(t *testing.T) {
			got, err := emitColumnDef(tbl, c.in, emitOpts{})
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if got != c.want {
				t.Errorf("\n got  %q\n want %q", got, c.want)
			}
		})
	}
}

// TestEmitCheckConstraint covers the standalone CHECK fragment used
// inline in CREATE TABLE bodies. Verbatim-passthrough policy: the
// expression text is preserved as-is.
func TestEmitCheckConstraint(t *testing.T) {
	cases := []struct {
		name string
		in   *ir.CheckConstraint
		want string
	}{
		{
			name: "named with comparison",
			in:   &ir.CheckConstraint{Name: "orders_qty_chk", Expr: "qty >= 0"},
			want: `CONSTRAINT "orders_qty_chk" CHECK (qty >= 0)`,
		},
		{
			name: "named with IN list",
			in: &ir.CheckConstraint{
				Name: "orders_status_chk",
				Expr: "status IN ('open','closed','cancelled')",
			},
			want: `CONSTRAINT "orders_status_chk" CHECK (status IN ('open','closed','cancelled'))`,
		},
		{
			name: "unnamed",
			in:   &ir.CheckConstraint{Expr: "start_date <= end_date"},
			want: `CHECK (start_date <= end_date)`,
		},
	}
	for _, c := range cases {
		c := c
		t.Run(c.name, func(t *testing.T) {
			got, err := emitCheckConstraint(c.in, nil, emitOpts{})
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if got != c.want {
				t.Errorf("\n got  %q\n want %q", got, c.want)
			}
		})
	}
}

// TestEmitTableDef_CheckRefusesUntranslatedCrossDialect pins Bug 77
// symmetric (task #73): the CREATE TABLE path (not just the Shape A
// AlterAddCheck path) must refuse a MySQL-source CHECK whose predicate
// the translator could not rewrite into PG before emitting verbatim
// DDL that fails on the PG parser with an opaque SQLSTATE 42601.
// Exercises every token in untranslatedMySQLToPGTokens — the class,
// not one representative; each routes the same emit path, but a
// per-token list miss (the MySQL-side v0.85.0 trap) would only be
// caught by covering all of them. The argument shapes here are ones
// the translator leaves untouched, so the token survives into the
// output and the output-only refuse fires.
func TestEmitTableDef_CheckRefusesUntranslatedCrossDialect(t *testing.T) {
	// Each expr is a form the translator does NOT rewrite, so the
	// MySQL-only token survives into the emitted PG output.
	cases := []struct {
		token string
		expr  string
	}{
		// JSON_EXTRACT with a non-simple path arg: not rewritten.
		{"json_extract(", "json_extract(payload, concat('$.', col)) = 'v'"},
		// JSON_UNQUOTE wrapping a non-JSON_EXTRACT arg: not rewritten.
		{"json_unquote(", "json_unquote(payload) = 'v'"},
		// DATE_FORMAT with a non-literal format: not rewritten.
		{"date_format(", "date_format(d, fmt) > '2020'"},
		// STR_TO_DATE has no PG rewrite at all.
		{"str_to_date(", "str_to_date(s, '%Y-%m-%d') > '2020-01-01'"},
	}
	for _, c := range cases {
		c := c
		t.Run(c.token, func(t *testing.T) {
			tbl := &ir.Table{
				Name: "events",
				Columns: []*ir.Column{
					{Name: "id", Type: ir.Integer{Width: 64}},
					{Name: "payload", Type: ir.JSON{}},
				},
				PrimaryKey: &ir.Index{
					Columns: []ir.IndexColumn{{Column: "id"}},
				},
				CheckConstraints: []*ir.CheckConstraint{
					{
						Name:        "events_payload_check",
						Expr:        c.expr,
						ExprDialect: "mysql",
					},
				},
			}
			_, err := emitTableDef("public", tbl, emitOpts{})
			if err == nil {
				t.Fatalf("expected refuse-loudly for token %q, got nil", c.token)
			}
			if !strings.Contains(err.Error(), "refuse loudly") {
				t.Errorf("error should be the refuse-loudly form, got: %v", err)
			}
			// The error must name the table and constraint so the
			// operator can act without reverse-engineering a 42601.
			if !strings.Contains(err.Error(), "events") {
				t.Errorf("error should name the table; got: %v", err)
			}
			if !strings.Contains(err.Error(), "events_payload_check") {
				t.Errorf("error should name the constraint; got: %v", err)
			}
		})
	}
}

// TestEmitTableDef_CheckAllowsTranslatedCrossDialect is the regression
// pin (Bug 77 symmetric, task #73): a MySQL-source CHECK whose
// predicate the translator DOES rewrite into a valid PG idiom must NOT
// be false-refused on the CREATE TABLE path. The source carries
// json_extract / json_unquote tokens, but the translator rewrites them
// to ->/->> so the output is clean PG — the earlier input-OR-output
// match would have wrongly refused this.
func TestEmitTableDef_CheckAllowsTranslatedCrossDialect(t *testing.T) {
	tbl := &ir.Table{
		Name: "events",
		Columns: []*ir.Column{
			{Name: "id", Type: ir.Integer{Width: 64}},
			{Name: "payload", Type: ir.JSON{}},
		},
		PrimaryKey: &ir.Index{
			Columns: []ir.IndexColumn{{Column: "id"}},
		},
		CheckConstraints: []*ir.CheckConstraint{
			{
				Name:        "events_kind_check",
				Expr:        "JSON_UNQUOTE(JSON_EXTRACT(payload, '$.kind')) = 'click'",
				ExprDialect: "mysql",
			},
		},
	}
	got, err := emitTableDef("public", tbl, emitOpts{})
	if err != nil {
		t.Fatalf("translatable cross-dialect CHECK must not be refused: %v", err)
	}
	// Sanity: the emitted CHECK uses the PG ->> idiom, not the MySQL
	// JSON_UNQUOTE/JSON_EXTRACT spelling.
	if strings.Contains(strings.ToLower(got), "json_extract") ||
		strings.Contains(strings.ToLower(got), "json_unquote") {
		t.Errorf("output should have rewritten the MySQL JSON funcs; got:\n%s", got)
	}
	if !strings.Contains(got, "->>") {
		t.Errorf("output should contain the PG ->> operator; got:\n%s", got)
	}
}

// TestEmitTableDef_CheckConstraints exercises the inline-emission
// path: user-declared CHECK clauses appear in the CREATE TABLE body
// alongside any synthetic SET-membership CHECKs, in source order.
func TestEmitTableDef_CheckConstraints(t *testing.T) {
	tbl := &ir.Table{
		Name: "orders",
		Columns: []*ir.Column{
			{Name: "id", Type: ir.Integer{Width: 64}},
			{Name: "qty", Type: ir.Integer{Width: 32}},
			{Name: "status", Type: ir.Varchar{Length: 20}},
		},
		PrimaryKey: &ir.Index{
			Columns: []ir.IndexColumn{{Column: "id"}},
		},
		CheckConstraints: []*ir.CheckConstraint{
			{Name: "orders_qty_chk", Expr: "qty >= 0"},
			{Name: "orders_status_chk", Expr: "status IN ('open','closed')"},
		},
	}
	got, err := emitTableDef("public", tbl, emitOpts{})
	if err != nil {
		t.Fatalf("emitTableDef: %v", err)
	}
	wants := []string{
		`CONSTRAINT "orders_qty_chk" CHECK (qty >= 0)`,
		`CONSTRAINT "orders_status_chk" CHECK (status IN ('open','closed'))`,
		`PRIMARY KEY ("id")`,
	}
	for _, w := range wants {
		if !strings.Contains(got, w) {
			t.Errorf("output missing %q; got:\n%s", w, got)
		}
	}
}

func TestEmitCreateEnumType(t *testing.T) {
	// MySQL-source enum (no type name) → synthesized table+column name.
	got := emitCreateEnumType(ir.Enum{Values: []string{"admin", "user", "guest"}}, "public", "users", "role")
	want := `CREATE TYPE "public"."users_role_enum" AS ENUM ('admin', 'user', 'guest');`
	if got != want {
		t.Errorf("\n got  %q\n want %q", got, want)
	}
}

// Bug 19c: a same-engine PG source carries the original enum type
// name; it must round-trip verbatim instead of being renamed to the
// synthesized <table>_<col>_enum.
func TestEmitCreateEnumType_PreservesSourceTypeName(t *testing.T) {
	got := emitCreateEnumType(
		ir.Enum{Values: []string{"draft", "published", "archived", "deleted"}, TypeName: "post_status"},
		"public", "posts", "status",
	)
	want := `CREATE TYPE "public"."post_status" AS ENUM ('draft', 'published', 'archived', 'deleted');`
	if got != want {
		t.Errorf("\n got  %q\n want %q", got, want)
	}
}

// Bug 154: cold-start's enum CREATE TYPE must be idempotent so a
// resumed/restarted cold-start (interrupted after the CREATE but before
// commit) doesn't crash-loop on SQLSTATE 42710. PG has no `CREATE TYPE
// IF NOT EXISTS`, so guardedCreateEnumType wraps the bare CREATE in a
// DO block swallowing duplicate_object. This pins the wrapper shape; the
// integration test pins the behavior (create twice → no error).
func TestGuardedCreateEnumType_WrapsInDuplicateObjectGuard(t *testing.T) {
	bare := emitCreateEnumType(ir.Enum{Values: []string{"a", "b"}}, "public", "t", "c")
	got := guardedCreateEnumType(bare)
	want := `DO $$ BEGIN ` + bare + ` EXCEPTION WHEN duplicate_object THEN NULL; END $$;`
	if got != want {
		t.Errorf("\n got  %q\n want %q", got, want)
	}
}

func TestEmitTableDef(t *testing.T) {
	table := &ir.Table{
		Name: "users",
		Columns: []*ir.Column{
			{Name: "id", Type: ir.Integer{Width: 64, AutoIncrement: true}},
			{Name: "email", Type: ir.Varchar{Length: 255}},
		},
		PrimaryKey: &ir.Index{
			Name:    "PRIMARY",
			Unique:  true,
			Columns: []ir.IndexColumn{{Column: "id"}},
		},
	}
	got, err := emitTableDef("public", table, emitOpts{})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	wants := []string{
		`CREATE TABLE IF NOT EXISTS "public"."users" (`,
		`"id" BIGINT GENERATED BY DEFAULT AS IDENTITY NOT NULL,`,
		`"email" VARCHAR(255) NOT NULL,`,
		`PRIMARY KEY ("id")`,
	}
	for _, w := range wants {
		if !strings.Contains(got, w) {
			t.Errorf("output missing %q; got:\n%s", w, got)
		}
	}
}

// Bug 45 (v0.25.1): when `--target-schema=NAME` is set on a PG
// target, the column-type ident and the `::cast` suffix on enum
// DEFAULT expressions must both be schema-qualified — the bare
// ident relies on `search_path` which doesn't include per-source
// namespaces, and CREATE TABLE fails with SQLSTATE 42704 "type does
// not exist". Verifies emitColumnDef + emitTableDef produce
// `"customer_svc"."t_c_enum"` shape under non-empty TargetSchema.
func TestEmitColumnDef_TargetSchemaQualifiesEnumIdentAndCast(t *testing.T) {
	tbl := &ir.Table{Name: "orders"}
	col := &ir.Column{
		Name:    "status",
		Type:    ir.Enum{Values: []string{"pending", "paid", "shipped"}},
		Default: ir.DefaultLiteral{Value: "pending"},
	}
	got, err := emitColumnDef(tbl, col, emitOpts{TargetSchema: "customer_svc"})
	if err != nil {
		t.Fatalf("emitColumnDef: %v", err)
	}
	want := `"status" "customer_svc"."orders_status_enum" NOT NULL DEFAULT 'pending'::"customer_svc"."orders_status_enum"`
	if got != want {
		t.Errorf("\n got  %q\n want %q", got, want)
	}
}

// TestEmitColumnDef_NoTargetSchemaPreservesUnqualifiedShape pins
// the pre-Bug-45 shape: when TargetSchema is empty, enum idents and
// casts emit unqualified — the type lives in the DSN's default
// schema (typically `public`) which is in `search_path`, so the
// bare ident resolves correctly.
func TestEmitColumnDef_NoTargetSchemaPreservesUnqualifiedShape(t *testing.T) {
	tbl := &ir.Table{Name: "orders"}
	col := &ir.Column{
		Name:    "status",
		Type:    ir.Enum{Values: []string{"pending", "paid"}},
		Default: ir.DefaultLiteral{Value: "pending"},
	}
	got, err := emitColumnDef(tbl, col, emitOpts{})
	if err != nil {
		t.Fatalf("emitColumnDef: %v", err)
	}
	want := `"status" "orders_status_enum" NOT NULL DEFAULT 'pending'::"orders_status_enum"`
	if got != want {
		t.Errorf("\n got  %q\n want %q", got, want)
	}
}

// TestEmitTableDef_TargetSchemaQualifiesEnumColumnAndCast asserts
// the full CREATE TABLE statement under --target-schema carries
// schema-qualified type idents inside the column list — the
// load-bearing shape from BUG-CATALOG Bug 45's repro section.
func TestEmitTableDef_TargetSchemaQualifiesEnumColumnAndCast(t *testing.T) {
	tbl := &ir.Table{
		Name: "orders",
		Columns: []*ir.Column{
			{Name: "id", Type: ir.Integer{Width: 64, AutoIncrement: true}},
			{
				Name:    "status",
				Type:    ir.Enum{Values: []string{"pending", "paid", "shipped"}},
				Default: ir.DefaultLiteral{Value: "pending"},
			},
		},
		PrimaryKey: &ir.Index{
			Name:    "orders_pkey",
			Unique:  true,
			Columns: []ir.IndexColumn{{Column: "id"}},
		},
	}
	got, err := emitTableDef("customer_svc", tbl, emitOpts{TargetSchema: "customer_svc"})
	if err != nil {
		t.Fatalf("emitTableDef: %v", err)
	}
	wants := []string{
		`CREATE TABLE IF NOT EXISTS "customer_svc"."orders" (`,
		`"status" "customer_svc"."orders_status_enum" NOT NULL DEFAULT 'pending'::"customer_svc"."orders_status_enum"`,
	}
	for _, w := range wants {
		if !strings.Contains(got, w) {
			t.Errorf("output missing %q; got:\n%s", w, got)
		}
	}
}

// TestSchemaWriter_QualifyingSchema_TogglesOnSetSchema verifies the
// schema_writer's qualifyingSchema policy: bare ident before
// SetSchema, schema-qualified after the operator-supplied override.
func TestSchemaWriter_QualifyingSchema_TogglesOnSetSchema(t *testing.T) {
	w := &SchemaWriter{schema: "public"}
	if got := w.qualifyingSchema(); got != "" {
		t.Errorf("qualifyingSchema() before SetSchema = %q; want empty (default-public preserves bare ident)", got)
	}
	w.SetSchema("customer_svc")
	if got := w.qualifyingSchema(); got != "customer_svc" {
		t.Errorf("qualifyingSchema() after SetSchema = %q; want customer_svc", got)
	}
}

func boolPtr(b bool) *bool { return &b }

func TestEmitCreateIndex(t *testing.T) {
	cases := []struct {
		name string
		idx  *ir.Index
		want string
	}{
		{
			name: "secondary unique btree",
			idx: &ir.Index{
				Name:    "users_email_unique",
				Unique:  true,
				Kind:    ir.IndexKindBTree,
				Columns: []ir.IndexColumn{{Column: "email"}},
			},
			want: `CREATE UNIQUE INDEX "users_email_unique" ON "public"."users" USING btree ("email");`,
		},
		{
			name: "non-unique multi-column",
			idx: &ir.Index{
				Name: "users_lookup",
				Kind: ir.IndexKindBTree,
				Columns: []ir.IndexColumn{
					{Column: "tenant_id"},
					{Column: "created_at", Desc: true},
				},
			},
			want: `CREATE INDEX "users_lookup" ON "public"."users" USING btree ("tenant_id", "created_at" DESC);`,
		},
		{
			// Index name doesn't start with the table name, so the
			// PG emitter prefixes it to disambiguate against the
			// schema-scoped Postgres namespace (a sibling table
			// might use the same source-side index name).
			name: "gin index — name gets table prefix",
			idx: &ir.Index{
				Name:    "posts_search",
				Kind:    ir.IndexKindGIN,
				Columns: []ir.IndexColumn{{Column: "tsv"}},
			},
			want: `CREATE INDEX "users_posts_search" ON "public"."users" USING gin ("tsv");`,
		},
		{
			// Source-side name like MySQL's idx_fk_film_id, which
			// appears on multiple tables in real-world schemas
			// (sakila has it on inventory, film_actor, etc.). The
			// table-prefix disambiguation is what makes the
			// cross-engine migration work.
			name: "common collision-prone name gets table prefix",
			idx: &ir.Index{
				Name:    "idx_fk_film_id",
				Kind:    ir.IndexKindBTree,
				Columns: []ir.IndexColumn{{Column: "film_id"}},
			},
			want: `CREATE INDEX "users_idx_fk_film_id" ON "public"."users" USING btree ("film_id");`,
		},
		{
			// Same-dialect (PG-source) expression — passes through
			// verbatim with no translation.
			name: "same-dialect expression passes through verbatim",
			idx: &ir.Index{
				Name: "users_lower_email",
				Kind: ir.IndexKindBTree,
				Columns: []ir.IndexColumn{
					{Expression: "lower(email)", ExpressionDialect: "postgres"},
				},
			},
			want: `CREATE INDEX "users_lower_email" ON "public"."users" USING btree ((lower(email)));`,
		},
		{
			// MySQL-source expression with a JSON_UNQUOTE+JSON_EXTRACT
			// chain — the ADR-0016 translator rewrites to PG's ->>
			// idiom on emit (Bug 16 follow-up). Without this, the
			// expression would emit verbatim and PG would reject the
			// CREATE INDEX with "function json_unquote(json) does not
			// exist".
			name: "MySQL-source JSON expression rewrites to PG ->>",
			idx: &ir.Index{
				Name: "containers_pickup_status",
				Kind: ir.IndexKindBTree,
				Columns: []ir.IndexColumn{
					{Expression: "json_unquote(json_extract(meta,'$.color'))", ExpressionDialect: "mysql"},
				},
			},
			want: `CREATE INDEX "users_containers_pickup_status" ON "public"."users" USING btree (((meta->>'color')));`,
		},
		{
			// SP-GiST round-trip (Bug 50): pre-fix the enum was
			// IndexKindUnspecified for spgist and the writer dropped
			// the AM, falling back to btree. With the enum populated
			// the writer emits `USING spgist`.
			name: "spgist index",
			idx: &ir.Index{
				Name:    "geom_spgist",
				Kind:    ir.IndexKindSPGist,
				Columns: []ir.IndexColumn{{Column: "geom", OperatorClass: "spgist_geometry_ops_2d"}},
			},
			want: `CREATE INDEX "users_geom_spgist" ON "public"."users" USING spgist ("geom" spgist_geometry_ops_2d);`,
		},
		{
			// BRIN round-trip (Bug 50): same shape as SP-GiST above.
			name: "brin index",
			idx: &ir.Index{
				Name:    "geom_brin",
				Kind:    ir.IndexKindBRIN,
				Columns: []ir.IndexColumn{{Column: "geom", OperatorClass: "brin_geometry_inclusion_ops_2d"}},
			},
			want: `CREATE INDEX "users_geom_brin" ON "public"."users" USING brin ("geom" brin_geometry_inclusion_ops_2d);`,
		},
		{
			// Untagged expression (older IR / hand-built fixtures)
			// emits verbatim — same as the pre-Bug-16-follow-up
			// behaviour.
			name: "untagged expression passes through verbatim",
			idx: &ir.Index{
				Name: "users_lower_legacy",
				Kind: ir.IndexKindBTree,
				Columns: []ir.IndexColumn{
					{Expression: "lower(email)"},
				},
			},
			want: `CREATE INDEX "users_lower_legacy" ON "public"."users" USING btree ((lower(email)));`,
		},
		{
			// Bug 19a: partial index with a WHERE predicate + DESC.
			// Pre-fix the predicate and DESC were silently dropped,
			// turning a partial index into a full one (exit 0, wrong
			// DDL — the worst class).
			name: "partial index — DESC + WHERE predicate preserved",
			idx: &ir.Index{
				Name: "users_published",
				Kind: ir.IndexKindBTree,
				Columns: []ir.IndexColumn{
					{Column: "published_at", Desc: true},
				},
				Predicate:        "status = 'published'::post_status",
				PredicateDialect: "postgres",
			},
			want: `CREATE INDEX "users_published" ON "public"."users" USING btree ("published_at" DESC) WHERE status = 'published'::post_status;`,
		},
		{
			// Bug 19b: covering index — INCLUDE payload columns kept
			// distinct from the key list. Pre-fix they were flattened
			// into the key, silently changing index semantics.
			name: "covering index — INCLUDE non-key columns preserved",
			idx: &ir.Index{
				Name:           "users_acct",
				Kind:           ir.IndexKindBTree,
				Columns:        []ir.IndexColumn{{Column: "account_id"}},
				IncludeColumns: []string{"title", "score"},
			},
			want: `CREATE INDEX "users_acct" ON "public"."users" USING btree ("account_id") INCLUDE ("title", "score");`,
		},
		{
			// Bug 19a: explicit non-default NULLS ordering. ASC default
			// is NULLS LAST; NULLS FIRST is non-default and must be
			// emitted.
			name: "ASC with explicit NULLS FIRST",
			idx: &ir.Index{
				Name:    "users_ranked",
				Kind:    ir.IndexKindBTree,
				Columns: []ir.IndexColumn{{Column: "rank", NullsFirst: boolPtr(true)}},
			},
			want: `CREATE INDEX "users_ranked" ON "public"."users" USING btree ("rank" NULLS FIRST);`,
		},
		{
			// Unique partial index — the silent uniqueness-scope-change
			// case called out in catalog Bug 19a.
			name: "unique partial index",
			idx: &ir.Index{
				Name:             "users_active_email",
				Unique:           true,
				Kind:             ir.IndexKindBTree,
				Columns:          []ir.IndexColumn{{Column: "email"}},
				Predicate:        "active",
				PredicateDialect: "postgres",
			},
			want: `CREATE UNIQUE INDEX "users_active_email" ON "public"."users" USING btree ("email") WHERE active;`,
		},
	}
	for _, c := range cases {
		c := c
		t.Run(c.name, func(t *testing.T) {
			got, err := emitCreateIndex("public", "users", c.idx, emitOpts{})
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if got != c.want {
				t.Errorf("\n got  %q\n want %q", got, c.want)
			}
		})
	}
}

func TestEmitAddForeignKey(t *testing.T) {
	fk := &ir.ForeignKey{
		Name:              "posts_user_id_fk",
		Columns:           []string{"user_id"},
		ReferencedTable:   "users",
		ReferencedColumns: []string{"id"},
		OnDelete:          ir.FKActionCascade,
		OnUpdate:          ir.FKActionRestrict,
	}
	got, err := emitAddForeignKey("public", "posts", fk)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	want := `ALTER TABLE "public"."posts" ADD CONSTRAINT "posts_user_id_fk" FOREIGN KEY ("user_id") REFERENCES "public"."users" ("id") ON DELETE CASCADE ON UPDATE RESTRICT;`
	if got != want {
		t.Errorf("\n got  %q\n want %q", got, want)
	}
}

// TestEmitAddForeignKey_SelfReferential pins the self-referential FK
// shape (employees.manager_id → employees.id). Sluice's three-phase
// apply (tables → bulk_copy → indexes → constraints) sidesteps the
// create-order problem because FKs land in phase 5 after all tables
// exist; this test pins the DDL emit so a regression couldn't drop
// the self-ref support silently. Per design/schema-completeness.md.
func TestEmitAddForeignKey_SelfReferential(t *testing.T) {
	fk := &ir.ForeignKey{
		Name:              "employees_manager_fk",
		Columns:           []string{"manager_id"},
		ReferencedTable:   "employees", // same table as the parent
		ReferencedColumns: []string{"id"},
		OnDelete:          ir.FKActionSetNull,
	}
	got, err := emitAddForeignKey("public", "employees", fk)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	want := `ALTER TABLE "public"."employees" ADD CONSTRAINT "employees_manager_fk" FOREIGN KEY ("manager_id") REFERENCES "public"."employees" ("id") ON DELETE SET NULL;`
	if got != want {
		t.Errorf("\n got  %q\n want %q", got, want)
	}
}

// TestEmitAddForeignKey_CompositePK pins the composite-PK FK shape
// (a child whose FK references a parent's two-column primary key).
// Real-world tenant-scoped data models often use (tenant_id, id)
// composite PKs; their FKs from child tables need both columns.
func TestEmitAddForeignKey_CompositePK(t *testing.T) {
	fk := &ir.ForeignKey{
		Name:              "orders_customer_fk",
		Columns:           []string{"tenant_id", "customer_id"},
		ReferencedTable:   "customers",
		ReferencedColumns: []string{"tenant_id", "id"},
		OnDelete:          ir.FKActionCascade,
		OnUpdate:          ir.FKActionCascade,
	}
	got, err := emitAddForeignKey("public", "orders", fk)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	want := `ALTER TABLE "public"."orders" ADD CONSTRAINT "orders_customer_fk" FOREIGN KEY ("tenant_id", "customer_id") REFERENCES "public"."customers" ("tenant_id", "id") ON DELETE CASCADE ON UPDATE CASCADE;`
	if got != want {
		t.Errorf("\n got  %q\n want %q", got, want)
	}
}

// TestEmitAddForeignKey_AllOnDeleteActions pins the round-trip of
// every supported ir.FKAction value through the PG emitter. Each
// action has a different SQL keyword; a regression that swapped two
// of them would silently change cascade behavior on the target.
func TestEmitAddForeignKey_AllOnDeleteActions(t *testing.T) {
	cases := []struct {
		action ir.FKAction
		want   string
	}{
		{ir.FKActionNoAction, ""}, // omitted from output
		{ir.FKActionRestrict, "RESTRICT"},
		{ir.FKActionCascade, "CASCADE"},
		{ir.FKActionSetNull, "SET NULL"},
		{ir.FKActionSetDefault, "SET DEFAULT"},
	}
	for _, c := range cases {
		c := c
		t.Run(c.action.String(), func(t *testing.T) {
			fk := &ir.ForeignKey{
				Name:              "fk_test",
				Columns:           []string{"x"},
				ReferencedTable:   "parent",
				ReferencedColumns: []string{"id"},
				OnDelete:          c.action,
			}
			got, err := emitAddForeignKey("public", "child", fk)
			if err != nil {
				t.Fatalf("emitAddForeignKey: %v", err)
			}
			if c.want == "" {
				// NO ACTION shouldn't render an explicit clause —
				// the column-default behavior matches.
				if strings.Contains(got, "ON DELETE") {
					t.Errorf("FKActionNoAction should not render ON DELETE clause; got:\n%s", got)
				}
				return
			}
			if !strings.Contains(got, "ON DELETE "+c.want) {
				t.Errorf("expected ON DELETE %s in output; got:\n%s", c.want, got)
			}
		})
	}
}

func TestQuoteSQLString(t *testing.T) {
	cases := []struct{ in, want string }{
		{"hello", "'hello'"},
		{"it's", "'it''s'"},
		{"", "''"},
	}
	for _, c := range cases {
		if got := quoteSQLString(c.in); got != c.want {
			t.Errorf("quoteSQLString(%q) = %q; want %q", c.in, got, c.want)
		}
	}
}

// TestQuoteSQLString_BackslashDeliberatelyNotEscaped is the SEC-1b
// reverse-direction pin (MySQL-source backslash-bearing literals → PG target).
// PG's quoteSQLString doubles ONLY the interior single quote; it deliberately
// does NOT escape backslashes, because every sluice PG session pins
// standard_conforming_strings=on (see connect.go's pinStandardConformingStrings),
// under which a backslash is an ORDINARY character in a '…' literal — so the raw
// value bytes round-trip verbatim without any backslash doubling. Doubling here
// would instead STORE a second backslash (silent corruption). The MySQL reader
// hands the writer RAW value bytes (COLUMN_DEFAULT / COLUMN_COMMENT / TABLE_COMMENT
// arrive decoded; parseEnumOrSet decodes ENUM/SET labels — ground-truthed on
// MySQL 8.0), so these inputs are exactly what a MySQL source produces. The
// {plain, trailing, doubled, quote-adjacent} backslash matrix:
func TestQuoteSQLString_BackslashDeliberatelyNotEscaped(t *testing.T) {
	cases := []struct {
		name, in, want string
	}{
		{"plain interior backslash", `a\b`, `'a\b'`},
		{"trailing backslash", `ab\`, `'ab\'`},
		{"doubled backslash", `a\\b`, `'a\\b'`},
		{"quote-adjacent backslash", `a\'b`, `'a\''b'`},
	}
	for _, c := range cases {
		if got := quoteSQLString(c.in); got != c.want {
			t.Errorf("%s: quoteSQLString(%q) = %q; want %q (backslash is literal under standard_conforming_strings=on — do NOT double it)", c.name, c.in, got, c.want)
		}
	}
}

// TestEmitTableDef_NoPKInlineUniqueKey is the Bug 125 PG pin: a PK-less
// table with a NOT-NULL UNIQUE key emits the chosen unique key inline as
// a CONSTRAINT ... UNIQUE (...) so PG's ON CONFLICT (cols) has a real
// matching unique index to infer against while the cold-start COPY lands
// rows. The deterministic pick (fewest cols, then lex-smallest name) is
// the same key effectiveUpsertKeyColumns / the applier resolve.
func TestEmitTableDef_NoPKInlineUniqueKey(t *testing.T) {
	tbl := &ir.Table{
		Name: "connections",
		Columns: []*ir.Column{
			{Name: "id", Type: ir.Integer{Width: 64}, Nullable: false},
			{Name: "payload", Type: ir.Text{}, Nullable: true},
		},
		Indexes: []*ir.Index{
			{Name: "uq_id", Unique: true, Columns: []ir.IndexColumn{{Column: "id"}}},
		},
	}
	got, err := emitTableDef("public", tbl, emitOpts{})
	if err != nil {
		t.Fatalf("emitTableDef: %v", err)
	}
	want := `CONSTRAINT "connections_uq_id" UNIQUE ("id")`
	if !strings.Contains(got, want) {
		t.Errorf("CREATE TABLE missing inline unique constraint %q\n--- got ---\n%s", want, got)
	}
	// No PRIMARY KEY clause — this table has none.
	if strings.Contains(got, "PRIMARY KEY") {
		t.Errorf("PK-less table emitted a PRIMARY KEY clause:\n%s", got)
	}
}

// TestEmitTableDef_NoPKCompositeInlineUniqueKey pins the composite-key
// shape: a PK-less table whose only qualifying key is a 2-column
// NOT-NULL UNIQUE index emits both columns in the inline CONSTRAINT.
func TestEmitTableDef_NoPKCompositeInlineUniqueKey(t *testing.T) {
	tbl := &ir.Table{
		Name: "edges",
		Columns: []*ir.Column{
			{Name: "src", Type: ir.Integer{Width: 64}, Nullable: false},
			{Name: "dst", Type: ir.Integer{Width: 64}, Nullable: false},
			{Name: "weight", Type: ir.Integer{Width: 32}, Nullable: true},
		},
		Indexes: []*ir.Index{
			{Name: "uq_src_dst", Unique: true, Columns: []ir.IndexColumn{{Column: "src"}, {Column: "dst"}}},
		},
	}
	got, err := emitTableDef("public", tbl, emitOpts{})
	if err != nil {
		t.Fatalf("emitTableDef: %v", err)
	}
	want := `CONSTRAINT "edges_uq_src_dst" UNIQUE ("src", "dst")`
	if !strings.Contains(got, want) {
		t.Errorf("CREATE TABLE missing composite inline unique constraint %q\n--- got ---\n%s", want, got)
	}
}

// TestEmitTableDef_PKTableNoInlineUniquePromotion confirms a PK table is
// unchanged by the Bug-125 path: the PK serves as the conflict key, so
// no extra inline UNIQUE constraint is promoted (the secondary unique
// index defers to Phase 2 as before).
func TestEmitTableDef_PKTableNoInlineUniquePromotion(t *testing.T) {
	tbl := &ir.Table{
		Name: "users",
		Columns: []*ir.Column{
			{Name: "id", Type: ir.Integer{Width: 64}, Nullable: false},
			{Name: "email", Type: ir.Varchar{Length: 255}, Nullable: false},
		},
		PrimaryKey: &ir.Index{Columns: []ir.IndexColumn{{Column: "id"}}},
		Indexes: []*ir.Index{
			{Name: "uq_email", Unique: true, Columns: []ir.IndexColumn{{Column: "email"}}},
		},
	}
	got, err := emitTableDef("public", tbl, emitOpts{})
	if err != nil {
		t.Fatalf("emitTableDef: %v", err)
	}
	if !strings.Contains(got, `PRIMARY KEY ("id")`) {
		t.Errorf("PK table missing PRIMARY KEY clause:\n%s", got)
	}
	// The secondary unique index is NOT promoted inline — it defers to
	// Phase 2's CREATE UNIQUE INDEX. (inlineUniqueKeyForCopy returns nil
	// when a PK is present.)
	if strings.Contains(got, `CONSTRAINT "users_uq_email" UNIQUE`) {
		t.Errorf("PK table wrongly promoted a secondary unique index inline:\n%s", got)
	}
}

// TestEmitTableDef_KeylessNoInlinePromotion confirms a truly-keyless
// PK-less table (no qualifying non-null unique index) emits no inline
// UNIQUE constraint. (The cold-start writer refuses such tables loudly
// at copy time; the DDL stays a plain column list.)
func TestEmitTableDef_KeylessNoInlinePromotion(t *testing.T) {
	tbl := &ir.Table{
		Name: "log_lines",
		Columns: []*ir.Column{
			{Name: "ts", Type: ir.Timestamp{}, Nullable: false},
			{Name: "msg", Type: ir.Text{}, Nullable: true},
		},
	}
	got, err := emitTableDef("public", tbl, emitOpts{})
	if err != nil {
		t.Fatalf("emitTableDef: %v", err)
	}
	if strings.Contains(got, "UNIQUE") || strings.Contains(got, "PRIMARY KEY") {
		t.Errorf("keyless table emitted an unexpected key constraint:\n%s", got)
	}
}

// TestInlineUniqueKeyForCopy_NullableUniqueExcluded pins that a PK-less
// table whose only unique index is over a NULLABLE column does NOT get
// inline-promoted (PG NULLS DISTINCT makes it an unreliable conflict
// key — same hazard as no key).
func TestInlineUniqueKeyForCopy_NullableUniqueExcluded(t *testing.T) {
	tbl := &ir.Table{
		Name: "t",
		Columns: []*ir.Column{
			{Name: "id", Type: ir.Integer{Width: 64}, Nullable: true},
		},
		Indexes: []*ir.Index{
			{Name: "uq_id", Unique: true, Columns: []ir.IndexColumn{{Column: "id"}}},
		},
	}
	if idx := inlineUniqueKeyForCopy(tbl); idx != nil {
		t.Errorf("inlineUniqueKeyForCopy promoted a nullable-unique key %q; want nil", idx.Name)
	}
}

// TestInlineSkipIndexNames pins the index-build skip: the inline-promoted
// unique key is in the skip set, an unrelated secondary index is not.
func TestInlineSkipIndexNames(t *testing.T) {
	tbl := &ir.Table{
		Name: "connections",
		Columns: []*ir.Column{
			{Name: "id", Type: ir.Integer{Width: 64}, Nullable: false},
			{Name: "host", Type: ir.Varchar{Length: 255}, Nullable: false},
		},
		Indexes: []*ir.Index{
			{Name: "uq_id", Unique: true, Columns: []ir.IndexColumn{{Column: "id"}}},
			{Name: "idx_host", Unique: false, Columns: []ir.IndexColumn{{Column: "host"}}},
		},
	}
	skip := inlineSkipIndexNames(tbl)
	if _, ok := skip["uq_id"]; !ok {
		t.Errorf("inlineSkipIndexNames omitted the promoted unique key uq_id: %v", skip)
	}
	if _, ok := skip["idx_host"]; ok {
		t.Errorf("inlineSkipIndexNames wrongly skipped the secondary index idx_host: %v", skip)
	}
	// PK table: nothing inline-promoted, so nothing skipped.
	pkTbl := &ir.Table{
		Name:       "u",
		Columns:    []*ir.Column{{Name: "id", Nullable: false}},
		PrimaryKey: &ir.Index{Columns: []ir.IndexColumn{{Column: "id"}}},
		Indexes:    []*ir.Index{{Name: "uq_id", Unique: true, Columns: []ir.IndexColumn{{Column: "id"}}}},
	}
	if len(inlineSkipIndexNames(pkTbl)) != 0 {
		t.Errorf("inlineSkipIndexNames on a PK table = %v; want empty", inlineSkipIndexNames(pkTbl))
	}
}
