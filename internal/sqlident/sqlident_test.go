// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package sqlident

import (
	"strings"
	"testing"

	"sluicesync.dev/sluice/internal/sluicecode"
)

// TestIsBareAcceptsEveryRealValue pins the accept side against the values
// the readers actually produce at the five bare positions. This is the
// half that keeps the guard from becoming a regression: an access method
// or opclass sluice refuses is a migration that used to work and now
// does not.
func TestIsBareAcceptsEveryRealValue(t *testing.T) {
	for _, s := range []string{
		// index access methods, core + extension (ADR-0032)
		"btree", "hash", "gin", "gist", "spgist", "brin", "ivfflat", "hnsw", "bm25", "rum",
		// operator classes, core + extension
		"gin_trgm_ops", "gist_trgm_ops", "text_pattern_ops", "varchar_pattern_ops",
		"jsonb_path_ops", "vector_l2_ops", "vector_cosine_ops", "gist__int_ops", "_int4_ops",
		// sequence types
		"smallint", "integer", "bigint",
		// MySQL charsets + collations
		"utf8mb4", "utf8mb3", "latin1", "binary", "armscii8",
		"utf8mb4_0900_ai_ci", "utf8mb4_zh_0900_as_cs", "latin1_swedish_ci", "utf8mb4_bin",
		// shapes the pattern deliberately allows
		"a", "A1", "_leading_underscore", "has$dollar", "MixedCase99",
	} {
		if !IsBare(s) {
			t.Errorf("IsBare(%q) = false; a legitimate value would be refused", s)
		}
	}
}

// TestIsBareRefusesEveryInjectionShape pins the refuse side across the
// shapes that make a bare position dangerous — separators, quotes,
// comment introducers, whitespace, and the leading-digit/empty edges.
func TestIsBareRefusesEveryInjectionShape(t *testing.T) {
	for _, s := range []string{
		`btree; CREATE ROLE attacker SUPERUSER; --`,
		`btree;DROP TABLE t`,
		`btree --`,
		`btree /* c */`,
		`btree, hash`,
		`bt ree`,
		`"btree"`,
		`'btree'`,
		"bt\nree",
		"bt\tree",
		"bt`ree",
		`public.gin_trgm_ops`, // qualified: no reader produces it, and the dot is a boundary
		`1btree`,
		`$btree`,
		``,       // empty is refused by IsBare; Check() accepts it as "no clause"
		` `,      // whitespace-only
		`btree `, // trailing space
	} {
		if IsBare(s) {
			t.Errorf("IsBare(%q) = true; an injection shape would be emitted bare", s)
		}
	}
}

// TestCheckAcceptsEmptyAndRefusesShape pins Check's contract: empty means
// "the emitter omits the clause", anything else must be bare.
func TestCheckAcceptsEmptyAndRefusesShape(t *testing.T) {
	if err := Check("index access method", "postgres: index \"i\"", ""); err != nil {
		t.Errorf("empty value refused: %v", err)
	}
	if err := Check("index access method", "postgres: index \"i\"", "hnsw"); err != nil {
		t.Errorf("legitimate value refused: %v", err)
	}
	err := Check("index access method", `postgres: index "i" on "public"."t"`, `btree; DROP TABLE t; --`)
	if err == nil {
		t.Fatal("hostile value accepted")
	}
	ce, ok := sluicecode.FromError(err)
	if !ok || ce.Code != sluicecode.CodeSchemaIdentifierInvalid {
		t.Fatalf("refused with %v, want code %q", err, sluicecode.CodeSchemaIdentifierInvalid)
	}
	// The refusal names the object, the position, and the value — all
	// three, because an operator with a large schema needs to find it.
	for _, want := range []string{`"public"."t"`, "index access method", "DROP TABLE"} {
		if !strings.Contains(err.Error(), want) {
			t.Errorf("refusal does not carry %q: %v", want, err)
		}
	}
}

// TestCheckOneOfIsAClosedSet pins that the keyword positions are closed:
// a value can be a perfectly bare identifier and still be refused.
func TestCheckOneOfIsAClosedSet(t *testing.T) {
	const where = `postgres: sequence public.s`
	if err := CheckOneOf("sequence data type", where, "bigint", "smallint", "integer", "bigint"); err != nil {
		t.Errorf("accepted value refused: %v", err)
	}
	if err := CheckOneOf("sequence data type", where, "", "smallint", "integer", "bigint"); err != nil {
		t.Errorf("empty value refused: %v", err)
	}
	// Bare, and still not allowed here.
	err := CheckOneOf("sequence data type", where, "text", "smallint", "integer", "bigint")
	if err == nil {
		t.Fatal("out-of-set value accepted")
	}
	if ce, ok := sluicecode.FromError(err); !ok || ce.Code != sluicecode.CodeSchemaIdentifierInvalid {
		t.Fatalf("refused with %v, want code %q", err, sluicecode.CodeSchemaIdentifierInvalid)
	}
	if !strings.Contains(err.Error(), "smallint, integer, bigint") {
		t.Errorf("refusal does not list the accepted values: %v", err)
	}
}
