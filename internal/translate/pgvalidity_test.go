// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package translate

import (
	"strings"
	"testing"

	"github.com/orware/sluice/internal/ir"
)

// schemaWithCheck builds a one-table schema with a single mysql-dialect
// CHECK constraint carrying expr. Helper for the adversarial pins.
func schemaWithCheck(expr string) *ir.Schema {
	return &ir.Schema{Tables: []*ir.Table{{
		Name: "t",
		CheckConstraints: []*ir.CheckConstraint{{
			Name: "t_ck", Expr: expr, ExprDialect: "mysql",
		}},
	}}}
}

// schemaWithGenerated builds a one-table schema with a single
// mysql-dialect GENERATED column carrying expr.
func schemaWithGenerated(expr string) *ir.Schema {
	return &ir.Schema{Tables: []*ir.Table{{
		Name: "t",
		Columns: []*ir.Column{{
			Name: "g", Type: ir.Integer{Width: 32},
			GeneratedExpr: expr, GeneratedExprDialect: "mysql",
		}},
	}}}
}

// schemaWithDefault builds a one-table schema with a single
// mysql-dialect DEFAULT expression carrying expr.
func schemaWithDefault(expr string) *ir.Schema {
	return &ir.Schema{Tables: []*ir.Table{{
		Name: "t",
		Columns: []*ir.Column{{
			Name: "c", Type: ir.Text{},
			Default: ir.DefaultExpression{Expr: expr, Dialect: "mysql"},
		}},
	}}}
}

// ---------------------------------------------------------------------
// ADVERSARIAL LOUD-REFUSE PINS — the whole point of Bug #14: these
// MySQL-only functions are NOT in the curated gapPatterns denylist, so
// v0.68.1's RefuseOnLoudGaps would have let them false-green. The
// general allowlist gate MUST refuse each at BOTH preview and migrate.
// ---------------------------------------------------------------------

// TestUntranslatable_NonCurated_LoudRefuse is the headline pin:
// SOUNDEX + a set of other real MySQL builtins, none in gapPatterns,
// each must loud-refuse at BOTH "schema preview" and "migrate".
func TestUntranslatable_NonCurated_LoudRefuse(t *testing.T) {
	// Each entry: a real MySQL builtin ABSENT from gaps.go's curated
	// gapPatterns set, in CHECK / GENERATED / DEFAULT position.
	cases := []struct {
		name string
		fn   string
		sch  *ir.Schema
	}{
		{"SOUNDEX in CHECK", "soundex", schemaWithCheck("SOUNDEX(name) = SOUNDEX('Smith')")},
		{"ELT in GENERATED", "elt", schemaWithGenerated("ELT(1, 'a', 'b', 'c')")},
		{"MAKE_SET in CHECK", "make_set", schemaWithCheck("MAKE_SET(1, 'a', 'b') <> ''")},
		{"BIT_COUNT in GENERATED", "bit_count", schemaWithGenerated("BIT_COUNT(flags)")},
		{"UUID_SHORT in DEFAULT", "uuid_short", schemaWithDefault("UUID_SHORT()")},
		{"INET6_ATON in CHECK", "inet6_aton", schemaWithCheck("INET6_ATON(addr) IS NOT NULL")},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			// Sanity: confirm the curated denylist does NOT catch it
			// (this is why Bug #14 exists — proves the gap is real).
			if loud := LoudGaps(ScanMySQLToPGGaps(c.sch, "mysql", "postgres", nil)); len(loud) != 0 {
				t.Fatalf("curated gapPatterns unexpectedly caught %s — pick a "+
					"function genuinely absent from the curated set", c.fn)
			}
			for _, ctxID := range []string{"schema preview", "migrate"} {
				err := RefuseOnUntranslatableExprs(c.sch, "mysql", "postgres", ctxID, nil)
				if err == nil {
					t.Fatalf("%s: %s did NOT loud-refuse (false-green — the exact Bug #14 failure)", ctxID, c.fn)
				}
				if !strings.Contains(strings.ToLower(err.Error()), c.fn) {
					t.Errorf("%s: refusal does not name %q; got: %s", ctxID, c.fn, err.Error())
				}
				if !strings.Contains(err.Error(), "--expr-override") {
					t.Errorf("%s: refusal missing --expr-override remedy", ctxID)
				}
				if ctxID == "migrate" && !strings.Contains(err.Error(), "partially creating the target") {
					t.Errorf("migrate: refusal should warn about partial target; got: %s", err.Error())
				}
			}
		})
	}
}

// TestUntranslatable_Bug12_CastUnsignedAndRegexpLike pins #12: both
// `CAST(... AS UNSIGNED)` and `regexp_like()` now LOUD-refuse (they
// were previously silent false-green / advisory-only).
func TestUntranslatable_Bug12(t *testing.T) {
	cases := []struct {
		name string
		want string
		sch  *ir.Schema
	}{
		{"CAST AS UNSIGNED", "cast(... as unsigned)", schemaWithGenerated("CAST(qty AS UNSIGNED)")},
		{"CAST AS SIGNED", "cast(... as signed)", schemaWithGenerated("CAST(qty AS SIGNED)")},
		{"CAST AS UNSIGNED INTEGER", "cast(... as unsigned)", schemaWithGenerated("CAST(qty AS UNSIGNED INTEGER)")},
		{"regexp_like in CHECK", "regexp_like", schemaWithCheck("regexp_like(code, '^[A-Z]+$')")},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			err := RefuseOnUntranslatableExprs(c.sch, "mysql", "postgres", "migrate", nil)
			if err == nil {
				t.Fatalf("%s did NOT loud-refuse (#12 must now loud-refuse, not silent)", c.name)
			}
			if !strings.Contains(strings.ToLower(err.Error()), c.want) {
				t.Errorf("refusal does not reference %q; got: %s", c.want, err.Error())
			}
		})
	}
}

// TestUntranslatable_Bug13_SpatialPointInGenerated pins #13: a
// `POINT(x, y)` spatial constructor in a generated-column expression
// now LOUD-refuses (was silent false-green; PG point() is deliberately
// absent from the allowlist for the cross-engine spatial case).
func TestUntranslatable_Bug13(t *testing.T) {
	sch := schemaWithGenerated("POINT(lon, lat)")
	for _, ctxID := range []string{"schema preview", "migrate"} {
		err := RefuseOnUntranslatableExprs(sch, "mysql", "postgres", ctxID, nil)
		if err == nil {
			t.Fatalf("%s: POINT(x,y) did NOT loud-refuse (#13 must now loud-refuse)", ctxID)
		}
		if !strings.Contains(strings.ToLower(err.Error()), "point") {
			t.Errorf("%s: refusal does not name point; got: %s", ctxID, err.Error())
		}
	}
}

// TestUntranslatable_Bug16_CastTargetTypeNotFlagged pins Bug #16: the
// v0.68.3 #14 scanner misread a *parameterized CAST target type*
// (`DECIMAL(10,2)`, `CHAR(20)`, `BINARY(16)`, `NCHAR(5)`, `x::decimal(10,2)`)
// as an unknown function call and spuriously refused schemas v0.68.2
// migrated CLEAN (translator rewrites CHAR→VARCHAR; PG accepts decimal
// natively). A CAST/`::` *target type specifier* is never a function
// call and must NOT trip the gate.
func TestUntranslatable_Bug16_CastTargetTypeNotFlagged(t *testing.T) {
	valid := []struct {
		name string
		expr string
		fld  string
	}{
		{"CAST AS DECIMAL(p,s) in GENERATED", "CAST(amount AS DECIMAL(10,2))", "GENERATED"},
		{"CAST AS CHAR(n) in CHECK", "CAST(code AS CHAR(8)) <> ''", "CHECK"},
		{"CAST AS BINARY(n) in GENERATED", "CAST(h AS BINARY(16))", "GENERATED"},
		{"CAST AS NCHAR(n) in DEFAULT", "CAST('x' AS NCHAR(5))", "DEFAULT"},
		{"CAST AS DEC(p,s)", "CAST(v AS DEC(12,4))", "GENERATED"},
		{"CAST AS CHARACTER(n)", "CAST(s AS CHARACTER(10))", "GENERATED"},
		{":: decimal(p,s)", "amount::decimal(10,2)", "GENERATED"},
		{"CAST AS DECIMAL nested in expr", "CAST(qty AS DECIMAL(10,2)) > 0", "CHECK"},
		{"CAST AS CHAR(n) with surrounding fn", "lower(CAST(code AS CHAR(8)))", "GENERATED"},
	}
	for _, v := range valid {
		t.Run(v.name, func(t *testing.T) {
			var sch *ir.Schema
			switch v.fld {
			case "CHECK":
				sch = schemaWithCheck(v.expr)
			case "GENERATED":
				sch = schemaWithGenerated(v.expr)
			case "DEFAULT":
				sch = schemaWithDefault(v.expr)
			}
			if err := RefuseOnUntranslatableExprs(sch, "mysql", "postgres", "migrate", nil); err != nil {
				t.Fatalf("Bug #16 FALSE POSITIVE — valid CAST-target %q refused: %s", v.expr, err.Error())
			}
		})
	}
}

// TestUntranslatable_Bug16_GuardStillRefuses pins the bidirectional
// guard: the #16 fix must be CONTEXT-AWARE (only a CAST/`::` *target*
// type-name is exempt). A type-name spelling used as an ordinary
// function call OUTSIDE cast-target position — MySQL's `CHAR(65)`
// scalar (no PG form; translator does NOT rewrite it) — must STILL
// loud-refuse, as must an unknown CAST target type. A blanket
// type-name allowlist (the wrong fix) would re-open the v0.68.1-class
// false-green here.
func TestUntranslatable_Bug16_GuardStillRefuses(t *testing.T) {
	cases := []struct {
		name string
		want string
		sch  *ir.Schema
	}{
		{"MySQL CHAR() scalar outside cast", "char", schemaWithGenerated("CHAR(code_point)")},
		{"unknown CAST target type", "bogustype", schemaWithGenerated("CAST(x AS bogustype(1))")},
		{"CAST AS UNSIGNED still refused", "cast(... as unsigned)", schemaWithGenerated("CAST(qty AS UNSIGNED)")},
		{"BINARY() scalar outside cast", "binary", schemaWithCheck("BINARY(name) = name")},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			err := RefuseOnUntranslatableExprs(c.sch, "mysql", "postgres", "migrate", nil)
			if err == nil {
				t.Fatalf("%s: must STILL loud-refuse (#16 fix must be context-aware, not a blanket allowlist)", c.name)
			}
			if !strings.Contains(strings.ToLower(err.Error()), c.want) {
				t.Errorf("refusal does not reference %q; got: %s", c.want, err.Error())
			}
		})
	}
}

// ---------------------------------------------------------------------
// FALSE-POSITIVE-SAFETY PINS — the load-bearing risk. A broad set of
// genuinely-PG-valid expressions must NOT trip the gate. A failure here
// means the gate refuses a valid schema (the real hazard, strictly
// worse than the pre-fix late-failure).
// ---------------------------------------------------------------------

func TestUntranslatable_FalsePositiveSafety_ValidExprs(t *testing.T) {
	valid := []struct {
		name string
		expr string
		fld  string // "CHECK" / "GENERATED" / "DEFAULT"
	}{
		// Bug-8 translated outputs (8a/8b/8c) and the v0.68.1 forms.
		{"8a JSON_VALID→IS JSON", "JSON_VALID(payload)", "CHECK"},
		{"8b <=>→IS NOT DISTINCT FROM", "a <=> b", "CHECK"},
		{"8c CURRENT_TIMESTAMP(N)", "CURRENT_TIMESTAMP(6)", "DEFAULT"},
		{"CURRENT_DATE", "CURDATE()", "DEFAULT"},
		{"NOW()", "NOW()", "DEFAULT"},
		// Common CHECK / generated / DEFAULT forms.
		{"bare comparison", "col > 0", "CHECK"},
		{"lower()", "lower(name) = name", "CHECK"},
		{"coalesce()", "coalesce(a, b)", "GENERATED"},
		{"gen_random_uuid()", "gen_random_uuid()", "DEFAULT"},
		{"arithmetic", "(price * qty) - discount", "GENERATED"},
		{"string concat ||", "first_name || ' ' || last_name", "GENERATED"},
		{":: cast", "amount::numeric(12,2)", "GENERATED"},
		{"CAST AS numeric (PG-valid)", "CAST(amount AS numeric(20,0))", "GENERATED"},
		{"CAST AS bigint (PG-valid)", "CAST(x AS bigint)", "GENERATED"},
		{"IN list", "status IN ('a','b','c')", "CHECK"},
		{"BETWEEN", "age BETWEEN 0 AND 120", "CHECK"},
		{"CASE expr", "CASE WHEN x > 0 THEN 1 ELSE 0 END", "GENERATED"},
		// Translator-rewritten MySQL functions (never reach PG verbatim).
		{"CONCAT (translated)", "CONCAT(a, b, c)", "GENERATED"},
		{"IFNULL (translated)", "IFNULL(a, b)", "GENERATED"},
		{"DATE_FORMAT (translated)", "DATE_FORMAT(d, '%Y-%m-%d')", "GENERATED"},
		{"SUBSTR (translated)", "SUBSTR(name, 1, 3)", "GENERATED"},
		// A column literally NAMED like a function — not a call.
		{"column named like func (no paren)", "soundex > 0", "CHECK"},
		{"column named coalesce (no paren)", "coalesce_flag = 1", "CHECK"},
		// Function name inside a string literal — data, not a call.
		{"func name in string literal", "note = 'call SOUNDEX(x) here'", "CHECK"},
		// Qualified call — conservatively skipped (not refused).
		{"schema-qualified call", "public.my_custom_fn(x) > 0", "CHECK"},
	}
	for _, v := range valid {
		t.Run(v.name, func(t *testing.T) {
			var sch *ir.Schema
			switch v.fld {
			case "CHECK":
				sch = schemaWithCheck(v.expr)
			case "GENERATED":
				sch = schemaWithGenerated(v.expr)
			case "DEFAULT":
				sch = schemaWithDefault(v.expr)
			}
			if err := RefuseOnUntranslatableExprs(sch, "mysql", "postgres", "migrate", nil); err != nil {
				t.Fatalf("FALSE POSITIVE — valid expr %q was refused: %s", v.expr, err.Error())
			}
		})
	}
}

// TestUntranslatable_FalsePositiveSafety_EnabledExtensionFn pins that
// an extension-owned function does NOT trip the gate when the operator
// enabled the extension (pgcrypto/uuid-ossp/pgvector).
func TestUntranslatable_FalsePositiveSafety_EnabledExtensionFn(t *testing.T) {
	cases := []struct {
		name string
		expr string
		ext  string
	}{
		{"pgcrypto digest enabled", "encode(digest(secret, 'sha256'), 'hex')", "pgcrypto"},
		{"uuid-ossp v4 enabled", "uuid_generate_v4()", "uuid-ossp"},
		{"pgvector l2_distance enabled", "l2_distance(embedding, embedding) < 1", "vector"},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			sch := schemaWithDefault(c.expr)
			enabled := map[string]bool{c.ext: true}
			if err := RefuseOnUntranslatableExprs(sch, "mysql", "postgres", "migrate", enabled); err != nil {
				t.Fatalf("FALSE POSITIVE — enabled-extension fn refused: %s", err.Error())
			}
			// And WITHOUT the flag it SHOULD refuse — the gate is real
			// and extension-gated. This includes pgcrypto's digest():
			// `digest` is deliberately NOT in pgValidFunctions (it is
			// pgcrypto-owned, not core), so without
			// `--enable-pg-extension pgcrypto` a bare digest() correctly
			// loud-refuses, consistent with gaps.go's SHA1/SHA2 gate.
			if err := RefuseOnUntranslatableExprs(sch, "mysql", "postgres", "migrate", nil); err == nil {
				t.Errorf("%s: expected refusal WITHOUT --enable-pg-extension %s", c.name, c.ext)
			}
		})
	}
}

// TestUntranslatable_ExprOverrideSuppresses pins that an
// `--expr-override` (which retags the expression dialect off "mysql")
// suppresses the refusal for that expression — exactly as the curated
// scan already respects post-override schema.
func TestUntranslatable_ExprOverrideSuppresses(t *testing.T) {
	// Same SOUNDEX expr, but dialect retagged to "postgres" (what
	// ApplyExpressionOverrides does for an overridden expression).
	sch := &ir.Schema{Tables: []*ir.Table{{
		Name: "t",
		Columns: []*ir.Column{{
			Name: "g", Type: ir.Integer{Width: 32},
			GeneratedExpr: "soundex_via_pg_equivalent(name)", GeneratedExprDialect: "postgres",
		}},
	}}}
	if err := RefuseOnUntranslatableExprs(sch, "mysql", "postgres", "migrate", nil); err != nil {
		t.Fatalf("post-override (non-mysql dialect) expr must NOT be gated: %s", err.Error())
	}
}

// TestUntranslatable_NonCrossEngine_NoRefusal pins the scoping: PG→MySQL
// and same-engine pairs are never gated (mirrors ScanMySQLToPGGaps).
func TestUntranslatable_NonCrossEngine_NoRefusal(t *testing.T) {
	sch := schemaWithCheck("SOUNDEX(name) > 0")
	for _, p := range [][2]string{
		{"postgres", "mysql"}, {"mysql", "mysql"}, {"postgres", "postgres"},
	} {
		if err := RefuseOnUntranslatableExprs(sch, p[0], p[1], "migrate", nil); err != nil {
			t.Errorf("%s→%s should not be gated; got: %s", p[0], p[1], err.Error())
		}
	}
	if got := ScanUntranslatableMySQLToPGExprs(nil, "mysql", "postgres", nil); got != nil {
		t.Errorf("nil schema must return nil; got %v", got)
	}
}

// TestUntranslatable_MultiSiteMessageNaming pins that every offending
// site is named and the message is consistent across both surfaces.
func TestUntranslatable_MultiSiteMessageNaming(t *testing.T) {
	sch := &ir.Schema{Tables: []*ir.Table{{
		Name: "orders",
		Columns: []*ir.Column{{
			Name: "code", Type: ir.Text{},
			Default: ir.DefaultExpression{Expr: "UUID_SHORT()", Dialect: "mysql"},
		}},
		CheckConstraints: []*ir.CheckConstraint{{
			Name: "orders_phon", Expr: "SOUNDEX(name) <> ''", ExprDialect: "mysql",
		}},
	}}}
	prev := RefuseOnUntranslatableExprs(sch, "mysql", "postgres", "schema preview", nil)
	migr := RefuseOnUntranslatableExprs(sch, "mysql", "postgres", "migrate", nil)
	if prev == nil || migr == nil {
		t.Fatal("expected refusals at both surfaces")
	}
	for _, err := range []error{prev, migr} {
		if !strings.Contains(err.Error(), "uuid_short") || !strings.Contains(err.Error(), "soundex") {
			t.Errorf("refusal must name BOTH offending sites; got: %s", err.Error())
		}
		if !strings.Contains(err.Error(), `column "code"`) || !strings.Contains(err.Error(), `constraint "orders_phon"`) {
			t.Errorf("refusal must name column and constraint sites; got: %s", err.Error())
		}
	}
}

// TestExtensionOwnedFunctions_NoCoreCollision is the false-positive
// guard mirror of ADR-0044's gen_random_uuid pin: a core function must
// NOT be claimed by the engine-neutral extension catalog (else enabling
// the extension would mask a core-function bug, and — more relevantly —
// the duplication must not drift to over-claim core names).
func TestExtensionOwnedFunctions_NoCoreCollision(t *testing.T) {
	for ext, funcs := range extensionOwnedFunctions {
		for fn := range funcs {
			if pgValidFunctions[fn] {
				t.Errorf("extension %q claims %q which is also in pgValidFunctions "+
					"(core) — a core function must not be extension-gated", ext, fn)
			}
		}
	}
	// gen_random_uuid is core, not pgcrypto (the ADR-0044 guard).
	if extensionFunctionEnabled("gen_random_uuid", map[string]bool{"pgcrypto": true}) {
		t.Error("gen_random_uuid must be core, never pgcrypto-gated")
	}
}
