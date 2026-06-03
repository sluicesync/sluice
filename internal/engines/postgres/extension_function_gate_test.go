// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package postgres

import (
	"strings"
	"testing"

	"sluicesync.dev/sluice/internal/ir"
)

// TestPGExtensionCatalog_Tier3Entries pins the ADR-0044 catalog
// surface: uuid-ossp is registered and pgcrypto carries the
// defaultExprFunctions set, while both keep the presence-gate /
// no-types shape (build + emitColumn refuse loudly).
func TestPGExtensionCatalog_Tier3Entries(t *testing.T) {
	uo, ok := pgExtensionCatalog["uuid-ossp"]
	if !ok {
		t.Fatal("pgExtensionCatalog missing 'uuid-ossp' entry")
	}
	for _, fn := range []string{
		"uuid_generate_v1", "uuid_generate_v1mc", "uuid_generate_v4",
		"uuid_generate_v5", "uuid_nil", "uuid_ns_dns", "uuid_ns_url",
		"uuid_ns_oid", "uuid_ns_x500",
	} {
		if _, has := uo.defaultExprFunctions[fn]; !has {
			t.Errorf("uuid-ossp defaultExprFunctions missing %q", fn)
		}
	}
	if len(uo.typesByName) != 0 {
		t.Errorf("uuid-ossp typesByName should be empty, got %v", uo.typesByName)
	}
	// build / emitColumn must refuse loudly (framework-misuse guards),
	// mirroring pgCryptoDef's shape exactly.
	if _, err := uo.build("uuid", -1); err == nil {
		t.Error("uuid-ossp build() = nil error; want loud refusal")
	}
	if _, err := uo.emitColumn(ir.ExtensionType{Extension: "uuid-ossp"}); err == nil {
		t.Error("uuid-ossp emitColumn() = nil error; want loud refusal")
	}

	pc, ok := pgExtensionCatalog["pgcrypto"]
	if !ok {
		t.Fatal("pgExtensionCatalog missing 'pgcrypto' entry")
	}
	for _, fn := range []string{
		"digest", "hmac", "crypt", "gen_salt", "gen_random_bytes",
		"encrypt", "decrypt", "encrypt_iv", "decrypt_iv",
		"pgp_sym_encrypt", "pgp_sym_decrypt", "pgp_pub_encrypt",
		"pgp_pub_decrypt",
	} {
		if _, has := pc.defaultExprFunctions[fn]; !has {
			t.Errorf("pgcrypto defaultExprFunctions missing %q", fn)
		}
	}
}

// TestScanExtensionFunction_GenRandomUUIDNotGated is the load-bearing
// ADR-0044 core-vs-extension correctness guard. gen_random_uuid() is
// core PostgreSQL 13+, NOT pgcrypto-owned on any supported modern PG.
// It MUST NOT be in any defaultExprFunctions set, so the scanner must
// never report it — gating it would refuse valid core-PG schemas.
func TestScanExtensionFunction_GenRandomUUIDNotGated(t *testing.T) {
	if _, _, found := scanExtensionFunctionInExpr("gen_random_uuid()"); found {
		t.Fatal("gen_random_uuid() was gated — ADR-0044 core-vs-extension " +
			"guard violated: it is core PG 13+, not an extension function")
	}
	// And the catalog lookup must agree.
	if ext, ok := lookupExtensionOwningFunction("gen_random_uuid"); ok {
		t.Fatalf("lookupExtensionOwningFunction(gen_random_uuid) = %q, true; "+
			"want \"\", false (core PG, must not be owned by any extension)", ext)
	}
	// The gate over a real column default must pass with NO extensions
	// enabled — scenario 4 in ADR-0044 §Testing.
	if err := extensionFunctionDefaultGate(
		"users", "id", "DEFAULT", "gen_random_uuid()", nil,
	); err != nil {
		t.Fatalf("gate refused gen_random_uuid() with no flag: %v "+
			"(must succeed — core function)", err)
	}
	// Other core functions also sail through.
	for _, core := range []string{"now()", "nextval('s'::regclass)", "CURRENT_TIMESTAMP"} {
		if _, _, found := scanExtensionFunctionInExpr(core); found {
			t.Errorf("core function %q was gated", core)
		}
	}
}

func TestScanExtensionFunctionInExpr_Positive(t *testing.T) {
	cases := []struct {
		expr    string
		wantFn  string
		wantExt string
	}{
		{"uuid_generate_v4()", "uuid_generate_v4", "uuid-ossp"},
		{"uuid_generate_v1mc()", "uuid_generate_v1mc", "uuid-ossp"},
		{"UUID_GENERATE_V4()", "UUID_GENERATE_V4", "uuid-ossp"},  // case-insensitive
		{"uuid_generate_v4 ()", "uuid_generate_v4", "uuid-ossp"}, // whitespace before paren
		{"digest('x', 'sha256')", "digest", "pgcrypto"},
		{"crypt('pw', gen_salt('bf'))", "crypt", "pgcrypto"}, // first hit wins (crypt before gen_salt)
		{"(uuid_generate_v4())", "uuid_generate_v4", "uuid-ossp"},
		{"COALESCE(NULL, uuid_generate_v4())", "uuid_generate_v4", "uuid-ossp"},
	}
	for _, c := range cases {
		fn, ext, found := scanExtensionFunctionInExpr(c.expr)
		if !found {
			t.Errorf("scan(%q) found=false; want %q/%s", c.expr, c.wantFn, c.wantExt)
			continue
		}
		if !strings.EqualFold(fn, c.wantFn) || ext != c.wantExt {
			t.Errorf("scan(%q) = %q/%s; want %q/%s", c.expr, fn, ext, c.wantFn, c.wantExt)
		}
	}
}

func TestScanExtensionFunctionInExpr_Negative(t *testing.T) {
	cases := []string{
		"",
		"42",
		"'hello'",
		"now()",
		"gen_random_uuid()",
		"'uuid_generate_v4()'",               // inside a string literal — data, not a call
		"'prefix uuid_generate_v4() suffix'", // still inside the literal
		"my_uuid_generate_v4()",              // substring: longer bareword, not the catalog name
		"uuid_generate_v4_custom()",          // substring suffix
		"public.uuid_generate_v4()",          // schema-qualified — conservatively NOT gated
		"x.digest('a')",                      // qualified — conservatively NOT gated
		"uuid_generate_v4",                   // no call parens
		"a_uuid_generate_v4()",               // identifier-byte before name
	}
	for _, expr := range cases {
		if fn, ext, found := scanExtensionFunctionInExpr(expr); found {
			t.Errorf("scan(%q) = %q/%s, found=true; want not found", expr, fn, ext)
		}
	}
}

// TestScanExtensionFunctionInExpr_StringLiteralWithEscapedQuote
// confirms the scanner's string-literal skip handles the doubled-quote
// escape so a function name after an escaped quote inside a literal is
// still treated as data.
func TestScanExtensionFunctionInExpr_StringLiteralWithEscapedQuote(t *testing.T) {
	expr := "'it''s uuid_generate_v4() here'"
	if _, _, found := scanExtensionFunctionInExpr(expr); found {
		t.Errorf("scan(%q) found a function inside a string literal with "+
			"an escaped quote", expr)
	}
	// A real call AFTER a closed literal IS matched.
	expr2 := "'literal' || uuid_generate_v4()"
	if _, ext, found := scanExtensionFunctionInExpr(expr2); !found || ext != "uuid-ossp" {
		t.Errorf("scan(%q) = found %v ext %q; want true/uuid-ossp", expr2, found, ext)
	}
}

func TestExtensionFunctionDefaultGate(t *testing.T) {
	// Not enabled → loud refusal naming fn, extension, both flags.
	err := extensionFunctionDefaultGate(
		"users", "id", "DEFAULT", "uuid_generate_v4()", nil,
	)
	if err == nil {
		t.Fatal("gate = nil; want refusal (uuid-ossp not enabled)")
	}
	msg := err.Error()
	for _, frag := range []string{
		`"users"."id"`, "uuid_generate_v4", "uuid-ossp",
		"--enable-pg-extension uuid-ossp", "--exclude-table", "DEFAULT",
	} {
		if !strings.Contains(msg, frag) {
			t.Errorf("refusal message missing %q; got: %s", frag, msg)
		}
	}

	// Enabled → passes through (nil).
	if err := extensionFunctionDefaultGate(
		"users", "id", "DEFAULT", "uuid_generate_v4()",
		map[string]bool{"uuid-ossp": true},
	); err != nil {
		t.Errorf("gate with uuid-ossp enabled = %v; want nil", err)
	}

	// GENERATED clause kind is woven into the message.
	gerr := extensionFunctionDefaultGate(
		"t", "c", "GENERATED", "digest(c, 'sha256')", nil,
	)
	if gerr == nil || !strings.Contains(gerr.Error(), "GENERATED") ||
		!strings.Contains(gerr.Error(), "pgcrypto") {
		t.Errorf("GENERATED/pgcrypto gate message wrong: %v", gerr)
	}

	// Core function with no flag → never gated.
	if err := extensionFunctionDefaultGate(
		"t", "c", "DEFAULT", "now()", nil,
	); err != nil {
		t.Errorf("gate refused core now(): %v", err)
	}
}
