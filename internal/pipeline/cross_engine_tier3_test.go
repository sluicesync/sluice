// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package pipeline

import (
	"strings"
	"testing"

	"github.com/orware/sluice/internal/ir"
)

// TestCheckCrossEngineSupportable_PGtoMySQL_PgcryptoDefaultRefuses
// exercises the ADR-0044 §3 cross-engine refusal: a PG-source column
// with a pgcrypto crypto-function DEFAULT has no honest MySQL
// equivalent — translating crypto would change security semantics, so
// the migration is refused with a --expr-override pointer rather than
// fake-translated.
func TestCheckCrossEngineSupportable_PGtoMySQL_PgcryptoDefaultRefuses(t *testing.T) {
	s := &ir.Schema{Tables: []*ir.Table{{
		Name: "secrets",
		Columns: []*ir.Column{
			{Name: "id", Type: ir.Integer{Width: 64}},
			{Name: "token", Type: ir.Text{}, Default: ir.DefaultExpression{
				Expr:    "encode(gen_random_bytes(32), 'hex')",
				Dialect: "postgres",
			}},
		},
	}}}
	err := checkCrossEngineSupportable(s, "postgres", "mysql", "secrets-migration")
	if err == nil {
		t.Fatal("err = nil; want pgcrypto DEFAULT refusal")
	}
	for _, frag := range []string{
		"gen_random_bytes", "secrets", "token", "--expr-override", "DEFAULT",
	} {
		if !strings.Contains(err.Error(), frag) {
			t.Errorf("err missing %q; got: %v", frag, err)
		}
	}
}

// Generated-column pgcrypto expression is refused the same way.
func TestCheckCrossEngineSupportable_PGtoMySQL_PgcryptoGeneratedRefuses(t *testing.T) {
	s := &ir.Schema{Tables: []*ir.Table{{
		Name: "users",
		Columns: []*ir.Column{
			{Name: "pw", Type: ir.Text{}},
			{
				Name:            "pw_hash",
				Type:            ir.Text{},
				GeneratedExpr:   "digest(pw, 'sha256'::text)",
				GeneratedStored: true,
			},
		},
	}}}
	err := checkCrossEngineSupportable(s, "postgres", "mysql", "users-migration")
	if err == nil {
		t.Fatal("err = nil; want pgcrypto GENERATED refusal")
	}
	if !strings.Contains(err.Error(), "digest") ||
		!strings.Contains(err.Error(), "GENERATED") {
		t.Errorf("err = %v; want digest/GENERATED mention", err)
	}
}

// uuid-ossp generators DO have an honest MySQL mapping (→ UUID()) and
// must NOT be refused by the cross-engine check — they pass through
// to the pgToMySQLDefaultExpr translator. This pins ADR-0044's
// "translate the safe, refuse the unsafe" split.
func TestCheckCrossEngineSupportable_PGtoMySQL_UUIDOSSPDefaultAllowed(t *testing.T) {
	for _, expr := range []string{
		"uuid_generate_v4()", "uuid_generate_v1()", "uuid_generate_v1mc()",
	} {
		s := &ir.Schema{Tables: []*ir.Table{{
			Name: "t",
			Columns: []*ir.Column{
				{Name: "id", Type: ir.UUID{}, Default: ir.DefaultExpression{
					Expr: expr, Dialect: "postgres",
				}},
			},
		}}}
		if err := checkCrossEngineSupportable(s, "postgres", "mysql", "t-mig"); err != nil {
			t.Errorf("expr %q refused: %v; want allowed (honest UUID() mapping)", expr, err)
		}
	}
}

// gen_random_uuid() (core PG) must never trip the cross-engine
// pgcrypto refusal — it is not a pgcrypto function. Core-vs-extension
// guard, cross-engine side.
func TestCheckCrossEngineSupportable_PGtoMySQL_GenRandomUUIDAllowed(t *testing.T) {
	s := &ir.Schema{Tables: []*ir.Table{{
		Name: "t",
		Columns: []*ir.Column{
			{Name: "id", Type: ir.UUID{}, Default: ir.DefaultExpression{
				Expr: "gen_random_uuid()", Dialect: "postgres",
			}},
		},
	}}}
	if err := checkCrossEngineSupportable(s, "postgres", "mysql", "t-mig"); err != nil {
		t.Errorf("gen_random_uuid() refused: %v; want allowed (core PG)", err)
	}
}

// String-literal and substring forms must not false-positive the
// cross-engine scanner either.
func TestScanForCryptoFn_Conservative(t *testing.T) {
	negatives := []string{
		"",
		"'digest of the message'",  // inside a string literal
		"my_digest(x)",             // substring
		"digest_v2(x)",             // substring suffix
		"public.digest('a','md5')", // qualified — conservatively skipped
		"digest",                   // no call parens
	}
	for _, n := range negatives {
		if got := scanForCryptoFn(n); got != "" {
			t.Errorf("scanForCryptoFn(%q) = %q; want \"\"", n, got)
		}
	}
	if got := scanForCryptoFn("crypt('pw', gen_salt('bf'))"); got != "crypt" {
		t.Errorf("scanForCryptoFn = %q; want \"crypt\"", got)
	}
	if got := scanForCryptoFn("DIGEST(x,'sha256')"); got != "digest" {
		t.Errorf("scanForCryptoFn case-insensitive = %q; want \"digest\"", got)
	}
}
