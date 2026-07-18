// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package rowpredicate

import (
	"testing"

	"sluicesync.dev/sluice/internal/engines/mysql"
	"sluicesync.dev/sluice/internal/ir"
)

// Test resolvers for the collation-driven fidelity tests. These are the REAL
// production resolvers the pipeline threads: MySQL's Vitess-backed resolver
// (audit M2.1 — it lives in the engine now, not this package) and Postgres's
// byte-exact-or-refuse determinism resolver. Importing the mysql engine in a
// rowpredicate test is cycle-free (mysql imports ir, never rowpredicate).
var (
	testMySQLResolver                      = mysql.Engine{}.CollationResolver()
	testPGResolver    ir.CollationResolver = ir.ByteExactCollationResolver{}
)

// stringPolicy is the resolved client-side string-comparison policy a
// [ColumnInfo] encodes, made explicit for the assertions below.
type stringPolicy int

const (
	polRefuse    stringPolicy = iota // !Faithful — comparison refused
	polByteExact                     // Faithful, Compare == nil — Go `==`
	polFold                          // Faithful, Compare != nil — collation-aware comparator
)

func (p stringPolicy) String() string {
	switch p {
	case polByteExact:
		return "byte-exact"
	case polFold:
		return "collation-fold"
	default:
		return "refused"
	}
}

// policyOf classifies a ColumnInfo's string-comparison policy.
func policyOf(ci ColumnInfo) stringPolicy {
	if !ci.Faithful {
		return polRefuse
	}
	if ci.Compare != nil {
		return polFold
	}
	return polByteExact
}

// wantString asserts a column resolved to FamilyString with the given policy.
func wantString(t *testing.T, m map[string]ColumnInfo, col string, want stringPolicy) {
	t.Helper()
	if m[col].Family != FamilyString {
		t.Fatalf("%s: family = %d; want FamilyString", col, m[col].Family)
	}
	if got := policyOf(m[col]); got != want {
		t.Errorf("%s: string policy = %s; want %s", col, got, want)
	}
}
