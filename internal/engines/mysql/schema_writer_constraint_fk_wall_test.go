// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package mysql

import (
	"errors"
	"fmt"
	"testing"

	gomysql "github.com/go-sql-driver/mysql"

	"sluicesync.dev/sluice/internal/ir"
)

// TestFKWallRecoveryArmed pins the gate for the roadmap-item-109 FK
// statement-wall recovery — and, load-bearing, the same-engine MySQL→MySQL
// NO-OP: on a vanilla-MySQL target the recovery is ALWAYS disarmed even when
// the orchestrator declared the copied rows FK-consistent, so the constraints
// phase is byte-identical to before (no foreign_key_checks toggling, plain
// validating ADD). Only a VStream flavor (PlanetScale/Vitess — the sole MySQL
// family with the errno-3024 wall) AND an explicit declaration arms it.
func TestFKWallRecoveryArmed(t *testing.T) {
	cases := []struct {
		name       string
		flavor     Flavor
		consistent bool
		want       bool
	}{
		{"vanilla + declared consistent = DISARMED (same-engine no-op)", FlavorVanilla, true, false},
		{"vanilla + not declared = disarmed", FlavorVanilla, false, false},
		{"planetscale + declared consistent = ARMED", FlavorPlanetScale, true, true},
		{"planetscale + not declared = disarmed", FlavorPlanetScale, false, false},
		{"vitess + declared consistent = ARMED", FlavorVitess, true, true},
		{"mariadb + declared consistent = DISARMED (no VStream wall)", FlavorMariaDB, true, false},
	}
	for _, c := range cases {
		c := c
		t.Run(c.name, func(t *testing.T) {
			w := &SchemaWriter{flavor: c.flavor, copiedRowsFKConsistent: c.consistent}
			if got := w.fkWallRecoveryArmed(); got != c.want {
				t.Errorf("fkWallRecoveryArmed() = %v; want %v", got, c.want)
			}
		})
	}
}

// TestSetCopiedRowsForeignKeyConsistent pins the setter and the zero-value
// default (v0.99.51 lesson: the SAFE behaviour — full validation — must be the
// zero value, so every non-migrate construction path validates).
func TestSetCopiedRowsForeignKeyConsistent(t *testing.T) {
	w := &SchemaWriter{}
	if w.copiedRowsFKConsistent {
		t.Fatal("zero value must be false (validate) — the safe default for every non-migrate path")
	}
	var d ir.ForeignKeyConsistencyDeclarer = w
	d.SetCopiedRowsForeignKeyConsistent(true)
	if !w.copiedRowsFKConsistent {
		t.Fatal("SetCopiedRowsForeignKeyConsistent(true) did not set the field")
	}
	d.SetCopiedRowsForeignKeyConsistent(false)
	if w.copiedRowsFKConsistent {
		t.Fatal("SetCopiedRowsForeignKeyConsistent(false) did not clear the field")
	}
}

// TestIsConstraintBuildWalled pins the recovery trigger, deliberately narrower
// than the index build's isIndexBuildWalled: ONLY errno 3024 (the statement
// wall foreign_key_checks=0 can sidestep) triggers the metadata-only recovery.
// A safe-migrations 1105 direct-DDL block, an orphaned-child 1452, a generic
// 1105, a non-MySQL error, and nil must all be NOT-walled — recovering any of
// them would either be futile (1105) or silently land a violated FK (1452).
func TestIsConstraintBuildWalled(t *testing.T) {
	cases := []struct {
		name string
		err  error
		want bool
	}{
		{"errno 3024 statement wall", &gomysql.MySQLError{Number: 3024, Message: "Query execution was interrupted, maximum statement execution time exceeded"}, true},
		{"errno 3024 wrapped", fmt.Errorf("mysql: add foreign key: %w", &gomysql.MySQLError{Number: 3024, Message: "maximum statement execution time exceeded"}), true},
		{"errno 1105 direct DDL disabled (safe-migrations) — NOT walled", &gomysql.MySQLError{Number: 1105, Message: "direct DDL is disabled"}, false},
		{"errno 1452 orphaned child — NOT walled (must fail loudly)", &gomysql.MySQLError{Number: 1452, Message: "Cannot add or update a child row: a foreign key constraint fails"}, false},
		{"errno 1062 duplicate — NOT walled", &gomysql.MySQLError{Number: 1062, Message: "Duplicate entry"}, false},
		{"generic 1105 — NOT walled", &gomysql.MySQLError{Number: 1105, Message: "vttablet: something else"}, false},
		{"non-MySQL error — NOT walled", errors.New("dial tcp: connection refused"), false},
		{"nil — NOT walled", nil, false},
	}
	for _, c := range cases {
		c := c
		t.Run(c.name, func(t *testing.T) {
			if got := isConstraintBuildWalled(c.err); got != c.want {
				t.Errorf("isConstraintBuildWalled(%v) = %v; want %v", c.err, got, c.want)
			}
		})
	}
}
