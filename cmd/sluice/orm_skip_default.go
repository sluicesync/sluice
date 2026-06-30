// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package main

import "strings"

// ormEngineFamily groups engine driver names by the database family whose
// DDL / migration semantics they share, for the ADR-0143 ORM-skip default
// decision. An ORM migration-history table is VALID on a same-family target
// (the schema sluice replicates is exactly what those migrations would build
// on that engine, so the recorded history is consistent) and INVALID on a
// different family (the history references migrations written for the source
// engine, which never ran against the differently-built target) — so the
// skip-by-default is scoped to CROSS-family migrations.
//
// Flavors share a family: mysql / planetscale / vitess; postgres /
// postgres-trigger; sqlite / d1 / sqlite-trigger / d1-trigger. An unknown
// driver maps to itself (treated as its own family — cross-engine unless the
// names are identical), which is the conservative choice.
func ormEngineFamily(driver string) string {
	switch strings.ToLower(driver) {
	case "mysql", "planetscale", "vitess":
		return "mysql"
	case "postgres", "postgres-trigger":
		return "postgres"
	case "sqlite", "d1", "sqlite-trigger", "d1-trigger":
		return "sqlite"
	default:
		return strings.ToLower(driver)
	}
}

// resolveSkipORMTables computes the effective ADR-0143 SkipORMTables for a
// migrate / sync CLI run: skip ORM migration-history tables by default ONLY on
// a CROSS-family migration (where the carried history is invalid on the target
// engine); a same-family migration keeps them by default (the history is valid
// — a faithful copy is the right default). `--include-orm-tables` forces keep
// and `--skip-orm-tables` forces skip, each overriding the cross-engine
// default; the two flags are mutually exclusive (the caller rejects both).
func resolveSkipORMTables(sourceDriver, targetDriver string, includeFlag, skipFlag bool) bool {
	switch {
	case includeFlag:
		return false
	case skipFlag:
		return true
	default:
		return ormEngineFamily(sourceDriver) != ormEngineFamily(targetDriver)
	}
}
