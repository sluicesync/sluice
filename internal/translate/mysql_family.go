// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package translate

import "strings"

// IsMySQLFamily reports whether the named engine speaks the MySQL SQL
// dialect — the gate every MySQL↔PG cross-engine notice/gap scanner in
// this package keys its engine-pair short-circuit on.
//
// This is the single source of truth for that list (Bug 186: the
// unsigned-bigint advisory keyed on a two-name subset, so the same
// schema that WARNed via --source-driver mysql was silent via
// mydumper — and ScanMySQLToPGGaps keyed on "mysql" alone, silently
// skipping planetscale/vitess/mydumper sources). The list must match
// the engines that declare ir.DDLDialectMySQL in their Capabilities;
// a registry-parity test enforces that in both directions, so
// registering a new MySQL-dialect engine without updating this helper
// fails CI rather than silently missing the notices.
//
// Used for both source and target gates: mydumper is source-only (it
// refuses as a target long before any scanner runs), so its presence
// here is inert on the target side.
func IsMySQLFamily(engine string) bool {
	return strings.EqualFold(engine, "mysql") ||
		strings.EqualFold(engine, "planetscale") ||
		strings.EqualFold(engine, "vitess") ||
		strings.EqualFold(engine, "mariadb") ||
		strings.EqualFold(engine, "mydumper")
}

// ProjectsJSONAsLongText reports whether the named SOURCE engine's CDC
// schema snapshots describe a JSON column as a long TEXT. MariaDB stores
// JSON as LONGTEXT with a JSON_VALID check: the cold start's schema read
// reports JSON (so the target holds JSON), while the change stream's
// snapshot of the same column is a long TEXT. On every other engine a
// long-TEXT-vs-JSON difference is a real one.
func ProjectsJSONAsLongText(engine string) bool {
	return strings.EqualFold(engine, "mariadb")
}
