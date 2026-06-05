// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package mysql

import (
	"strings"

	"vitess.io/vitess/go/vt/schema"
)

// isVitessInternalTable reports whether tableName is a Vitess-internal
// lifecycle / online-DDL artifact (a shadow table, a GC-renamed table,
// or a vreplication/gh-ost/pt-osc helper) rather than a user table.
//
// This is the single source of truth for the ADR-0073 exclusion. Both
// the VStream COPY pump (cdc_vstream_snapshot.go) and the CDC dispatch
// (cdc_vstream.go) route through it BEFORE buffering, applying, or
// schema-snapshotting a row, so a Vitess-internal table that vtgate
// streams under the `Match:"/.*/"` filter is dropped, never copied or
// applied. Vitess's RE2 stream filters can't negative-lookahead-exclude
// these names, so the filter stays `/.*/` and the exclusion happens
// here at the dispatch/buffer choke points.
//
// The matcher delegates to Vitess's own
// [schema.IsInternalOperationTableName], which recognizes the
// version-dependent naming conventions of the Vitess release sluice
// vendors (v0.24.1). That covers:
//
//   - the v20+ unified format `_vt_<op>_<uuid32hex>_<ts14>_`, where
//     <op> is any 3-char code — `hld`/`prg`/`evc`/`drp` (GC states),
//     `vrp` (vreplication / online-DDL), `gho`/`ghc`/`del` (gh-ost).
//     `_vt_vrp_*` (the Bug-125 shadow) is this format's online-DDL code;
//   - the legacy gh-ost / vreplication form
//     `_<uuid>_<ts>_(gho|ghc|del|new|vrepl)`;
//   - the pt-online-schema-change form `_..._old`.
//
// The matcher is anchored to the Vitess helper rather than a
// hand-maintained prefix list precisely because the naming convention
// changed across Vitess versions (vitessio/vitess#14582); delegating
// outsources tracking to Vitess. A unit pin
// (vstream_internal_table_test.go) asserts the recognized set so a
// Vitess wording change fails a test rather than silently leaking an
// internal table into the stream.
//
// vtgate may qualify table names as "keyspace.table"; the helper expects
// the bare table name, so callers strip the keyspace prefix first (the
// internal-table naming is on the table component, never the keyspace).
func isVitessInternalTable(tableName string) bool {
	return schema.IsInternalOperationTableName(tableName)
}

// isVitessInternalDDL reports whether a DDL statement targets a
// Vitess-internal table — i.e. a shadow-table operation an online-DDL
// migration performs (`CREATE TABLE _vt_vrp_*`, `ALTER TABLE _vt_vrp_*`,
// `DROP TABLE _vt_*`, `RENAME TABLE _vt_*` …). Phase A (ADR-0073)
// ground-truthed that vttestserver's vtgate streams exactly these as DDL
// VEvents during an online ALTER, with the internal name in the
// statement text and an empty event `table` field.
//
// Why this matters for cutover survival: the VStream DDL handlers
// invalidate the WHOLE field cache on every DDL ("a DDL might have
// changed the column shape"). A shadow-table DDL does NOT change the
// logical user table's schema, so clearing the logical table's cached
// FIELDs on a `_vt_*` DDL would force a "row event without preceding
// FIELD event" loud floor on the next logical ROW if vtgate doesn't
// re-emit FIELD for the unchanged logical table — wedging an otherwise
// healthy stream over an internal artifact. Skipping the cache-clear for
// internal-table DDLs keeps the logical table identity-stable across the
// shadow build (ADR-0073 decision #2). The atomic cutover's rename swaps
// the shadow onto the logical name; that surfaces as a FIELD re-emit on
// the LOGICAL table (not an internal-table DDL), so it still flows
// through the normal ADR-0049 schema-history path.
//
// The matcher is deliberately conservative: it extracts the first table
// identifier after the DDL verb and tests it with
// [isVitessInternalTable]. On any parse it doesn't recognize it returns
// false (fail-safe: the cache is cleared, which is the pre-ADR-0073
// behaviour — correct, just possibly an unnecessary clear). It never
// returns true for a logical-table DDL, so it can't suppress a real
// schema-change invalidation.
func isVitessInternalDDL(stmt string) bool {
	tbl, ok := ddlTargetTable(stmt)
	if !ok {
		return false
	}
	return isVitessInternalTable(tbl)
}

// ddlTargetTable extracts the (unqualified) target table name from a
// CREATE / ALTER / DROP / RENAME TABLE statement. Returns ok=false for
// anything it doesn't confidently recognize. This is intentionally
// minimal — it exists only to spot Vitess-internal shadow-table DDLs
// (isVitessInternalDDL); it is NOT a general DDL parser.
func ddlTargetTable(stmt string) (table string, ok bool) {
	s := strings.TrimSpace(stmt)
	upper := strings.ToUpper(s)

	// Strip the leading verb.
	switch {
	case strings.HasPrefix(upper, "CREATE TABLE"):
		s = s[len("CREATE TABLE"):]
	case strings.HasPrefix(upper, "ALTER TABLE"):
		s = s[len("ALTER TABLE"):]
	case strings.HasPrefix(upper, "DROP TABLE"):
		s = s[len("DROP TABLE"):]
	case strings.HasPrefix(upper, "RENAME TABLE"):
		s = s[len("RENAME TABLE"):]
	default:
		return "", false
	}
	s = strings.TrimSpace(s)

	// Strip optional IF [NOT] EXISTS modifiers (CREATE/DROP).
	for _, mod := range []string{"IF NOT EXISTS", "IF EXISTS"} {
		if len(s) >= len(mod) && strings.EqualFold(s[:len(mod)], mod) {
			s = strings.TrimSpace(s[len(mod):])
			break
		}
	}
	if s == "" {
		return "", false
	}

	// The table reference is the first token, terminated by whitespace,
	// '(', or — for RENAME — the keyword separating it from the target.
	// A quoted identifier (`name`) is taken whole.
	var ref string
	if s[0] == '`' {
		if end := strings.IndexByte(s[1:], '`'); end >= 0 {
			ref = s[1 : 1+end]
		} else {
			return "", false
		}
	} else {
		ref = s
		if i := strings.IndexAny(ref, " \t\n\r("); i >= 0 {
			ref = ref[:i]
		}
	}
	if ref == "" {
		return "", false
	}

	// Strip an optional schema/keyspace qualifier ("ks.table") — the
	// internal-table naming is on the table component. The qualifier
	// can't itself be quoted-with-dots here (we already took a whole
	// backticked identifier above), so a plain last-dot split is safe.
	if dot := strings.LastIndexByte(ref, '.'); dot >= 0 {
		ref = ref[dot+1:]
	}
	ref = strings.Trim(ref, "`")
	if ref == "" {
		return "", false
	}
	return ref, true
}
