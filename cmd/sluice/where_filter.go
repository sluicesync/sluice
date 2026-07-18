// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package main

import (
	"fmt"
	"strings"
)

// parseWhereFilters converts the operator's repeatable
// `--where TABLE=<predicate>` values (ADR-0173 Phase 1) into a
// source-table-name-keyed predicate map. The value is a native SOURCE-SQL
// boolean predicate emitted verbatim (same trust model as `backfill
// --where` — it is the operator's own argv; sluice always parenthesizes it
// when composing it into the read WHERE, but does no other injection
// defense).
//
// The split is at the FIRST `=`, so the predicate may itself contain `=`
// (e.g. `--where users=country = 'US'`). Table keys are the SOURCE table
// name (a `--map-database`/`--map-schema` rename still matches the
// original, like `--redact`) — exact names for v1, no globs.
//
// Refuses loudly on: a value with no `=`, an empty table name, an empty
// predicate, or a duplicate table key (two `--where` for the same table is
// almost certainly a mistake, and silently keeping the last would drop the
// operator's first predicate). Returns (nil, nil) when values is empty.
func parseWhereFilters(values []string) (map[string]string, error) {
	if len(values) == 0 {
		return nil, nil
	}
	out := make(map[string]string, len(values))
	for _, raw := range values {
		eq := strings.IndexByte(raw, '=')
		if eq < 0 {
			return nil, fmt.Errorf("--where %q: missing '=' between table and predicate (expected TABLE=<predicate>)", raw)
		}
		table := strings.TrimSpace(raw[:eq])
		predicate := strings.TrimSpace(raw[eq+1:])
		if table == "" {
			return nil, fmt.Errorf("--where %q: empty table name before '='", raw)
		}
		if predicate == "" {
			return nil, fmt.Errorf("--where %q: empty predicate after '='", raw)
		}
		if _, dup := out[table]; dup {
			return nil, fmt.Errorf("--where: table %q given more than once; combine the conditions into one predicate (e.g. 'a AND b')", table)
		}
		out[table] = predicate
	}
	return out, nil
}
