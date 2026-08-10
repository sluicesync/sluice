// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package docsync

import (
	"sort"
	"testing"

	"sluicesync.dev/sluice/internal/engines"
	"sluicesync.dev/sluice/internal/ir"

	// Blank-imported for their init() self-registration, the same set
	// cmd/sluice/main.go links — this test iterates the REAL registry, so a
	// new engine package added there must be added here too (the non-vacuity
	// floor below fails rather than silently under-reporting).
	_ "sluicesync.dev/sluice/internal/engines/d1-trigger"
	_ "sluicesync.dev/sluice/internal/engines/flatfile"
	_ "sluicesync.dev/sluice/internal/engines/mydumper"
	_ "sluicesync.dev/sluice/internal/engines/mysql"
	_ "sluicesync.dev/sluice/internal/engines/pgtrigger"
	_ "sluicesync.dev/sluice/internal/engines/postgres"
	_ "sluicesync.dev/sluice/internal/engines/sqlite"
	_ "sluicesync.dev/sluice/internal/engines/sqlite-trigger"
)

// WHICH ENGINES ANSWER THE PRE-COPY COLUMN-TYPE QUESTION, kept honest against
// the registry.
//
// # Why the fail-by-default shape, again
//
// [ir.ColumnTypeEmitPreflighter] is OPTIONAL, so a target engine that never
// implemented it is skipped in silence and the `var _ ir.ColumnTypeEmitPreflighter
// = Engine{}` assertions in the engines that DID implement it read as coverage
// of the whole set. That is not hypothetical for this interface specifically:
// `postgres-trigger` composes postgres.Engine and delegates its entire schema
// write to it, but a method on the composed struct is not promoted through the
// wrapper's own method set unless it is written — the identical omission the
// index roster next door exists to catch.
//
// # Scope of THIS gate, stated so it cannot be read as broader
//
//   - It proves each target-capable engine ANSWERS the question. It does not
//     prove the answer is right, and it does not prove the answer is COMPLETE:
//     each engine's own TestPreflightColumnTypesAgreesWithTheTableEmitter is
//     what compares the preflight's verdict against the real emitter's over the
//     whole [ir.AllTypes] universe, in BOTH directions.
//   - It says nothing about the two refusals that need a connection or a flag
//     (Postgres's PostGIS-absent and `--enable-pg-extension` arms). Those are a
//     declared GAP of the interface itself, named on
//     [ir.ColumnTypeEmitPreflighter], not something an engine here is exempt
//     from.
//   - It does not prove the orchestrator CALLS it — that is
//     TestIndexEmitPreflightReachesEveryCopyEntryPoint in internal/pipeline.
var columnTypePreflightExempt = map[string]string{
	// Source-only engines: no column DDL is ever emitted for them as a target,
	// so there is no target type surface to render into. Same set and same
	// reasons as indexPreflightExempt.
	"csv":            "source only — OpenSchemaWriter returns ErrNotImplemented, so no column DDL is ever emitted for this engine as a target",
	"tsv":            "source only — the same flatfile engine as csv, registered once per format",
	"ndjson":         "source only — the same flatfile engine as csv, registered once per format",
	"mydumper":       "source only — a dump directory is read, never written; OpenSchemaWriter returns ErrNotImplemented",
	"d1":             "migrate/sync SOURCE only — OpenSchemaWriter returns ErrD1NotImplemented. There is no D1 target engine at all: a D1-bound migration writes a SQLite FILE via the `sqlite` engine, which does implement the preflight",
	"sqlite-trigger": "CDC source only (ADR-0134) — a SQLite target uses the `sqlite` engine",
	"d1-trigger":     "CDC source only — OpenSchemaWriter returns ErrNotImplemented",
}

func TestEveryTargetCapableEngineAnswersTheColumnTypePreflight(t *testing.T) {
	names := engines.Names()

	// Anti-vacuity floor: an empty or truncated registry would pass every
	// assertion below. The blank imports above are the registry's content.
	if len(names) < 8 {
		t.Fatalf("registry holds %d engines (%v); the blank-import list has drifted from cmd/sluice and "+
			"this gate is checking a subset of the fleet", len(names), names)
	}

	var implementors, exempted []string
	for _, name := range names {
		e, ok := engines.Get(name)
		if !ok {
			t.Fatalf("engines.Names() reported %q but engines.Get did not return it", name)
		}
		_, isPreflighter := e.(ir.ColumnTypeEmitPreflighter)
		reason, isExempt := columnTypePreflightExempt[name]
		switch {
		case isPreflighter && isExempt:
			t.Errorf("%q implements ir.ColumnTypeEmitPreflighter AND is listed exempt. Remove the "+
				"exemption — a stale exemption reads as a decision and hides the real state.", name)
		case isPreflighter:
			implementors = append(implementors, name)
		case isExempt && reason == "":
			t.Errorf("%q is exempt with an EMPTY reason; an exemption without a reason is "+
				"indistinguishable from an oversight", name)
		case isExempt:
			exempted = append(exempted, name)
		default:
			t.Errorf("engine %q does not implement ir.ColumnTypeEmitPreflighter and is not in "+
				"columnTypePreflightExempt.\n\nIf it can be a migration TARGET, every \"I cannot render "+
				"this column type\" refusal it owns fires from its own CREATE TABLE — after the plan is "+
				"printed, the writers are open, and the tables ahead of the offending one are already "+
				"created. Implement PreflightColumnTypes by running the engine's OWN emitColumnType over "+
				"the schema (derive, never declare a supported-type list — the 2026-08-10 audit deleted "+
				"seven such fields). If it is a source only, add it to the roster with that reason.", name)
		}
	}

	// A stale exemption is the same defect as a missing one, read backwards:
	// it names an engine nobody registers any more and quietly inflates the
	// roster's apparent coverage.
	registered := map[string]bool{}
	for _, n := range names {
		registered[n] = true
	}
	for name := range columnTypePreflightExempt {
		if !registered[name] {
			t.Errorf("columnTypePreflightExempt names %q, which is not a registered engine — the "+
				"exemption is stale; drop it", name)
		}
	}

	// The other half of the anti-vacuity floor: an exemption map that grew to
	// cover everything would silence the gate entirely.
	if len(exempted) >= len(names) {
		t.Fatalf("every registered engine (%d of %d) is exempt from the column-type preflight; the "+
			"roster has stopped requiring anything", len(exempted), len(names))
	}
	sort.Strings(implementors)
	if len(implementors) < 4 {
		t.Errorf("only %d engines implement the preflight (%v). mysql (×4 flavors), postgres, "+
			"postgres-trigger and sqlite are all migration targets and all must.",
			len(implementors), implementors)
	}
}
