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

// WHICH ENGINES ANSWER THE PRE-COPY INDEX QUESTION, kept honest against the
// registry (roadmap item 118).
//
// [ir.IndexEmitPreflighter] is OPTIONAL, and an optional interface is exactly
// the shape this project keeps losing an implementor to: the orchestrator
// type-asserts, a target engine that never implemented it is skipped in
// silence, and the compile-time `var _ ir.IndexEmitPreflighter = Engine{}`
// assertions in the engines that DID implement it read as coverage of the
// whole set.
//
// That is not hypothetical here. `postgres-trigger` composes postgres.Engine
// and delegates OpenSchemaWriter to it, so it writes the identical DDL — but a
// method on the composed struct is not promoted through the wrapper's own
// method set unless it is written. Omitting it would have left the trigger
// target un-gated with postgres.Engine's assertion still green.
//
// So the roster is fail-by-default: every registered engine is either an
// implementor or carries a written reason it is not.
//
// # Scope of this gate, stated so it cannot be read as broader
//
// It proves each target-capable engine ANSWERS the question. It does not prove
// the answer is right — that is each engine's own
// TestPreflightIndexesAgreesWithTheEmitter, which compares the preflight's
// verdict against the real emitter's, shape by shape — nor that the
// orchestrator CALLS it, which is
// TestIndexEmitPreflightReachesEveryCopyEntryPoint in internal/pipeline.
var indexPreflightExempt = map[string]string{
	"csv":            "source only — OpenSchemaWriter returns ErrNotImplemented, so no index DDL is ever emitted for this engine as a target",
	"tsv":            "source only — the same flatfile engine as csv, registered once per format",
	"ndjson":         "source only — the same flatfile engine as csv, registered once per format",
	"mydumper":       "source only — a dump directory is read, never written; OpenSchemaWriter returns ErrNotImplemented",
	"d1":             "migrate/sync SOURCE only — OpenSchemaWriter returns ErrD1NotImplemented. A D1 TARGET would go through the `sqlite` engine, which does implement the preflight",
	"sqlite-trigger": "CDC source only (ADR-0134) — a SQLite target uses the `sqlite` engine",
	"d1-trigger":     "CDC source only — OpenSchemaWriter returns ErrNotImplemented",
}

func TestEveryTargetCapableEngineAnswersTheIndexPreflight(t *testing.T) {
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
		_, isPreflighter := e.(ir.IndexEmitPreflighter)
		reason, isExempt := indexPreflightExempt[name]
		switch {
		case isPreflighter && isExempt:
			t.Errorf("%q implements ir.IndexEmitPreflighter AND is listed exempt. Remove the exemption — a "+
				"stale exemption reads as a decision and hides the real state.", name)
		case isPreflighter:
			implementors = append(implementors, name)
		case isExempt && reason == "":
			t.Errorf("%q is exempt with an EMPTY reason; an exemption without a reason is "+
				"indistinguishable from an oversight", name)
		case isExempt:
			exempted = append(exempted, name)
		default:
			t.Errorf("engine %q does not implement ir.IndexEmitPreflighter and is not in "+
				"indexPreflightExempt.\n\nIf it can be a migration TARGET, every "+
				"\"this index cannot be represented\" refusal it owns fires in the index phase — after the "+
				"whole table has been copied (roadmap item 118). Implement PreflightIndexes by running the "+
				"engine's OWN emit/check helpers over the schema. If it is a source only, add it to the "+
				"roster with that reason.", name)
		}
	}

	// A stale exemption is the same defect as a missing one, read backwards:
	// it names an engine nobody registers any more and quietly inflates the
	// roster's apparent coverage.
	registered := map[string]bool{}
	for _, n := range names {
		registered[n] = true
	}
	for name := range indexPreflightExempt {
		if !registered[name] {
			t.Errorf("indexPreflightExempt names %q, which is not a registered engine — the exemption is "+
				"stale; drop it", name)
		}
	}

	// The other half of the anti-vacuity floor: an exemption map that grew to
	// cover everything would silence the gate entirely.
	if len(exempted) >= len(names) {
		t.Fatalf("every registered engine (%d of %d) is exempt from the index preflight; the roster has "+
			"stopped requiring anything", len(exempted), len(names))
	}
	sort.Strings(implementors)
	if len(implementors) < 4 {
		t.Errorf("only %d engines implement the preflight (%v). mysql (×4 flavors), postgres, "+
			"postgres-trigger and sqlite are all migration targets and all must.",
			len(implementors), implementors)
	}
}
