// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package docsync

import (
	"sort"
	"testing"

	"sluicesync.dev/sluice/internal/engines"

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

// WHICH TARGETS ENFORCE FOREIGN KEYS DURING THE COLD COPY, kept honest
// against the registry (roadmap item 140).
//
// The pre-copy foreign-key refusal in internal/pipeline reads ONE bit —
// [ir.Capabilities.BulkCopyBypassesForeignKeys] — to decide whether a
// pre-existing target constraint can bite. A bool has a zero value, so a
// new target engine that never considers the question is silently
// treated as enforcing. That direction is the SAFE one (it keeps the
// refusal armed), but the reverse mistake is not visible at all: an
// engine whose copy path starts bypassing FKs, or one that flips the
// flag by accident, changes an operator-facing refusal with nothing
// failing. So the roster pins the EXPECTED value per engine and is
// fail-by-default for anything registered and unlisted.
//
// # Scope of this gate, stated so it cannot be read as broader
//
// It proves each target-capable engine's DECLARATION is the one this
// roster expects, and that no engine escapes the question. It does NOT
// itself prove the declaration is TRUE of the server — that is what
// these ground-truth tests are for, and each declaration below names
// the one that binds it:
//
//   - bypasses=false (mysql family, postgres, postgres-trigger):
//     internal/pipeline's
//     TestMigrate_PreExistingTargetForeignKeys_MySQLOutOfScopeParentThenTheServerRejects
//     and ..._PostgresOutOfScopeParentThenTheServerRejects — both let the
//     copy run into a pre-existing constraint and require the SERVER to
//     reject it. postgres-trigger is covered by the postgres one by
//     composition: its OpenRowWriter delegates to postgres.Engine's, so
//     it is the same write path, and its capability value is pinned here.
//   - bypasses=true (sqlite): internal/engines/sqlite's
//     TestWriteRows_ChildBeforeParentIsAcceptedOnAPreExistingForeignKey,
//     which writes a child before its parent through the real writer.
//
// Source-only engines carry a written exemption rather than a value:
// they cannot be a migration target at all, so the bit is unreachable.
var bulkCopyFKEnforcement = map[string]bool{
	"mysql":            false,
	"planetscale":      false,
	"vitess":           false,
	"mariadb":          false,
	"postgres":         false,
	"postgres-trigger": false,
	"sqlite":           true,
}

var bulkCopyFKExempt = map[string]string{
	"csv":            "source only — OpenRowWriter returns ErrNotImplemented, so no cold copy ever writes into this engine",
	"tsv":            "source only — the same flatfile engine as csv, registered once per format",
	"ndjson":         "source only — the same flatfile engine as csv, registered once per format",
	"mydumper":       "source only — a dump directory is read, never written",
	"d1":             "migrate/sync SOURCE only — OpenRowWriter returns ErrD1NotImplemented. A D1 TARGET goes through the `sqlite` engine, whose declaration is pinned above",
	"sqlite-trigger": "CDC source only (ADR-0134) — a SQLite target uses the `sqlite` engine",
	"d1-trigger":     "CDC source only — OpenRowWriter returns ErrNotImplemented",
}

func TestEveryTargetCapableEngineDeclaresItsBulkCopyFKEnforcement(t *testing.T) {
	names := engines.Names()

	// Anti-vacuity floor: an empty or truncated registry would pass every
	// assertion below. The blank imports above are the registry's content.
	if len(names) < 8 {
		t.Fatalf("registry holds %d engines (%v); the blank-import list has drifted from cmd/sluice and "+
			"this gate is checking a subset of the fleet", len(names), names)
	}

	var declared, exempted []string
	for _, name := range names {
		e, ok := engines.Get(name)
		if !ok {
			t.Fatalf("engines.Names() reported %q but engines.Get did not return it", name)
		}
		want, isDeclared := bulkCopyFKEnforcement[name]
		reason, isExempt := bulkCopyFKExempt[name]
		switch {
		case isDeclared && isExempt:
			t.Errorf("%q is BOTH declared and exempt. Remove one — a stale entry reads as a decision and "+
				"hides the real state.", name)
		case isDeclared:
			if got := e.Capabilities().BulkCopyBypassesForeignKeys; got != want {
				t.Errorf("%q declares BulkCopyBypassesForeignKeys = %v; the roster expects %v.\n\n"+
					"If the engine's cold-copy write path genuinely changed, update the roster AND the "+
					"ground-truth test named in this file's doc comment — the bit decides whether an "+
					"operator gets the SLUICE-E-TARGET-PREEXISTING-FOREIGN-KEY refusal or a mid-copy "+
					"Error 1452 / SQLSTATE 23503.", name, got, want)
			}
			declared = append(declared, name)
		case isExempt && reason == "":
			t.Errorf("%q is exempt with an EMPTY reason; an exemption without a reason is "+
				"indistinguishable from an oversight", name)
		case isExempt:
			exempted = append(exempted, name)
		default:
			t.Errorf("engine %q is neither in bulkCopyFKEnforcement nor in bulkCopyFKExempt.\n\n"+
				"If it can be a migration TARGET, answer the question its cold-copy write path answers: "+
				"does that path load rows with the server's foreign-key enforcement OFF? Declare the value "+
				"in ir.Capabilities.BulkCopyBypassesForeignKeys, add it here, and name the test that "+
				"ground-truths it. If it is a source only, add it to the exempt roster with that reason.", name)
		}
	}

	// A stale entry is the same defect as a missing one, read backwards:
	// it names an engine nobody registers any more and quietly inflates
	// the roster's apparent coverage.
	registered := map[string]bool{}
	for _, n := range names {
		registered[n] = true
	}
	for name := range bulkCopyFKEnforcement {
		if !registered[name] {
			t.Errorf("bulkCopyFKEnforcement names %q, which is not a registered engine — drop it", name)
		}
	}
	for name := range bulkCopyFKExempt {
		if !registered[name] {
			t.Errorf("bulkCopyFKExempt names %q, which is not a registered engine — the exemption is stale; drop it", name)
		}
	}

	// The other half of the anti-vacuity floor: an exemption map that
	// grew to cover everything would silence the gate entirely.
	if len(exempted) >= len(names) {
		t.Fatalf("every registered engine (%d of %d) is exempt; the roster has stopped requiring anything",
			len(exempted), len(names))
	}
	sort.Strings(declared)
	if len(declared) < 7 {
		t.Errorf("only %d engines declare their bulk-copy FK enforcement (%v). mysql (x4 flavors), postgres, "+
			"postgres-trigger and sqlite are all migration targets and all must.", len(declared), declared)
	}

	// The declarations must not be uniform, or the gate would pass on a
	// build where the capability was never wired to anything: the whole
	// point is that ONE target (sqlite) answers differently from the
	// rest, and that difference is what the pipeline branches on.
	var bypassing int
	for _, name := range declared {
		if bulkCopyFKEnforcement[name] {
			bypassing++
		}
	}
	if bypassing == 0 || bypassing == len(declared) {
		t.Fatalf("all %d declared engines answer the same way (%d bypassing); the roster no longer "+
			"distinguishes the enforcing targets from the bypassing one and would pass on an unwired "+
			"capability", len(declared), bypassing)
	}
}
