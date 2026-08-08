// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

// The half of the non-transactional-DDL argument that lives in this
// package.
//
// [BatchConfig.TransactionalDDL] is documented as the "one structural
// divergence between the engines' batch loops", and on the false (MySQL)
// side its whole job is to keep a DDL statement off the batch transaction —
// because MySQL commits implicitly around DDL, which ends the transaction
// mid-batch and makes the loop's `tx.Rollback()` a no-op over rows that are
// already durable.
//
// The flag alone does not do that. It gates on [isSchemaEvent], so the
// protection is only as wide as that predicate: a predicate covering
// SchemaSnapshot and not Truncate would leave a `TRUNCATE TABLE` riding the
// batch tx with the flag still, correctly, false. Both facts were true and
// nothing bound them — the 2026-08-07 invariant sweep's "two facts can each
// be pinned and leave the ARGUMENT unpinned" shape.
//
// This is the fail-by-default roster over the predicate: every [ir.Change]
// implementor is classified DDL-bearing or not, with a reason, and the
// predicate must agree. A NEW change type fails the build until someone
// says which side it is on, which is the only durable way to keep this from
// silently narrowing. Its engine-side twin is
// TestMySQLDeclaresNonTransactionalDDL in `internal/engines/mysql`.

package appliershared

import (
	"go/ast"
	"go/parser"
	"go/token"
	"testing"

	"sluicesync.dev/sluice/internal/ir"
)

// ddlBearingChangeTypes classifies every change type by whether an engine
// can render it as a DDL statement on the target. The question is NOT "is
// this a schema event" — it is "can applying this emit a statement that a
// non-transactional-DDL engine implicit-commits around", which is what
// decides whether it may share a batch transaction.
var ddlBearingChangeTypes = map[string]struct {
	ddl    bool
	reason string
}{
	"Insert": {false, "renders INSERT … ON DUPLICATE KEY UPDATE / INSERT; pure DML"},
	"Update": {false, "renders UPDATE; pure DML"},
	"Delete": {false, "renders DELETE; pure DML"},
	"Truncate": {
		true,
		"renders TRUNCATE TABLE, which is DDL on MySQL and implicit-commits the open transaction " +
			"(pinned against a real server by mysql.TestApplyOne_TruncateSurvivesTheRollback)",
	},
	"SchemaSnapshot": {
		true,
		"the boundary event around a source DDL; its own target write is the ADR-0049 schema-history " +
			"INSERT (DML), but it marks a schema boundary the batch must not straddle, and ADR-0049 " +
			"locked decision #4a requires its version write to be atomic with its position write",
	},
	"TxBegin":  {false, "a source-transaction marker; emits no target statement at all"},
	"TxCommit": {false, "a source-transaction marker; emits no target statement at all"},
}

// TestIsSchemaEvent_CoversEveryDDLBearingChangeType is the roster. It
// enumerates the change types from the classification map, requires every
// one to be a real [ir.Change] (a stale entry silently shrinks the gate),
// and requires [isSchemaEvent] to agree with the classification in BOTH
// directions — a missing DDL-bearing type would ride the batch tx, and a
// spurious extra one would force every batch to flush around a pure-DML
// change for no reason.
//
// Anti-vacuity floors: at least one type on each side, and the roster must
// name every implementor the [ir.Change] interface has. The second is what
// makes a NEW change type fail here rather than default into "not a schema
// event", which is the silent direction.
func TestIsSchemaEvent_CoversEveryDDLBearingChangeType(t *testing.T) {
	samples := map[string]ir.Change{
		"Insert":         ir.Insert{},
		"Update":         ir.Update{},
		"Delete":         ir.Delete{},
		"Truncate":       ir.Truncate{},
		"SchemaSnapshot": ir.SchemaSnapshot{},
		"TxBegin":        ir.TxBegin{},
		"TxCommit":       ir.TxCommit{},
	}

	// A sample per classified name, and a classification per sample: the two
	// lists are maintained separately on purpose, so a type added to one and
	// forgotten in the other fails rather than quietly dropping out.
	for name := range ddlBearingChangeTypes {
		if _, ok := samples[name]; !ok {
			t.Errorf("ddlBearingChangeTypes classifies %q, which has no sample value — a stale entry here "+
				"silently shrinks this gate", name)
		}
	}
	for name := range samples {
		if _, ok := ddlBearingChangeTypes[name]; !ok {
			t.Errorf("change type %q has no entry in ddlBearingChangeTypes. Say whether applying it can emit "+
				"a DDL statement on a non-transactional-DDL engine: if it can and isSchemaEvent does not "+
				"claim it, it rides the batch transaction and its implicit commit ends that tx with other "+
				"rows still pending", name)
		}
	}

	ddlCount, dmlCount := 0, 0
	for name, sample := range samples {
		want, ok := ddlBearingChangeTypes[name]
		if !ok {
			continue
		}
		if want.ddl {
			ddlCount++
		} else {
			dmlCount++
		}
		got := isSchemaEvent(sample)
		if got == want.ddl {
			continue
		}
		if want.ddl {
			t.Errorf("isSchemaEvent(%T) = false, but this type is classified DDL-bearing: %s. With "+
				"TransactionalDDL=false the batch loop only diverts what isSchemaEvent claims, so this "+
				"change would be dispatched onto the batch transaction — and MySQL's implicit commit "+
				"around the DDL ends that transaction, leaving the batch's earlier rows durable with no "+
				"position write and tx.Rollback unable to undo them",
				sample, want.reason)
			continue
		}
		t.Errorf("isSchemaEvent(%T) = true, but this type is classified pure DML: %s. Every such change "+
			"forces the in-flight batch to flush and apply alone, which is a throughput cost paid for "+
			"nothing", sample, want.reason)
	}

	if ddlCount < 1 || dmlCount < 1 {
		t.Fatalf("roster graded %d DDL-bearing and %d DML change types; a one-sided roster proves nothing "+
			"about a predicate", ddlCount, dmlCount)
	}

	// The completeness floor, DERIVED rather than asserted. A hand-written
	// count would be self-referential: adding an ir.Change implementor does
	// not change len(samples), so the count would keep agreeing with itself
	// while the roster silently stopped covering the interface. So read the
	// implementor set out of the source — every `func (T) isChange()` in
	// internal/ir/change.go — and require the roster to name each one.
	implementors := changeImplementorsFromSource(t)
	if len(implementors) < 3 {
		t.Fatalf("found only %d ir.Change implementors in the source (%v); the walker has stopped matching "+
			"and this floor is vacuous", len(implementors), implementors)
	}
	for _, name := range implementors {
		if _, ok := samples[name]; !ok {
			t.Errorf("ir.Change implementor %q is not in this roster. Classify it: if applying it can emit a "+
				"DDL statement on a non-transactional-DDL engine and isSchemaEvent does not claim it, it "+
				"rides the batch transaction and the DDL's implicit commit ends that tx mid-batch", name)
		}
	}
}

// changeImplementorsFromSource returns the receiver type name of every
// `func (T) isChange()` declared in internal/ir/change.go — the sealed
// interface's implementor set, read from the code rather than restated here.
func changeImplementorsFromSource(t *testing.T) []string {
	t.Helper()
	const src = "../ir/change.go"
	fset := token.NewFileSet()
	f, err := parser.ParseFile(fset, src, nil, parser.SkipObjectResolution)
	if err != nil {
		t.Fatalf("parse %s: %v", src, err)
	}
	var out []string
	for _, d := range f.Decls {
		fn, ok := d.(*ast.FuncDecl)
		if !ok || fn.Name.Name != "isChange" || fn.Recv == nil || len(fn.Recv.List) != 1 {
			continue
		}
		if id, ok := fn.Recv.List[0].Type.(*ast.Ident); ok {
			out = append(out, id.Name)
		}
	}
	return out
}
