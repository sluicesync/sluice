// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package pipeline

import (
	"context"
	"errors"
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"

	"sluicesync.dev/sluice/internal/ir"
	"sluicesync.dev/sluice/internal/sluicecode"
)

// The invariant under test, in one sentence: a MISSING per-table progress
// row means the table was never copied.
//
// [classifyTableForResume] acts on that when it returns resumeActionFresh,
// and fresh does NOT truncate. Measured 2026-09-10 on a sharded PlanetScale
// Neki target whose control tables refuse every INSERT for want of the
// database's shard key (reported to PlanetScale): a 40-row keyless table came
// out of a --resume holding 80 rows, because the breadcrumb write had been
// swallowed as a WARN and the second copy had nothing to conflict on.
//
// Two independent defences, pinned separately below:
//
//  1. [persistTableBreadcrumb] refuses the run when the breadcrumb cannot be
//     written, before that table's first row moves. Stops the state from
//     being CREATED.
//  2. [refuseResumedFreshTableThatHasRows] refuses a resumed fresh table
//     whose target already holds rows. Stops the state from being ACTED ON,
//     including state written by every release that shipped before (1).

func breadcrumbState() (*ir.MigrationState, *sync.Mutex) {
	return &ir.MigrationState{TableProgress: map[string]ir.TableProgress{}}, &sync.Mutex{}
}

// A migrate context whose store cannot accept the breadcrumb must ABORT.
// Warning past it is what produced the duplicated table.
func TestPersistTableBreadcrumbRefusesWhenTheStoreCannotRecordIt(t *testing.T) {
	t.Parallel()
	store := newFakeStateStore()
	store.writeErr = errors.New("ERROR: shard-key column \"tenant_id\" is required but missing from INSERT (SQLSTATE NK306)")
	rc := resumeContext{store: store, migrationID: "m1", enabled: true}
	state, mu := breadcrumbState()

	err := persistTableBreadcrumb(context.Background(), rc, state, mu, "events",
		ir.TableProgress{State: ir.TableProgressInProgress})
	if err == nil {
		t.Fatal("an unrecordable breadcrumb was swallowed; a later --resume would read the missing row as " +
			"\"never copied\" and append a second copy of every row of a keyless table")
	}
	ce, ok := sluicecode.FromError(err)
	if !ok || ce.Code != sluicecode.CodeMigrateProgressUnrecordable {
		t.Errorf("refusal carried code %v (coded=%v), want %q", ce, ok, sluicecode.CodeMigrateProgressUnrecordable)
	}
	if !strings.Contains(err.Error(), "events") {
		t.Errorf("the refusal does not name the table the operator has to act on: %v", err)
	}
	// The underlying store error is the only thing that tells the operator
	// WHY the store refused; losing it makes the refusal unactionable.
	if !strings.Contains(err.Error(), "NK306") {
		t.Errorf("the underlying store error was dropped: %v", err)
	}
	// The in-memory map is still updated: the run is aborting, but the
	// failure path reads this map to mark the phase failed.
	if state.TableProgress["events"].State != ir.TableProgressInProgress {
		t.Error("the in-memory entry was not set, so the failure path has nothing to report")
	}
}

// A sync cold start's progress rows are a `sync status` heartbeat that
// loadOrInitState refuses to resume from, so no correctness argument rests
// on them. Killing a cold start over one would be pure loss.
func TestPersistTableBreadcrumbIsExemptForARecordOnlyContext(t *testing.T) {
	t.Parallel()
	store := newFakeStateStore()
	store.writeErr = errors.New("control table unavailable")
	state, mu := breadcrumbState()

	rc := resumeContext{store: store, migrationID: "sync-x", enabled: true, noResume: true}
	if err := persistTableBreadcrumb(context.Background(), rc, state, mu, "events",
		ir.TableProgress{State: ir.TableProgressInProgress}); err != nil {
		t.Fatalf("a record-only (sync cold start) context was killed by a heartbeat write failure: %v", err)
	}
	// And a context with no store at all stays a silent no-op, as
	// everywhere else in this file's neighbourhood.
	if err := persistTableBreadcrumb(context.Background(), resumeContext{}, state, mu, "events",
		ir.TableProgress{State: ir.TableProgressInProgress}); err != nil {
		t.Fatalf("a storeless context was refused: %v", err)
	}
	// Both exempt paths must still update the in-memory map — the sync
	// cold start's own bookkeeping reads it.
	if state.TableProgress["events"].State != ir.TableProgressInProgress {
		t.Error("an exempt path skipped the in-memory set as well as the write")
	}
}

// The healthy path is silent and records the row.
func TestPersistTableBreadcrumbWritesTheRow(t *testing.T) {
	t.Parallel()
	store := newFakeStateStore()
	rc := resumeContext{store: store, migrationID: "m1", enabled: true}
	state, mu := breadcrumbState()
	if err := persistTableBreadcrumb(context.Background(), rc, state, mu, "events",
		ir.TableProgress{State: ir.TableProgressInProgress}); err != nil {
		t.Fatalf("a healthy breadcrumb write returned %v", err)
	}
	if store.tableWrites != 1 {
		t.Errorf("store saw %d per-table writes, want 1", store.tableWrites)
	}
}

// emptinessWriter is a RowWriter that answers the emptiness probe. It
// deliberately does NOT embed a full writer: the guard must reach its
// verdict from the probe alone.
type emptinessWriter struct {
	ir.RowWriter
	empty  bool
	err    error
	probes int
}

func (w *emptinessWriter) IsTableEmpty(context.Context, *ir.Table) (bool, error) {
	w.probes++
	return w.empty, w.err
}

type bareRowWriter struct{ ir.RowWriter }

func TestRefuseResumedFreshTableThatHasRows(t *testing.T) {
	t.Parallel()
	table := &ir.Table{Name: "events"}

	t.Run("refuses a resumed fresh table whose target holds rows", func(t *testing.T) {
		t.Parallel()
		w := &emptinessWriter{}
		err := refuseResumedFreshTableThatHasRows(context.Background(), w, table, true)
		if err == nil {
			t.Fatal("a resumed fresh table with rows on the target was accepted; the copy would start at PK > nil " +
				"without truncating, so a keyless table would end up holding every row twice")
		}
		ce, ok := sluicecode.FromError(err)
		if !ok || ce.Code != sluicecode.CodeResumeFreshTableNotEmpty {
			t.Errorf("refusal carried code %v (coded=%v), want %q", ce, ok, sluicecode.CodeResumeFreshTableNotEmpty)
		}
		if !strings.Contains(err.Error(), "events") {
			t.Errorf("the refusal does not name the table: %v", err)
		}
		// The two remedies are the whole point of refusing rather than
		// truncating: the operator is the only one who can tell the two
		// causes apart.
		if !strings.Contains(ce.Hint, "--reset-target-data") || !strings.Contains(ce.Hint, "--exclude-table") {
			t.Errorf("the hint does not offer both remedies, so the operator cannot act on it: %q", ce.Hint)
		}
	})

	t.Run("passes when the target is empty", func(t *testing.T) {
		t.Parallel()
		w := &emptinessWriter{empty: true}
		if err := refuseResumedFreshTableThatHasRows(context.Background(), w, table, true); err != nil {
			t.Fatalf("the ordinary resume case (previous attempt never reached this table) was refused: %v", err)
		}
	})

	t.Run("does not probe at all when not resuming", func(t *testing.T) {
		t.Parallel()
		// A fresh (non-resume) run is already governed by the cold-start
		// pre-flight; probing every table again would be a second round
		// trip per table for an answer nothing acts on.
		w := &emptinessWriter{}
		if err := refuseResumedFreshTableThatHasRows(context.Background(), w, table, false); err != nil {
			t.Fatalf("a non-resume run was refused: %v", err)
		}
		if w.probes != 0 {
			t.Errorf("probed %d times on a non-resume run; want 0", w.probes)
		}
	})

	t.Run("skips engines that do not expose the probe", func(t *testing.T) {
		t.Parallel()
		if err := refuseResumedFreshTableThatHasRows(context.Background(), bareRowWriter{}, table, true); err != nil {
			t.Fatalf("a writer without the emptiness surface was refused: %v", err)
		}
	})

	t.Run("a probe FAILURE is not a refusal", func(t *testing.T) {
		t.Parallel()
		// A probe that could not run is not a verdict. Reporting a
		// network error as this refusal would tell the operator their
		// target is in a corrupt state on no evidence.
		w := &emptinessWriter{err: errors.New("connection reset")}
		err := refuseResumedFreshTableThatHasRows(context.Background(), w, table, true)
		if err == nil {
			t.Fatal("a failed probe was swallowed; the caller learns nothing")
		}
		if ce, ok := sluicecode.FromError(err); ok && ce.Code == sluicecode.CodeResumeFreshTableNotEmpty {
			t.Error("a probe failure was reported as the placement refusal")
		}
		if !strings.Contains(err.Error(), "connection reset") {
			t.Errorf("the underlying cause was dropped: %v", err)
		}
	})
}

// TestProgressBreadcrumbsDoNotRideTheBestEffortHelper is the sibling sweep,
// mechanised.
//
// The split this fix rests on is: a TERMINAL progress entry may be written
// best-effort ([setTableProgressAndWrite]), because losing it degrades to
// re-copying work the resume path handles correctly — while a NON-TERMINAL
// entry (the breadcrumb, written before any of the table's rows move) may
// not, because losing it inverts the meaning of a missing row.
//
// A promise that all four breadcrumb sites were converted is worth nothing
// the moment a fifth is added. So this derives its own universe: it walks
// the package's AST, finds every call to the best-effort helper, and
// requires each one's entry argument to be a composite literal whose State
// is ir.TableProgressComplete. Anything else — a non-terminal literal, or a
// computed entry whose terminality this walker cannot see — fails, and the
// author either routes it through persistTableBreadcrumb or extends this
// gate to prove the entry is terminal.
//
// Mutation-run 2026-09-10 in both directions: pointing one converted site
// back at setTableProgressAndWrite fails here with that site named, and
// changing a terminal literal's State to InProgress fails here too.
func TestProgressBreadcrumbsDoNotRideTheBestEffortHelper(t *testing.T) {
	t.Parallel()

	const (
		bestEffort = "setTableProgressAndWrite"
		durable    = "persistTableBreadcrumb"
		terminal   = "TableProgressComplete"
	)

	entries, err := os.ReadDir(".")
	if err != nil {
		t.Fatalf("read package dir: %v", err)
	}
	fset := token.NewFileSet()

	var (
		bestEffortCalls int
		durableCalls    int
		violations      []string
	)
	for _, e := range entries {
		name := e.Name()
		if e.IsDir() || !strings.HasSuffix(name, ".go") || strings.HasSuffix(name, "_test.go") {
			continue
		}
		f, err := parser.ParseFile(fset, filepath.Join(".", name), nil, 0)
		if err != nil {
			t.Fatalf("parse %s: %v", name, err)
		}
		ast.Inspect(f, func(n ast.Node) bool {
			call, ok := n.(*ast.CallExpr)
			if !ok {
				return true
			}
			id, ok := call.Fun.(*ast.Ident)
			if !ok {
				return true
			}
			switch id.Name {
			case durable:
				durableCalls++
				return true
			case bestEffort:
				bestEffortCalls++
			default:
				return true
			}
			// Signature: (ctx, rc, state, stateMu, tableName, entry).
			if len(call.Args) != 6 {
				violations = append(violations, fset.Position(call.Pos()).String()+
					": unexpected argument count; this gate reads the 6th argument as the progress entry")
				return true
			}
			lit, ok := call.Args[5].(*ast.CompositeLit)
			if !ok {
				violations = append(violations, fset.Position(call.Pos()).String()+
					": entry is not a composite literal, so this gate cannot prove it is terminal")
				return true
			}
			var state string
			for _, el := range lit.Elts {
				kv, ok := el.(*ast.KeyValueExpr)
				if !ok {
					continue
				}
				if k, ok := kv.Key.(*ast.Ident); !ok || k.Name != "State" {
					continue
				}
				if sel, ok := kv.Value.(*ast.SelectorExpr); ok {
					state = sel.Sel.Name
				}
			}
			if state != terminal {
				violations = append(violations, fset.Position(call.Pos()).String()+
					": entry State is "+state+", not "+terminal)
			}
			return true
		})
	}

	// Violations are reported BEFORE the anti-vacuity floor, deliberately.
	// Converting a breadcrumb site back to the best-effort helper trips
	// both — and if the floor spoke first it would be the only thing in
	// the output, which reads as "the walker is broken" rather than "this
	// call site is wrong". A mutant caught by the wrong guard is not
	// evidence that the right one works.
	for _, v := range violations {
		t.Errorf("%s\n  a non-terminal progress entry must be written through %s, not %s: losing it makes a "+
			"missing progress row mean \"never copied\" for a table that WAS copied, and a --resume then appends a "+
			"second copy of every row of a keyless table (reported to PlanetScale)", v, durable, bestEffort)
	}

	// Anti-vacuity floor. A walker that found nothing passes for free, and
	// this one has two universes to keep honest: if either collapses, the
	// gate is measuring an empty set and must be repaired, not trusted.
	if bestEffortCalls < 4 {
		t.Errorf("found only %d %s calls; the walker is not reaching the code it grades", bestEffortCalls, bestEffort)
	}
	if durableCalls < 4 {
		t.Errorf("found only %d %s calls; the four breadcrumb sites this fix converted are not being seen", durableCalls, durable)
	}
}
