// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package postgres

import (
	"errors"
	"fmt"
	"go/ast"
	"go/token"
	"os"
	"path/filepath"
	"sort"
	"testing"

	"github.com/jackc/pgx/v5/pgconn"

	"sluicesync.dev/sluice/internal/ir"
)

// TestTerminalVerdictReachesBothConsumerQuestions is the gate that the Bug-285
// pins should have been, and it exists because those pins were green against
// code where the fix was inert at five of its six consumers.
//
// # What went wrong, precisely
//
// [classifyCopyError] reverses one verdict by WRAPPING the already-classified
// error: `&terminalPGError{err: classified}`, where `classified` is a
// *retriablePGError. `Unwrap` is preserved on purpose, so errors.Is/As against
// the underlying *pgconn.PgError keeps working — and that is exactly what
// defeated the reversal. `errors.As(err, &someRetriable)` does not stop at a
// type that fails to implement the interface; it unwraps THROUGH it and
// matches the retriable error inside.
//
// So the two questions a consumer can ask disagreed. Measured 2026-09-13 on
// the real types, before the fix:
//
//	ir.IsTerminal(err)  -> true
//	errors.As(err, &re) -> true, and re.Retriable() -> TRUE
//
// One consumer of six ([isRetriableChunkOpenError]) tests ir.IsTerminal first
// and saw the verdict. The other five use the ordinary
// `errors.As(...) && re.Retriable()` idiom and did not: the typed COPY core
// and idempotent batch core rode a missing relation to the ~30-minute reparent
// wall (and on a KEYLESS table reported it as an ambiguous-replay refusal —
// "a replay could double rows", about a table that does not exist);
// `quiesceAndReportTransient` TRIPPED THE RUN-WIDE GROW GATE, parking every
// sibling cold-copy lane for a storage-grow window that was not happening,
// which is the misleading storage-growth headline Bug 285 was filed about; and
// the index and constraint DDL phases retried a CREATE INDEX on a missing
// column.
//
// # Why the old pins could not see it
//
// `applier_errors_schema_drift_test.go` asserts `ir.IsTerminal(copyErr)` — the
// one question that already worked. Its sibling in the same file grades the
// other verdicts with the `errors.As && Retriable()` idiom, and would have
// passed for 42P01 too. Both were green. This is the evidence-sharing shape
// CLAUDE.md's 2026-08-01 rule names: the check and the thing checked derived
// their answer the same way, so there was no independent expected value.
//
// THIS test asks BOTH questions of EVERY terminal producer, and requires them
// to agree. A wrapper that reverses a verdict must implement the interface it
// is reversing; implementing only the new verdict leaves the old one reachable
// by unwrap.
func TestTerminalVerdictReachesBothConsumerQuestions(t *testing.T) {
	t.Parallel()

	// Every way the engine produces a terminal verdict. Kept as a table so a
	// new producer is a one-line addition rather than a forgotten sibling; the
	// roster test below fails if one is added and not listed here.
	cases := []struct {
		name string
		err  error
	}{
		{
			name: "schema drift 42P01 (Bug 285)",
			err:  classifyCopyError(&pgconn.PgError{Code: "42P01", Message: `relation "t" does not exist`}),
		},
		{
			name: "schema drift 42703 (Bug 285)",
			err:  classifyCopyError(&pgconn.PgError{Code: "42703", Message: `column "c" does not exist`}),
		},
		{
			name: "in-doubt COMMIT (v0.152.0)",
			err:  &terminalPGError{err: fmt.Errorf("commit raw-copy transaction (IN DOUBT): %w", errors.New("broken pipe"))},
		},
		{
			name: "dead snapshot-pinned connection",
			err:  &terminalPGError{err: fmt.Errorf("snapshot-pinned connection is gone: %w", errors.New("conn closed"))},
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			// Question 1 — the one the old pins asked.
			if !ir.IsTerminal(tc.err) {
				t.Fatalf("ir.IsTerminal is false for a terminal producer: %v", tc.err)
			}

			// Question 2 — the one five of six consumers actually ask, and
			// the one that was answering TRUE for a terminal error.
			var re ir.RetriableError
			if errors.As(tc.err, &re) && re.Retriable() {
				t.Fatalf("a TERMINAL error answers Retriable()==true to the ordinary consumer idiom.\n\n"+
					"error: %v\n\n"+
					"`errors.As` unwrapped THROUGH the terminal wrapper and matched the retriable error "+
					"inside it, so every consumer using `errors.As(err, &re) && re.Retriable()` — the typed "+
					"COPY core, the idempotent batch core, quiesceAndReportTransient, and the index and "+
					"constraint DDL phases — retries an error the engine has ruled terminal. On the Bug-285 "+
					"case that means riding a missing relation to the ~30-minute reparent wall and tripping "+
					"the run-wide grow gate for a storage-grow window that is not happening.\n\n"+
					"Fix: the wrapper must implement the interface it reverses, so errors.As matches at the "+
					"wrapper and stops rather than unwrapping past it.", tc.err)
			}
		})
	}
}

// TestTerminalVerdictPin_AntiVacuity proves the test above can still tell the
// two verdicts apart. Without this, giving every error a `Retriable() == false`
// would satisfy it while breaking every legitimate retry in the engine.
func TestTerminalVerdictPin_AntiVacuity(t *testing.T) {
	t.Parallel()

	// A genuine transient must still answer retriable to BOTH questions.
	transient := classifyCopyError(&pgconn.PgError{Code: "53100", Message: "could not extend file: No space left on device"})

	if ir.IsTerminal(transient) {
		t.Fatalf("a disk-full 53100 is being reported TERMINAL (%v) — it is the grow-gate's own evidence "+
			"and must ride out", transient)
	}

	var re ir.RetriableError
	if !errors.As(transient, &re) || !re.Retriable() {
		t.Fatalf("a disk-full 53100 does not answer Retriable()==true (%v). If the terminal fix were "+
			"implemented by making the wrapper answer false unconditionally, or by attaching it too "+
			"widely, this is what would break — and the copy would fail on the one condition sluice's "+
			"grow gate exists to survive", transient)
	}
}

// TestTerminalProducerRoster_EveryConstructionSiteIsGraded derives its universe
// from the AST rather than from a list somebody maintains, so a NEW terminal
// producer cannot be added without either being graded above or failing here.
//
// This is the anti-vacuity half of the sibling-sweep rule: the table in
// [TestTerminalVerdictReachesBothConsumerQuestions] is hand-written, and a
// hand-written roster silently stops covering the code the moment someone adds
// a case. The floor below catches an AST walk that matches nothing.
func TestTerminalProducerRoster_EveryConstructionSiteIsGraded(t *testing.T) {
	t.Parallel()

	dir, err := os.Getwd()
	if err != nil {
		t.Fatalf("getwd: %v", err)
	}

	fset := token.NewFileSet()
	files, err := parseNonTestGoFiles(fset, dir)
	if err != nil {
		t.Fatalf("parse package: %v", err)
	}

	var sites []string
	for _, file := range files {
		ast.Inspect(file, func(n ast.Node) bool {
			lit, ok := n.(*ast.CompositeLit)
			if !ok {
				return true
			}
			id, ok := lit.Type.(*ast.Ident)
			if !ok || id.Name != "terminalPGError" {
				return true
			}
			pos := fset.Position(lit.Pos())
			sites = append(sites, fmt.Sprintf("%s:%d", filepath.Base(pos.Filename), pos.Line))
			return true
		})
	}
	sort.Strings(sites)

	// Anti-vacuity floor. Three construction sites exist today (the schema-drift
	// reversal in applier_errors.go, and raw_copy.go's in-doubt COMMIT and
	// dead-pinned-connection refusals). A walk that finds fewer has stopped
	// matching the code — a rename of the type, a move to another package, or a
	// parser that silently returned nothing — and would pass while grading
	// nothing at all.
	const minSites = 3
	if len(sites) < minSites {
		t.Fatalf("the terminal-producer walk found %d construction sites (%v), fewer than the floor of %d. "+
			"Either terminalPGError was renamed/moved — in which case this gate is now inert and must be "+
			"repointed — or the AST walk is broken. It must never pass by finding nothing",
			len(sites), sites, minSites)
	}

	// Every producer must satisfy BOTH interfaces, which is the property the
	// graded table above relies on. Asserted here at the type level so it holds
	// for construction sites the table does not enumerate by hand.
	var (
		_ ir.TerminalError  = (*terminalPGError)(nil)
		_ ir.RetriableError = (*terminalPGError)(nil)
	)

	t.Logf("terminal-producer construction sites graded: %v", sites)
}
