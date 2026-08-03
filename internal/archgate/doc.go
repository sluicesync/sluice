// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

// Package archgate enforces sluice's LAYERING tenet (audit 2026-08-01 A3).
//
// CLAUDE.md states the architecture as a rule: the IR is the only shared
// contract, source knowledge lives in readers, target knowledge in writers,
// "no engine-specific imports leaking into the orchestrator". The audit found
// every boundary intact — and nothing enforcing any of them. A tenet held only
// by the care of whoever last edited an import block is a tenet with a
// half-life; this is the mechanical form, in the spirit of the sibling gates
// (internal/docsync, internal/errclassgate).
//
// It asks the Go toolchain for each package's TRANSITIVE dependency set rather
// than reading import blocks, because the interesting violation is rarely
// direct: the orchestrator importing a helper that imports an engine breaks the
// tenet exactly as thoroughly as importing the engine, and only the transitive
// view sees it.
//
// Every rule carries the CONSEQUENCE of breaking it, not just the fact, so a
// failure explains why the boundary exists to someone who did not write it.
// Exceptions are named individually with a reason — a blanket "engines may
// import engines" would erase the distinction between a variant building on its
// BASE engine (legitimate: pgtrigger on postgres, the sqlite trigger variants
// on sqlite, mydumper on mysql) and MySQL reaching into Postgres (not).
//
// One caution for anyone extending this. `go list -deps <pkg>` on the COMMAND
// LINE folds in the named package's test dependencies; the `.Deps` field does
// not. A first pass at the engine boundary read the former, saw the same names,
// and concluded the production coupling was test-only. It is not. When checking
// a boundary by hand, check the same way this gate does.
//
// The package intentionally exports nothing; it exists for its tests.
package archgate
