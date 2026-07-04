// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package main

import (
	"context"
	"log/slog"
	"os"
	"strings"
	"sync"

	"sluicesync.dev/sluice/internal/ir"
)

// ADR-0118: a couple of the CLI ergonomics fixes need to know whether the
// operator EXPLICITLY passed a particular flag *spelling* — not whether the
// resolved field differs from its default (a default-equal explicit value
// must still count, the zero-value-safety property the ADR is careful about).
//
// kong tracks "was this Value set at all" (model.Value.Set), but it does NOT
// record WHICH alias spelling matched, and the Run methods here take *Globals,
// not the *kong.Context. The robust, spelling-precise signal is the literal
// argv token, so these helpers read os.Args directly. That is the standard
// approach for deprecation/inert warnings keyed on a specific flag name and
// keeps the detection independent of the default value entirely.

// flagPassedIn reports whether --name appears in args, in either the
// "--name value" or "--name=value" form (and the bool "--name" form). It is
// the spelling-precise "was this flag explicitly set" signal ADR-0118's WARNs
// need: it never infers intent from the resolved value, so an unset flag
// sitting at its default never trips it. Taking args as a parameter (rather
// than reading os.Args inside) keeps the predicate pure and unit-testable.
func flagPassedIn(args []string, name string) bool {
	want := "--" + name
	for _, a := range args {
		if a == want || strings.HasPrefix(a, want+"=") {
			return true
		}
	}
	return false
}

// sequenceMarginDeprecatedAliasUsed reports whether the operator passed the
// OLD --cutover-sequence-margin spelling (ADR-0118 finding 3). Pure over args
// so the WARN-firing condition is unit-pinned without touching os.Args/slog.
func sequenceMarginDeprecatedAliasUsed(args []string) bool {
	return flagPassedIn(args, "cutover-sequence-margin")
}

// inertParallelismFlagUsed reports whether the operator EXPLICITLY set one of
// the FAST-cold-start parallelism flags (ADR-0118 finding 1) on a source whose
// `sync start` cold-start never takes the ADR-0079 fast path (so the flag is
// inert — the MySQL/VStream cold-copy parallelism is the engine-internal
// copy-table axis instead). Returns the flag
// name that tripped it (for the message) and true; "" / false otherwise. Pure
// over (args, source) so both the source-class gate and the per-flag detection
// are unit-pinned.
func inertParallelismFlagUsed(args []string, source ir.Engine) (string, bool) {
	if source == nil || !sourceHasSerialColdStart(source) {
		return "", false
	}
	for _, name := range []string{"bulk-parallelism", "table-parallelism", "bulk-parallel-min-rows"} {
		if flagPassedIn(args, name) {
			return name, true
		}
	}
	return "", false
}

// warnDeprecatedSequenceMargin emits the ADR-0118 finding 3 one-time
// deprecation WARN — but ONLY when the operator passed the OLD
// --cutover-sequence-margin alias specifically (the canonical
// --sequence-margin spelling is silent). Mirrors the ADR-0091
// --forward-schema-add-column deprecation posture.
var warnDeprecatedSequenceMarginOnce sync.Once

func warnDeprecatedSequenceMargin() {
	if !sequenceMarginDeprecatedAliasUsed(os.Args[1:]) {
		return
	}
	warnDeprecatedSequenceMarginOnce.Do(func() {
		slog.WarnContext(context.Background(),
			"--cutover-sequence-margin is deprecated (ADR-0118): use --sequence-margin. "+
				"The old name still works and will be removed in a future release.")
	})
}

// warnInertParallelismFlags emits the ADR-0118 finding 1(b) one-time WARN when
// the operator EXPLICITLY set one of the FAST-cold-start parallelism flags on a
// `sync start` whose source engine has no effect for them — MySQL (binlog) or
// PlanetScale/Vitess (VStream). Those flags govern only the ADR-0079 PG-source
// fast cold-start; a MySQL/VStream source's cold-copy parallelism is the
// engine-internal copy-table axis (--copy-fanout-degree /
// --vstream-copy-table-parallelism / --copy-table-parallelism — the native
// axis defaults to auto:4 since the perf-parity gap-3 chunk). This turns the
// silent no-op into a loud
// one — the loud-failure tenet applied to a UX hazard — without changing any
// behaviour.
var warnInertParallelismFlagsOnce sync.Once

func warnInertParallelismFlags(ctx context.Context, source ir.Engine) {
	name, ok := inertParallelismFlagUsed(os.Args[1:], source)
	if !ok {
		return
	}
	warnInertParallelismFlagsOnce.Do(func() {
		slog.WarnContext(ctx,
			"--"+name+" has no effect for a MySQL/VStream source on `sync start` "+
				"(it governs the ADR-0079 PG-source fast cold-start only); "+
				"use --copy-fanout-degree to tune VStream cold-copy write concurrency, "+
				"and --vstream-copy-table-parallelism / --copy-table-parallelism for the read axis.")
	})
}

// sourceHasSerialColdStart reports whether the named source engine never takes
// the ADR-0079 fast cold-start on `sync start` — i.e. the FAST-cold-start
// parallelism flags (--bulk-parallelism / --table-parallelism /
// --bulk-parallel-min-rows) are inert for it. True for the MySQL family
// (binlog — whose cold-copy parallelism is the engine-internal
// --copy-table-parallelism axis, auto:4 by default) and PlanetScale/Vitess
// (VStream); false for Postgres, whose ADR-0079 fast cold-start honors them.
// The name is historical (these sources' cold-copy is no longer serial by
// default); it survives because the predicate is pinned by name in tests.
// Keyed on the engine registry name so a new MySQL flavor slots in by name.
func sourceHasSerialColdStart(source ir.Engine) bool {
	switch source.Name() {
	case "mysql", "planetscale", "vitess":
		return true
	default:
		return false
	}
}
