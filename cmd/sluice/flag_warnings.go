// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package main

import (
	"context"
	"log/slog"
	"os"
	"strings"
	"sync"

	"sluicesync.dev/sluice/internal/engines"
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

// inertFlagMarker is the grep-stable token every ADR-0118 inert-flag WARN
// carries, so an operator reading a log can find every accepted-and-dropped
// flag with one search (the same discipline as POSITION-MODE /
// STALE-CAPTURE-FUNCTION). The gate in inert_flag_gate_test.go pins it.
const inertFlagMarker = "INERT-FLAG"

// inertFlagsUsed returns the registry rows whose flag the operator EXPLICITLY
// passed (by argv spelling, never by resolved value) on command and whose
// inertWhen predicate holds for the resolved engines — i.e. the flags that
// are about to be accepted and silently dropped. Pure over
// (args, command, source, target) so the per-row × per-engine matrix is
// unit-pinned without touching os.Args/slog. A nil engine never satisfies a
// predicate (the caller's own resolveEngine refuses an unknown driver; this
// pass has nothing to judge), so a command that carries only one side passes
// nil for the other.
func inertFlagsUsed(args []string, command string, source, target ir.Engine) []inertFlag {
	var hits []inertFlag
	for _, row := range inertFlagRegistry {
		if row.command != command || !flagPassedIn(args, row.flag) {
			continue
		}
		if row.inertWhen(source, target) {
			hits = append(hits, row)
		}
	}
	return hits
}

// warnedInertFlags dedupes the WARN per (command, flag) for the process
// lifetime — kong dispatches one Run per process, but a command's engine
// resolution can run more than once (a multi-namespace fan-out, a retry),
// and the WARN is a one-time notice, not a per-attempt one.
var warnedInertFlags sync.Map

// warnInertFlags emits the ADR-0118 finding 1(b) one-time WARN for every
// registry row of command whose flag the operator explicitly set on a run
// where the row's capability predicate says it is inert. The engines are
// resolved by driver NAME from the registry (an empty or unknown name
// resolves to nil and judges nothing — the command's own resolveEngine is
// the refusal for that), so every entry point wires this with one line,
// before or after its own resolution, and never has to thread a wrapped
// engine value in. The WARN names the flag, the command, the engine it is
// inert on and WHY (the row's reason), and carries the INERT-FLAG marker.
// This turns the silent no-op into a loud one — the loud-failure tenet
// applied to a UX hazard — without changing any behaviour.
func warnInertFlags(ctx context.Context, command, sourceDriver, targetDriver string) {
	source := engineByNameOrNil(sourceDriver)
	target := engineByNameOrNil(targetDriver)
	for _, row := range inertFlagsUsed(os.Args[1:], command, source, target) {
		key := row.command + " --" + row.flag
		if _, dup := warnedInertFlags.LoadOrStore(key, true); dup {
			continue
		}
		slog.WarnContext(ctx, inertFlagMarker+": --"+row.flag+" has no effect on `"+row.command+"` "+
			row.inertOn(source, target)+" — "+row.reason,
			slog.String("flag", "--"+row.flag),
			slog.String("command", row.command))
	}
}

// warnInertFleetControlKeyspace is the `sync run` (fleet YAML) sibling of the
// `sync start --control-keyspace` registry row. A fleet spec has no argv, so
// the registry's spelling signal cannot reach it; the honest "explicitly
// set" signal for a string key whose default is empty is "non-empty", and
// the two coincide for this key. It reuses the row's predicate and reason
// so the fleet operator reads the same sentence a CLI operator would. The
// other registry flags have no fleet-spec key (SyncSpec carries none of the
// cold-start tuning knobs), so this is the only fleet sibling — the wiring
// roster in inert_flag_gate_test.go does not reach it; the sync_run tests do.
func warnInertFleetControlKeyspace(ctx context.Context, streamID, keyspace string, target ir.Engine) {
	if keyspace == "" || !targetLacksControlKeyspace(nil, target) {
		return
	}
	slog.WarnContext(ctx, inertFlagMarker+": control-keyspace has no effect on `sync run` against a "+target.Name()+" target — "+reasonNoKeyspace,
		slog.String("stream_id", streamID),
		slog.String("command", "sync run"))
}

// engineByNameOrNil is the registry lookup with the "unknown → nil" edge the
// WARN pass wants: it must never refuse (the command's own resolveEngine
// owns that) and never panic on an empty --source-driver (diagnose /
// sync health take an optional source).
func engineByNameOrNil(name string) ir.Engine {
	if name == "" {
		return nil
	}
	e, ok := engines.Get(name)
	if !ok {
		return nil
	}
	return e
}
