// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package main

import (
	"testing"

	"sluicesync.dev/sluice/internal/pipeline"
)

// EVERY SURFACE THAT NAMES A REPLICATION SLOT RESOLVES IT THE SAME WAY.
//
// # What went wrong
//
// `--slot-name` is a SUFFIX: sluice prepends `sluice_`, so
// `sync start --slot-name shard_a` creates `sluice_shard_a`. The stream
// path applies that through [pipeline.ResolveSlotName]. Two read-only
// PROBE paths did the OTHER half — filling in the `sluice_slot` default
// for a PostgreSQL source — and skipped the prefix, so they looked up
// `shard_a`, a slot that does not exist:
//
//   - `sync health`'s effectiveSlotName returned the flag verbatim.
//   - the `diagnose` bundle re-implemented the default inline.
//
// The miss is SILENT, which is what makes it worth a gate rather than a
// fix. `SlotSpillStats` on an absent slot returns ok=false, and the
// caller's "no signal" branch returns WITHOUT setting a probe reason —
// so the operator's spill counters were simply absent, indistinguishable
// from a healthy slot that had not spilled. Audit 2026-09-09
// A0909-AQ-M-1.
//
// # Why a parity test rather than one more assertion
//
// Both defects came from the same cause: the resolution was
// RE-IMPLEMENTED at each site, and `sync health`'s own doc-comment
// argued for duplicating the default constant. Duplicated policy drifts;
// the divergence is not visible at either site because each one reads
// correct on its own. This test fails when any surface stops agreeing
// with [pipeline.SlotNameForSource], which is now the single answer.
//
// # What it reaches
//
// The CLI-level resolvers, called as the commands call them. It does NOT
// reach a surface that names a slot without going through one of these
// functions — if you add a third, add it here; the roster below is
// hand-listed precisely because a new one is a deliberate act.
func TestSlotNameResolutionIsIdenticalAcrossSurfaces(t *testing.T) {
	for _, tc := range []struct {
		name     string
		flag     string
		engine   string
		wantSlot string
	}{
		{
			name:     "unset on postgres is the built-in default",
			flag:     "",
			engine:   "postgres",
			wantSlot: "sluice_slot",
		},
		{
			name:     "a bare name gets the sluice_ prefix",
			flag:     "shard_a",
			engine:   "postgres",
			wantSlot: "sluice_shard_a",
		},
		{
			name:     "an already-prefixed name is idempotent",
			flag:     "sluice_shard_a",
			engine:   "postgres",
			wantSlot: "sluice_shard_a",
		},
		{
			name:     "the default spelled out is unchanged",
			flag:     "sluice_slot",
			engine:   "postgres",
			wantSlot: "sluice_slot",
		},
		{
			name:     "unset on an engine with no slot concept probes nothing",
			flag:     "",
			engine:   "mysql",
			wantSlot: "",
		},
		{
			// A slot name supplied against a slotless engine still
			// resolves rather than vanishing: the flag is documented as
			// ignored there, and returning the prefixed name keeps the
			// two halves of this helper independent of each other.
			name:     "a name supplied to a slotless engine still resolves",
			flag:     "shard_a",
			engine:   "mysql",
			wantSlot: "sluice_shard_a",
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if got := pipeline.SlotNameForSource(tc.flag, tc.engine); got != tc.wantSlot {
				t.Fatalf("pipeline.SlotNameForSource(%q, %q) = %q; want %q — this is the single answer "+
					"every surface below is graded against, so fix it here first",
					tc.flag, tc.engine, got, tc.wantSlot)
			}

			health := (&SyncHealthCmd{SlotName: tc.flag}).effectiveSlotName(mustEngine(t, tc.engine))
			if health != tc.wantSlot {
				t.Errorf("`sync health` would probe slot %q for --slot-name %q on %s; the stream uses %q.\n\n"+
					"A probe that names a slot the stream did not create finds nothing, and the miss is "+
					"SILENT: SlotSpillStats returns ok=false and the caller returns without a probe "+
					"reason, so the operator sees absent counters rather than a wrong slot name "+
					"(audit 2026-09-09 A0909-AQ-M-1). Route it through pipeline.SlotNameForSource.",
					health, tc.flag, tc.engine, tc.wantSlot)
			}

			// `sync start`'s own resolver is PostgreSQL-shaped: it is
			// reached only where a slot exists, so it has no engine
			// argument and always defaults. Graded on the PG rows only.
			if tc.engine == "postgres" {
				if run := resolvedSlotName(tc.flag); run != tc.wantSlot {
					t.Errorf("`sync start` would create slot %q for --slot-name %q; the probes look up %q",
						run, tc.flag, tc.wantSlot)
				}
			}
		})
	}
}

// TestDiagnoseRequestCarriesAResolvedSlotName pins the OTHER half of the
// same defect, which no parity table can reach: `internal/diagnose`
// cannot resolve a slot name itself, because `internal/pipeline` imports
// it and the reverse would be an import cycle. The resolution therefore
// has to happen in the CLI builder, and this asserts that it does.
//
// Without this, the bundle silently probes whatever it was handed — the
// exact shape that shipped.
func TestDiagnoseRequestCarriesAResolvedSlotName(t *testing.T) {
	req := crashHookRequestForStreamer(
		"s1", mustEngine(t, "postgres"), mustEngine(t, "postgres"),
		"postgres://src", "postgres://dst", "shard_a",
	)
	if req.SlotName != "sluice_shard_a" {
		t.Errorf("the crash-hook diagnose request carries slot %q for --slot-name shard_a; want "+
			"sluice_shard_a. internal/diagnose cannot resolve this itself (pipeline imports diagnose, so "+
			"the reverse is an import cycle), so an unresolved name here reaches the bundle's slot probe "+
			"verbatim and finds nothing (audit 2026-09-09 A0909-AQ-M-1).", req.SlotName)
	}

	// A nil source engine must not panic: the crash hook is built from a
	// sync-start invocation that may have resolved no source engine.
	nilSource := crashHookRequestForStreamer("s1", nil, mustEngine(t, "postgres"), "", "postgres://dst", "")
	if nilSource.SlotName != "" {
		t.Errorf("a crash-hook request with no source engine resolved slot %q; want empty (nothing to probe)",
			nilSource.SlotName)
	}
}
