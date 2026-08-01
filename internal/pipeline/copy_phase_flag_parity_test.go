// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package pipeline

import (
	"reflect"
	"strings"
	"testing"
)

// The migrate/sync copy-phase flag-parity gate, kept honest against the code.
//
// # Why this exists (roadmap item 111)
//
// [Migrator] (migrate) and [Streamer] (sync cold-start) share the schema-apply
// + bulk-copy + index/constraint work — a sync cold-start does the identical
// large-table copy and index/FK build before the CDC handoff, so it hits the
// exact same statement-time / storage-headroom walls migrate does. Yet the
// errno-3024 headroom knobs landed on the Migrator only: `UpfrontIndexes` and
// `RaiseQueryTimeout` were wired to migrate and silently missed on sync, even
// though `IndexBuildFallback` — the third knob of the same family — WAS wired
// to both. That asymmetry was an accident, not a decision, and nothing caught
// it: a copy-phase capability can be added to one struct and omitted from the
// other with a green build.
//
// This gate closes the class. It holds a CURATED list of copy-phase
// capabilities that MUST exist on BOTH structs (they share the copy phases), and
// fails the build when one exists on the Migrator XOR the Streamer without an
// explicit [parityExempt] entry carrying a reason. A deliberate or pending
// asymmetry is a decision you RECORD here with its architectural reason — never
// a default. It is the flag/capability analogue of the perf-parity-matrix +
// perf-parity-checker machinery: the mechanical enforcement of the CLAUDE.md
// working agreement that a shared-pipeline-phase flag applies to both entry
// points unless a written reason says otherwise.
//
// The policy is the curated list below; the check reflects over the two struct
// types purely to answer "is a field of this name present?". Reflection is the
// mechanism, not the policy — a struct-reflection-with-naming-convention gate
// would flag every non-copy field and drown the signal, so the flags are named
// explicitly and each exemption is reasoned.
func TestCopyPhaseFlagParityMigratorStreamer(t *testing.T) {
	// parityFlags: copy-phase capabilities that MUST exist on both Migrator and
	// Streamer (they share the copy phases), keyed by the struct field name each
	// carries. The un-exempt entries are the known-good "wired to both" anchors
	// that prove the positive case (UpfrontIndexes joined them in item 111 phase
	// 2); the exempt entries are the recorded asymmetries.
	parityFlags := []string{
		"IndexBuildFallback", // both: the deploy-request out-of-band index build
		"Redactor",           // both: PII redaction policy applied per copied row
		"InjectShardColumn",  // both: ADR-0048 shard-discriminator stamp
		"BulkBatchSize",      // both: per-batch row count for the copy
		"UpfrontIndexes",     // both: build secondary indexes before the copy (item 111 phase 2)
		"RaiseQueryTimeout",  // both: ADR-0182 query-timeout raise around the copy (item 111 phase 3)
		"AnalyzeAfter",       // both: advisory post-copy ANALYZE statistics refresh (item 111)
	}

	// parityExempt: field -> reason, for deliberate or pending asymmetries. An
	// exempt flag is asserted present on EXACTLY ONE struct, so the day someone
	// wires it to both (or renames it away) this gate fails and forces the
	// exemption to be removed — a stale exemption cannot rot silently.
	//
	// EMPTY as of item 111: every copy-phase flag above is wired to BOTH
	// entry points — full parity. The map and its enforcement stay in place so
	// the moment a future copy-phase flag lands on one struct and not the other,
	// the gate fails until it's either wired to both or exempted here with a
	// reason. (UpfrontIndexes/RaiseQueryTimeout/AnalyzeAfter were each exempt in
	// turn during item 111 and dropped as they were wired.)
	parityExempt := map[string]string{}

	migType := reflect.TypeOf(Migrator{})
	strType := reflect.TypeOf(Streamer{})

	hasField := func(typ reflect.Type, name string) bool {
		_, ok := typ.FieldByName(name)
		return ok
	}

	// Every parityExempt key must appear in parityFlags — otherwise an exemption
	// documents an asymmetry the gate never actually checks (a decorative,
	// unenforced reason).
	for f := range parityExempt {
		found := false
		for _, pf := range parityFlags {
			if pf == f {
				found = true
				break
			}
		}
		if !found {
			t.Errorf("parityExempt has %q but it is not listed in parityFlags; add it to parityFlags or the exemption checks nothing", f)
		}
	}

	for _, f := range parityFlags {
		onMig := hasField(migType, f)
		onStr := hasField(strType, f)

		if reason, exempt := parityExempt[f]; exempt {
			if strings.TrimSpace(reason) == "" {
				t.Errorf("parityExempt[%q] must carry a non-empty architectural reason", f)
			}
			// Stale-exemption catch: an exempt flag must be present on EXACTLY
			// one struct. onMig == onStr means either it is now on both (wire
			// done — drop the exemption) or on neither (the field was renamed or
			// removed — drop or update the exemption).
			if onMig == onStr {
				switch {
				case onMig && onStr:
					t.Errorf("exempt flag %q is now present on BOTH Migrator and Streamer; remove its parityExempt entry so the gate enforces parity", f)
				default:
					t.Errorf("exempt flag %q is present on NEITHER struct; the field was renamed or removed — update or drop its parityExempt entry", f)
				}
			}
			continue
		}

		if !onMig || !onStr {
			t.Errorf("copy-phase flag %q must exist on BOTH Migrator (present=%v) and Streamer (present=%v): "+
				"a copy-phase capability wired to only one entry point is the item-111 accident this gate exists to catch. "+
				"Wire it to both, or add a parityExempt entry naming the architectural reason it is one-sided.",
				f, onMig, onStr)
		}
	}
}
