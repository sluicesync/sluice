// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package pipeline

import (
	"os"
	"path/filepath"
	"sort"
	"strings"
	"testing"
)

// The default PostgreSQL replication slot identifier — `sluice_slot` — is
// declared independently in five non-test files across three packages. Each
// duplication carries its own comment justifying it (the pipeline must not
// import an engine package; `cmd/sluice` renders operator advice), and all
// five spell it identically today, so this is not a live defect.
//
// It is the SUBSTRATE a live defect grew in. Audit A0909-AQ-M-1: `sync health`
// and the `diagnose` bundle each filled in "the default slot" for a PostgreSQL
// source and skipped the `sluice_` prefix that `--slot-name` is a suffix to,
// so an operator passing the flag had both read-only probes querying an object
// that does not exist — and a lookup against an absent slot returns no rows,
// which the caller could not tell from a healthy slot with nothing to report.
// The spill counters were simply missing, with no reason given.
//
// Consolidating the five is a wider refactor across code that pass did not
// read, and is deliberately NOT what this gate asks for. This is the cheap
// half: the SIXTH declaration fails the build, so the next one is a decision
// somebody makes rather than a copy somebody pastes (audit A0909-SLOTCONST-1).
//
// Scope, stated so the name cannot read broader than the truth: it grades
// STRING LITERALS in non-test Go files under the three package roots below. A
// name built by concatenation (`slotPrefix + "slot"`) is invisible to it, and
// so is any file outside those roots. What it reliably catches is the shape
// that actually recurs here — a new `const x = "sluice_slot"` beside a comment
// explaining why importing the existing one was inconvenient.

// defaultSlotLiteral is the identifier this gate counts.
const defaultSlotLiteral = `"sluice_slot"`

// defaultSlotLiteralRoots are the package directories scanned, relative to
// this package's own directory.
var defaultSlotLiteralRoots = []string{
	".",
	"../engines/postgres",
	"../../cmd/sluice",
	"../diagnose",
}

// defaultSlotLiteralAllowed is every non-test file permitted to spell the
// literal, with the reason. A file here has been looked at; a file NOT here
// has not. Adding an entry is a claim that a sixth home is genuinely better
// than reaching the five that exist — say why.
var defaultSlotLiteralAllowed = map[string]string{
	"../engines/postgres/cdc_reader.go":  "the ORIGIN: the engine that actually creates the slot (`defaultSlot`). Everything else is a copy of this.",
	"../engines/postgres/slot_create.go": "prose only — two comments about quoting, explaining why the default name needs none.",
	"add_table.go":                       "`defaultActiveSlotName` — pipeline must not import an engine package.",
	"streamer_coldstart_stop.go":         "`defaultSlotNameForAdvice` — the STOPPED-SLOT-KEPT remedy must name a slot that exists; held to the engine by TestStoppedSlotAdviceNamesTheRealDefault.",
	"streamer_slot_health.go":            "`defaultPGSlotName` — the health probe's resolver; its comment already points at the engine's copy.",
	"streamer_slot_policy.go":            "prose only — a worked example of the suffix→resolved mapping.",
	"replication_preflight.go":           "prose only — a quoted PostgreSQL error message.",
	"../../cmd/sluice/sync_run.go":       "the CLI's operator-facing default, rendered into help and advice.",
	"../diagnose/bundle.go":              "prose only — a comment about the default this code no longer fills in (audit A0909-AQ-M-1).",
}

func TestDefaultSlotLiteralHasNoNewHome(t *testing.T) {
	found := map[string]struct{}{}
	for _, root := range defaultSlotLiteralRoots {
		entries, err := os.ReadDir(root)
		if err != nil {
			t.Fatalf("read dir %q: %v", root, err)
		}
		for _, e := range entries {
			name := e.Name()
			if e.IsDir() || !strings.HasSuffix(name, ".go") || strings.HasSuffix(name, "_test.go") {
				continue
			}
			path := filepath.Join(root, name)
			b, err := os.ReadFile(path)
			if err != nil {
				t.Fatalf("read %q: %v", path, err)
			}
			if !strings.Contains(string(b), defaultSlotLiteral) {
				continue
			}
			key := name
			if root != "." {
				key = filepath.ToSlash(filepath.Join(root, name))
			}
			found[key] = struct{}{}
		}
	}

	// Anti-vacuity: the five known declarations plus the prose homes. A
	// matcher that stopped reading files would find none and pass.
	if len(found) < 8 {
		t.Fatalf("anti-vacuity: found %s in only %d files (floor 8) — the scan is not reading the package "+
			"roots, and this gate would pass however many new homes appeared", defaultSlotLiteral, len(found))
	}

	var unexpected []string
	for f := range found {
		if _, ok := defaultSlotLiteralAllowed[f]; !ok {
			unexpected = append(unexpected, f)
		}
	}
	if len(unexpected) > 0 {
		sort.Strings(unexpected)
		t.Fatalf("a NEW home for %s:\n  %s\n\n"+
			"This identifier is already declared in five places, and that duplication is what audit "+
			"A0909-AQ-M-1 grew in: two read-only probes each filled in \"the default slot\" and each skipped "+
			"the `sluice_` prefix that --slot-name is a suffix to, so both queried an object that does not "+
			"exist and reported nothing rather than saying why.\n"+
			"Reach for one of the existing declarations (ResolveSlotName is usually the right answer — it "+
			"resolves the suffix the way `sync start` does), or add an entry to defaultSlotLiteralAllowed "+
			"stating why a sixth home is better than the five that exist.",
			defaultSlotLiteral, strings.Join(unexpected, "\n  "))
	}

	// The reverse guard: an allowlist entry whose file no longer spells the
	// literal is a stale classification, and a stale allowlist is how a gate
	// silently widens.
	for f := range defaultSlotLiteralAllowed {
		if _, ok := found[f]; !ok {
			t.Errorf("stale allowlist entry %q: it no longer contains %s — remove it, so the allowlist keeps "+
				"describing the code", f, defaultSlotLiteral)
		}
	}
}
