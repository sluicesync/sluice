// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package docsync

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// notValidEmitter is one function that renders a constraint carrying
// ir.*.NotValid, and the verdict recorded for it.
type notValidEmitter struct {
	file    string // path relative to the repo root
	fn      string // the func declaration line, matched verbatim
	verdict string // "carries" | "warns" | "exempt"
	why     string
}

// TestNotValidReachesEveryConstraintEmitter is the class gate for UPR-1/1b/1c.
//
// WHY IT EXISTS. `NotValid` was added to three IR structs (ForeignKey,
// CheckConstraint, DomainCheck) and reaches SEVEN rendering functions across
// three engines. The hand sweep that shipped with it enumerated ONE — MySQL's
// foreign key — and every other emitter dropped the state silently. A pre-tag
// review found two of those, and one was a regression the fix itself caused:
// making a malformed domain-check body parse correctly meant MySQL's
// translator stopped failing, which retired the warning that failure used to
// produce, so MySQL silently gained an ENFORCED check for a constraint the
// source declared unvalidated.
//
// That is the shape CLAUDE.md calls the most expensive recurring failure here,
// and the reason it keeps recurring is that "did I get all the emitters?" is a
// question a person answers from memory. This asks the source instead.
//
// THE RULE: every listed emitter must either CARRY the state, WARN that it
// cannot, or be EXEMPT with a reason recorded here. Silence is the one thing
// that is not allowed, because silence is what turns a strength difference
// into a target that enforces something the source does not — or, on the
// exclude path of the same class, does not enforce something the source does.
//
// WHAT IT REACHES: the seven functions named below, checked for a reference to
// NotValid in their body. It cannot see a NEW emitter nobody adds here — which
// is why the roster is fail-by-default on the file set: a constraint-rendering
// file that stops matching its recorded path fails rather than vanishing.
func TestNotValidReachesEveryConstraintEmitter(t *testing.T) {
	root := repoRootFromDocsync(t)

	roster := []notValidEmitter{
		{
			file: "internal/engines/postgres/ddl_emit.go", fn: "func emitAddForeignKey(",
			verdict: "carries",
			why:     "ALTER-based, so PG's grammar accepts NOT VALID after the DEFERRABLE clause",
		},
		{
			file: "internal/engines/postgres/ddl_emit.go", fn: "func emitCheckConstraint(",
			verdict: "warns",
			why: "PG rejects NOT VALID inline in CREATE TABLE (measured on 16) and sluice emits one " +
				"statement per DDL, so carrying it needs a separate ALTER pass (UPR-1b)",
		},
		{
			file: "internal/engines/postgres/ddl_emit.go", fn: "func emitCreateDomainType(",
			verdict: "warns",
			why: "PG rejects NOT VALID inline in CREATE DOMAIN. This one had NO warn when v0.141.4 " +
				"was drafted while its notes claimed one, which is why this roster exists",
		},
		{
			file: "internal/engines/mysql/ddl_emit.go", fn: "func emitAddForeignKey(",
			verdict: "warns",
			why:     "InnoDB has no unvalidated state; the FK lands enforced and fails loudly at errno 1452",
		},
		{
			file: "internal/engines/mysql/domain_check_translate.go", fn: "func (m mysqlEmitter) translateDomainCheckToMySQL(",
			verdict: "warns",
			why: "MySQL CHECKs are enforced or absent (errno 3819). The warn is load-bearing: before " +
				"UPR-1 the malformed body made this translation FAIL, and the drop-warn that failure " +
				"produced was the only notification — fixing the parse retired it",
		},
		{
			file: "internal/engines/mysql/ddl_emit.go", fn: "func emitCheckConstraint(",
			verdict: "warns",
			why: "MySQL CHECKs are enforced or absent (errno 3819). Closed as part of UPR-1c after " +
				"this roster recorded it as a known gap — which is the roster doing its job: it made " +
				"the hole a listed item rather than an oversight",
		},
		{
			file: "internal/engines/sqlite/ddl_emit.go", fn: "func emitForeignKey(",
			verdict: "warns",
			why: "SQLite foreign keys are enforced or absent, so the state cannot be carried and a " +
				"warn is the honest outcome. Closed as part of UPR-1c",
		},
	}

	if len(roster) < 7 {
		t.Fatalf("roster holds %d emitters; it held SEVEN when written — entries were removed rather "+
			"than their emitters", len(roster))
	}

	var carried, warned, exempt int
	for _, e := range roster {
		path := filepath.Join(root, filepath.FromSlash(e.file))
		b, err := os.ReadFile(path)
		if err != nil {
			t.Errorf("%s: unreadable (%v) — the emitter moved, and a roster that cannot find its "+
				"subject grades nothing", e.file, err)
			continue
		}
		body := string(b)
		i := strings.Index(body, e.fn)
		if i < 0 {
			t.Errorf("%s: %q not found. Either it was renamed, or the constraint rendering moved — "+
				"re-anchor this entry rather than deleting it, or the silent-drop class reopens for "+
				"that engine.", e.file, e.fn)
			continue
		}
		// Body of the function: from its declaration to the next top-level
		// func. Coarse, and sufficient — a reference anywhere inside is what
		// distinguishes "considered it" from "silently dropped it".
		rest := body[i+len(e.fn):]
		if j := strings.Index(rest, "\nfunc "); j >= 0 {
			rest = rest[:j]
		}
		mentions := strings.Contains(rest, "NotValid")

		switch e.verdict {
		case "carries", "warns":
			if !mentions {
				t.Errorf("%s %s is recorded as %q but its body never references NotValid — so the "+
					"state is dropped SILENTLY.\n\nRecorded reason: %s", e.file, e.fn, e.verdict, e.why)
			}
			if e.verdict == "carries" {
				carried++
			} else {
				warned++
			}
		case "exempt":
			exempt++
			if mentions {
				t.Errorf("%s %s is recorded EXEMPT but now references NotValid — if the gap was "+
					"closed, change the verdict to \"carries\" or \"warns\" so the roster stops "+
					"advertising a hole that no longer exists.\n\nRecorded reason: %s",
					e.file, e.fn, e.why)
			}
		default:
			t.Errorf("%s %s: unknown verdict %q", e.file, e.fn, e.verdict)
		}
	}

	// Anti-vacuity in both directions: at least one emitter must actually
	// carry the state (otherwise the feature does not exist), and at least one
	// must warn (otherwise the honest-about-limits half is gone).
	if carried == 0 {
		t.Error("no emitter CARRIES NotValid — the feature is inert and every verdict above is " +
			"describing a no-op")
	}
	if warned == 0 {
		t.Error("no emitter WARNS about being unable to carry NotValid — the paths that cannot " +
			"honour it are silent again")
	}
	t.Logf("NotValid emitters: %d carry, %d warn, %d exempt", carried, warned, exempt)
}
