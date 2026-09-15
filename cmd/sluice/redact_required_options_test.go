// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package main

import (
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"sluicesync.dev/sluice/internal/config"
)

// THE TWO REDACTION PARSE SURFACES MUST AGREE ON WHAT IS REQUIRED
// (audit 2026-09-15 A0915-CFG-HIGH-1).
//
// `--redact users.age=randomize:int` was refused; the YAML sibling
// (`strategy: randomize`, `form: int`, nothing else) decoded the omitted
// bounds to the Go zero value and wrote `0` to every row at exit 0. Same
// class for `truncate` (every row emptied), `mask`/`inner` (whole value
// masked) and `mask`/`outer` (source value UNCHANGED under a rule that
// declared it masked). The YAML arms checked only `< 0` / `min > max`,
// and the doc-comment above one of them asserted a missing-bounds
// refusal no code performed.
//
// # What this gate derives, and from where
//
// The case list is NOT a hand list. It is read from strategyFromSpec's
// own unknown-strategy refusal, which enumerates every supported CLI
// spec shape with its option placeholders (`truncate:<n>`,
// `mask:inner:<m1>,<m2>[,<char>]`, `randomize:pan[:<brand>]`, …).
// Stripping the placeholders yields the option-less spelling of every
// shape; each is fed to BOTH parsers and the verdicts must match —
// refused on both, or accepted on both with the same strategy name. A
// new strategy or form added to the CLI list is graded automatically.
//
// For the refusals this fix introduced (`truncate`, `randomize:int`,
// `mask:inner`, `mask:outer`) the two surfaces must return the SAME
// error value, so the operator instruction cannot drift; every other
// refusal pair must at least name the same missing option.
//
// # Scope, stated so it cannot be read as broader
//
// This grades the OPTION-LESS spelling of each shape only. Present-but-
// invalid values (negative, min > max, multi-rune char) are pinned by the
// per-strategy refusal tables in redact_flag_test.go. The decode from a
// YAML FILE to the nil-vs-present pointer is pinned separately by
// TestLoadYAML_RedactionOptionPresence (internal/config) and by
// TestRedactYAML_MaskOuterWithoutMarginsRefusedThroughConfigLoad below.
func TestRedactCLIAndYAMLAgreeOnRequiredOptions(t *testing.T) {
	shapes := cliSupportedSpecShapes(t)

	// The values a required-option refusal must share across surfaces.
	shared := []struct {
		name string
		err  error
	}{
		{"truncate length", errTruncateRequiresLength},
		{"randomize:int bounds", errRandomizeIntRequiresBounds},
		{"mask inner/outer margins", errMaskGenericFormNoMargins},
	}
	// Tokens naming a missing option; a refusal pair without a shared
	// value must still agree on WHICH option is missing.
	tokens := []string{"bounds", "length", "margins", "dict", "algo", "form", "keyset"}

	refused, sharedValue := 0, 0
	for _, shape := range shapes {
		spec := optionlessSpec(shape)
		t.Run(spec, func(t *testing.T) {
			cliStrategy, cliErr := strategyFromSpec(spec, nil, "", nil)
			entry := yamlEquivalent(spec)
			yamlStrategy, yamlErr := yamlStrategyToSluice(entry, nil, "", nil)

			switch {
			case cliErr == nil && yamlErr == nil:
				if cliStrategy.Name() != yamlStrategy.Name() {
					t.Fatalf("both surfaces accept %q but build different strategies: CLI %q, YAML %q", spec, cliStrategy.Name(), yamlStrategy.Name())
				}
				return
			case cliErr != nil && yamlErr == nil:
				t.Fatalf("CLI refuses %q (%v) but the YAML sibling %+v ACCEPTS it as %q — "+
					"the YAML surface is running a rule the CLI would not; an omitted required "+
					"option is decoding to the Go zero value (A0915-CFG-HIGH-1)", spec, cliErr, entry, yamlStrategy.Name())
			case cliErr == nil && yamlErr != nil:
				t.Fatalf("YAML refuses %+v (%v) but the CLI ACCEPTS %q as %q — the CLI surface "+
					"has lost a required-option refusal", entry, yamlErr, spec, cliStrategy.Name())
			}
			refused++

			for _, s := range shared {
				cliHas, yamlHas := errors.Is(cliErr, s.err), errors.Is(yamlErr, s.err)
				if cliHas != yamlHas {
					t.Fatalf("%s: the %s refusal is shared by only one surface\n  CLI:  %v\n  YAML: %v",
						spec, s.name, cliErr, yamlErr)
				}
				if cliHas {
					sharedValue++
				}
			}
			if !shareToken(cliErr.Error(), yamlErr.Error(), tokens) {
				t.Fatalf("%s: both surfaces refuse, but name different missing options\n  CLI:  %v\n  YAML: %v",
					spec, cliErr, yamlErr)
			}
		})
	}

	// Anti-vacuity floors. Four option-less shapes are refused on the CLI
	// today for a MISSING option that the fix added to the YAML side
	// (truncate, randomize:int, mask:inner, mask:outer) — every one of
	// those must have matched a shared value above. A derivation that
	// stopped finding shapes, or a shared-value list nothing matches, is
	// green for exactly the defect this gate exists to catch.
	if refused < 4 {
		t.Fatalf("only %d option-less spec shape(s) refused on both surfaces; floor 4 — the case derivation from strategyFromSpec's supported list is vacuous", refused)
	}
	if sharedValue < 4 {
		t.Fatalf("only %d refusal pair(s) matched a shared error value; floor 4 (truncate, randomize:int, mask:inner, mask:outer) — a surface stopped returning the shared value", sharedValue)
	}
}

// cliSupportedSpecShapes reads the CLI parser's own supported-spec list
// out of its unknown-strategy refusal — `(supported: a, b:<x>, …)` —
// so the universe is the parser's, not this file's.
func cliSupportedSpecShapes(t *testing.T) []string {
	t.Helper()
	_, err := strategyFromSpec("no-such-strategy", nil, "", nil)
	if err == nil {
		t.Fatal("strategyFromSpec accepted an unknown strategy; the supported list cannot be derived")
	}
	msg := err.Error()
	start := strings.Index(msg, "(supported: ")
	end := strings.LastIndex(msg, ")")
	if start < 0 || end <= start {
		t.Fatalf("unknown-strategy refusal no longer carries a '(supported: …)' list to derive from: %q", msg)
	}
	var shapes []string
	for _, s := range strings.Split(msg[start+len("(supported: "):end], ", ") {
		if s = strings.TrimSpace(s); s != "" {
			shapes = append(shapes, s)
		}
	}
	if len(shapes) < 10 {
		t.Fatalf("derived only %d spec shapes from the supported list (%q); floor 10 — the list format changed", len(shapes), msg)
	}
	return shapes
}

// optionlessSpec strips the option placeholders from a supported-list
// shape: `truncate:<n>` → `truncate`, `mask:inner:<m1>,<m2>[,<char>]` →
// `mask:inner`, `randomize:pan[:<brand>]` → `randomize:pan`, `static:<v>`
// → `static`. Shapes with no placeholder are returned as written.
func optionlessSpec(shape string) string {
	cut := len(shape)
	for _, marker := range []string{"<", "["} {
		if i := strings.Index(shape, marker); i >= 0 && i < cut {
			cut = i
		}
	}
	return strings.TrimRight(shape[:cut], ":")
}

// yamlEquivalent renders an option-less CLI spec as the YAML entry an
// operator would write for it: the strategy name in `strategy:`, and
// the second segment in the key the YAML schema assigns it (`algo:` for
// hash, `form:` for everything else that has forms). No option keys are
// set — that is the shape under test.
func yamlEquivalent(spec string) config.Redaction {
	name, second, _ := strings.Cut(spec, ":")
	entry := config.Redaction{Table: "users.col", Strategy: name}
	switch name {
	case "hash":
		entry.Algo = second
	default:
		entry.Form = second
	}
	return entry
}

// shareToken reports whether some token names the missing option in
// BOTH messages.
func shareToken(a, b string, tokens []string) bool {
	for _, tok := range tokens {
		if strings.Contains(a, tok) && strings.Contains(b, tok) {
			return true
		}
	}
	return false
}

// TestMergeYAMLRedactions_OmittedRequiredOptionIsRefused pins the four
// observed shapes by name (the derived gate above covers them too; this
// is the one a reader greps for), and the other half of the contract:
// an option that is PRESENT with the value 0 is honoured exactly as the
// explicit CLI spelling is. Presence is the test, not the value.
func TestMergeYAMLRedactions_OmittedRequiredOptionIsRefused(t *testing.T) {
	refused := []struct {
		name  string
		entry config.Redaction
		want  error
	}{
		{"randomize int, no bounds", config.Redaction{Table: "users.age", Strategy: "randomize", Form: "int"}, errRandomizeIntRequiresBounds},
		{"randomize int, min only", config.Redaction{Table: "users.age", Strategy: "randomize", Form: "int", Min: i64p(1)}, errRandomizeIntRequiresBounds},
		{"randomize int, max only", config.Redaction{Table: "users.age", Strategy: "randomize", Form: "int", Max: i64p(9)}, errRandomizeIntRequiresBounds},
		{"truncate, no length", config.Redaction{Table: "users.email", Strategy: "truncate"}, errTruncateRequiresLength},
		{"mask inner, no margins", config.Redaction{Table: "users.pan", Strategy: "mask", Form: "inner"}, errMaskGenericFormNoMargins},
		{"mask inner, m1 only", config.Redaction{Table: "users.pan", Strategy: "mask", Form: "inner", M1: ip(4)}, errMaskGenericFormNoMargins},
		{"mask outer, no margins (the PII-unchanged arm)", config.Redaction{Table: "users.pan", Strategy: "mask", Form: "outer"}, errMaskGenericFormNoMargins},
		{"mask outer, m2 only", config.Redaction{Table: "users.pan", Strategy: "mask", Form: "outer", M2: ip(4)}, errMaskGenericFormNoMargins},
	}
	for _, c := range refused {
		t.Run(c.name, func(t *testing.T) {
			_, err := mergeYAMLRedactions(nil, []config.Redaction{c.entry}, nil, "", nil)
			if err == nil {
				t.Fatalf("accepted %+v; an omitted required option must be refused, not decoded to 0", c.entry)
			}
			if !errors.Is(err, c.want) {
				t.Fatalf("refused %+v with the wrong value:\n  got:  %v\n  want: %v", c.entry, err, c.want)
			}
		})
	}

	accepted := []struct {
		name  string
		entry config.Redaction
		want  string
	}{
		{"truncate length 0 (explicit)", config.Redaction{Table: "users.email", Strategy: "truncate", Length: ip(0)}, "truncate:0"},
		{"randomize int 0,0 (explicit)", config.Redaction{Table: "users.age", Strategy: "randomize", Form: "int", Min: i64p(0), Max: i64p(0)}, "randomize:int:0,0"},
		{"mask inner 0,0 (explicit)", config.Redaction{Table: "users.pan", Strategy: "mask", Form: "inner", M1: ip(0), M2: ip(0)}, "mask:inner:0,0"},
		{"mask outer 0,0 (explicit)", config.Redaction{Table: "users.pan", Strategy: "mask", Form: "outer", M1: ip(0), M2: ip(0)}, "mask:outer:0,0"},
	}
	for _, c := range accepted {
		t.Run(c.name, func(t *testing.T) {
			reg, err := mergeYAMLRedactions(nil, []config.Redaction{c.entry}, nil, "", nil)
			if err != nil {
				t.Fatalf("explicit 0 must be honoured (it is on the CLI): %v", err)
			}
			rules := reg.Rules()
			if len(rules) != 1 || rules[0].Strategy.Name() != c.want {
				t.Fatalf("got %d rule(s), first %q; want one rule named %q", len(rules), rules[0].Strategy.Name(), c.want)
			}
		})
	}

	// Spurious-option checks are presence-based too: `min: 0` on a form
	// that takes no bounds was silently tolerated when 0 meant absent.
	for _, c := range []struct {
		name  string
		entry config.Redaction
	}{
		{"min: 0 on randomize email", config.Redaction{Table: "u.x", Strategy: "randomize", Form: "email", Min: i64p(0)}},
		{"max: 0 on tokenize", config.Redaction{Table: "u.x", Strategy: "tokenize", Dict: "d", Max: i64p(0)}},
		{"m1: 0 on mask preset", config.Redaction{Table: "u.x", Strategy: "mask", Form: "ssn", M1: ip(0)}},
	} {
		t.Run(c.name, func(t *testing.T) {
			if _, err := mergeYAMLRedactions(nil, []config.Redaction{c.entry}, nil, "", nil); err == nil || !strings.Contains(err.Error(), "takes no") {
				t.Fatalf("a present-but-zero option on a form that takes none must be refused as spurious; got %v", err)
			}
		})
	}
}

// TestRedactYAML_MaskOuterWithoutMarginsRefusedThroughConfigLoad is the
// end-to-end pin for the compliance-shaped arm: a real YAML file with
// `strategy: mask` + `form: outer` and NO margins, through config.Load's
// koanf/mapstructure decode and into the registry merge. This is the
// row whose failure mode is a PII leak (mask:outer:0,0 masks nothing and
// keeps the whole value), and the one no test touched. The control half
// proves the same file WITH `m1: 0` / `m2: 0` decodes to present
// pointers and is honoured — so a refusal here is presence, not value.
func TestRedactYAML_MaskOuterWithoutMarginsRefusedThroughConfigLoad(t *testing.T) {
	load := func(t *testing.T, yaml string) []config.Redaction {
		t.Helper()
		path := filepath.Join(t.TempDir(), "sluice.yaml")
		if err := os.WriteFile(path, []byte(yaml), 0o600); err != nil {
			t.Fatal(err)
		}
		c, err := config.Load(path)
		if err != nil {
			t.Fatalf("config.Load: %v", err)
		}
		return c.Redactions
	}

	t.Run("omitted margins are refused", func(t *testing.T) {
		entries := load(t, "redactions:\n  - table: users.pan\n    strategy: mask\n    form: outer\n")
		_, err := mergeYAMLRedactions(nil, entries, nil, "", nil)
		if err == nil {
			t.Fatal("a `mask`/`outer` rule with no m1/m2 loaded and merged — it would mask NOTHING and ship the PAN unchanged at exit 0")
		}
		if !errors.Is(err, errMaskGenericFormNoMargins) {
			t.Fatalf("refused with the wrong value: %v", err)
		}
	})

	t.Run("explicit zero margins are present and honoured", func(t *testing.T) {
		entries := load(t, "redactions:\n  - table: users.pan\n    strategy: mask\n    form: outer\n    m1: 0\n    m2: 0\n")
		if len(entries) != 1 || entries[0].M1 == nil || entries[0].M2 == nil {
			t.Fatalf("explicit `m1: 0` / `m2: 0` did not decode as PRESENT: %+v", entries)
		}
		reg, err := mergeYAMLRedactions(nil, entries, nil, "", nil)
		if err != nil {
			t.Fatalf("explicit zero margins must be honoured as on the CLI (mask:outer:0,0): %v", err)
		}
		if got := reg.Rules()[0].Strategy.Name(); got != "mask:outer:0,0" {
			t.Fatalf("strategy = %q; want mask:outer:0,0", got)
		}
	})
}
