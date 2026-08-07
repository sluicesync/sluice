// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

// The instantiation of the runtime-message CLI-surface gate.
//
// The walker lives in internal/climsggate (which documents the class, the
// false-positive rule, and the stated limits). It lives there rather than
// here for the errclassgate reason: a gate that can only see the package it
// was born in re-learns the same lesson one package at a time — and the nine
// `sluice sync start --resume` instances that motivated it were spread
// across cmd/sluice, internal/pipeline and internal/engines/postgres.
//
// The TRUTH the walker is graded against has to come from here, though,
// because it is kong's own model and cmd/sluice is package main: nothing
// under internal/ can import it. docs/dev/cli-flags.txt is generated from
// the same model, but it records flags only — a command with no flags of its
// own leaves no line in it — so the gate reads the model directly rather
// than a projection of it.
package main

import (
	"path/filepath"
	"strings"
	"testing"

	"github.com/alecthomas/kong"

	"sluicesync.dev/sluice/internal/climsggate"
)

// cliSurface is the (command path -> valid long flags) map plus the command
// set, derived by walking kong's model. Flags are accumulated DOWN the tree
// because kong resolves a flag against every node on the path: a flag
// declared on `sync` is accepted on `sync start`.
func cliSurface(t *testing.T) (commands map[string]bool, flags map[string]map[string]bool, anyFlag map[string]bool) {
	t.Helper()

	parser, err := kong.New(
		&CLI{},
		kong.Name("sluice"),
		kong.Description("Open-source database migration and continuous-sync tool."),
		kong.Vars{"version": "test"},
	)
	if err != nil {
		t.Fatalf("build kong model: %v", err)
	}

	commands = map[string]bool{}
	flags = map[string]map[string]bool{}
	anyFlag = map[string]bool{}

	// names returns every long spelling the parser accepts for a flag:
	// its name, its aliases, and its negated form. Hidden flags count —
	// the binary accepts them, so a message naming one is not a phantom.
	names := func(f *kong.Flag) []string {
		out := []string{f.Name}
		out = append(out, f.Aliases...)
		if f.Tag != nil && f.Tag.Negatable != "" {
			if f.Tag.Negatable == "-" {
				out = append(out, "no-"+f.Name)
			} else {
				out = append(out, f.Tag.Negatable)
			}
		}
		return out
	}

	var walk func(node *kong.Node, path string, inherited map[string]bool)
	walk = func(node *kong.Node, path string, inherited map[string]bool) {
		own := map[string]bool{}
		for k := range inherited {
			own[k] = true
		}
		for _, f := range node.Flags {
			if f == nil || f.Name == "" {
				continue
			}
			for _, n := range names(f) {
				own[n] = true
				anyFlag[n] = true
			}
		}
		flags[path] = own
		if path != "" {
			commands[path] = true
		}
		for _, child := range node.Children {
			if child == nil || child.Type != kong.CommandNode {
				continue
			}
			for _, name := range append([]string{child.Name}, child.Aliases...) {
				walk(child, strings.TrimSpace(path+" "+name), own)
			}
		}
	}
	walk(parser.Model.Node, "", nil)

	return commands, flags, anyFlag
}

// TestRuntimeMessagesNameRealCLISurface is the gate.
//
// Scope, stated so the name cannot be read as broader than the truth:
//
//   - It reaches every NON-TEST Go file under cmd/sluice and internal/.
//     `_test.go` is excluded (walkRoot says why: a gate's own fixtures have
//     to be able to spell a phantom flag).
//   - It grades STRING LITERALS, not comments. A wrong command line in a
//     doc comment is a developer-facing error, not one an operator ever
//     reads, and the tree carries a long-standing internal shorthand
//     (`sync --where` for the filtered-sync feature) that is prose, not
//     instruction.
//   - It does NOT reach docs/, ADRs or skills/. ADRs legitimately describe
//     planned surfaces — that is precisely why THIS gate can be strict and
//     a docs gate could not — and skills/ has its own guard in
//     scripts/check-skills-flags.sh.
func TestRuntimeMessagesNameRealCLISurface(t *testing.T) {
	commands, flags, anyFlag := cliSurface(t)

	// Sanity on the truth side before grading anything against it: an empty
	// or near-empty surface would agree with any message.
	if len(commands) < 30 || len(anyFlag) < 200 {
		t.Fatalf("kong surface looks wrong: %d commands, %d distinct flags — the model walk broke",
			len(commands), len(anyFlag))
	}

	climsggate.Assert(t, climsggate.Config{
		Roots:    []string{".", filepath.Join("..", "..", "internal")},
		Commands: commands,
		Flags:    flags,
		AnyFlag:  anyFlag,
		SkipDir: func(path string) bool {
			base := filepath.Base(path)
			if base == "." || base == ".." {
				// The roots themselves. Skipping these would silently walk
				// nothing, which is what the anti-vacuity floors exist for.
				return false
			}
			return base == "testdata" || base == "vendor" || strings.HasPrefix(base, ".")
		},
		// Floors, sized about two thirds of what the tree carries today —
		// measured, not guessed: 312 spans, 38 distinct commands, 165 graded
		// flag tokens (2026-08-06). Routine churn does not trip them; a
		// broken tokenizer does. Mutation-verified in both directions:
		// breaking the pass-1 anchor drops the count to 34 and fails HERE
		// rather than passing green.
		MinSpans:         200,
		MinCommandsNamed: 25,
		MinFlagChecks:    100,
	})
}
