// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

// The bare-flag half of the runtime-message CLI-surface gate (Bug 230).
//
// cli_message_surface_test.go grades `sluice <cmd> --flag` INVOCATIONS
// anywhere in the tree. This file grades bare `--flag` tokens in PROSE — which
// the invocation walker deliberately cannot look at, because a bare flag
// carries no command context in the literal itself.
//
// It can grade them here because [migcore.HintTexts] is the one surface where
// the receiving command is known BY CONSTRUCTION rather than inferred: a
// per-command hint text is only ever shown to the command it is keyed under,
// and the neutral text is shown to every command that has no override. So the
// grading rule is exact, needs no call graph, and needs no exemption list.
//
// # Scope, stated so the name cannot be read as broader than the truth
//
//   - It grades the hint REGISTRY (plus the dynamic AAAA-only classifier), not
//     every runtime message. A bare flag in some other error string is not
//     covered; internal/climsggate's package doc carries the census that says
//     why, and docs/dev/audit-backlog.md carries the residual.
//   - It grades what a text SAYS. It does not prove which commands actually
//     reach a given phase — that is why the NEUTRAL text is graded against
//     EVERY command rather than against a rostered subset: an under-declared
//     roster is precisely the defect Bug 230 was.
//   - A missing per-command override is not a failure. It degrades to the
//     neutral text, which is less specific but always runnable.
package main

import (
	"go/ast"
	"go/parser"
	"go/token"
	"sort"
	"strings"
	"testing"

	"github.com/alecthomas/kong"

	"sluicesync.dev/sluice/internal/climsggate"
	"sluicesync.dev/sluice/internal/pipeline/migcore"
)

func TestHintRegistryTextsNameOnlyFlagsTheirCommandAccepts(t *testing.T) {
	commands, flags, _ := cliSurface(t)

	texts := migcore.HintTexts()

	// Anti-vacuity floors. A registry the walk stopped seeing, or a
	// tokenizer that stopped matching, would pass every assertion below on
	// an empty set — the failure mode this project rebuilt three gates to
	// avoid. Sized about two thirds of what the registry carries today —
	// measured, not guessed: 22 texts, 18 graded flag tokens, 10 per-command
	// overrides (2026-08-07). Mutation-verified in both directions.
	var graded, overrides int
	for _, ht := range texts {
		graded += len(climsggate.BareFlags(ht.Text))
		if ht.Command != "" {
			overrides++
		}
	}
	switch {
	case len(texts) < 14:
		t.Fatalf("migcore.HintTexts() returned only %d texts (floor 14) — the registry walk is not seeing it",
			len(texts))
	case graded < 12:
		t.Fatalf("only %d bare --flag tokens graded across %d hint texts (floor 12) — the tokenizer is "+
			"probably broken, which is the half of this gate that matters", graded, len(texts))
	case overrides < 6:
		t.Fatalf("only %d per-command hint overrides exist (floor 6) — Bug 230's fix has been unwound and "+
			"every command is back on one shared remedy", overrides)
	}

	for _, ht := range texts {
		// A per-command text is graded against THAT command; the neutral
		// text (Command == "") is graded against every command, because
		// every command without an override receives it.
		targets := []string{string(ht.Command)}
		if ht.Command == "" {
			targets = sortedKeys(commands)
		} else if !commands[string(ht.Command)] {
			t.Errorf("hint %s carries an override for %q, which is not a kong command path. The override is "+
				"DEAD — RunningCommand() holds kong's own path spelling, so it will never match and this "+
				"command silently gets the neutral text.", ht.Code, ht.Command)
			continue
		}

		for _, cmd := range targets {
			for _, name := range climsggate.BareFlags(ht.Text) {
				if flags[cmd][name] {
					continue
				}
				t.Errorf("hint %s names --%s in the text shown to `sluice %s`, which has no such flag.\n"+
					"  text: %s\n%s", ht.Code, name, cmd, ht.Text, bareFlagRemedy(ht.Command))
			}
		}
	}
}

// bareFlagRemedy is the failure message's second half: what to do about it,
// which differs sharply depending on whether the offending text is the
// fallback or a per-command one.
func bareFlagRemedy(cmd migcore.Command) string {
	if cmd == "" {
		return "  This is the COMMAND-NEUTRAL text — every command that has no override receives it, so it may " +
			"name only flags that are valid everywhere. Either drop the flag from it and describe the remedy in " +
			"words, or move the flag-bearing wording into a perCommand entry for the commands that have the flag."
	}
	return "  This text is keyed to that one command, so the flag has to exist there. Either fix the spelling " +
		"or move the clause to a command that accepts it."
}

func sortedKeys(m map[string]bool) []string {
	out := make([]string, 0, len(m))
	for k := range m {
		out = append(out, k)
	}
	sort.Strings(out)
	return out
}

// TestSelectedCommandPathMatchesTheGradedSurface binds the two halves that
// make the gate above meaningful: the path main.go RECORDS at runtime has to
// be spelled the same way the surface the gate GRADES spells it. Two facts can
// each be pinned and still leave the argument unpinned — kong's own
// Node.Path() renders `schema add-table <table>`, which would match no key in
// the flag map and silently drop every command onto its neutral text.
func TestSelectedCommandPathMatchesTheGradedSurface(t *testing.T) {
	commands, _, _ := cliSurface(t)

	for _, argv := range [][]string{
		{"migrate"},
		{"sync", "start"},
		{"sync", "run"},
		{"schema", "add-table", "widgets"},
		{"restore"},
	} {
		kp, err := kong.New(&CLI{}, kong.Name("sluice"), kong.Vars{"version": "test"}, kong.Exit(func(int) {}))
		if err != nil {
			t.Fatalf("build kong parser: %v", err)
		}
		// Trace, not Parse: command SELECTION is all that is under test, and
		// Parse would additionally demand every required flag (--source-driver
		// and friends), which would make this pin a list of DSNs to maintain.
		kctx, err := kong.Trace(kp, argv)
		if err != nil {
			t.Fatalf("trace %v: %v", argv, err)
		}
		got := selectedCommandPath(kctx.Selected())
		if !commands[got] {
			t.Errorf("`sluice %s` records running command %q, which is not a key in the graded command "+
				"surface — every hint for it would silently fall back to the neutral text",
				strings.Join(argv, " "), got)
		}
	}
}

// TestRunningCommandReachesTheHint is the end-to-end pin on the wiring: with
// the command recorded, the bulk-copy failure hint an operator sees is the one
// keyed to that command. Bug 230 was that all of them saw `migrate`'s.
func TestRunningCommandReachesTheHint(t *testing.T) {
	defer migcore.SetRunningCommand("")

	copyFailure := errString("pipeline: copy table \"bad\": postgres: value out of range")

	cases := []struct {
		cmd     migcore.Command
		want    string
		wantNot string
	}{
		{cmd: migcore.CommandMigrate, want: "--resume"},
		{cmd: migcore.CommandSyncStart, want: "--exclude-table", wantNot: "--resume to continue"},
		{cmd: migcore.CommandSyncRun, want: "fleet config", wantNot: "--resume"},
		{cmd: migcore.CommandAddTable, want: "exactly the ONE table", wantNot: "--resume"},
		{cmd: "", want: "take the table out of this run's table scope", wantNot: "--resume"},
	}
	for _, c := range cases {
		t.Run(string(c.cmd)+"/", func(t *testing.T) {
			migcore.SetRunningCommand(string(c.cmd))
			got := migcore.WrapWithHint(migcore.PhaseBulkCopy, copyFailure).Error()
			if !strings.Contains(got, c.want) {
				t.Errorf("hint for %q does not contain %q:\n%s", c.cmd, c.want, got)
			}
			if c.wantNot != "" && strings.Contains(got, c.wantNot) {
				t.Errorf("hint for %q still contains %q (that is Bug 230):\n%s", c.cmd, c.wantNot, got)
			}
		})
	}
}

// TestMainRecordsTheRunningCommand pins the one wiring call the rest of this
// file cannot reach: main() is not callable from a test, so without this the
// whole per-command mechanism could be correct and never engaged in the
// shipped binary — a hint dispatch nobody dispatches, which is exactly the
// "written invariant nobody checks" shape.
func TestMainRecordsTheRunningCommand(t *testing.T) {
	fset := token.NewFileSet()
	f, err := parser.ParseFile(fset, "main.go", nil, 0)
	if err != nil {
		t.Fatalf("parse main.go: %v", err)
	}

	var found, sawMain bool
	for _, d := range f.Decls {
		fn, ok := d.(*ast.FuncDecl)
		if !ok || fn.Recv != nil || fn.Name.Name != "main" {
			continue
		}
		sawMain = true
		ast.Inspect(fn, func(n ast.Node) bool {
			call, ok := n.(*ast.CallExpr)
			if !ok {
				return true
			}
			if id, ok := call.Fun.(*ast.Ident); ok && id.Name == "recordRunningCommand" {
				found = true
			}
			return true
		})
	}
	if !sawMain {
		t.Fatal("no func main() found in main.go — this gate is parsing the wrong file")
	}
	if !found {
		t.Error("main() does not call recordRunningCommand. Every runtime hint would fall back to its " +
			"command-NEUTRAL text in the shipped binary, so `migrate` would silently stop naming --resume " +
			"and Bug 230's fix would be dead code that all its unit tests still pass.")
	}
}

// errString is a minimal error whose text drives the registry's substring
// match without dragging fmt.Errorf's %w semantics into the fixture.
type errString string

func (e errString) Error() string { return string(e) }
