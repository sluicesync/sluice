// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package main

import (
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strings"
	"testing"

	"github.com/alecthomas/kong"
)

// A remedy an operator cannot paste is not a remedy.
//
// WHY THIS EXISTS. A refusal fires mid-incident and its remedy line is the
// operator's entire recovery path at that moment. `sluice trigger setup` has
// TWO flags kong marks required — `--dsn` and `--tables` — so a message that
// says "re-run `sluice trigger setup` to reinstall" produces, when pasted, a
// second refusal: "--dsn is required". The operator is now two failures deep
// on advice sluice gave them.
//
// It has also been filed twice and got WORSE in between. The 2026-08-31 audit
// counted 9 of 13 such remedies bare; by v0.142.1 it was 11 of 15 in one file
// and 40 across the repo, because a fix landing in that same file (the SLP-1
// OID door, `c6632629`) added two more bare arms while the finding was open.
// A hand-count filed in a report does not stop the next commit; this does.
//
// HOW THE UNIVERSE IS DERIVED, because a hand-typed list of commands and
// their required flags would rot exactly like the remedies did: the required
// flags come from the REAL kong model, walked at test time. Add a required
// flag to a command and every remedy naming that command must gain it, with
// no list for anyone to remember to update.
//
// SCOPE, stated because a gate that reads broader than its reach is worse
// than none:
//
//   - IN: Go string literals under internal/ and cmd/ that PRESCRIBE running
//     a sluice command — "run `sluice …`" / "re-run `sluice …`". These are
//     the operator-facing remedies.
//   - OUT: prose that merely MENTIONS a command ("`trigger setup` records the
//     digest"), comments, and test files. Mentioning is not prescribing, and
//     a doc comment is not something an operator pastes.
//   - OUT: docs and release notes. Those are covered by
//     TestDocsNameOnlyRealCommands and the notes-claims gate; this one is
//     about runtime messages, which no doc gate reads.
//
// A remedy that deliberately shows a partial invocation declares itself with
// `remedy-partial:` in a comment on the same line, and says why.
func TestRemedyCommandsAreRunnable(t *testing.T) {
	var cli CLI
	parser, err := kong.New(&cli, kong.Vars{"version": "test"}, kong.Exit(func(int) {}))
	if err != nil {
		t.Fatalf("kong.New: %v", err)
	}
	required := requiredFlagsByCommand(parser.Model.Node, nil, map[string][]string{})
	// Anti-vacuity on the derivation itself: `trigger setup` is known to
	// carry two required flags. A walk that stopped finding them would make
	// every assertion below trivially true.
	if got := required["trigger setup"]; len(got) < 2 {
		t.Fatalf("kong walk found %d required flag(s) for `trigger setup` (%v); it has at least two (--dsn, --tables). "+
			"The model walk has broken, and every check below would pass vacuously.", len(got), got)
	}

	// "run `sluice <command>`" or "re-run `sluice <command>`", capturing the
	// command words and whatever follows inside the backticks.
	// "run" / "re-run" / "running" / "re-running" -- the -ing forms defeated
	// the first cut of this matcher and hid three real bare prescriptions,
	// including the direct sibling of a finding in the same file.
	prescribe := regexp.MustCompile("(?:re-)?runn?(?:ing)? `sluice ([a-z][a-z0-9-]*(?: [a-z][a-z0-9-]*)*)([^`]*)`")

	type gap struct {
		file, command string
		line          int
		missing       []string
	}
	var gaps []gap
	scanned, prescriptions := 0, 0

	for _, root := range []string{"..", filepath.Join("..", "..", "internal")} {
		err := filepath.Walk(root, func(path string, info os.FileInfo, err error) error {
			if err != nil {
				return err
			}
			if info.IsDir() {
				if info.Name() == "testdata" || info.Name() == ".claude" || info.Name() == "workspace" {
					return filepath.SkipDir
				}
				return nil
			}
			if !strings.HasSuffix(path, ".go") || strings.HasSuffix(path, "_test.go") {
				return nil
			}
			b, readErr := os.ReadFile(path)
			if readErr != nil {
				return readErr
			}
			scanned++
			// Go string CONCATENATION is joined before scanning. A remedy
			// written as `"Re-run \`sluice trigger " + "setup --dsn=...\`"`
			// spans two source lines, and a line-oriented scan sees neither
			// half — so a bare prescription written that way was invisible to
			// the first cut of this gate. Line attribution stays on the FIRST
			// line of the join, which is where a reader looks.
			for i, line := range joinConcatenatedLines(strings.Split(string(b), "\n")) {
				trimmed := strings.TrimSpace(line)
				// Comments are documentation, not something anyone pastes out
				// of a terminal.
				if strings.HasPrefix(trimmed, "//") {
					continue
				}
				if strings.Contains(line, "remedy-partial:") {
					continue
				}
				for _, m := range prescribe.FindAllStringSubmatch(line, -1) {
					cmd, tail := m[1], m[2]
					// The longest command path that actually exists — the
					// regex is greedy across words, and "trigger setup to
					// reinstall" must resolve to "trigger setup".
					name, ok := longestKnownCommand(cmd, required)
					if !ok {
						continue
					}
					prescriptions++
					var missing []string
					for _, f := range required[name] {
						if !strings.Contains(tail, "--"+f) {
							missing = append(missing, "--"+f)
						}
					}
					if len(missing) > 0 {
						gaps = append(gaps, gap{file: filepath.ToSlash(path), line: i + 1, command: name, missing: missing})
					}
				}
			}
			return nil
		})
		if err != nil {
			t.Fatalf("walk %s: %v", root, err)
		}
	}

	// Anti-vacuity on the SCAN: this repo has dozens of such remedies. A
	// matcher that stopped matching would report a clean sheet.
	if scanned < 100 || prescriptions < 10 {
		t.Fatalf("scanned %d Go files and found %d command prescriptions (floors: 100 files, 10 prescriptions) — "+
			"the walker or the matcher has drifted; fix it rather than the floor, or this gate checks nothing", scanned, prescriptions)
	}

	sort.Slice(gaps, func(i, j int) bool {
		if gaps[i].file != gaps[j].file {
			return gaps[i].file < gaps[j].file
		}
		return gaps[i].line < gaps[j].line
	})
	for _, g := range gaps {
		t.Errorf("%s:%d prescribes `sluice %s` without %s, which kong marks required — "+
			"pasting this remedy produces a second refusal, mid-incident, on sluice's own advice",
			g.file, g.line, g.command, strings.Join(g.missing, " and "))
	}
	if len(gaps) > 0 {
		t.Logf("\n%d unrunnable remedy/remedies. Either name the required flags (a placeholder like "+
			"`--dsn=... --tables=...` is fine, and rendering the real table list is better), or mark the line "+
			"`remedy-partial: <why>` if a partial invocation is deliberate.", len(gaps))
	}
}

// requiredFlagsByCommand walks the kong model and records, per full command
// path, the names of the flags kong marks required. Derived rather than
// listed so a newly-required flag propagates to this gate for free.
func requiredFlagsByCommand(node *kong.Node, prefix []string, out map[string][]string) map[string][]string {
	for _, child := range node.Children {
		if child.Name == "" {
			continue
		}
		path := append(append([]string(nil), prefix...), child.Name)
		name := strings.Join(path, " ")
		var req []string
		for _, f := range child.Flags {
			if f.Required && f.Name != "" {
				req = append(req, f.Name)
			}
		}
		sort.Strings(req)
		out[name] = req
		requiredFlagsByCommand(child, path, out)
	}
	return out
}

// longestKnownCommand resolves the greedily-captured word run to the longest
// prefix that is a real command, so "trigger setup to reinstall" resolves to
// "trigger setup" and trailing prose is discarded.
func longestKnownCommand(words string, known map[string][]string) (string, bool) {
	parts := strings.Fields(words)
	for n := len(parts); n > 0; n-- {
		candidate := strings.Join(parts[:n], " ")
		if _, ok := known[candidate]; ok {
			return candidate, true
		}
	}
	return "", false
}

// joinConcatenatedLines collapses Go string-concatenation continuations into
// the line that starts them, so a remedy split across source lines is scanned
// as the one string it renders to. The returned slice keeps the original
// index of each starting line, so reported line numbers still point where a
// reader would look.
//
// A line is treated as continuing when it ends with a quote followed by "+",
// which is how this repo wraps long messages. Deliberately textual: this is a
// gate, not a parser, and a join it misses fails LOUDLY (the remedy looks
// bare) rather than silently passing.
func joinConcatenatedLines(lines []string) []string {
	// Raw string literals, so there is nothing here for a shell here-doc or an
	// editing script to mangle: quotePlus is the two characters a wrapped Go
	// string ends with, quote is one double-quote.
	const (
		quotePlus = `"+`
		quote     = `"`
	)
	out := make([]string, len(lines))
	for i := 0; i < len(lines); i++ {
		joined := strings.TrimSpace(lines[i])
		start := i
		for strings.HasSuffix(joined, quotePlus) && i+1 < len(lines) {
			next := strings.TrimSpace(lines[i+1])
			// Drop the trailing `"+` and the next fragment's leading quote so
			// the two string bodies abut exactly as they do at runtime.
			joined = strings.TrimSuffix(joined, quotePlus) + strings.TrimPrefix(next, quote)
			i++
		}
		out[start] = joined
	}
	return out
}
