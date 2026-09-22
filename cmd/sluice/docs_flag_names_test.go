// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package main

import (
	"path/filepath"
	"regexp"
	"sort"
	"strings"
	"testing"
)

// TestDocsNameOnlyRealFlags is TestDocsNameOnlyRealCommands' sibling for
// FLAGS: every `--flag` an operator-facing doc names must be a flag the
// binary accepts.
//
// The command gate ran clean for months while docs/production-readiness.md's
// Known-limitations list — the page whose stated contract is that every
// limitation "names its workaround" — handed operators `--view-override`, a
// flag that has never existed (the only mention in the tree is an IR comment
// saying a future phase "will be" it). A lowercase hyphenated flag is none of
// the shapes the notes-claims gate extracts, and a bare flag is not a command
// path, so nothing graded it (gap census 2026-09-22, GC-12). The first run of
// this gate also found `sluice backup stream run … --stream-id` in a cookbook
// recipe (no such flag on that command) and `sluice migrate … --schema-only
// --no-create-schema` in another (neither exists on migrate).
//
// # What is graded, and against what
//
//   - A `--flag` inside a backticked span or fenced-block command line that
//     invokes a sluice command (`sluice sync start --x`, or the bare
//     `sync start --x` once the page has established the tool) is graded
//     against THAT command's flag set — the Bug 230 shape, in docs. This is
//     how `sluice sync start --resume` in a runbook fails here, where the
//     existence check alone would pass it (`--resume` is a real flag, on
//     `migrate`). A span naming a command GROUP (`sync --where`, prose
//     shorthand for `sync start --where`) is graded against the union of the
//     group's subcommands.
//   - Every other `--flag` token must exist on SOME command. Kong aliases
//     count (`--cutover-sequence-margin` is `--sequence-margin`'s deprecated
//     spelling and the binary still accepts it).
//
// # What is deliberately not graded
//
//   - A flag inside a backticked span or fenced-block line that invokes
//     another binary (`pg_dump --blobs`, `aws kms create-key --description …`
//     and the continuation lines under it), or inside a `$(…)` substitution
//     on a sluice line (`--source "$(heroku config:get DATABASE_URL --app
//     myapp)"`). The span's or line's first word says whose flag it is.
//   - A flag of another tool named in bare prose ("MySQL's
//     `--binlog-do-db`"). Those carry no binary in the text, so they live in
//     foreignToolFlags (docrows_command_gate_test.go, shared with the
//     published-remedy gate) with the tool named; an entry no doc still
//     mentions fails here, so the list cannot rot in either direction.
//   - A prefix family (`--notify-*`, `--include/exclude-table`), a token
//     glued to a word (a markdown anchor's `--bug-187`), and an
//     underscore-spelled flag of a Go binary that is not sluice
//     (`--track_schema_versions`).
//
// A doc that deliberately names a sluice-shaped flag the binary does not have
// — "there is no `--rename-table` flag", or a flag that was removed and is
// named so operators know it is gone — carries a marker, exactly like the
// command gate's:
//
//	<!-- cli-flag-exempt: rename-table - named as absent; there is no such flag -->

var (
	docFlagRe       = regexp.MustCompile(`--[a-z][a-z0-9-]*`)
	docFlagExemptRe = regexp.MustCompile(`<!--\s*cli-flag-exempt:\s*([a-z][a-z0-9-]*)\s*[-\x{2014}]+\s*[^\s>][^>]*-->`)
	docSpanRe       = regexp.MustCompile("(?s)`([^`]+)`")
	docSubstRe      = regexp.MustCompile(`\$\([^)]*\)`)
)

func TestDocsNameOnlyRealFlags(t *testing.T) {
	repo := repoRootForDocs(t)
	commands, flagsByCommand, anyFlag := cliSurface(t)
	topLevel, _ := commandTree(commands)
	if len(anyFlag) < 200 {
		t.Fatalf("kong yielded only %d flag names; the CLI struct moved and this gate is measuring nothing", len(anyFlag))
	}
	// A group path grades against every flag any of its subcommands takes.
	acceptedUnder := func(command, name string) bool {
		if flagsByCommand[command][name] {
			return true
		}
		for cmd, fl := range flagsByCommand {
			if strings.HasPrefix(cmd, command+" ") && fl[name] {
				return true
			}
		}
		return false
	}

	type hit struct{ file, flag, detail string }
	var bad []hit
	var graded, perCommand, files, exempted int
	foreignSeen := map[string]bool{}
	seen := map[string]bool{}

	for _, root := range docCommandRoots {
		recurse := strings.HasSuffix(root, "/...")
		root = strings.TrimSuffix(root, "/...")
		eachDocFile(t, filepath.Join(repo, root), recurse, func(path string, body []byte) {
			if seen[path] {
				return
			}
			seen[path] = true
			files++
			rel, _ := filepath.Rel(repo, path)
			rel = filepath.ToSlash(rel)

			exempt := map[string]bool{}
			for _, m := range docFlagExemptRe.FindAllStringSubmatch(string(body), -1) {
				exempt[m[1]] = true
			}
			noteForeign := func(text string) {
				for _, name := range boundedFlagTokens(text) {
					if _, ok := foreignToolFlags[name]; ok {
						foreignSeen[name] = true
					}
				}
			}
			grade := func(name, command string) {
				graded++
				if exempt[name] {
					exempted++
					return
				}
				if command != "" {
					perCommand++
					if !acceptedUnder(command, name) {
						bad = append(bad, hit{rel, name, "written under `sluice " + command + "`, which has no such flag"})
					}
					return
				}
				if anyFlag[name] {
					return
				}
				if _, ok := foreignToolFlags[name]; ok {
					foreignSeen[name] = true
					return
				}
				bad = append(bad, hit{rel, name, "not a flag on any sluice command"})
			}

			// Fenced blocks are walked line by line: each command line's
			// flags belong to its first word, continuation lines (previous
			// line ended with `\`) inherit it, and only sluice's lines are
			// graded. Everything outside a fence is prose.
			var prose strings.Builder
			inFence, continued := false, false
			binary, command := "", ""
			for _, line := range strings.Split(string(body), "\n") {
				trimmed := strings.TrimSpace(line)
				if strings.HasPrefix(trimmed, "```") {
					inFence = !inFence
					continued, binary, command = false, "", ""
					continue
				}
				if !inFence {
					prose.WriteString(line + "\n")
					continue
				}
				if !continued && trimmed != "" && !strings.HasPrefix(trimmed, "-") {
					binary, command = "", ""
					if cmd := commandPathOfSpan(trimmed, commands, topLevel); cmd != "" {
						binary, command = "sluice", cmd
					} else if w := strings.Fields(trimmed); len(w) > 0 {
						binary = w[0]
					}
				}
				continued = strings.HasSuffix(trimmed, "\\")
				if binary != "sluice" {
					noteForeign(line)
					continue
				}
				for _, name := range boundedFlagTokens(docSubstRe.ReplaceAllString(line, " ")) {
					grade(name, command)
				}
			}

			// Backticked spans in the prose: a span invoking a sluice command
			// grades per command; a span invoking another binary is skipped;
			// any other span's flags are graded for existence.
			rest := prose.String()
			outside := rest
			for _, m := range docSpanRe.FindAllStringSubmatchIndex(rest, -1) {
				span := rest[m[2]:m[3]]
				outside = strings.Replace(outside, rest[m[0]:m[1]], " ", 1)
				if cmd := commandPathOfSpan(span, commands, topLevel); cmd != "" {
					for _, name := range boundedFlagTokens(docSubstRe.ReplaceAllString(span, " ")) {
						grade(name, cmd)
					}
					continue
				}
				if isForeignInvocation(span) {
					noteForeign(span)
					continue
				}
				for _, name := range boundedFlagTokens(span) {
					grade(name, "")
				}
			}
			for _, name := range boundedFlagTokens(outside) {
				grade(name, "")
			}
		})
	}

	// Anti-vacuity floors (measured 2026-09-22 and logged: ~1,900 graded
	// tokens across ~60 files, ~600 graded under a resolved command, 4
	// exemptions).
	t.Logf("graded %d flag tokens across %d files: %d under a resolved sluice command, %d exempted", graded, files, perCommand, exempted)
	switch {
	case files < 25:
		t.Fatalf("only %d doc files scanned; a root moved and this gate is measuring nothing", files)
	case graded < 100:
		t.Fatalf("only %d --flag tokens graded (floor 100); the tokenizer stopped matching", graded)
	case perCommand < 100:
		t.Fatalf("only %d --flag tokens graded under a resolved sluice command (floor 100); the invocation resolver stopped matching and the per-command arm is vacuous", perCommand)
	case exempted == 0:
		t.Fatal("no cli-flag-exempt marker matched; docFlagExemptRe has stopped parsing them, so a deliberate mention would now fail for the wrong reason")
	}
	for name, tool := range foreignToolFlags {
		if !foreignSeen[name] {
			bad = append(bad, hit{"cmd/sluice/docrows_command_gate_test.go", name, "listed in foreignToolFlags (" + tool + ") but no doc still names it — drop the stale entry"})
		}
	}

	if len(bad) > 0 {
		sort.Slice(bad, func(i, j int) bool {
			if bad[i].file != bad[j].file {
				return bad[i].file < bad[j].file
			}
			return bad[i].flag < bad[j].flag
		})
		var b strings.Builder
		b.WriteString("operator-facing docs name flags the binary does not accept:\n")
		for _, h := range bad {
			b.WriteString("  " + h.file + ": --" + h.flag + " — " + h.detail + "\n")
		}
		b.WriteString("\nThe flag set is walked from kong, not listed by hand. Either the doc is wrong, or the flag was\n")
		b.WriteString("renamed and its docs were not. Another tool's flag named in bare prose joins foreignToolFlags with\n")
		b.WriteString("the tool named; a sluice-shaped flag named DELIBERATELY (as absent, or as removed) carries a marker:\n")
		b.WriteString("  <!-- cli-flag-exempt: <flag> - <why> -->")
		t.Fatal(b.String())
	}
}

// boundedFlagTokens returns the long-flag names in text that stand as flags:
// preceded by a boundary character (not glued to a word or a markdown anchor),
// not a prefix family (`--notify-*`, `--include/exclude-table`), and not
// followed by an underscore (a non-sluice Go binary's `--track_schema_versions`).
func boundedFlagTokens(text string) []string {
	var out []string
	for _, m := range docFlagRe.FindAllStringIndex(text, -1) {
		start, end := m[0], m[1]
		if start > 0 && !strings.ContainsRune(" \t\n`(\"'[=,/|>", rune(text[start-1])) {
			continue
		}
		// `--include/exclude-table` is a family shorthand; `--dbhost/--dbport`
		// is two flags.
		if end < len(text) && (text[end] == '_' || (text[end] == '/' && !strings.HasPrefix(text[end:], "/--"))) {
			continue
		}
		name := text[start+2 : end]
		if strings.HasSuffix(name, "-") {
			continue
		}
		out = append(out, name)
	}
	return out
}
