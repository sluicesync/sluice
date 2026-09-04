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

// A doc that names a command sluice does not have is worse than a doc that
// says nothing: the operator runs it mid-incident, gets "unknown command",
// and then doubts the rest of the page.
//
// This ran for a long time before anyone noticed. `sluice sync add-table` was
// written into an operator page, a CHANGELOG entry and a release-notes
// archive; the command has always been `sluice schema add-table`, and the
// source file that defines it opens by saying "add-table lives here rather
// than under sync" — which is exactly the confusion that produced the wrong
// spelling. Every home was prose written from the shape of the feature rather
// than from the command tree.
//
// The notes-claims gate could not have caught it: that gate extracts
// SLUICE-E codes, file names, Test names, ALL-CAPS-HYPHENATED markers and
// backticked camelCase Go symbols, and a lowercase hyphenated subcommand is
// none of those. This is its complement, and it derives its universe the same
// way — walking the real kong model rather than consulting a list of command
// names somebody has to remember to update.
//
// SCOPE, stated here rather than left to be inferred from the name, because a
// gate whose reach is narrower than it sounds is worse than no gate.
//
// IN: the trees whose prose an operator is expected to ACT on, and which are
// expected to describe the CURRENT binary — docs/operator, docs/cookbook, the
// top-level docs/*.md guides, skills/, and README.md.
//
// OUT, each for a reason, not for convenience:
//
//   - docs/adr — an ADR records a decision, including designs considered and
//     rejected. Naming a command that was never built is what an ADR is for.
//   - docs/dev — roadmap, design notes and the audit backlog deliberately
//     name commands that do not exist yet, or that were checked and found not
//     to exist (the backlog entry for `sluice doctor` says exactly that).
//   - docs/releases — frozen published artifacts. Correcting one would be
//     rewriting what was actually published.
//   - docs/research — speculative by construction.
//   - CHANGELOG.md — a historical record of what shipped when.
//
// Those exclusions mean this gate does NOT protect the CHANGELOG, and the
// `sync add-table` error lived there too. That is a deliberate trade: the
// alternative fails on every honest sentence about future or rejected work.
//
// An in-scope file that legitimately names a command sluice does not have —
// "there is no `sluice keyset rotate` CLI in v1" is a true and useful
// sentence — carries a marker naming the path and saying why:
//
//	<!-- cli-command-exempt: keyset rotate - named as deferred future work -->

// docCommandRe matches a backticked invocation beginning `sluice `, capturing
// the words after it.
var docCommandRe = regexp.MustCompile("`sluice ([a-z][a-z0-9- ]*)`")

// docBareCommandRe matches a backticked command path written WITHOUT the
// binary name -- `sync start`, `schema add-table` -- which is how most of
// this repo's prose refers to a subcommand once the page has established
// which tool it is talking about. It is deliberately capped at three words
// and only considered when the FIRST word is a real top-level command, so
// an ordinary backticked phrase is not read as an invocation.
//
// This half exists because the bug that motivated the gate had both forms.
// A gate that closed only the spelling it was shown would have left the
// same error standing in the redaction guide and the redaction skill.
var docBareCommandRe = regexp.MustCompile("`([a-z][a-z0-9-]*(?: [a-z][a-z0-9-]*){1,2})`")

// docExemptRe reads the per-file exemption markers described above. The
// reason text is required by the pattern: an exemption with no stated reason
// does not parse, and therefore does not exempt.
var docExemptRe = regexp.MustCompile(`<!--\s*cli-command-exempt:\s*([a-z][a-z0-9 ]*[a-z0-9])\s*[-\x{2014}]+\s*[^\s>][^>]*-->`)

// docCommandRoots is the in-scope surface. A plain file is read directly; a
// directory contributes the markdown directly inside it. Nothing recurses, so
// a new docs subdirectory joins this gate by a deliberate edit rather than as
// a side effect of being created.
// A "/..." suffix means recurse. skills/ needs it because each skill is a
// directory holding its own SKILL.md -- scanning skills/ non-recursively
// found nothing at all, which is how the redaction skill kept the wrong
// command name through the first cut of this gate.
var docCommandRoots = []string{"docs", "docs/operator", "docs/cookbook", "skills/...", "README.md"}

func TestDocsNameOnlyRealCommands(t *testing.T) {
	repo := repoRootForDocs(t)

	var cli CLI
	parser, err := kong.New(&cli, kong.Vars{"version": "test"}, kong.Exit(func(int) {}))
	if err != nil {
		t.Fatalf("kong.New: %v", err)
	}
	known := commandPaths(parser.Model.Node, nil, map[string]bool{})
	// The bare-form check needs to know which words START a command, and it
	// takes them from the same model rather than a list.
	topLevel := map[string]bool{}
	for _, c := range parser.Model.Children {
		if c.Name != "" {
			topLevel[c.Name] = true
		}
	}
	if len(known) < 20 {
		t.Fatalf("kong yielded only %d command paths; the CLI struct moved and this gate is measuring nothing", len(known))
	}

	type hit struct{ file, path string }
	var bad []hit
	checked, bareChecked, files, exempted := 0, 0, 0, 0
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
			for _, m := range docExemptRe.FindAllStringSubmatch(string(body), -1) {
				exempt[strings.TrimSpace(m[1])] = true
			}

			for _, m := range docCommandRe.FindAllStringSubmatch(string(body), -1) {
				// A flag ends the command path. The capture class includes
				// "-" because subcommands are hyphenated, so it swallows a
				// trailing --flag and any lowercase value after it;
				// truncating here separates "sync stop" from
				// "sync stop --wait".
				words := strings.Fields(m[1])
				for i, w := range words {
					if strings.HasPrefix(w, "-") {
						words = words[:i]
						break
					}
				}
				if len(words) == 0 {
					continue
				}
				cmdPath := strings.Join(words, " ")
				checked++
				if known[cmdPath] {
					continue
				}
				if exempt[cmdPath] {
					exempted++
					continue
				}
				// The WHOLE path has to resolve. Accepting the longest
				// resolvable PREFIX is how the first cut of this gate passed
				// the very defect it was written for: "sync" is a real
				// command, so `sluice sync add-table` matched at "sync" and
				// the wrong subcommand was never looked at.
				bad = append(bad, hit{rel, cmdPath})
			}

			for _, m := range docBareCommandRe.FindAllStringSubmatch(string(body), -1) {
				words := strings.Fields(m[1])
				if len(words) == 0 || !topLevel[words[0]] {
					continue
				}
				cmdPath := strings.Join(words, " ")
				bareChecked++
				if known[cmdPath] || exempt[cmdPath] {
					continue
				}
				bad = append(bad, hit{rel, cmdPath})
			}
		})
	}

	// Anti-vacuity floor. Without it this gate passes by finding nothing the
	// moment the regex stops matching or a root moves.
	if files < 25 {
		t.Fatalf("only %d doc files scanned; a root moved and this gate is measuring nothing", files)
	}
	if checked < 60 {
		t.Fatalf("only %d command invocations extracted; the regex stopped matching and this gate is vacuous", checked)
	}
	if bareChecked < 40 {
		t.Fatalf("only %d bare command paths extracted; docBareCommandRe stopped matching and half this gate is vacuous", bareChecked)
	}
	if exempted == 0 {
		t.Fatal("no exemption marker matched; docExemptRe has stopped parsing them, so a deliberate mention would now pass for the wrong reason")
	}

	if len(bad) > 0 {
		sort.Slice(bad, func(i, j int) bool { return bad[i].file < bad[j].file })
		var b strings.Builder
		b.WriteString("operator-facing docs name commands that do not exist in the kong tree:\n")
		for _, h := range bad {
			b.WriteString("  " + h.file + ": `" + h.path + "`\n")
		}
		b.WriteString("\nEither the doc is wrong, or the command was renamed and its docs were not.\n")
		b.WriteString("The command tree is the authority here; it is walked from kong, not listed by hand.\n")
		b.WriteString("A doc that names a nonexistent command DELIBERATELY carries a marker saying so:\n")
		b.WriteString("  <!-- cli-command-exempt: <path> - <why> -->")
		t.Fatal(b.String())
	}
}

// commandPaths flattens the kong model into the set of space-joined command
// paths ("sync", "sync start", "schema add-table", …).
func commandPaths(node *kong.Node, prefix []string, out map[string]bool) map[string]bool {
	for _, child := range node.Children {
		if child.Name == "" {
			continue
		}
		path := append(append([]string(nil), prefix...), child.Name)
		out[strings.Join(path, " ")] = true
		commandPaths(child, path, out)
	}
	return out
}

// eachDocFile reads one markdown file, or the markdown directly inside one
// directory. It does not recurse — see docCommandRoots.
func eachDocFile(t *testing.T, root string, recurse bool, fn func(path string, body []byte)) {
	t.Helper()
	info, err := os.Stat(root)
	if err != nil {
		return // a missing root is caught by the file-count floor
	}
	read := func(p string) {
		body, rerr := os.ReadFile(p)
		if rerr != nil {
			t.Fatalf("read %s: %v", p, rerr)
		}
		fn(p, body)
	}
	if !info.IsDir() {
		read(root)
		return
	}
	if recurse {
		if err := filepath.Walk(root, func(p string, fi os.FileInfo, werr error) error {
			if werr != nil || fi.IsDir() || !strings.HasSuffix(p, ".md") {
				return werr
			}
			read(p)
			return nil
		}); err != nil {
			t.Fatalf("walk %s: %v", root, err)
		}
		return
	}
	entries, err := os.ReadDir(root)
	if err != nil {
		t.Fatalf("read dir %s: %v", root, err)
	}
	for _, e := range entries {
		if e.IsDir() || !strings.HasSuffix(e.Name(), ".md") {
			continue
		}
		read(filepath.Join(root, e.Name()))
	}
}

func repoRootForDocs(t *testing.T) string {
	t.Helper()
	dir, err := os.Getwd()
	if err != nil {
		t.Fatalf("getwd: %v", err)
	}
	for i := 0; i < 6; i++ {
		if _, err := os.Stat(filepath.Join(dir, "go.mod")); err == nil {
			return dir
		}
		dir = filepath.Dir(dir)
	}
	t.Fatal("could not locate the repo root (no go.mod above the test's cwd)")
	return ""
}
