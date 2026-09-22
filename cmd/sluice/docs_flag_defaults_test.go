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

// A tuning guide that states the wrong default is worse than one that
// states none: the operator hand-picks a value the tool already adapts, or
// sizes a budget around a number the binary never used.
//
// docs/throughput-tuning.md said `--apply-batch-size` "Default: `1`" for
// ~65 releases after ADR-0089 flipped it to `auto` — and the repo carried
// TestApplyBatchSizeDefaultIsAuto the whole time, a test written
// specifically so that default could never silently regress. It pinned the
// code; nothing pinned the sentence (gap census 2026-09-22, GC-17). The same
// page called `--analyze-after` migrate-only while the copy-phase parity gate
// required it on `sync start`.
//
// This gate holds every "default `X`" the tuning guide states to kong's own
// `default:` tag, derived by walking the model — the same source the flag
// manifest and the command-name gate use.
//
// # The grammar it reads
//
// Within one section (heading to heading), the most recently named backticked
// flag owns every "default `VALUE`" statement that follows it:
//
//	`--bulk-parallelism` (default `0` = auto: min(8, NumCPU))
//	Default: `auto` on `sync start` … `sync from-backup run` defaults to `100`
//
// The phrase may be "default", "Default:", "defaults to" or "(default"; the
// value is the first backticked token after it. Prose that explains what an
// auto sentinel RESOLVES to stays free-form — only the literal the binary
// accepts is graded, which is also the one an operator can paste.
//
// When a flag's default differs between the commands that carry it (as
// `--apply-batch-size` does: `auto` on `sync start`, `100` on `sync
// from-backup run`), the statement must name the command in a backticked
// path near the value, and is graded against that command only. Otherwise it
// is graded against every command carrying the flag.
//
// A kong bool with no `default:` tag is spelled `off` in the doc, since that
// is what the help text says and what an operator reads.
//
// # Scope, stated so the name cannot be read as broader than the truth
//
// One file. The grammar is deliberately narrow — a wider sweep would have to
// grade "default" in prose about MySQL's `sql_mode` and PG's GUCs, which is
// exactly the exemption-list shape this project keeps paying for. If a second
// doc grows a defaults table, add it to docFlagDefaultFiles and it joins.

var docFlagDefaultFiles = []string{"docs/throughput-tuning.md"}

var (
	docFlagTokenRe    = regexp.MustCompile("`--([a-z][a-z0-9-]*)`")
	docDefaultStmtRe  = regexp.MustCompile("(?i)\\bdefaults?(?: to|:)? `([^`\n]+)`")
	docCmdQualifierRe = regexp.MustCompile("`([a-z][a-z0-9-]*(?: [a-z][a-z0-9-]*){0,2})`")
	docHeadingRe      = regexp.MustCompile(`(?m)^#{1,6} `)
)

// flagDefault is one kong flag's default on one command, normalised to the
// spelling the doc uses.
type flagDefault struct {
	command string
	value   string
}

func TestThroughputTuningDefaultsMatchKong(t *testing.T) {
	repo := repoRootForDocs(t)
	commands, _, _ := cliSurface(t)
	defaults := kongFlagDefaults(t)

	type pair struct {
		file, flag, value, command string
	}
	var pairs []pair
	for _, rel := range docFlagDefaultFiles {
		raw, err := os.ReadFile(filepath.Join(repo, filepath.FromSlash(rel)))
		if err != nil {
			t.Fatalf("read %s: %v", rel, err)
		}
		for _, section := range docHeadingRe.Split(string(raw), -1) {
			// Walk flag tokens and default statements in document order.
			// A default statement pairs with the last flag token before it.
			flagLocs := docFlagTokenRe.FindAllStringSubmatchIndex(section, -1)
			for _, m := range docDefaultStmtRe.FindAllStringSubmatchIndex(section, -1) {
				stmtStart, valStart, valEnd := m[0], m[2], m[3]
				owner := ""
				ownerEnd := 0
				for _, f := range flagLocs {
					if f[0] < stmtStart {
						owner = section[f[2]:f[3]]
						ownerEnd = f[1]
					}
				}
				if owner == "" {
					continue
				}
				// A command qualifier is a backticked command path between
				// the flag and the value ("`sync from-backup run` defaults
				// to `100`"), or an "on `cmd`" immediately after the value
				// ("Default: `auto` on `sync start`"). A command merely
				// LISTED later in the sentence is not a qualifier.
				command := ""
				for _, q := range docCmdQualifierRe.FindAllStringSubmatch(section[ownerEnd:valStart], -1) {
					if commands[q[1]] {
						command = q[1]
					}
				}
				after := section[valEnd:min(len(section), valEnd+48)]
				if rest, ok := strings.CutPrefix(strings.TrimLeft(after, "` "), "on "); ok {
					if q := docCmdQualifierRe.FindStringSubmatch(rest); q != nil && strings.HasPrefix(rest, "`") && commands[q[1]] {
						command = q[1]
					}
				}
				pairs = append(pairs, pair{rel, owner, strings.TrimSpace(section[valStart:valEnd]), command})
			}
		}
	}

	// Anti-vacuity floor. The guide states eleven defaults today; a grammar
	// that stopped matching, or a file that moved, would pass on nothing.
	if len(pairs) < 8 {
		t.Fatalf("only %d flag/default pairs extracted from %v (floor 8); the grammar stopped matching or the doc lost its defaults", len(pairs), docFlagDefaultFiles)
	}
	for _, p := range pairs {
		t.Logf("graded --%s default `%s` (command %q)", p.flag, p.value, p.command)
	}

	for _, p := range pairs {
		have := defaults[p.flag]
		if len(have) == 0 {
			t.Errorf("%s states a default for --%s, which is not a flag on any command", p.file, p.flag)
			continue
		}
		if p.command != "" {
			found := false
			for _, d := range have {
				if d.command != p.command {
					continue
				}
				found = true
				if d.value != p.value {
					t.Errorf("%s says --%s defaults to `%s` on `sluice %s`; kong's default there is `%s`", p.file, p.flag, p.value, p.command, d.value)
				}
			}
			if !found {
				t.Errorf("%s qualifies --%s's default with `%s`, which does not carry that flag", p.file, p.flag, p.command)
			}
			continue
		}
		distinct := map[string][]string{}
		for _, d := range have {
			distinct[d.value] = append(distinct[d.value], d.command)
		}
		if len(distinct) > 1 {
			t.Errorf("%s says --%s defaults to `%s`, but the default differs by command (%s); name the command next to the value", p.file, p.flag, p.value, renderDefaults(distinct))
			continue
		}
		if _, ok := distinct[p.value]; !ok {
			t.Errorf("%s says --%s defaults to `%s`; kong's default on %s is `%s`", p.file, p.flag, p.value, strings.Join(sortedCommands(have), ", "), have[0].value)
		}
	}
}

// kongFlagDefaults walks the model and returns, per long flag name, its
// default on every command that carries it. A flag declared on a parent node
// is inherited by its children, matching how kong resolves it.
func kongFlagDefaults(t *testing.T) map[string][]flagDefault {
	t.Helper()
	parser, err := kong.New(&CLI{}, kong.Name("sluice"), kong.Vars{"version": "test"}, kong.Exit(func(int) {}))
	if err != nil {
		t.Fatalf("build kong model: %v", err)
	}
	out := map[string][]flagDefault{}
	var walk func(node *kong.Node, path string, inherited []*kong.Flag)
	walk = func(node *kong.Node, path string, inherited []*kong.Flag) {
		own := append(append([]*kong.Flag(nil), inherited...), node.Flags...)
		if path != "" {
			for _, f := range own {
				if f == nil || f.Name == "" {
					continue
				}
				out[f.Name] = append(out[f.Name], flagDefault{command: path, value: docDefaultSpelling(f)})
			}
		}
		for _, child := range node.Children {
			if child == nil || child.Type != kong.CommandNode {
				continue
			}
			walk(child, strings.TrimSpace(path+" "+child.Name), own)
		}
	}
	walk(parser.Model.Node, "", nil)
	return out
}

// docDefaultSpelling is the doc's spelling of a kong default: the tag's
// literal, or `off` for a bool that has none (a bool's zero value).
func docDefaultSpelling(f *kong.Flag) string {
	if f.Default != "" {
		return f.Default
	}
	if f.IsBool() {
		return "off"
	}
	return ""
}

func renderDefaults(distinct map[string][]string) string {
	values := make([]string, 0, len(distinct))
	for v := range distinct {
		values = append(values, v)
	}
	sort.Strings(values)
	parts := make([]string, 0, len(values))
	for _, v := range values {
		cmds := distinct[v]
		sort.Strings(cmds)
		parts = append(parts, "`"+v+"` on "+strings.Join(cmds, "/"))
	}
	return strings.Join(parts, "; ")
}

func sortedCommands(ds []flagDefault) []string {
	out := make([]string, 0, len(ds))
	for _, d := range ds {
		out = append(out, "`sluice "+d.command+"`")
	}
	sort.Strings(out)
	return out
}
