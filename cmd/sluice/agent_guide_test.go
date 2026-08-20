// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package main

import (
	"bytes"
	"strings"
	"testing"

	"github.com/alecthomas/kong"
)

// TestSkillAndAgentGuideWiredIntoKong pins the CLI surface through the REAL
// kong model (the Bug-180 through-the-parser discipline): the root `--skill`
// global flag and the `agent-guide` command must both exist, or `sluice
// --skill` / `sluice agent-guide` silently stop working.
func TestSkillAndAgentGuideWiredIntoKong(t *testing.T) {
	var cli CLI
	parser, err := kong.New(&cli, kong.Vars{"version": "test"}, kong.Exit(func(int) {}))
	if err != nil {
		t.Fatalf("kong.New: %v", err)
	}
	root := parser.Model.Node

	hasSkill := false
	for _, f := range root.Flags {
		if f.Name == "skill" {
			hasSkill = true
		}
	}
	if !hasSkill {
		t.Error("root --skill flag is not wired into the kong model")
	}

	hasAgentGuide := false
	for _, c := range root.Children {
		if c.Name == "agent-guide" {
			hasAgentGuide = true
		}
	}
	if !hasAgentGuide {
		t.Error("agent-guide command is not wired into the kong model")
	}
}

// exitSentinel unwinds parsing when the skill flag calls kong.Exit in a test
// (BeforeReset prints then Exit(0)s; in production Exit terminates the process,
// so the panic stands in for that so parsing does not fall through to
// "expected a command").
type exitSentinel struct{}

// TestSkillFlag_PrintsSkillFileAndExitsZero drives `--skill` through the parser
// and asserts it emits the installable skill file (frontmatter first) to the
// kong Stdout and exits 0 — WITHOUT requiring a subcommand, and without routing
// through any per-command output envelope (the planetscale/cli #1365 trap).
func TestSkillFlag_PrintsSkillFileAndExitsZero(t *testing.T) {
	var out bytes.Buffer
	var cli CLI
	code := -1
	parser, err := kong.New(
		&cli,
		kong.Vars{"version": "test"},
		kong.Writers(&out, &out),
		kong.Exit(func(c int) { code = c; panic(exitSentinel{}) }),
	)
	if err != nil {
		t.Fatalf("kong.New: %v", err)
	}

	func() {
		defer func() {
			if r := recover(); r != nil {
				if _, ok := r.(exitSentinel); !ok {
					panic(r)
				}
			}
		}()
		_, _ = parser.Parse([]string{"--skill"})
	}()

	if code != 0 {
		t.Errorf("--skill exit code = %d; want 0", code)
	}
	got := out.String()
	if !strings.HasPrefix(got, "---\nname: sluice\n") {
		t.Errorf("--skill did not emit the skill file frontmatter first; got %.50q", got)
	}
	if !strings.Contains(got, "# AGENTS.md") {
		t.Error("--skill output does not contain the embedded guide body")
	}
}
