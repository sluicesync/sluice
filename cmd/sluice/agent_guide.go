// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package main

import (
	"fmt"

	"github.com/alecthomas/kong"

	"sluicesync.dev/sluice/internal/agentguide"
)

// skillFlag is the root `--skill` flag. Like kong's helpFlag / VersionFlag it
// prints and exits DURING PARSING (BeforeReset), so `sluice --skill` works with
// no subcommand — the CLI's `cmd:""` fields would otherwise demand one.
// Printing here, before command resolution and any per-command --format
// envelope, is also what keeps the raw skill file from being swallowed by a
// format-aware printer (the planetscale/cli #1365 empty-`--format json` trap):
// the command, and its envelope, never run. IgnoreDefault (mirroring helpFlag)
// keeps the hook from firing on invocations that do not pass --skill.
type skillFlag bool

func (skillFlag) IgnoreDefault() {}

func (skillFlag) BeforeReset(ctx *kong.Context) error {
	fmt.Fprint(ctx.Stdout, agentguide.Skill())
	ctx.Exit(0)
	return nil
}

// AgentGuideCmd prints sluice's operator-facing AI-agent guide (the embedded
// AGENTS.md) as a RAW dump. It carries NO --skill field of its own: `--skill`
// is the root global flag above, which kong inherits into every subcommand, so
// `sluice agent-guide --skill` short-circuits into skillFlag.BeforeReset (the
// installable skill file) before this Run is reached — byte-identical to
// `sluice --skill`. Bare `sluice agent-guide` reaches here and prints the guide
// without frontmatter.
type AgentGuideCmd struct{}

// Run prints the bare guide as a raw dump — deliberately not through any output
// envelope, so the result is always the file itself.
func (*AgentGuideCmd) Run() error {
	fmt.Print(agentguide.Guide())
	return nil
}
