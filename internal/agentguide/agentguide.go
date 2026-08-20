// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

// Package agentguide embeds sluice's operator-facing AI-agent guide
// (AGENTS.md) into the binary and renders it as an installable agent "skill"
// file. It exists so `sluice --skill` (and `sluice agent-guide --skill`) can
// cold-start an agent with structured guidance — drop the output into a skills
// directory and get trigger-based loading — without the agent needing the repo
// or the docs site (the planetscale/cli #1365 pattern).
//
// The embedded AGENTS.md is a COPY of the repo-root AGENTS.md (go:embed cannot
// reach a file outside the package directory). TestEmbeddedGuideMatchesRoot is
// the drift gate: it fails the build if the copy and the canonical root file
// diverge, so the single source of truth stays the root AGENTS.md.
package agentguide

import (
	_ "embed"
	"strings"
)

//go:embed AGENTS.md
var guide string

// skillName is the skill's identifier (the `name:` frontmatter key). Kept
// short and stable — agents key trigger-based loading off name + description.
const skillName = "sluice"

// skillDescription is the `description:` frontmatter — the one line an agent
// matches against to decide whether to load the skill. It names the tool, its
// scope, and the safety axis that makes the guide worth loading before acting.
const skillDescription = "Drive sluice — the MySQL/Postgres/SQLite/Cloudflare-D1 database migration and continuous-sync CLI — safely as an AI agent: the read-only vs state-changing vs production-mutating vs destructive command taxonomy, the standard migrate/sync/verify workflow, and the flags that require explicit human approval."

// Guide returns the raw embedded agent guide (the AGENTS.md content), with a
// trailing newline guaranteed so a bare `sluice agent-guide` ends cleanly.
func Guide() string {
	if strings.HasSuffix(guide, "\n") {
		return guide
	}
	return guide + "\n"
}

// Skill returns the guide as an installable agent skill file: YAML frontmatter
// (name + description) followed by the full guide. This is the `--skill`
// output — a drop-in skill an agent installs for trigger-based loading.
func Skill() string {
	var b strings.Builder
	b.WriteString("---\n")
	b.WriteString("name: ")
	b.WriteString(skillName)
	b.WriteString("\n")
	b.WriteString("description: ")
	b.WriteString(skillDescription)
	b.WriteString("\n")
	b.WriteString("---\n\n")
	b.WriteString(Guide())
	return b.String()
}
