// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package agentguide

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// TestEmbeddedGuideMatchesRoot is the drift gate: the embedded copy
// (internal/agentguide/AGENTS.md, which go:embed requires in-package) must be
// byte-identical to the canonical repo-root AGENTS.md. The root file is the
// single source of truth (GitHub + agent convention look for it there); this
// test fails the build if the copy is stale, so the two cannot silently
// diverge. `go test` runs with cwd = this package dir, so the root is two up.
func TestEmbeddedGuideMatchesRoot(t *testing.T) {
	rootPath := filepath.Join("..", "..", "AGENTS.md")
	root, err := os.ReadFile(rootPath)
	if err != nil {
		t.Fatalf("read root AGENTS.md at %s: %v", rootPath, err)
	}
	if string(root) != guide {
		t.Fatalf("the embedded internal/agentguide/AGENTS.md has DRIFTED from the root AGENTS.md.\n"+
			"The root is canonical — re-copy it: `cp AGENTS.md internal/agentguide/AGENTS.md`.\n"+
			"(root %d bytes, embedded %d bytes)", len(root), len(guide))
	}
}

// TestGuideNonEmpty guards against a vacuous embed (an empty or missing file
// embedding as "") so a green build can never ship a `--skill` that emits
// nothing — the anti-vacuity floor.
func TestGuideNonEmpty(t *testing.T) {
	if len(strings.TrimSpace(guide)) < 200 {
		t.Fatalf("embedded guide is %d bytes after trim — the embed is empty or truncated, not the real AGENTS.md", len(strings.TrimSpace(guide)))
	}
	if !strings.Contains(guide, "# AGENTS.md") {
		t.Error("embedded guide does not look like AGENTS.md (missing its H1)")
	}
}

// TestSkillFrontmatter pins the installable-skill-file shape: valid opening
// YAML frontmatter carrying name + description, a closing `---`, then the full
// guide. An agent parses the frontmatter to decide trigger-based loading, so a
// malformed header would make the skill silently un-loadable.
func TestSkillFrontmatter(t *testing.T) {
	s := Skill()
	if !strings.HasPrefix(s, "---\n") {
		t.Fatalf("skill file must open with YAML frontmatter `---`; got %.20q", s)
	}
	// Frontmatter is the block between the first `---` and the closing `---`.
	rest := strings.TrimPrefix(s, "---\n")
	end := strings.Index(rest, "\n---\n")
	if end < 0 {
		t.Fatal("skill file frontmatter is not closed by a `---` line")
	}
	front := rest[:end]
	if !strings.Contains(front, "name: "+skillName) {
		t.Errorf("frontmatter missing `name: %s`; got:\n%s", skillName, front)
	}
	if !strings.Contains(front, "description: ") {
		t.Errorf("frontmatter missing a `description:` line; got:\n%s", front)
	}
	// The body after the frontmatter must be the guide, unmodified.
	body := rest[end+len("\n---\n"):]
	if !strings.Contains(body, Guide()) {
		t.Error("skill file body does not contain the full guide verbatim")
	}
}
