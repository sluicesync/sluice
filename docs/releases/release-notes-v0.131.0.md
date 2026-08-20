# sluice v0.131.0

**A small feature release.** sluice can now hand an AI agent its own operating guide as an installable skill file, so an agent can cold-start on how to drive sluice — without the repository or the docs site. Plus a CI-hygiene guard and a documented design decision. **Drop-in from v0.130.2 — no schema, format, or existing-flag change; the new command and flag are purely additive.**

## Added

**`sluice --skill` and `sluice agent-guide` — the built-in agent guide, as an installable skill file.** sluice ships an operator-facing `AGENTS.md`: the command taxonomy (read-only vs state-changing vs production-mutating vs destructive), the standard migrate/sync/verify workflow, and the flags that require explicit human approval. It is now embedded in the binary and printable two ways:

- `sluice agent-guide` prints the bare guide.
- `sluice --skill` (a global flag) prints it as an **installable agent skill file** — YAML frontmatter (`name` + `description`, for trigger-based loading) followed by the full guide. Write that into a skills directory and a skill-aware assistant — [Claude Code](https://www.anthropic.com/claude-code), Cursor, or anything following the open agent-skills convention — can drive the `sluice` CLI without needing the repository or the docs site.

`sluice agent-guide --skill` is identical to `sluice --skill`. The output is a raw dump, deliberately not routed through any `--format` envelope, so it is always the file itself. The embedded copy is byte-identical to the repo's `AGENTS.md`, held to it by a build-time gate so the two cannot drift.

This sits *on top of* the task-scoped agent skills sluice already ships under `skills/` — the guide is the cold-start map; the skills are the per-task playbooks.

## Fixed

**CI hygiene: staticcheck's SA5011 no longer risks an intermittent false failure in test files.** staticcheck does not model `t.Fatal` as terminating, so the standard `if x == nil { t.Fatal(...) }` guard-then-dereference pattern that fills the test suite can look like a possible nil dereference — an intermittent, golangci-version-dependent lint failure other Go projects have hit. This is a preventive exclusion scoped to SA5011 in `_test.go` files only; the check stays fully live in non-test code.

## Compatibility

Drop-in from v0.130.2 — no schema, format, or error-code change, and no change to any existing flag or command. The new `agent-guide` command and `--skill` flag are purely additive and opt-in; a migrate or sync that worked before is byte-identical after. Internally, the backup path's deliberate exemption from the TINYINT(1) fail-fast preflight is now recorded at the code site — backup capture is still fully guarded by the per-read-path decode floor; the exemption avoids charging a scheduled backup the per-table probe cost on every run.

**Who needs this:** anyone driving sluice from an AI coding agent — `sluice --skill` gives that agent a cold-start playbook. Everyone else: no action.

## Install

```
# Homebrew
brew upgrade sluice

# Scoop (Windows)
scoop update sluice

# Go
go install sluicesync.dev/sluice/cmd/sluice@v0.131.0
```

Container images: `ghcr.io/sluicesync/sluice:0.131.0` (multi-arch; the image tag carries no `v` prefix).
