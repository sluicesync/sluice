# Research: how "AI-friendly" is sluice, and what would close the gap?

Date: 2026-07-03 · Trigger: [planetscale/cli#1280](https://github.com/planetscale/cli/pull/1280) ("make pscale agent-friendly") · Method: repo audit of sluice's CLI surface at `ba26c833` + review of the PR's mechanisms + the 2026 convention landscape (AGENTS.md, llms.txt, MCP). Status: research only — recommendations at the end, nothing implemented.

## What "AI-friendly" concretely means in 2026

The PlanetScale PR is a useful reference implementation because it decomposes the buzzword into six mechanisms: (1) an `AGENTS.md` usage guide for agents driving the CLI (flag conventions, auth workflow, placeholder syntax); (2) `--format json` with **typed envelopes** carrying `status`, payload, and `next_steps` so an agent knows what to do after every command; (3) fully non-interactive flows (device-flow auth reported as JSON); (4) destructive-operation gating — dangerous SQL returns `status: action_required` until the agent retries with `--force` after human approval; (5) structured errors (`JSONReportedError`); (6) shared `agent_steps` builders so suggested next commands always have correct flag ordering.

Around that sit two repo/web conventions with real adoption: **AGENTS.md** (an open standard read natively by 30+ agent tools, adopted by 60k+ repositories, now stewarded under the Linux Foundation) and **llms.txt** (a structured markdown index of a website for assistants that can't usefully scrape HTML). A third, heavier option is shipping an **MCP server** so agents call the tool through typed tool-calls instead of shelling out.

## Scorecard: sluice today

| Dimension | Grade | Evidence |
|---|---|---|
| Non-interactive operation | **A** | Zero interactive prompts in the entire codebase (grep: no `bufio.NewReader(os.Stdin)`, no `Scanln`, no password prompts, no survey lib). Every flow is flag/env/config-driven end to end — agents never get stuck on a hidden prompt. PlanetScale had to *build* this; sluice has it by design. |
| Destructive-op safety | **A** | The dangerous operations (`--reset-target-data`, `--force-cold-start`, `--skip-*` overrides) are opt-in flags, never defaults, and the loud-failure tenet means ambiguous states refuse with a named error instead of proceeding — which is exactly the `action_required` → human-approves → retry-with-flag loop the PR built, expressed as CLI design instead of protocol. |
| Structured logs | **A−** | Global `--log-format json` (one JSON object per line, slog) ships and is now documented in the service guide. Gap: log events carry no stable machine-parsable `code` field — an agent can read the text but can't reliably branch on error class. |
| Machine-readable command output | **B** | Seven read/verify-side subcommands have `--format json` (`verify`, `diff`, `preview`, `cutover`, `matview`, `sync-health`, `status`-family — `cmd/sluice/{verify,diff,preview,cutover,matview,sync_health}.go` + `cli.go:1721`). **But the primary verbs — `migrate`, `sync run`, `backup`, `restore` — emit no JSON result envelope**, and `--dry-run` prints a human text plan only. An agent driving the *main* workflow gets structured data everywhere except the step that matters most. |
| Error actionability | **B+ (text) / C (structured)** | sluice already produces exactly the right *content* — the operator-hint system (e.g. errno-3024 → "use `--upfront-indexes` or `--resume`") is `next_steps` in all but name, and refusals name the remedy (`--enable-pg-extension hstore`, `--type-override COL=bytea`). But it's all prose: no stable error codes, no structured `hint` field, so an agent must regex the message. |
| Auth / secrets ergonomics | **A−** | Every credential has an env-var path (`SLUICE_SOURCE/TARGET`, `CLOUDFLARE_API_TOKEN`, `PLANETSCALE_METRICS_*`, SMTP password), no interactive auth exists at all, and crash-bundle redaction now covers the credential flags. Nothing to build here; just document the env-first convention for agents. |
| Agent-facing docs | **D** | No `AGENTS.md`. `CLAUDE.md` is contributor-facing (how to develop sluice), not operator-agent-facing (how to *drive* sluice). The raw material is excellent — all docs are in-repo markdown with task-shaped cookbook recipes — but no file tells an agent the command taxonomy, the safe/destructive split, the env-auth convention, or the dry-run-first workflow. |
| Exit codes | **C** | Effectively 0/1 (`cmd/sluice/main.go`). Fine for humans; agents (and systemd) benefit from a small documented taxonomy (config error vs source-refusal vs target-refusal vs mid-copy failure vs verify-mismatch) — the service guide already gestures at exit-code-driven restart without a table to point at. |
| Web discoverability (llms.txt) | **unknown / likely absent** | Lives in the sluicesync.com website repo, not here. The docs being markdown makes generating `llms.txt` + `llms-full.txt` nearly free. |
| MCP server | **absent (by choice, so far)** | No MCP surface. For a batch migration tool this is genuinely lower priority than for a query CLI like pscale — the long-running verbs don't fit tool-call semantics well — but the *read* surface (`status`, `sync-health`, `verify`, metrics) would map cleanly if demand appears. |

**Overall: B.** The bones are unusually good — sluice is accidentally most of the way to agent-friendly because the loud-failure tenet, zero-prompt design, and env-first credentials are the hard parts, and they're done. What's missing is the thin, cheap presentation layer: an AGENTS.md, JSON envelopes on the four primary verbs, and machine-parsable error codes.

## Recommendations, in order

1. **`AGENTS.md` at the repo root (S).** Sections: what sluice is in one paragraph; the command taxonomy split into *read-only* (`preview`, `diff`, `verify`, `sync-health`, `status`, `--dry-run` anything) vs *state-changing* (`migrate`, `sync run`, `backup`, `restore`, `cutover`) vs *destructive-flag-gated* (`--reset-target-data`, `--force-cold-start`) with an explicit "never pass these without human approval" note; env-first credential convention with the full env-var table; the dry-run-first workflow (always `--dry-run`/`preview` before `migrate`; always `verify` after); the `--format json` / `--log-format json` map; exit-code meanings. This is a docs file — highest value-per-hour on the list, and the 60k-repo convention means every major agent reads it automatically.
2. **JSON result envelopes on the primary verbs (M).** `--format json` on `migrate`/`sync run`/`backup`/`restore` emitting one terminal object: `status` (`completed`/`refused`/`failed`), per-table rows/bytes/duration, warnings emitted, resume position/token, and `next_steps` (e.g. after `migrate`: "run `sluice verify --source … --target …`"). Follow the seven existing `Format` fields' pattern; render at the same code point that prints today's human summary.
3. **JSON `--dry-run` plan (M, possibly folded into #2).** The plan is the natural human/agent approval artifact — the agent shows the JSON plan, the human approves, the agent proceeds. This is sluice's equivalent of the PR's `action_required` loop and the single most agent-shaped feature on the list.
4. **Stable error codes + structured hints (M).** Give the operator-hint system an ID space (`SLUICE-E-3024-INDEX-BUILD` style), emit `code` + `hint` fields on the JSON log records and in the #2 envelopes. The content already exists; this is plumbing, not invention.
5. **`llms.txt` + `llms-full.txt` on sluicesync.com (S, website repo).** Generate from the docs tree; near-zero cost given markdown sources.
6. **Exit-code taxonomy (S).** Small documented table; wire the 3–5 distinguishable classes in `main.go`; reference it from AGENTS.md and the service guide.
7. **MCP server (research-tier, demand-gated).** Revisit if/when users ask to wire sluice into agent stacks; scope to the read-only surface first (`status`, `sync-health`, `verify`, metrics snapshot). Do not build ahead of demand — the CLI-with-JSON path above serves agents fine.

Items 1–4 + 6 are roughly one focused chunk each; together they'd move the scorecard to a defensible A and make "works great with agents" an honest launch-page claim — which, given that AI agents increasingly *perform* migrations, is a real differentiator for the promotion push, not a checkbox.

## Sources

- [planetscale/cli#1280 — the reference agent-friendly CLI PR](https://github.com/planetscale/cli/pull/1280)
- [AGENTS.md — the open standard](https://agents.md/)
- [AGENTS.md spec guide (2026): recommended sections, adoption status](https://www.morphllm.com/agents-md-guide)
- [llms.txt / markdown-first content architecture](https://www.digitalapplied.com/blog/markdown-first-content-architecture-llms-txt-spec)
- [AGENTS.md patterns: what actually changes agent behavior](https://blakecrosley.com/blog/agents-md-patterns)
