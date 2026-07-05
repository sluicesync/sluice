# Research: an AI agent-skills pack for sluice (operator-flagged 2026-07-05)

Status: **RESEARCH / PROPOSAL — not started.** Roadmap item 56.

Prompted by [`planetscale/skills`](https://github.com/planetscale/skills) — a repository of open agent-skills (`SKILL.md` files, the vendor-neutral convention that Claude Code, Cursor, and other agents load) that turn an AI into a database reviewer/operator. This doc records the concept, why sluice is a strong fit, a concrete skill catalog, the patterns worth borrowing, and a shipping/sequencing plan.

## What `planetscale/skills` actually is

13 numbered `SKILL.md` playbooks with a single spine: **read-only assessment → evidence-backed proposal → approval-gated execution.** Notable structure:

- Plain markdown, no plugins or proprietary tooling; sibling-relative path references; a setup script that installs into whichever agents are present.
- A governance layer: a **Class A–E permission model** (`change-gates-and-approval-contract`) and an explicit **autonomous-execution mode** that requires the operator to acknowledge scope + production-inclusion by name.
- Report/evidence templates so findings are measured, not asserted.
- Load-bearing rule throughout: **every change is verified by reading state back, not by trusting an exit code.**

The skills cover assessment (inventory, safety review, query insights), schema operations (reviewable branch+PR+deploy units), query optimization (SQLCommenter tagging, N+1/index fixes), automation (scheduled recommendation sweeps), and governance.

## Why sluice is a strong fit

1. **The spine is already sluice's identity.** "Read-back-to-verify," loud-failure, and approval-gated destructive ops are the project's tenets (correctness/trust gate throughput; validate end-to-end; contain-PG-complexity). A skills pack encodes those tenets as reusable operator playbooks instead of leaving them implicit in the docs.
2. **The substrate already shipped (v0.99.175).** `AGENTS.md`, `llms.txt`/`llms-full.txt`, machine-readable JSON envelopes on `migrate`/`sync`/`backup`/`restore`, `SLUICE-E-*` error codes with hints (`docs/operator/error-codes.md`), and the exit taxonomy (2 = config, 3 = refusal). Skills are the *next layer up*: task-scoped playbooks that consume that structured surface. Very little new product code is required — most skills orchestrate existing commands and interpret existing output.
3. **Differentiated framing.** PlanetScale's skills are "operate a database." sluice's would be "**migrate/sync without silently corrupting data**" — a pre-flight → execute → verify loop with loud-failure recovery baked in. That is exactly the promise the tool is built to keep, so the skills reinforce the product thesis rather than bolt onto it.

## Skill catalog (proposed)

Each skill is a `SKILL.md` with: trigger/when-to-use, inputs, the sluice commands it drives, how it interprets the structured output, the go/no-go or report it emits, and (for destructive legs) the approval gate. Ranked by fit-to-sluice-identity.

### Tier 1 — lean into what makes sluice sluice

1. **`migrate-preflight`** — the assessment analog. Given source + target DSNs, drive `migrate --dry-run` (JSON plan) plus the type-mapping report, capability declarations, and the RLS/ownership/keyless preflights, and emit a **go/no-go with risks named**: unsupported types, extension gaps, Postgres object-ownership (the `pscale_api_*` advisory), no-PK/keyless tables, verbatim-extension refusals. Read-only; produces an evidence-backed report, never mutates.
2. **`fidelity-verify`** — the trust core. After a migrate/sync/restore, run `verify`/reconcile and cross-check row counts + checksums against the `docs/value-types.md` contract, then produce a fidelity report. Encodes the Bug-74 "pin the class, not the representative" discipline as an operator-facing check (verify every value *family*, not one representative).
3. **`sluice-error-triage`** — the loud-failure companion. Map any `SLUICE-E-*` code + exit code (2 = config, 3 = refusal, kong = 80) to root cause + recovery, straight off `docs/operator/error-codes.md`. The "it failed loudly — now what" skill; this is where the loud-failure investment pays off for an agent.
4. **`backup-chain-operator`** — plan/operate encrypted backup chains: full → incrementals → compaction → prune → restore-test, **including the `--encrypt-mode` per-chunk/per-chain rules** (exactly the Bug 179 footgun — a skill steers around mismatched modes and always restore-tests). Borrow the Class-style approval gate for the destructive legs (`prune`, `restore --reset-target-data`, cutover).

### Tier 2 — high-value operational

5. **`cdc-sync-operator`** — set up + operate continuous sync: cold-start → CDC handoff, slot/position/resume semantics, reconcile, cutover; interpret slot-health, reparent signals, and watermarks.
6. **`planetscale-migration`** — the PlanetScale gotchas codified: `--source-driver planetscale` is required for VStream (the README trap corrected in v0.99.169), storage-grow/reparent handling, the `pscale_api_*` ownership advisory, `metrics-watch`, and the Vitess volume floors.
7. **`fleet-operator`** — operate `sync run` fleets (multi-DB) with per-sync options and the JSON fleet envelope.

## Patterns worth borrowing

- **Read-back-to-verify** — already a tenet; make it explicit in every skill's success criterion (assert observed target state, never trust exit 0 alone).
- **Approval gates for destructive ops** — sluice already refuses loudly; a Class-style gate turns those refusals into a first-class operator contract (name the scope + whether production is included before executing `--reset-target-data`/`prune`/cutover).
- **Evidence-backed reports** — the JSON envelopes are already the evidence substrate; a report template standardizes findings.
- **Autonomous-execution mode** — mirrors the project's own autonomous-release authorization; gate it behind an explicit risk acknowledgment naming scope.

## How to ship

- A top-level `skills/` directory in-repo, following the open agent-skills convention (plain `SKILL.md` + sibling-relative references), so any agent (Claude Code, Cursor) operating sluice picks them up. This is the natural continuation of the v0.99.175 AI-friendly investment and keeps everything in text (nothing agent-specific), consistent with `llms.txt`/`AGENTS.md`.
- A small setup script (the pattern `planetscale/skills` uses) that detects which agents are present and installs the `SKILL.md` files into each one's skills location.
- Cross-link from `AGENTS.md` and the site nav so the skills are discoverable alongside `llms.txt`.

### Repo location — in-repo vs. a sibling `sluicesync/skills` repo (recommendation: in-repo)

Both are viable; the decision turns on drift risk versus iteration independence.

- **A sibling repo** (`planetscale/skills`'s choice) buys independent versioning, a clean install surface, and a natural home if skills ever span multiple sluicesync tools or attract external contributors. Its cost is **drift**: a skill that names a flag/subcommand lives in a *different* repo from the CLI it drives, so a flag rename or a new `SLUICE-E-*` code silently breaks a skill until someone notices — and it forces a "which skills version works with which sluice version?" matrix onto users.
- **In-repo** (`sluice/skills/`) versions the skills *with* the binary they drive. That is the load-bearing advantage for sluice specifically, because it enables a **CI doc-sync guard** — the same mechanism that already generates `llms.txt` from NAV and the bidirectional error-codes test — so a renamed flag or a removed subcommand *fails a test* instead of a user's session. For a tool whose whole thesis is correctness and loud failure, a skill that tells the agent to run a flag that no longer exists is exactly the first-impression failure to avoid. The usual "in-repo couples release cadence" objection is weak here: skills are text and ship as ordinary docs-only commits, independent of binary releases.

**Recommendation: start in-repo** for the CI coupling. Revisit a split to `sluicesync/skills` only on a concrete trigger — the skills grow to cover multiple sluicesync tools, or an external community forms that wants to contribute skills without touching core. Those are post-1.0 "if it grows" conditions, not day-one concerns.

## Getting started (user onboarding, once implemented)

The flow mirrors the open agent-skills convention on top of the surface sluice already ships, so onboarding is light:

1. **Prerequisites the user already has** — the `sluice` binary (`brew install sluicesync/tap/sluice`, `go install …`, or the GHCR container) and an agent that reads skills (Claude Code, Cursor, or anything following the convention). Skills are plain markdown, so nothing is vendor-locked.
2. **Get the skills** — pull the `skills/` directory (in-repo per the recommendation above); being markdown, a clone/download is enough.
3. **Install into their agent** — run the setup script, which detects the present agents and drops each `SKILL.md` into the right place — for Claude Code that is `~/.claude/skills/<name>/` (personal) or `.claude/skills/<name>/` (checked into a project); Cursor and others have equivalents.
4. **Invoke** — the user describes the task in natural language; each skill's `when-to-use` trigger auto-loads the relevant one (e.g. "migrate this Postgres DB to PlanetScale" → `migrate-preflight`; "why did this restore fail?" → `sluice-error-triage`), or they invoke it explicitly (`/migrate-preflight`). The skill then drives the `sluice` CLI on the user's behalf — running `migrate --dry-run` and reading the JSON plan, mapping `SLUICE-E-*` codes + the exit taxonomy, checking fidelity against the value contract — and returns a go/no-go with risks named or an evidence-backed report. Read-only skills run freely; destructive legs (`--reset-target-data`, `prune`, cutover) hit the approval gate first.

Ship a short **"Getting started with sluice skills"** page in the docs/site (cross-linked from `AGENTS.md`) as the canonical entry point, and land the `migrate-preflight` + `sluice-error-triage` pair first so there is something concrete to install and try on day one.

## Sequencing

Start with **`migrate-preflight` + `sluice-error-triage`** — the two cheapest and most self-contained, both riding surface that already exists (dry-run JSON plan + error-codes doc), no new product code. Ship them as a proof-of-concept, validate the format against a real migration, then add `fidelity-verify` and `backup-chain-operator` (Tier 1), then Tier 2 on demand.

## Open questions

1. **Scope for 1.0** — ship the two-skill PoC as a demo of the format, or the full Tier-1 set? (Recommendation: PoC first, judge the shape.)
2. **Governance depth** — adopt PlanetScale's full Class A–E model, or a lighter "destructive vs read-only" two-tier gate that matches sluice's existing loud-refusal boundary? (Lean two-tier initially.)
3. **Maintenance coupling** — a skill that names flags/commands can drift from the CLI. Consider a lightweight doc-sync guard (the pattern already used for `llms.txt` generation from NAV and the error-codes bidirectional test) so a renamed flag fails a test rather than a user's session.
4. **Repo location (in-repo vs. sibling `sluicesync/skills`)** — analyzed under "Repo location" above; recommendation is **in-repo** for the CI doc-sync coupling, revisiting only on a concrete "if it grows" trigger. Left here as the one call worth an explicit owner sign-off before implementation.
