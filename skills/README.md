# sluice skills

Task-scoped operator playbooks for driving [sluice](https://sluicesync.com) — the database migration and continuous-sync CLI — from an AI agent (Claude Code, Cursor, or anything following the [open agent-skills convention](https://sluicesync.com/llms.txt)). Each skill is a plain `SKILL.md`: no plugins, nothing agent-specific.

These sit on top of the machine-readable surface sluice already ships (`AGENTS.md`, `llms.txt`, `--format json` envelopes, `SLUICE-E-*` error codes, the exit taxonomy). A skill does not re-document the CLI — it references those canonical sources and encodes the *decision tree* for one task: what to run, how to read the result, what to report, and where the human must approve.

## Catalog

**Tier 1 — the core loop**

| Skill | Use it to | Writes? |
|---|---|---|
| `migrate-preflight` | Assess a migrate/sync before running it → go/no-go with risks named | read-only |
| `fidelity-verify` | Confirm a completed migrate/sync/restore is faithful → fidelity report | read-only |
| `sluice-error-triage` | Turn a `SLUICE-E-*` code + exit code into root cause + recovery | read-only |
| `backup-chain-operator` | Plan/operate an encrypted backup chain (full → incr → compact → prune → restore-test) | gated |

**Tier 2 — operational + engine-specific**

| Skill | Use it to | Writes? |
|---|---|---|
| `cdc-sync-operator` | Stand up + operate continuous sync (cold-start → CDC → cutover) | gated |
| `planetscale-migration` | Migrate/sync against PlanetScale/Vitess (VStream, reparent, ownership, metrics-watch) | gated |
| `fleet-operator` | Operate a `sync run` fleet (many syncs, one process) | gated |
| `redaction-setup` | Configure + verify PII redaction during migrate/sync | gated |
| `sqlite-d1-import` | Import SQLite / Cloudflare D1 (`--stage-local`, `--infer-types`, big-int/CPU gotchas) | gated |

## The safety model (every skill honors it)

sluice's command taxonomy (see `AGENTS.md`) is the gate:

- **Read-only** commands (`--dry-run`, `verify`, `schema preview`/`diff`, `sync health`/`status`, `backup verify`, `engines`) run freely.
- **State-changing** commands (`migrate`, `sync start`/`run`, `backup *`, `restore`, `cutover`, …) run only as part of an approved task.
- **Destructive flags** (`--reset-target-data`, `--force-cold-start`, `--yes`, `backup prune`/`compact` without `--dry-run`) and `--accept-unforwarded-schema-change` are **NEVER** passed without explicit human approval for *that specific invocation*. The acknowledgement flag accepts a source/target schema difference permanently unless the change was applied to the target first.

Every skill also follows sluice's own discipline: **verify by reading state back, never trust an exit code alone**, and treat `status:"refused"` / exit 3 as a decision point — surface `error.hint` and wait, don't retry unchanged. The same applies to an exit-1 `UNFORWARDED-SCHEMA-CHANGE`, which has no error code yet.

## Getting started

**Prerequisites** — the `sluice` binary (`brew install sluicesync/tap/sluice`, `go install sluicesync.dev/sluice/cmd/sluice@latest`, or the `ghcr.io/sluicesync/sluice` container) and an agent that reads skills.

**Install** — run the setup script; it detects the agents present and installs each `SKILL.md` into the right place:

```sh
./skills/install.sh
```

For Claude Code that is `~/.claude/skills/<name>/SKILL.md` (personal, all projects) or `.claude/skills/<name>/SKILL.md` (checked into a project); Cursor and others have equivalents. Because the skills are markdown, you can also copy the directories by hand.

**Use** — describe the task in natural language; the matching skill's `description` trigger loads it automatically ("migrate this Postgres DB to PlanetScale" → `migrate-preflight`; "why did this restore fail?" → `sluice-error-triage`), or invoke it explicitly (`/migrate-preflight`). The skill then drives the `sluice` CLI on your behalf and returns a go/no-go, a report, or a gated action.

## Canonical surface (skills point here, don't duplicate)

- `AGENTS.md` — command taxonomy, the standard workflow, the JSON envelope shape, env-first credentials.
- `docs/operator/error-codes.md` — the full `SLUICE-E-*` table + exit codes.
- `https://sluicesync.com/llms.txt` (and `/llms-full.txt`) — the docs index for assistants.
- `sluice <command> --help` — the primary, always-accurate flag documentation.

Skills are versioned in-repo with the CLI they drive, so a renamed flag is caught by the doc-sync guard (CI) rather than in a user's session.
