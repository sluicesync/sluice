# AGENTS.md — driving sluice as an AI agent

sluice is a single-binary CLI that migrates and continuously syncs databases (MySQL ↔ Postgres in all four directions, plus SQLite / Cloudflare D1 import and sync). It is built for you: **every command is non-interactive** (flags, environment variables, optional YAML config — there are no prompts, ever), destructive operations require explicit opt-in flags, and ambiguous states refuse loudly with a named remedy instead of proceeding. This file is the operating guide for agents *using* the CLI; contributor/development guidance lives in `CLAUDE.md` and `CONTRIBUTING.md`.

## Command taxonomy

**Read-only — safe to run without approval.** These never modify either database: `sluice schema preview`, `sluice schema diff`, `sluice verify`, `sluice sync health`, `sluice sync status`, `sluice engines`, `sluice backup verify`, and **any command with `--dry-run`**.

**State-changing — run only as part of an approved task.** These write to the target (and create bookkeeping objects — see `docs/database-objects` on the docs site): `sluice migrate`, `sluice sync start`, `sluice sync run`, `sluice backup *` (writes to the backup store), `sluice restore`, `sluice cutover`, `sluice schema add-table`, `sluice trigger setup/teardown/prune`.

**Destructive flags — NEVER pass without explicit human approval for that specific invocation.** Approval to migrate is not approval to destroy:

- `--reset-target-data` — drops/truncates target tables before copy
- `--force-cold-start` — bypasses the populated-target safety preflight
- `--yes` — suppresses the reset confirmation
- `backup prune` / `backup compact` without `--dry-run` — irreversibly drop backup history

## The standard workflow

1. **Preview first**: `sluice migrate --dry-run --format json ...` (or `sluice preview`) — emits the full plan as JSON. Show it to the human before proceeding.
2. **Run**: `sluice migrate --format json ...` — one JSON result envelope on stdout (see below).
3. **Verify**: `sluice verify --format json ...` — never report a migration done without it.
4. For continuous sync: `sync start --dry-run` → `sync start` → poll `sync health --format json` (exits 1 on breached thresholds — cron/agent-friendly).

## Credentials: env-first, never in argv

Pass DSNs and secrets via environment variables, not flags — flags leak into process listings and shell history (crash-bundle redaction masks them, but env is the documented path):

| Variable | Holds |
|---|---|
| `SLUICE_SOURCE` / `SLUICE_TARGET` | source / target DSN (equivalent to `--source` / `--target`) |
| `CLOUDFLARE_API_TOKEN` | Cloudflare D1 API token (env-only; there is no flag) |
| `PLANETSCALE_METRICS_TOKEN_ID` / `PLANETSCALE_METRICS_TOKEN` | PlanetScale telemetry token |
| `SLUICE_NOTIFY_WEBHOOK` / `SLUICE_NOTIFY_SLACK` | notification sink URLs (the URL path is the credential) |
| `SLUICE_NOTIFY_SMTP_PASSWORD` | SMTP auth secret (env-only by policy) |
| `--encryption-passphrase-env NAME` | names the env var holding the backup passphrase |

## Machine-readable output

- **`--format json`** on the primary verbs — `migrate`, `sync start`, `backup full`, `restore` — emits exactly one JSON result envelope as the last write to stdout on every exit path: `{"command","status":"completed|refused|failed","elapsed_seconds","source_engine","target_engine","tables":[...],"resume":{...},"error":{"message","code","hint"},"next_steps":[...]}`. `status:"refused"` means sluice declined by policy (loud-failure) — read `error.hint` for the remedy; `"failed"` means something broke mid-run. With `--dry-run` the same flag emits the plan instead: `{"command","dry_run":true,"plan":{...}}`.
- **`--format json`** also on: `verify`, `schema diff`, `schema preview`, `cutover`, `sync health`, `sync status`, `matview refresh`.
- **`--log-format json`** (global) — one JSON slog object per line on stderr; terminal coded errors carry `code` and `hint` attributes.
- Envelopes and logs never contain credentials; DSNs render as credential-free locators.

## Error codes and exit codes

Stable machine-parsable error codes (`SLUICE-E-*`) with remedy hints ride on the JSON envelope's `error` object and the JSON log stream — branch on `code`, show `hint` to the human. The full table: `docs/operator/error-codes.md`.

| Exit | Meaning |
|---|---|
| 0 | success |
| 1 | runtime failure (also: verify/diff drift found) |
| 2 | config-file error (verify-family: could not run) |
| 3 | named refusal — sluice declined by policy; the error names the remedy |
| 80 | CLI usage/parse error (kong) |

`!= 0` always means not-success. On exit 3, do not retry unchanged — surface `error.hint` to the human and wait for a decision (the remedy is often a destructive flag that needs approval).

## Where to read more

- Docs site index for assistants: `https://sluicesync.com/llms.txt` (full text: `/llms-full.txt`)
- In-repo markdown: `README.md`, `docs/` (architecture, type/value contracts, cookbook recipes, operator guides, `docs/operator/error-codes.md`)
- **Agent skills** (`skills/`): task-scoped playbooks that drive sluice for one job each — `migrate-preflight`, `fidelity-verify`, `sluice-error-triage`, `backup-chain-operator`, `cdc-sync-operator`, `planetscale-migration`, `fleet-operator`, `redaction-setup`, `sqlite-d1-import`. Install with `skills/install.sh`; see `skills/README.md`. They reference this file and `error-codes.md` as canonical rather than duplicating them.
- `sluice --help` and `sluice <command> --help` are complete and accurate; help text is maintained as the primary flag documentation.
