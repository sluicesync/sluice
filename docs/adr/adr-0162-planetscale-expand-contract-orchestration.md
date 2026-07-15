# ADR-0162: PlanetScale expand-contract orchestration (`sluice expand-contract`)

- **Status:** Accepted (implemented; roadmap item 62 Phase 3)
- **Date:** 2026-07-14
- **Related:** ADR-0159 (the standalone backfill this orchestrates — Phases 1–2), ADR-0148 (the deploy-request prototype whose ground truth this reuses: endpoints, DR lifecycle, the safe-migrations findings, the no-SDK posture), ADR-0107 (the telemetry client whose HTTP/auth shape the shared client generalizes), `docs/research/data-migration-backfill.md` §Phase 3.

## Context

ADR-0159 shipped the middle step of expand→migrate→contract as `sluice backfill`; the expand and contract steps on PlanetScale still required the operator to hand-drive the deploy-request workflow (dev branch → DDL on the branch → deploy request → deploy → finalize) twice, around the backfill. ADR-0148 had already validated that entire chain end-to-end on a real PS-10 — including the two findings that shape any orchestration: **safe migrations must be enabled on the production branch** for deploy-request creation, and the enable/disable **propagation lag (~100 s+, sometimes wedged)** makes toggling it around a run unsafe. The roadmap noted the two control-plane features (this one and ADR-0148's deferred index path) "want a common thin PS API client."

## Decision

### One opt-in orchestrator command

`sluice expand-contract` drives the full pattern: **preflight** (token, org/database/branch, safe-migrations, table + walkable PK) → **expand** (dev branch + verbatim `--expand-ddl` + deploy request, deployed and finalized) → **migrate** (`pipeline.Backfiller` with `Verify: true`, reused whole — the orchestrator in `internal/planetscale/expandcontract` composes it, never forks it) → **verify** (the ADR-0159 whole-table remaining-count gate on `--where`, which is *required* here) → **contract** (second dev branch + `--contract-ddl` + deploy request) → **cleanup** (delete the dev branches this run created — always, including on failure, best-effort with WARN; `--keep-branches` opts out). The command is PlanetScale-specific by construction, so the data-plane engine is fixed to the `planetscale` flavor — no `--driver` flag to mis-set. The engine-neutral pipeline imports nothing from this package.

### Thin shared PS client (`internal/planetscale/api`)

A verbs-only HTTP client — service-token auth (`Authorization: {ID}:{TOKEN}`, env `PLANETSCALE_SERVICE_TOKEN_ID`/`PLANETSCALE_SERVICE_TOKEN`, the pscale CLI convention), JSON encode/decode, PlanetScale error-envelope decoding, modest 429 backoff (Retry-After honoured, capped, ≤4 attempts), and the audit-N-12 no-URL/no-token-in-errors guarantee. No `planetscale-go` SDK (ADR-0148 posture). The telemetry provider's authenticated SD leg was refactored onto it (its signed-scrape leg stays local — that request goes to the metrics host, unauthenticated); its leak pins now cover the shared client. Workflow logic — DR polling, branch lifecycle ordering — lives in the orchestrator, not the client, so ADR-0148's index path (or any future deploy-request feature) composes the same verbs.

### DR lifecycle: tolerant terminal-state classification

The poller codes the ADR-0148 ground truth (`pending` → `ready`/`deployable` → `queued` → … → `complete_pending_revert`, finalized via `skip-revert`) but classifies only *terminal* states by name — success `{complete, complete_pending_revert}`, failure `{error, complete_error, cancelled, complete_cancel, complete_revert, complete_revert_error}`, plus `no_changes` (empty diff ⇒ that leg's DDL is already deployed ⇒ refuse with resume guidance) and DR `state: closed`. **Everything else keeps waiting until `--deploy-timeout`** — a new intermediate PlanetScale state must not fail a healthy deploy; the deadline bounds the unknown-terminal risk. Failures/timeouts are the coded runtime `SLUICE-E-PS-DEPLOY-REQUEST-FAILED`, always carrying the leg, DR number, observed state, and URL. A `skip-revert` failure after a successful deploy is a WARN, not a run failure (the schema change is applied; the operator can finalize from the DR page).

### Refuse-on-disabled safe migrations; never auto-enable

Preflight reads the production branch's `safe_migrations` and refuses with `SLUICE-E-PS-SAFE-MIGRATIONS-DISABLED`, naming the UI toggle and the `pscale branch safe-migrations enable` command. Auto-enabling is rejected (contain-complexity tenet): it is a behavior change on the operator's production branch — direct DDL becomes blocked for every future schema change — and ADR-0148 finding #7 (the propagation lag) rules out enable-then-restore around the run.

### The contract gate

The contract leg runs only when the verify gate passed (a dirty verify surfaces as the reused `SLUICE-E-BACKFILL-INCOMPLETE` — no new code minted for the same class) **and** `--yes` was given. Without `--contract-ddl`, or without `--yes`, the run stops after verify **as a success** and prints the exact resume command (`--resume-from contract --contract-ddl '…' --yes`). There is no interactive confirm: sluice commands are non-interactive by contract (the `slot drop` precedent), and a stop-with-instructions is strictly safer than a prompt that reads EOF on a non-TTY. `--dry-run` and `--yes` are kong-xor'd — a plan never confirms a drop.

### Resumability: deterministic branch names + refuse-on-leftover + `--resume-from` (no persisted state machine)

The v1 design is the simplest honest one:

- Dev branch names are **deterministic** — `sluice-expand-<10-hex sha256(table, ddl)>` / `sluice-contract-<…>` — so a crashed run's branch is found *by name* on re-run and **refused** with guidance (inspect the DR; `--resume-from` past the leg if its DDL deployed, else delete the branch and re-run). Guessing whether a leftover branch's state is reusable would be the silent path.
- The migrate leg is already natively resumable (the ADR-0159 cursor store); a re-run continues the walk.
- `--resume-from expand|migrate|contract` is the operator's explicit continue point. `verify` is deliberately **not** a resume point: it always runs before contract, even under `--resume-from contract` (as `--verify-only`, so the walk's PK requirements don't re-apply).

A persisted control-plane state machine (recording DR numbers, leg completion, branch ids) was **deferred**: every leg is either idempotent (migrate), refusing (expand/contract on leftover), or re-derivable from PlanetScale itself (the DR page), and the state store would add a second source of truth that can drift from the control plane it describes. Revisit if real runs show operators losing track of multi-hour deploys.

### `--dry-run`: zero control-plane calls, pinned

The plan (branch names, DDLs, the DR flow, the rendered per-chunk backfill statement via `BackfillStatement`, the gates, the cleanup posture) prints with **zero PlanetScale API calls** — pinned by an httptest call counter. It does open the data-plane DSN read-only (schema + statement render), the same posture as `backfill --dry-run`. The data-plane preflight (table exists, walkable PK) runs under dry-run too; `--set` column existence is deliberately *not* preflighted anywhere pre-expand — the expand leg is what creates those columns (the migrate leg checks them post-expand).

### Branch DDL execution

The expand/contract DDL is applied to the dev branch over a direct MySQL connection using a just-minted branch password (PS allows direct DDL on dev branches — safe migrations gate only production). This is a deliberate, contained `go-sql-driver` use inside the PS-specific package (not the mysql engine): the credential/host pair comes from the control plane (`api.BranchPassword`), not an operator DSN, so none of the engine's DSN/flavor machinery applies. The DDL itself is verbatim operator SQL — the ADR-0159 `--set` posture; sluice does not parse or validate it beyond the database's own answer.

## Consequences

- Operators get the whole expand→migrate→contract pattern as one command with the destructive half hard-gated, instead of hand-driving two deploy requests around a backfill.
- Two new coded errors (`SLUICE-E-PS-SAFE-MIGRATIONS-DISABLED` refusal, `SLUICE-E-PS-DEPLOY-REQUEST-FAILED` runtime) plus doc rows; the verify gate reuses `SLUICE-E-BACKFILL-INCOMPLETE`.
- A second credential surface (the service token) on a command whose siblings need only a DSN — accepted as inherent to the control plane, and scoped to this one opt-in command.
- The telemetry provider now rides the shared client for its authenticated leg; its behavior is unchanged (pins updated in place).
- Live validation is gated behind the `psverify` tag (`TestPSVerify_ExpandContract`), env-driven, and runs the full pattern net-zero against a pre-existing table.

## Alternatives considered

- **Persisted state machine (deferred).** See above — a second source of truth against a control plane that already renders its own state; the deterministic-name + refuse + `--resume-from` design covers the crash shapes without it.
- **Auto-enable safe migrations (rejected).** Changes the operator's production branch behavior permanently-ish and the toggle's propagation lag makes wrap-around toggling unsafe (ADR-0148 findings #1/#7). Refuse-and-name-the-toggle is the contain-complexity answer.
- **Interactive TTY confirm for contract (rejected).** sluice commands are non-interactive by contract; the stop-after-verify-with-instructions shape gives the same safety with a scriptable resume path.
- **Extending `sluice backfill` with flags instead of a new command (rejected).** The control-plane coupling (token, branches, DRs) would leak into an engine-neutral command that runs on plain MySQL/Postgres; the orchestration is PlanetScale-specific and stays behind its own opt-in surface.

## Live-validation findings (2026-07-15, pre-release — all three fixed before the feature shipped)

The first `psverify` run against a real PlanetScale database (sluicesync/sluice-ec-demo, PS-10) caught three defects the unit fakes could not, one per leg of the control-plane surface. Each is now pinned; together they are the argument for the live-gate-before-tag rule on control-plane features.

1. **`deployable` lives inside the nested `deployment` object, not at the top level of GET `/deploy-requests/{number}`.** The first cut read a top-level `"deployable"` — a field the real response does not have — so `waitDeployable` never fired and **every real run timed out at `--deploy-timeout`** with misleading requires-review guidance. The hand-written httptest fixtures had encoded the same wrong shape (self-consistent marshal/unmarshal of the client's own struct proves nothing about the real API). Fixed: `DeployRequest.CanDeploy()` reads both locations; the api package now pins a sanitized **verbatim capture** of the real response, and the orchestrator fake serves the nested shape.
2. **Safe migrations blocks the backfill's own control-table bootstrap.** The migrate leg died with `Error 1105 "direct DDL is disabled"`: safe migrations — this command's prerequisite — refuses every direct DDL statement on the production branch, including the ADR-0159 state store's `CREATE TABLE IF NOT EXISTS` (Vitess refuses the *statement*, table-exists or not). Fixed twice over: `EnsureControlTable` is now detect-then-create (zero DDL statements when the tables exist), and the expand leg stages the control tables **on the dev branch** so they ship to production inside the expand deploy request — the governed channel PlanetScale mandates.
3. **A new dev branch's schema base can lag production.** A branch created 14 minutes after a deploy still lacked the deployed column, and a deploy request from it diffed as **dropping that column from production** (empirically deployed as cleanup — the phantom revert is real). The lag is intermittent — in the same session, a branch created one minute after a deploy came up current — and the seeding mechanism/timing is not documented (branches appear to seed from a parent snapshot/backup whose freshness varies), so the guard treats freshness as unknowable a priori: `provisionFreshBranch` compares every dev branch's schema to production's via the API before any DDL, self-heals a stale base once (delete branch → on-demand backup of production → recreate → recheck), and refuses coded (`SLUICE-E-PS-BRANCH-STALE-BASE`) if still stale. The fresh path costs two schema GETs; the stale path takes one on-demand backup, narrated (duration scales with database size). Without the guard, a stale contract branch would silently drop the freshly backfilled expand column.

## Future scope (recorded 2026-07-15, operator concern)

The expand-contract shape may prove too narrow: users may want **general deploy-request tooling** from sluice — open/deploy/watch *arbitrary* deploy requests (and ADR-0148's errno-3024 index fallback is exactly such a consumer). Explicitly **deferred until demand shows.** The factoring anticipates it: `internal/planetscale/api` already carries the branch/DR verbs with no expand-contract assumptions, so generalization is additive (a `sluice deploy-request …` surface or the ADR-0148 fallback would reuse the client and the tolerant poller pattern unchanged, not refactor them).
